import json
import logging
import re
import time
import uuid

import attr
import boto3
import botocore

import tasks
import utils


__all__ = ['WorkflowManager', 'poll_for_executions', 'start_workflow']


logger = logging.getLogger(__name__)


def encode_task_input(input_obj):
    return json.dumps(input_obj)


def decode_task_result(result_str):
    return json.loads(result_str)

RETRY_LAMBDA_TIMER = 'retryLambdaTimer'


@attr.s
class TaskState(object):
    WAITING_FOR_EVENT = 'WAITING_FOR_EVENT'
    NOT_STARTED = 'NOT_STARTED'
    WAITING_TO_RETRY = 'WAITING_TO_RETRY'
    SCHEDULED = 'SCHEDULED'
    STARTED = 'STARTED'
    FINISHED = 'FINISHED'
    FAILED = 'FAILED'
    CANCELED = 'CANCELED'

    DONE_STATES = (FINISHED, FAILED, CANCELED)
    FAILED_STATES = (FAILED, CANCELED)

    @property
    def is_done(self):
        return self.state in self.DONE_STATES

    @property
    def succeeded(self):
        return self.state == self.FINISHED

    @property
    def failed(self):
        return self.is_done and not self.succeeded

    task = attr.ib()

    result = attr.ib(default=None)

    # The event which triggered the most recent state change
    trigger_event = attr.ib(default=None)

    state = attr.ib(default=NOT_STARTED)

    # a list of events related to this task
    events = attr.ib(default=attr.Factory(list))

    # How many times this task has failed.  Used for retry implementation
    num_failures = attr.ib(default=0)


class TaskClientBase(object):
    def __init__(self, workflow_state):
        self.workflow_state = workflow_state
        ":type: pyflow.workflow_runner.WorkflowInstance"

    def make_decisions(self, task):
        raise NotImplementedError('TaskClientBase.make_decisions')


class LambdaTaskClient(TaskClientBase):
    def make_decisions(self, task_state):
        # TODO: handle WAITING_TO_RETRY state
        if task_state.state == TaskState.NOT_STARTED:
            attribs = {
                'id': self.workflow_state.new_task_id(task_state.task.name),
                'name': task_state.task.lambda_function_name
            }
            if task_state.task.depends_on or task_state.task.param is not None:
                attribs['input'] = self.workflow_state.prepare_task_input(task_state.task.name)
            return [{
                'decisionType': 'ScheduleLambdaFunction',
                'scheduleLambdaFunctionDecisionAttributes': attribs
            }]
        elif task_state.state == TaskState.WAITING_TO_RETRY:
            attribs = {
                'control': RETRY_LAMBDA_TIMER,
                'startToFireTimeout': str(task_state.task.retry_interval),
                'timerId': '{}@{}'.format(task_state.task.name, str(uuid.uuid4()))
            }
            return [{
                'decisionType': 'StartTimer',
                'startTimerDecisionAttributes': attribs
            }]
        else:
            return []

    def handle_lambda_function_completed(self, event):
        root_attributes = self.workflow_state.get_root_event_attributes(event)
        task_name = self.workflow_state.task_name_from_id(root_attributes['id'])
        attributes = self.workflow_state.get_event_attributes(event)
        result = attributes.get('result')
        if result:
            # TODO: handle parse failure
            result = decode_task_result(result)
        self.workflow_state.update_state_for_task(task_name, event, TaskState.FINISHED, result)

    def handle_lambda_function_failed(self, event):
        root_attributes = self.workflow_state.get_root_event_attributes(event)
        task_name = self.workflow_state.task_name_from_id(root_attributes['id'])
        self.workflow_state.handle_retrying(task_name, event)

    def handle_lambda_function_scheduled(self, event):
        root_attributes = self.workflow_state.get_root_event_attributes(event)
        task_name = self.workflow_state.task_name_from_id(root_attributes['id'])
        self.workflow_state.update_state_for_task(task_name, event, TaskState.SCHEDULED)

    def handle_lambda_function_started(self, event):
        root_attributes = self.workflow_state.get_root_event_attributes(event)
        task_name = self.workflow_state.task_name_from_id(root_attributes['id'])
        self.workflow_state.update_state_for_task(task_name, event, TaskState.STARTED)

    def handle_lambda_function_timed_out(self, event):
        root_attributes = self.workflow_state.get_root_event_attributes(event)
        task_name = self.workflow_state.task_name_from_id(root_attributes['id'])
        self.workflow_state.handle_retrying(task_name, event)

    def handle_timer_fired(self, event):
        attributes = self.workflow_state.get_event_attributes(event)
        task_name = self.workflow_state.task_name_from_id(attributes['timerId'])
        self.workflow_state.update_state_for_task(task_name, event, TaskState.NOT_STARTED)


class WorkflowInstance(object):
    """
    An instance of this class represents a running instance of a workflow.
    """

    # A (almost) complete list of possible event types is available at
    # http://docs.aws.amazon.com/amazonswf/latest/apireference/API_HistoryEvent.html
    # The list is missing the lambda events

    # Categorized list of event types that can be sent to a decider
    event_types = {
        'ActivityTask': [
            'ActivityTaskCancelRequested',
            'ActivityTaskCanceled',
            'ActivityTaskCompleted',
            'ActivityTaskFailed',
            'ActivityTaskScheduled',
            'ActivityTaskStarted',
            'ActivityTaskTimedOut',
            'RequestCancelActivityTaskFailed',
            'ScheduleActivityTaskFailed',
            'StartActivityTaskFailed'],
        'Timer': [
            'CancelTimerFailed',
            'StartTimerFailed',
            'TimerCanceled',
            'TimerFired',
            'TimerStarted'],
        'WorkflowExecution': [
            'CancelWorkflowExecutionFailed',
            'CompleteWorkflowExecutionFailed',
            'FailWorkflowExecutionFailed',
            'WorkflowExecutionCancelRequested',
            'WorkflowExecutionCanceled',
            'WorkflowExecutionCompleted',
            'WorkflowExecutionFailed',
            'WorkflowExecutionSignaled',
            'WorkflowExecutionStarted',
            'WorkflowExecutionTerminated',
            'WorkflowExecutionTimedOut'],
        'ChildWorkflowExecution': [
            'ChildWorkflowExecutionCanceled',
            'ChildWorkflowExecutionCompleted',
            'ChildWorkflowExecutionFailed',
            'ChildWorkflowExecutionStarted',
            'ChildWorkflowExecutionTerminated',
            'ChildWorkflowExecutionTimedOut',
            'StartChildWorkflowExecutionFailed',
            'StartChildWorkflowExecutionInitiated'],
        'ContinueAsNewWorkflowExecution': [
            'WorkflowExecutionContinuedAsNew',
            'ContinueAsNewWorkflowExecutionFailed'],
        'DecisionTask': [
            'DecisionTaskCompleted',
            'DecisionTaskScheduled',
            'DecisionTaskStarted',
            'DecisionTaskTimedOut'],
        'ExternalWorkflowExecution': [
            'ExternalWorkflowExecutionCancelRequested',
            'ExternalWorkflowExecutionSignaled',
            'RequestCancelExternalWorkflowExecutionFailed',
            'RequestCancelExternalWorkflowExecutionInitiated',
            'SignalExternalWorkflowExecutionFailed',
            'SignalExternalWorkflowExecutionInitiated'],
        'Marker': [
            'MarkerRecorded',
            'RecordMarkerFailed'],
        'LambdaFunction': [
            'LambdaFunctionCompleted',
            'LambdaFunctionFailed',
            'LambdaFunctionScheduled',
            'LambdaFunctionStarted',
            'LambdaFunctionTimedOut']
    }

    def __init__(self, client, workflow, workflow_execution, domain, task_list):
        """Initialize a workflow instance

        :param client: A boto3 SWF client
        :param workflow: a Workflow object
        :param workflow_execution: The SWF WorkflowExecution object
        :param domain: Domain for this workflow type
        :param task_list: Task list on which events for this workflow
        instance will be posted
        """
        self._client = client
        self._workflow = workflow
        ":type: pyflow.workflow.Workflow"
        self._workflow_execution = workflow_execution
        self._domain = domain
        self._task_list = task_list

        self._task_states = {t.name: TaskState(task=t) for t in workflow.tasks}
        self._last_seen_event_id = -1

        # If an event arrives which indicates the workflow has ended, it will be assigned to this member
        self._workflow_exit_event = None

        self._event_handlers = {
            'LambdaFunction': LambdaTaskClient(self)
        }

        self._task_handlers = {
            tasks.LambdaTask: self._event_handlers['LambdaFunction']
        }

        self.decision_task = None

    def process_decision_task(self, decision_task):
        self.decision_task = decision_task

        # Update workflow state from the event log
        for event in filter(lambda e: e['eventId'] > self._last_seen_event_id,
                            decision_task['events']):
            logger.debug('Processing event {!r}'.format(event))
            event_type = event['eventType']

            # handler name is computed from event type by prepending 'handle_' to the snake-case version of event type
            handler_name = 'handle_' + event_type[0].lower() + \
                           re.sub(r'([a-z])([A-Z])', lambda m: m.group(1) + '_' + m.group(2).lower(), event_type[1:])

            for category, types in self.event_types.iteritems():
                if event_type in types:
                    handler_obj = self._event_handlers.get(category, self)
                    if hasattr(handler_obj, handler_name):
                        getattr(handler_obj, handler_name)(event)
                    else:
                        self.handle_unhandled_event(event)

            self._last_seen_event_id = event['eventId']

        if self._workflow_exit_event is not None:
            # means we received an event indicating the workflow is finished executing.
            return True

        decisions = []

        if any(self._is_new_event(state.trigger_event) for state in self._task_states.itervalues()):
            # check if the workflow is finished
            if any(state.failed for state in self._task_states.itervalues()):
                failed_statuses = {state.task.name: state.state for state in self._task_states.values()
                                   if state.failed}
                decisions.append({
                    'decisionType': 'FailWorkflowExecution',
                    'failWorkflowExecutionDecisionAttributes': {
                        'reason': 'One or more tasks failed or were canceled',
                        'details': 'These tasks did not succeed: {!r}'.format(failed_statuses)
                    }
                })
            elif all(state.succeeded for state in self._task_states.itervalues()):
                # Workflow is finished.
                if self._workflow.result_task is None:
                    workflow_result = ''
                else:
                    result_task_state = self._task_states[self._workflow.result_task.name]
                    workflow_result = encode_task_input(result_task_state.result)

                decisions.append({
                    'decisionType': 'CompleteWorkflowExecution',
                    'completeWorkflowExecutionDecisionAttributes': {
                        'result': workflow_result
                    }
                })
            else:
                # Now decide on actions
                for state in self._task_states.itervalues():
                    if self.task_is_actionable(state.task.name):
                        task_handler = self._task_handlers.get(type(state.task), self)
                        task_decisions = task_handler.make_decisions(state)
                        decisions.extend(task_decisions or [])

        logger.debug('Sending decisions: {!r}'.format(decisions))

        self._client.respond_decision_task_completed(
            taskToken=decision_task['taskToken'],
            decisions=decisions)

    def task_is_actionable(self, task_name):
        """
        Return true if this decider should take action on a task

        :param task_name: The name of the workflow task to check
        :return: True if this decider should take action
        """
        state = self._task_states[task_name]
        if state.state not in (TaskState.NOT_STARTED, TaskState.WAITING_TO_RETRY):
            return False

        dependencies = [self._task_states[d.name]
                        for d in (state.task.depends_on or [self._workflow.start_task])]
        if not all(d.state == TaskState.FINISHED for d in dependencies):
            return False

        last_seen_id = self.decision_task['previousStartedEventId']
        if any(d.trigger_event['eventId'] > last_seen_id for d in dependencies):
            return True

        return state.trigger_event['eventId'] > last_seen_id

    def update_state_for_task(self, task_name, event, new_state=None, result=None):
        task_state = self._task_states[task_name]
        task_state.events.append(event)
        if result is not None:
            task_state.result = result

        if new_state is not None and new_state != task_state.state:
            task_state.state = new_state
            task_state.trigger_event = event

    def handle_retrying(self, task_name, event):
        """
        Handle updating the state of a task in response to a failure or timeout event.  Implements retry behavior.

        :param task_name:
        :param decision_task:
        :param event:
        :return:
        """
        task_state = self._task_states[task_name]
        task_node = task_state.task

        task_state.num_failures += 1
        if task_state.num_failures > task_node.num_retries:
            new_state = TaskState.FAILED
        elif task_node.retry_interval == 0:
            new_state = TaskState.NOT_STARTED
        else:
            new_state = TaskState.WAITING_TO_RETRY

        self.update_state_for_task(task_name, event, new_state)

    def handle_workflow_execution_started(self, event):
        attributes = self.get_event_attributes(event)
        workflow_input = attributes.get('input')
        if workflow_input:
            workflow_input = decode_task_result(workflow_input)
        self.update_state_for_task(self._workflow.start_task.name, event, TaskState.FINISHED, workflow_input)

    def handle_workflow_execution_failed(self, event):
        self._workflow_exit_event = event

    def handle_workflow_execution_timed_out(self, event):
        self._workflow_exit_event = event

    def handle_workflow_execution_canceled(self, event):
        self._workflow_exit_event = event

    def handle_workflow_execution_completed(self, event):
        self._workflow_exit_event = event

    def handle_workflow_execution_terminated(self, event):
        self._workflow_exit_event = event

    def handle_timer_fired(self, event):
        root_attributes = self.get_root_event_attributes(event)
        control = root_attributes.get('control')
        if control == RETRY_LAMBDA_TIMER:
            self._event_handlers['LambdaFunction'].handle_timer_fired(event)
        else:
            self.handle_unhandled_event(event)

    @classmethod
    def get_event_attributes(cls, event):
        """Return the event attributes value of an event"""
        event_type = event['eventType']
        attributes_key = '{}{}EventAttributes'.format(
            event_type[0].lower(), event_type[1:])
        return event.get(attributes_key)

    def get_root_event_attributes(self, event):
        """Return the attributes of the root event of an event.

        Many events concerning the progress of an activity refer back
        to an earlier event, rather than duplicate information from
        that earlier event.  For example, the ActivityTaskScheduled
        event contains a bunch of info about an activity that was
        scheduled.  Subsequent events concerning that activity just
        refer back to the ActivityTaskScheduledEvent rather than
        repeat the info.  This function finds the original event and
        returns its attributes.

        :param decision_task: DecisionTask object containing the latest
          event history
        :param event: The event in question
        :return: The attributes of the root event, or attributes of this
          event if this event doesn't refer to another
          event.

        """
        event_attrs = self.get_event_attributes(event)
        if not event_attrs:
            return None

        root_id = event_attrs.get('scheduledEventId',
                                  event_attrs.get('startedEventId'))
        if root_id:
            found = [e for e in self.decision_task['events']
                     if e['eventId'] == root_id]
            return self.get_event_attributes(found[0])
        else:
            return event_attrs

    @classmethod
    def new_task_id(cls, task_name):
        """Generate a new task id to pass to SWF"""
        return "{}@{}".format(task_name, str(uuid.uuid4()))

    @classmethod
    def task_name_from_id(cls, task_id):
        if '@' not in task_id:
            raise ValueError(
                'task_id {!r} is not in the expected format'.format(task_id))
        return task_id[:task_id.rindex('@')]

    def _is_new_event(self, event):
        return event is not None and event['eventId'] > self.decision_task['previousStartedEventId']

    def prepare_task_input(self, task_name):
        state = self._task_states[task_name]

        param = state.task.param
        from_deps = {d.name: self._task_states[d.name].result
                     for d in state.task.depends_on
                     if d.has_output}

        # handle special case where there is only one input
        num_inputs = (0 if param is None else 1) + len(from_deps)

        if num_inputs == 1:
            if param is None:
                task_input = from_deps.values()[0]
            else:
                task_input = param
        else:
            task_input = {'inputs': from_deps}
            if param is not None:
                task_input['param'] = param

        if state.task.transform_input is not None:
            task_input = state.task.transform_input(task_input)

        return encode_task_input(task_input)

    def handle_unhandled_event(self, event):
        logger.debug('Unhandled event {!r}'.format(event['eventType']))


class WorkflowManager(object):
    """
    An instance of this class acts as a decider, handling the execution of instances of one or more workflow types.
    """

    def __init__(self, workflows, domain, task_list, identity, client=None):
        for workflow in workflows:
            workflow.validate()

        self._workflows = workflows
        self._workflow_instances = {}
        self._domain = domain
        self._task_list = task_list
        self._identity = identity

        if client is None:
            client = boto3.client('swf', config=botocore.client.Config(read_timeout=70))

        self._client = client

    def ensure_workflows_registered(self):
        registered_workflow_types = [wt[u'workflowType']
                                     for wt in utils.list_workflow_types(self._client, self._domain)]

        for workflow in self._workflows:
            workflow_type = dict(name=workflow.name, version=workflow.version)
            if workflow_type not in registered_workflow_types:
                logger.info("Registering workflow: %r", workflow_type)
                self._client.register_workflow_type(
                    domain=self._domain,
                    name=workflow_type['name'],
                    version=workflow_type['version'],
                    defaultTaskList={'name': self._task_list},
                    defaultChildPolicy='TERMINATE',
                    defaultTaskStartToCloseTimeout='3600',
                    defaultExecutionStartToCloseTimeout=str(24 * 3600))

    def poll_for_decision(self, max_time=None):
        """
        Poll for decisions tasks for up to max_time seconds

        :param max_time: Maximum time in seconds to keep polling.  Defaults to forever.
        """

        self.ensure_workflows_registered()

        start_time = time.time()

        logger.info('Beginning to poll for decisions in domain {!r}, task_list {!r}'.format(
            self._domain, self._task_list))
        while max_time is None or time.time() < (start_time + max_time):
            decision_task = utils.poll_for_decision_tasks(
                self._client, self._domain, self._task_list, self._identity)
            if not decision_task.get('taskToken'):
                # poll timed out
                logger.debug('Poll timed out')
                continue

            # Find the WorkflowInstance object, creating it if necessary.
            workflow_execution = decision_task['workflowExecution']
            logger.debug('Received decision event for {!r}'.format(workflow_execution))
            run_id = workflow_execution['runId']
            workflow_instance = self._workflow_instances.get(run_id)
            if workflow_instance is None:
                # Need to instantiate a new instance object.  First find the workflow definition
                workflow_type = decision_task['workflowType']
                for workflow in self._workflows:
                    if workflow.name == workflow_type['name'] and workflow.version == workflow_type['version']:
                        workflow_instance = WorkflowInstance(self._client, workflow, workflow_execution,
                                                             self._domain, self._task_list)
                        self._workflow_instances[run_id] = workflow_instance
                        break

                if workflow_instance is None:
                    logger.error('Received decision task for unknown workflow type: %r', workflow_type)
                    continue

            workflow_finished = False
            try:
                if workflow_instance.process_decision_task(decision_task):
                    logger.info('Workflow instance %r finished', workflow_execution)
                    workflow_finished = True
            except Exception:
                workflow_finished = True
                logger.exception('Workflow instance raised uncaught exception: %r', workflow_execution)

            if workflow_finished:
                del self._workflow_instances[run_id]


def poll_for_executions(workflows, domain, task_list, identity, max_time=None, client=None):
    manager = WorkflowManager(workflows, domain, task_list, identity, client)

    return manager.poll_for_decision(max_time)


def start_workflow(domain, workflow_name, workflow_version, task_list, lambda_role, input=None, client=None):
    workflow_id = '{}@{}'.format(workflow_name, str(uuid.uuid4()))

    if client is None:
        client = boto3.client('swf')

    response = client.start_workflow_execution(
        domain=domain,
        workflowId=workflow_id,
        workflowType={'name': workflow_name, 'version': workflow_version},
        taskList={'name': task_list},
        input=input,
        lambdaRole=lambda_role)

    return {'workflowId': workflow_id, 'runId': response['runId']}
