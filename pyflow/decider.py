import logging
import re
import time
import traceback
import uuid

import boto3
import botocore

from pyflow import decision_task_helper as dth
from pyflow import exceptions
from pyflow import utils
from pyflow import workflow_invocation_helper as wih
from pyflow import workflow_state as ws

logger = logging.getLogger(__name__)


class EventHandler(object):
    def __init__(self, decision_helper):
        """:type decision_helper: dth.DecisionTaskHelper"""
        self.decision_helper = decision_helper

    def update_state_from_event(self, event):
        """
        Updates the workflow state in response to a decision event

        :param event: A workflow event object
        :return: True if any invocation state changed from a not-done state to a done-state.
        """
        self.decision_helper.workflow_state.last_seen_event_id = event['eventId']

        event_type = event['eventType']

        # handler name is computed from event type by prepending 'handle_' to the snake-case version of event type
        handler_name = 'handle_' + event_type[0].lower() + \
                       re.sub(r'([a-z])([A-Z])', lambda m: m.group(1) + '_' + m.group(2).lower(), event_type[1:])

        handler = getattr(self, handler_name, self.handle_unhandled_event)

        return handler(event)

    def _handle_state_change(self, event, new_state, failure_reason=None, failure_details=None, result=None):
        invocation_id = self.decision_helper.event_invocation_id(event)
        invocation_state = self.decision_helper.workflow_state.get_invocation_state(invocation_id)
        invocation_state.update_state(event, state=new_state, failure_reason=failure_reason,
                                      failure_details=failure_details, result=result)
        return invocation_state.done

    def handle_unhandled_event(self, event):
        logger.debug('Skipping handling of event %r', event['eventType'])
        return False

    # Lambda event handlers

    def handle_lambda_function_completed(self, event):
        attributes = self.decision_helper.event_attributes(event)
        result = utils.decode_task_result(attributes.get('result'))
        return self._handle_state_change(event, ws.InvocationState.SUCCEEDED, result=result)

    def handle_lambda_function_failed(self, event):
        attributes = self.decision_helper.event_attributes(event)
        return self._handle_state_change(event, ws.InvocationState.FAILED,
                                         failure_reason=attributes.get('reason'),
                                         failure_details=attributes.get('details'))

    def handle_lambda_function_scheduled(self, event):
        return self._handle_state_change(event, ws.InvocationState.HANDLED)

    def handle_lambda_function_started(self, event):
        return self._handle_state_change(event, ws.InvocationState.STARTED)

    def handle_lambda_function_timed_out(self, event):
        return self._handle_state_change(event, ws.InvocationState.TIMED_OUT,
                                         failure_reason='Lambda function timed out')

    def handle_start_lambda_function_failed(self, event):
        attributes = self.decision_helper.event_attributes(event)
        return self._handle_state_change(event, ws.InvocationState.FAILED,
                                         failure_reason=attributes.get('cause'))

    def handle_schedule_lambda_function_failed(self, event):
        attributes = self.decision_helper.event_attributes(event)
        return self._handle_state_change(event, ws.InvocationState.FAILED,
                                         failure_reason=attributes.get('cause'))

    # Workflow event handlers

    def handle_workflow_execution_started(self, event):
        attributes = self.decision_helper.event_attributes(event)
        self.decision_helper.workflow_state.workflow_start_time = event['eventTimestamp']
        self.decision_helper.workflow_state.lambda_role = attributes.get('lambdaRole')
        try:
            self.decision_helper.workflow_state.input = utils.decode_task_result(attributes.get('input'))
        except ValueError as e:
            self.decision_helper.fail_workflow('Invalid input to workflow', str(e))
        return True

    def handle_workflow_execution_timed_out(self, event):
        self.decision_helper.workflow_state.completed = True
        self.decision_helper.should_delete = True
        return False

    def handle_workflow_execution_failed(self, event):
        self.decision_helper.workflow_state.completed = True
        self.decision_helper.should_delete = True
        return False

    def handle_workflow_execution_completed(self, event):
        self.decision_helper.workflow_state.completed = True
        self.decision_helper.should_delete = True
        return False

    def handle_workflow_execution_terminated(self, event):
        self.decision_helper.workflow_state.completed = True
        self.decision_helper.should_delete = True
        return False

    def handle_workflow_execution_cancel_requested(self, event):
        attributes = self.decision_helper.event_attributes(event)
        self.decision_helper.cancel_workflow(attributes.get('cause'))
        return False

    # Timer event handlers

    def handle_start_timer_failed(self, event):
        attributes = self.decision_helper.event_attributes(event)
        return self._handle_state_change(event, ws.InvocationState.FAILED,
                                         failure_reason=attributes.get('cause'))

    def handle_timer_fired(self, event):
        return self._handle_state_change(event, ws.InvocationState.SUCCEEDED)

    def handle_timer_started(self, event):
        return self._handle_state_change(event, ws.InvocationState.STARTED)

    def handle_timer_canceled(self, event):
        return self._handle_state_change(event, ws.InvocationState.CANCELED)

    # Activity Event Handlers

    def handle_activity_task_scheduled(self, event):
        return self._handle_state_change(event, ws.InvocationState.HANDLED)

    def handle_activity_task_started(self, event):
        return self._handle_state_change(event, ws.InvocationState.STARTED)

    def handle_activity_task_cancel_requested(self, event):
        return self.handle_unhandled_event(event)

    def handle_activity_task_completed(self, event):
        attributes = self.decision_helper.event_attributes(event)
        try:
            result = utils.decode_task_result(attributes.get('result'))
        except ValueError as e:
            return self._handle_state_change(event, ws.InvocationState.FAILED, failure_reason='Decoding result failed',
                                             failure_details=str(e))

        return self._handle_state_change(event, ws.InvocationState.SUCCEEDED, result=result)

    def handle_activity_task_canceled(self, event):
        attributes = self.decision_helper.event_attributes(event)
        return self._handle_state_change(event, ws.InvocationState.CANCELED,
                                         failure_details=attributes.get('details'))

    def handle_activity_task_failed(self, event):
        attributes = self.decision_helper.event_attributes(event)
        return self._handle_state_change(event, ws.InvocationState.FAILED,
                                         failure_reason=attributes.get('reason'),
                                         failure_details=attributes.get('details'))

    def handle_activity_task_timed_out(self, event):
        return self._handle_state_change(event, ws.InvocationState.TIMED_OUT,
                                         failure_reason='Activity task timed out')

    def handle_request_cancel_activity_task_failed(self, event):
        return self.handle_unhandled_event(event)

    def handle_schedule_activity_task_failed(self, event):
        attributes = self.decision_helper.event_attributes(event)
        return self._handle_state_change(event, ws.InvocationState.FAILED,
                                         failure_reason=attributes.get('cause'))

    def handle_start_activity_task_failed(self, event):
        attributes = self.decision_helper.event_attributes(event)
        return self._handle_state_change(event, ws.InvocationState.FAILED,
                                         failure_reason=attributes.get('cause'))
    
    # Child workflow event handlers

    def handle_start_child_workflow_execution_initiated(self, event):
        return self._handle_state_change(event, ws.InvocationState.HANDLED)

    def handle_child_workflow_execution_started(self, event):
        return self._handle_state_change(event, ws.InvocationState.STARTED)

    def handle_child_workflow_execution_completed(self, event):
        attributes = self.decision_helper.event_attributes(event)
        try:
            result = utils.decode_task_result(attributes.get('result'))
        except ValueError as e:
            return self._handle_state_change(event, ws.InvocationState.FAILED,
                                             failure_reason='Failed to decode child workflow output',
                                             failure_details=str(e))
        return self._handle_state_change(event, ws.InvocationState.SUCCEEDED, result=result)

    def handle_child_workflow_execution_failed(self, event):
        attributes = self.decision_helper.event_attributes(event)
        return self._handle_state_change(event, ws.InvocationState.FAILED,
                                         failure_reason=attributes.get('reason'),
                                         failure_details=attributes.get('details'))

    def handle_child_workflow_execution_canceled(self, event):
        attributes = self.decision_helper.event_attributes(event)
        return self._handle_state_change(event, ws.InvocationState.CANCELED,
                                         failure_details=attributes.get('details'))

    def handle_child_workflow_execution_terminated(self, event):
        return self._handle_state_change(event, ws.InvocationState.FAILED,
                                         failure_reason='Child workflow was terminated')

    def handle_child_workflow_execution_timed_out(self, event):
        return self._handle_state_change(event, ws.InvocationState.TIMED_OUT,
                                         failure_reason='Child workflow timed out')

    def handle_start_child_workflow_execution_failed(self, event):
        attributes = self.decision_helper.event_attributes(event)
        return self._handle_state_change(event, ws.InvocationState.FAILED,
                                         failure_reason=attributes.get('cause'))


class Decider(object):
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
            'LambdaFunctionTimedOut',
            'ScheduleLambdaFunctionFailed',
            'StartLambdaFunctionFailed']
    }

    def __init__(self, workflow_classes, domain, task_list, identity, client=None):
        self._workflow_classes = {(w.NAME, w.VERSION): w for w in workflow_classes}

        # A dictionary of workflow states, keyed by run_id
        self._workflow_states = {}
        self._domain = domain
        self._task_list = task_list
        self._identity = identity

        if client is None:
            # Read timeout needs to be > 60 seconds, because the poll_for_decision_tasks API call waits up to 60
            # seconds before returning.
            client = boto3.client('swf', config=botocore.client.Config(read_timeout=70))

        self._client = client

    def ensure_workflows_registered(self):
        """
        Register all the workflow types passed to the constructor with SWF, if they are not already registered
        """
        for workflow_class in self._workflow_classes.values():
            workflow_type = dict(name=workflow_class.NAME, version=workflow_class.VERSION)
            options = {k: str(v) for k, v in workflow_class.OPTIONS.items()}

            try:
                self._client.register_workflow_type(
                    domain=self._domain,
                    name=workflow_type['name'],
                    version=workflow_type['version'],
                    defaultTaskList={'name': self._task_list},
                    **options)
            except self._client.exceptions.TypeAlreadyExistsFault:
                logger.debug('Workflow %r already registered', workflow_type)
            else:
                logger.info("Registered workflow: %r", workflow_type)

    def process_decision_task(self, decision_task):
        """
        Handle a single decision task.  Takes care of updating the workflow state, and computing decisions, but doesn't
        actually send the response to SWF.

        :param decision_task: An SWF DecisionTask object
        """

        workflow_id = decision_task['workflowExecution']['workflowId']
        run_id = decision_task['workflowExecution']['runId']

        workflow_state = self._workflow_states.get(run_id)
        if workflow_state is None:
            workflow_state = self._workflow_states[run_id] = ws.WorkflowState(
                workflow_id=workflow_id, run_id=run_id)

        decision_helper = dth.DecisionTaskHelper(decision_task, workflow_state)
        event_handler = EventHandler(decision_helper)

        workflow_type = (decision_helper.workflow_name, decision_helper.workflow_version)
        workflow_class = self._workflow_classes.get(workflow_type)
        if workflow_class is None:
            raise exceptions.DeciderException('Received decision task for unknown workflow type: {!r}'.format(
                workflow_type))

        for event in [e for e in decision_helper.events
                      if e['eventId'] > decision_helper.workflow_state.last_seen_event_id]:
            logger.debug('Processing event %r', event)

            state_changed = event_handler.update_state_from_event(event)

            if not decision_helper.workflow_state.completed \
                    and (event['eventId'] == decision_helper.previous_started_event_id
                         or (state_changed and event['eventId'] > decision_helper.previous_started_event_id)):
                try:
                    is_replaying = event['eventId'] <= decision_helper.previous_started_event_id
                    invocation_helper = wih.WorkflowInvocationHelper(decision_helper, is_replaying)
                    workflow_obj = workflow_class(invocation_helper)
                    result = workflow_obj.run(decision_helper.workflow_state.input)
                except exceptions.WorkflowBlockedException:
                    pass
                except Exception:
                    msg = 'Caught exception from workflow function for workflow {!r}'.format(workflow_type)
                    logger.exception(msg)
                    decision_helper.fail_workflow(msg, traceback.format_exc())
                else:
                    try:
                        decision_helper.complete_workflow(result)
                    except Exception:
                        msg = 'Caught exception while processing workflow result for workflow {!r}'.format(workflow_type)
                        logger.exception(msg)
                        decision_helper.fail_workflow(msg, traceback.format_exc())

        if decision_helper.should_delete:
            del self._workflow_states[decision_helper.run_id]

        return decision_helper

    def poll_for_decision_tasks(self, max_time=None):
        self.ensure_workflows_registered()

        start_time = time.time()
        logger.info('Beginning to poll for decisions in domain {!r}, task_list {!r}'.format(
            self._domain, self._task_list))

        # save_dir = '/tmp/decision_tasks'
        # if not os.path.exists(save_dir):
        #     os.makedirs(save_dir)
        # save_counter = 0

        while max_time is None or time.time() < (start_time + max_time):
            decision_task = utils.poll_for_decision_tasks(
                self._client, self._domain, self._task_list, self._identity)
            if not decision_task.get('taskToken'):
                # poll timed out
                logger.debug('Poll timed out')
                continue

            logger.debug('Processing decision task')

            # with open(os.path.join(save_dir, 'decision_task{:02}.pickle'.format(save_counter)), 'w') as f:
            #     cPickle.dump(decision_task, f)
            #     save_counter += 1

            decision_helper = self.process_decision_task(decision_task)

            self._client.respond_decision_task_completed(
                taskToken=decision_helper.task_token,
                decisions=decision_helper.decisions)


def poll_for_executions(workflows, domain, task_list, identity, max_time=None):
    the_decider = Decider(workflows, domain, task_list, identity)
    the_decider.poll_for_decision_tasks(max_time)


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
