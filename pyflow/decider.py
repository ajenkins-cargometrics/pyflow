import time
import traceback
import uuid

import attr
import boto3
import botocore

from pyflow import decision_task_helper as dth
from pyflow import exceptions
from pyflow import utils
from pyflow import workflow_invocation_helper as wih
from pyflow import workflow_state as ws
from pyflow.utils import logger

__all__ = ['Decider', 'get_workflow_status', 'poll_for_executions', 'start_workflow', 'WorkflowStatus']


dashboard_url_template = 'https://{region}.console.aws.amazon.com/swf/home?region={region}#' \
                         'execution_summary:domain={domain};workflowId={workflow_id};runId={run_id}'


class Decider(object):
    def __init__(self, workflow_classes, domain, task_list, identity, client=None):
        self._workflow_classes = {(w.NAME, w.VERSION): w for w in workflow_classes}

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
            workflow_obj = workflow_class(None)
            workflow_type = dict(name=workflow_obj.name, version=workflow_obj.version)
            options = {k: str(v) for k, v in workflow_obj.options.items()}

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

        workflow_state = ws.WorkflowState(workflow_id=workflow_id, run_id=run_id)

        decision_helper = dth.DecisionTaskHelper(decision_task, workflow_state)

        if decision_helper.previous_started_event_id == 0:
            dashboard_url = dashboard_url_template.format(region=boto3.Session().region_name, domain=self._domain,
                                                          workflow_id=workflow_id, run_id=run_id)
            logger.info('Starting instance of workflow {}: {}'.format(
                {'name': decision_helper.workflow_name, 'version': decision_helper.workflow_version},
                {'workflow_id': decision_helper.workflow_id, 'run_id': decision_helper.run_id}))
            logger.info('Dashboard URL: {}'.format(dashboard_url))

        try:
            workflow_type = (decision_helper.workflow_name, decision_helper.workflow_version)
            workflow_class = self._workflow_classes.get(workflow_type)
            if workflow_class is None:
                raise exceptions.DeciderException('Received decision task for unknown workflow type: {!r}'.format(
                    workflow_type))

            # Process the first decision task's events to fill in workflow input
            decision_helper.process_next_decision_task()

            invocation_helper = wih.WorkflowInvocationHelper(decision_helper)
            workflow_obj = workflow_class(invocation_helper)
            result = workflow_obj.run(decision_helper.workflow_state.input)
        except exceptions.WorkflowBlockedException:
            pass
        except exceptions.WorkflowFailedException as e:
            logger.info('Workflow function threw WorkflowFailedException(reason={!r}, details={!r})'.format(
                e.reason, e.details))
            decision_helper.fail_workflow(e.reason, e.details)
        except Exception:
            msg = 'Caught exception while executing workflow {!r}'.format(workflow_type)
            logger.exception(msg)
            decision_helper.fail_workflow(msg, traceback.format_exc())
        else:
            try:
                decision_helper.complete_workflow(result)
            except Exception:
                msg = 'Caught exception while processing workflow result for workflow {!r}'.format(workflow_type)
                logger.exception(msg)
                decision_helper.fail_workflow(msg, traceback.format_exc())

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

            try:
                self._client.respond_decision_task_completed(
                    taskToken=decision_helper.task_token,
                    decisions=decision_helper.decisions)
            except Exception:
                logger.exception('respond_decision_task_completed failed')

                # Assume it failed due to a bad decision object.  Clear decisions list and fail the workflow
                decision_helper.decisions = []
                decision_helper.fail_workflow('respond_decision_task_completed failed', traceback.format_exc())
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


@attr.s
class WorkflowStatus(object):
    # 'OPEN'|'CLOSED'
    execution_status = attr.ib()

    # The full response returned from DescribeWorkflowExecution. In most cases the fields below should provide the
    # information you need, but this object has the full details.
    workflow_description = attr.ib()

    # Only set if execution_status is CLOSED
    # 'COMPLETED'|'FAILED'|'CANCELED'|'TERMINATED'|'CONTINUED_AS_NEW'|'TIMED_OUT'
    close_status = attr.ib(default=None)

    # timestamp of the latest activity for this workflow
    latest_activity_timestamp = attr.ib(default=None)

    # The result of the workflow, as a string, if workflow status is CLOSED and close_status is COMPLETED
    result = attr.ib(default=None)

    # The failure reason and details strings, if close_status is FAILED
    failure_reason = attr.ib(default=None)
    failure_details = attr.ib(default=None)

    # If execution_status is CLOSED, then this will point to the event describing why the workflow exited.  Normally
    # information you need can be gotten from other fields.
    exit_event = attr.ib(default=None)


def get_workflow_status(domain, workflow_id, run_id, client=None):
    """
    Get the status of a currently running or closed workflow instance.

    :param domain: SWF Domain
    :param workflow_id: Workflow ID of the workflow instance
    :param run_id: Run ID of the workflow instance
    :param client: Boto3 SWF client
    :return: A WorkflowStatus object, or None if no workflow is found matching the workflow id and run id
    """

    if client is None:
        client = boto3.client('swf')

    try:
        description = client.describe_workflow_execution(
            domain=domain, execution={'workflowId': workflow_id, 'runId': run_id})
    except client.exceptions.UnknownResourceFault:
        return None

    exec_info = description['executionInfo']
    exec_status = exec_info['executionStatus']

    status = WorkflowStatus(execution_status=exec_status, workflow_description=description,
                            latest_activity_timestamp=description.get('latestActivityTaskTimestamp'))

    exit_event_types = {'WorkflowExecutionCompleted', 'WorkflowExecutionFailed', 'WorkflowExecutionCanceled',
                        'WorkflowExecutionTerminated', 'WorkflowExecutionContinuedAsNew', 'WorkflowExecutionTimedOut'}

    if exec_status == 'CLOSED':
        status.close_status = exec_info['closeStatus']

        for event in utils.list_workflow_history(client, domain, workflow_id, run_id, reverse=True):
            if event['eventType'] in exit_event_types:
                status.exit_event = event
                if event['eventType'] == 'WorkflowExecutionCompleted':
                    status.result = event['workflowExecutionCompletedEventAttributes'].get('result')
                elif event['eventType'] == 'WorkflowExecutionFailed':
                    attributes = event['workflowExecutionFailedEventAttributes']
                    status.failure_reason = attributes.get('reason')
                    status.failure_details = attributes.get('details')
                break

    return status
