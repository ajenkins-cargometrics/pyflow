import re

from pyflow import utils
from pyflow import workflow_state as ws
from pyflow.utils import logger


class EventHandler(object):
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

    def __init__(self, decision_helper):
        """:type decision_helper: pyflow.decision_task_helper.DecisionTaskHelper"""
        self.decision_helper = decision_helper

    def update_state_from_event(self, event):
        """
        Updates the workflow state in response to a decision event

        :param event: A workflow event object
        :return: True if any invocation state changed from a not-done state to a done-state.
        """
        logger.debug('Processing event %r', event)
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
        invocation_state.update_state(state=new_state, failure_reason=failure_reason,
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
        invocation_id = self.decision_helper.event_invocation_id(event)
        invocation_state = self.decision_helper.workflow_state.get_invocation_state(invocation_id)

        if invocation_state.retries_left > 0 and self.should_retry_lambda(attributes):
            if not self.decision_helper.is_replaying:
                self.decision_helper.schedule_lambda_invocation(**invocation_state.invocation_args)
            invocation_state.retries_left -= 1
            return self._handle_state_change(event, ws.InvocationState.HANDLED)
        else:
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

    # marker event handlers

    def handle_marker_recorded(self, event):
        attributes = self.decision_helper.event_attributes(event)
        try:
            result = utils.decode_task_result(attributes.get('details'))
        except ValueError as e:
            return self._handle_state_change(event, ws.InvocationState.FAILED,
                                             failure_reason='Failed to decode marker value',
                                             failure_details=str(e))
        return self._handle_state_change(event, ws.InvocationState.SUCCEEDED, result=result)

    def handle_record_marker_failed(self, event):
        attributes = self.decision_helper.event_attributes(event)
        return self._handle_state_change(event, ws.InvocationState.FAILED, failure_reason=attributes['cause'])

    def should_retry_lambda(self, event_attributes):
        reason = event_attributes.get('reason')
        return reason in ('SdkClientException', 'ServiceException')
