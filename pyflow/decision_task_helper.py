from pyflow import exceptions
from pyflow import utils
from pyflow import workflow_state as ws


class DecisionTaskHelper(object):
    def __init__(self, decision_task, workflow_state):
        """
        Initialize a DecisionTaskHelper

        :param decision_task: An SWF DecisionTask object
        :param workflow_state: WorkflowState
        :type workflow_state: ws.WorkflowState
        """
        self._decision_task = decision_task
        self.workflow_state = workflow_state
        self.decisions = []
        self.should_delete = False

    @property
    def run_id(self):
        return self._decision_task['workflowExecution']['runId']

    @property
    def workflow_id(self):
        return self._decision_task['workflowExecution']['workflowId']

    @property
    def workflow_name(self):
        return self._decision_task['workflowType']['name']

    @property
    def workflow_version(self):
        return self._decision_task['workflowType']['version']

    @property
    def task_token(self):
        return self._decision_task['taskToken']

    @property
    def events(self):
        return self._decision_task['events']

    @property
    def previous_started_event_id(self):
        return self._decision_task['previousStartedEventId']

    @property
    def decision_task(self):
        return self._decision_task

    def is_replay_event(self, event):
        return event['eventId'] <= self.previous_started_event_id

    def schedule_lambda_invocation(self, invocation_id, function_name, input_arg, timeout=None):
        """
        Add a ScheduleLambdaFunction decision to the list of decisions.

        :param invocation_id: The id to use for the event
        :param function_name: Lambda function name
        :param input_arg: Input argument to the lambda function.  It should be a JSON-serializable object.
        :param timeout: Timeout value, in seconds, after which the lambda function is considered to have failed
          if it has not returned.
        """
        attributes = {
            'id': invocation_id,
            'name': function_name
        }

        if input_arg is not None:
            attributes['input'] = utils.encode_task_input(input_arg)

        if timeout:
            attributes['startToCloseTimeout'] = str(timeout)

        self.decisions.append({
            'decisionType': 'ScheduleLambdaFunction',
            'scheduleLambdaFunctionDecisionAttributes': attributes
        })

    def schedule_activity_invocation(self, invocation_id, activity_name, activity_version, input_arg, timeout=None,
                                     task_list=None):
        """
        Add a ScheduleActivityTask decision to the list of decisions.

        :param invocation_id: The id to use for the event
        :param activity_name: Activity type name
        :param activity_version: Activity type version
        :param input_arg: Input argument to the activity.  It should be a JSON-serializable object.
        :param timeout: Timeout value, in seconds, after which the activity task is considered to have failed
          if it has not returned.
        :param task_list: Name of the SWF task list that the activity worker is listening for events on.  If not given,
          The default registered with the activity type will be used.
        """
        attributes = {
            'activityId': invocation_id,
            'activityType': {'name': activity_name, 'version': activity_version}
        }

        if input_arg is not None:
            attributes['input'] = utils.encode_task_input(input_arg)

        if timeout:
            attributes['startToCloseTimeout'] = str(timeout)

        if task_list:
            attributes['taskList'] = {'name': task_list}

        self.decisions.append({
            'decisionType': 'ScheduleActivityTask',
            'scheduleActivityTaskDecisionAttributes': attributes
        })

    def schedule_child_workflow_invocation(self, invocation_id, name, version, input_arg=None,
                                           child_policy=None, lambda_role=None, task_list=None,
                                           execution_start_to_close_timeout=None):
        attributes = {
            'workflowId': invocation_id,
            'workflowType': {'name': name, 'version': version}
        }

        if input_arg is not None:
            attributes['input'] = utils.encode_task_input(input_arg)

        if child_policy is not None:
            attributes['childPolicy'] = child_policy

        if lambda_role is None:
            lambda_role = self.workflow_state.lambda_role

        if lambda_role is not None:
            attributes['lambdaRole'] = lambda_role

        if task_list is not None:
            attributes['taskList'] = {'name': task_list}

        if execution_start_to_close_timeout is not None:
            attributes['executionStartToCloseTimeout'] = str(execution_start_to_close_timeout)

        self.decisions.append({
            'decisionType': 'StartChildWorkflowExecution',
            'startChildWorkflowExecutionDecisionAttributes': attributes
        })

    def start_timer(self, invocation_id, seconds):
        self.decisions.append({
            'decisionType': 'StartTimer',
            'startTimerDecisionAttributes': {
                'startToFireTimeout': str(seconds),
                'timerId': invocation_id
            }
        })

    @staticmethod
    def set_invocation_result(invocation_state, result_future):
        """
        Set a future to contain the result of a completed invocation.

        :param invocation_state: The InvocationState of the completed invocation
        :type invocation_state: pyflow.workflow_state.InvocationState
        :param result_future: The future to set
        :type result_future: pyflow.future.Future
        """

        if invocation_state.done:
            state = invocation_state.state
            if state == ws.InvocationState.SUCCEEDED:
                result_future.set_result(invocation_state.result)
            else:
                # a failure of some type
                if state == ws.InvocationState.TIMED_OUT:
                    exception = exceptions.InvocationTimedOutException('InvocationStep {!r} timed out'.format(
                        invocation_state.invocation_id))
                elif state == ws.InvocationState.CANCELED:
                    exception = exceptions.InvocationCanceledException(
                        invocation_state.failure_reason, invocation_state.failure_details)
                elif state == ws.InvocationState.FAILED:
                    exception = exceptions.InvocationFailedException(
                        invocation_state.failure_reason, invocation_state.failure_details)

                result_future.set_exception(exception)

    def complete_workflow(self, result):
        """
        Schedule a CompleteWorkflow decision to indicate the workflow successfully completed

        :param result: The value to set as the result of the workflow
        """
        self.workflow_state.completed = True
        self.should_delete = True
        self.decisions.append({
            'decisionType': 'CompleteWorkflowExecution',
            'completeWorkflowExecutionDecisionAttributes': {
                'result': utils.encode_task_input(result)
            }
        })

    def fail_workflow(self, reason, details):
        """
        Schedule a FailWorkflowExecution decision to indicate the workflow failed

        :param reason: A short string describing why the workflow failed
        :param details: A longer description of the failure
        """
        self.workflow_state.completed = True
        self.should_delete = True
        self.decisions.append({
            'decisionType': 'FailWorkflowExecution',
            'failWorkflowExecutionDecisionAttributes': {
                'reason': reason,
                'details': details
            }
        })

    def cancel_workflow(self, details):
        self.workflow_state.completed = True
        self.should_delete = True
        attributes = {}
        if details is not None:
            attributes['details'] = details
        self.decisions.append({
            'decisionType': 'CancelWorkflowExecution',
            'cancelWorkflowExecutionDecisionAttributes': attributes
        })

    def event_invocation_id(self, event):
        """
        Return the invocation id associated with an event
        :param event: SWF event object
        :return: The invocation id, or None if not found
        """
        attributes = self.root_event_attributes(event)
        return attributes.get('id',
                              attributes.get('activityId',
                                             attributes.get('timerId',
                                                            attributes.get('workflowId'))))

    @staticmethod
    def event_attributes(event):
        """
        Return the attributes dict associated with an event

        :param event: SWF event object
        :return: The attributes dict
        """
        event_type = event['eventType']
        attributes_key = '{}{}EventAttributes'.format(
            event_type[0].lower(), event_type[1:])
        return event.get(attributes_key)

    def root_event_attributes(self, event):
        """Return the attributes of the root event of an event.

        Many events concerning the progress of an activity refer back
        to an earlier event, rather than duplicate information from
        that earlier event.  For example, the ActivityTaskScheduled
        event contains a bunch of info about an activity that was
        scheduled.  Subsequent events concerning that activity just
        refer back to the ActivityTaskScheduledEvent rather than
        repeat the info.  This function finds the original event and
        returns its attributes.

        :param event: The event in question
        :return: The attributes of the root event, or attributes of this
          event if this event doesn't refer to another
          event.

        """
        event_attrs = self.event_attributes(event)
        if not event_attrs:
            return None

        root_id = event_attrs.get('scheduledEventId',
                                  event_attrs.get('initiatedEventId',
                                                  event_attrs.get('startedEventId')))
        if root_id is not None:
            found = [e for e in self.events if e['eventId'] == root_id]
            if found:
                return self.event_attributes(found[0])
            else:
                return event_attrs
        else:
            return event_attrs

