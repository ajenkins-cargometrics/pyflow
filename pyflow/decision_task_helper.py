from . import exceptions
from . import utils
from . import workflow_state as ws


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

    def schedule_lambda_invocation(self, invocation_id, function_name, input_arg):
        """
        Add a ScheduleLambdaFunction decision to the list of decisions.

        :param invocation_id: The id to use for the event
        :param function_name: Lambda function name
        :param input_arg: Input argument to the lambda function.  It should be a JSON-serializable object.
        """
        attributes = {
            'id': invocation_id,
            'name': function_name
        }

        if input_arg is not None:
            attributes['input'] = utils.encode_task_input(input_arg)

        self.decisions.append({
            'decisionType': 'ScheduleLambdaFunction',
            'scheduleLambdaFunctionDecisionAttributes': attributes
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
                    exception = exceptions.TimedOutException('Invocation {!r} timed out'.format(
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
                'result': result
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
                                  event_attrs.get('startedEventId'))
        if root_id:
            found = [e for e in self.events if e['eventId'] == root_id]
            return self.event_attributes(found[0])
        else:
            return event_attrs

