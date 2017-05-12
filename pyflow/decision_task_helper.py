from pyflow import event_handler as eh
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
        self._event_handler = eh.EventHandler(self)

        self.workflow_state = workflow_state
        self.decisions = []
        self.is_replaying = False

        # Index into the events list of the decision task of the last event processed by process_next_decision_task
        self._last_event_idx = -1

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
    def started_event_id(self):
        return self._decision_task['startedEventId']

    @property
    def decision_task(self):
        return self._decision_task

    def is_replay_event(self, event):
        return event['eventId'] <= self.previous_started_event_id

    def process_next_decision_task(self):
        """
        Process events from the current decision task until encountering a DecisionTaskStarted event or end of the
        event list.

        :return: The last event processed, or None if there are no more events to process
        """
        event = None
        for self._last_event_idx in range(self._last_event_idx + 1, len(self.events)):
            event = self.events[self._last_event_idx]
            self.is_replaying = event['eventId'] <= self.previous_started_event_id
            self._event_handler.update_state_from_event(event)
            if event['eventType'] == 'DecisionTaskStarted':
                break

        return event

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

    def set_marker(self, invocation_id, value):
        self.decisions.append({
            'decisionType': 'RecordMarker',
            'recordMarkerDecisionAttributes': {
                'markerName': invocation_id,
                'details': utils.encode_task_input(value)
            }
        })

    def schedule_signal(self, invocation_id, workflow_id, input_arg=None,  run_id=None, control=None):
        attributes = {
            'signalName': invocation_id,
            'workflowId': workflow_id
        }

        if input_arg is not None:
            attributes['input'] = utils.encode_task_input(input_arg)

        if run_id is not None:
            attributes['runId'] = run_id

        if control is not None:
            attributes['control'] = control

        self.decisions.append({
            'decisionType': 'SignalExternalWorkflowExecution',
            'signalExternalWorkflowExecutionDecisionAttributes': attributes
        })

    def complete_workflow(self, result):
        """
        Schedule a CompleteWorkflow decision to indicate the workflow successfully completed

        :param result: The value to set as the result of the workflow
        """
        self.workflow_state.completed = True
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
        self.decisions.append({
            'decisionType': 'FailWorkflowExecution',
            'failWorkflowExecutionDecisionAttributes': {
                'reason': reason,
                'details': details
            }
        })

    def cancel_workflow(self, details):
        self.workflow_state.completed = True
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
        candidate_keys = ['id', 'activityId', 'timerId', 'workflowId', 'markerName', 'signalName']
        for key in candidate_keys:
            if key in attributes:
                return attributes[key]

        return None

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

