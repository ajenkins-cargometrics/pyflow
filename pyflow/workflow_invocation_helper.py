from pyflow import exceptions
from pyflow import future
from pyflow import workflow_state as ws


class WorkflowInvocationHelper(object):
    """
    Provides the interface and functionality that workflow functions use to interact with the decider.
    """

    def __init__(self, decision_helper):
        self._workflow_state = decision_helper.workflow_state
        ":type: pyflow.workflow_state.WorkflowState"

        self._decision_helper = decision_helper
        ":type: pyflow.decision_task_helper.DecisionTaskHelper"

        self._invocation_counters = {}

    @property
    def run_id(self):
        """The runId of the currently executing workflow"""
        return self._workflow_state.run_id

    @property
    def workflow_id(self):
        """The workflowId of the currently running workflow"""
        return self._workflow_state.workflow_id

    def invoke_lambda(self, function_name, input_arg, timeout=None):
        """
        Schedule a lambda invocation

        :param function_name: The name of the lambda function to invoke
        :param input_arg: Input argument for the lambda.  Must be a JSON-serializable object.
        :param timeout: Timeout value, in seconds, after which the lambda function is considered to have failed
          if it has not returned.  It can be any integer from 1-300.  If not specified, defaults to 300s.
        :return: A Future which yields the result of the lambda function
        """
        invocation_id = self._next_invocation_id('lambda')
        invocation_state = self._workflow_state.get_invocation_state(invocation_id)
        out_fut = future.Future()

        if invocation_state.state == ws.InvocationState.NOT_STARTED:
            invocation_state.state = ws.InvocationState.HANDLED

            if not self._workflow_state.is_replaying:
                self._decision_helper.schedule_lambda_invocation(invocation_id, function_name, input_arg, timeout)
        elif invocation_state.done:
            self._decision_helper.set_invocation_result(invocation_state, out_fut)

        return out_fut

    def invoke_activity(self, name, version, input_arg):
        """
        Schedule a SWF activity invocation

        :param name: Name of the activity type
        :param version: Version of the activity type
        :param input_arg: Input argument for the activity.  Must be a JSON-serializable object.
        :return: A Future which yields the result of the activity.
        """
        raise NotImplementedError("invoke_activity")

    def invoke_child_workflow(self, name, version, input_arg):
        raise NotImplementedError("invoke_child_workflow")

    def sleep(self, seconds):
        """
        Blocks for the specified amount of time.

        :param seconds: Number of seconds to sleep
        """
        invocation_id = self._next_invocation_id('sleep')
        invocation_state = self._workflow_state.get_invocation_state(invocation_id)

        if invocation_state.state == ws.InvocationState.NOT_STARTED:
            invocation_state.state = ws.InvocationState.HANDLED

            if not self._workflow_state.is_replaying:
                self._decision_helper.start_timer(invocation_id, seconds)
            raise exceptions.WorkflowBlockedException()
        elif invocation_state.done:
            if invocation_state.state == ws.InvocationState.CANCELED:
                raise exceptions.InvocationCanceledException(invocation_id)
            elif invocation_state.state != ws.InvocationState.SUCCEEDED:
                raise exceptions.InvocationFailedException(invocation_id)
        else:
            raise exceptions.WorkflowBlockedException()

    def wait_for_all(self, *futures):
        """
        Waits for all the futures to be done.

        :param futures: One or more futures.  If any arguments are a list, they are assumed to be a list of futures.
        """
        if any(isinstance(f, list) for f in futures):
            flattened = []
            for f in futures:
                if isinstance(f, list):
                    flattened.extend(f)
                else:
                    flattened.append(f)
            futures = flattened

        if not all(f.done for f in futures):
            raise exceptions.WorkflowBlockedException()

    def _next_invocation_id(self, prefix):
        """
        Deterministically generates the next invocation id for a given prefix.  The id will be unique in the context
        of this DecisionTaskHelper instance.

        :param prefix: Prefix of the invocation id
        :return: A new invocation id
        """
        new_id = self._invocation_counters.get(prefix, 0) + 1
        self._invocation_counters[prefix] = new_id
        return '{}{}'.format(prefix, new_id)

