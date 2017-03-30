from pyflow import exceptions
from pyflow import future
from pyflow import workflow_state as ws


class WorkflowInvocationHelper(object):
    # Possible arguments for the child_policy parameter of invoke_child_workflow
    CHILD_POLICY_TERMINATE = 'TERMINATE'
    CHILD_POLICY_REQUEST_CANCEL = 'REQUEST_CANCEL'
    CHILD_POLICY_ABANDON = 'ABANDON'

    """
    Provides the interface and functionality that workflow functions use to interact with the decider.
    """

    def __init__(self, decision_helper, is_replaying):
        self._workflow_state = decision_helper.workflow_state
        ":type: pyflow.workflow_state.WorkflowState"

        self._decision_helper = decision_helper
        ":type: pyflow.decision_task_helper.DecisionTaskHelper"

        self._is_replaying = is_replaying

        self._invocation_counters = {}

    @property
    def run_id(self):
        """The runId of the currently executing workflow"""
        return self._workflow_state.run_id

    @property
    def workflow_id(self):
        """The workflowId of the currently running workflow"""
        return self._workflow_state.workflow_id

    @property
    def lambda_role(self):
        """The lambdaRole of the currently running workflow"""
        return self._workflow_state.lambda_role

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

            if not self._is_replaying:
                self._decision_helper.schedule_lambda_invocation(invocation_id, function_name, input_arg, timeout)
        elif invocation_state.done:
            self._decision_helper.set_invocation_result(invocation_state, out_fut)

        return out_fut

    def invoke_activity(self, name, version, input_arg=None, timeout=None, task_list=None):
        """
        Schedule a SWF activity invocation

        :param name: Name of the activity type
        :param version: Version of the activity type
        :param input_arg: Input argument for the activity.  Must be a JSON-serializable object.
        :param timeout: Timeout value, in seconds, after which the activity is considered to have failed
          if it has not returned.
        :param task_list: Name of the SWF task list that the activity worker is listening for events on.  If not given,
          The default registered with the activity type will be used.
        :return: A Future which yields the result of the activity.
        """
        invocation_id = self._next_invocation_id('activity')
        invocation_state = self._workflow_state.get_invocation_state(invocation_id)
        out_fut = future.Future()

        if invocation_state.state == ws.InvocationState.NOT_STARTED:
            invocation_state.state = ws.InvocationState.HANDLED

            if not self._is_replaying:
                self._decision_helper.schedule_activity_invocation(invocation_id, name, version, input_arg,
                                                                   timeout=timeout, task_list=task_list)
        elif invocation_state.done:
            self._decision_helper.set_invocation_result(invocation_state, out_fut)

        return out_fut

    def invoke_child_workflow(self, name, version, input_arg=None, child_policy=None, lambda_role=None, task_list=None,
                              execution_start_to_close_timeout=None):
        """
        Schedule a child workflow invocation.

        For all of the optional arguments, if they aren't given then the defaults specified when registering the child
        workflow type will be used.

        :param name: The name of the child workflow type
        :param version: Version of child workflow type
        :param input_arg: Input argument to the workflow.  Must be a JSON-serializable object
        :param child_policy: Specify the policy to use for the child workflow execution if this workflow is terminated
          or times out.  Can be one of the class constants CHILD_POLICY_TERMINATE, CHILD_POLICY_REQUEST_CANCEL, or
          CHILD_POLICY_ABANDON.
        :param lambda_role: The IAM role the child workflow will use to invoke lambdas as.
        :param task_list: Name of the SWF task list that the decider for the child workflow is listening for events on.
        :param execution_start_to_close_timeout: Total duration for this workflow execution in seconds, after which it
          will be considered to have timed out if it hasn't finished.
        :return: A Future which yields the result of this child workflow
        """
        invocation_id = self._next_invocation_id('child_workflow')
        invocation_state = self._workflow_state.get_invocation_state(invocation_id)
        out_fut = future.Future()

        if invocation_state.state == ws.InvocationState.NOT_STARTED:
            invocation_state.state = ws.InvocationState.HANDLED

            if not self._is_replaying:
                self._decision_helper.schedule_child_workflow_invocation(
                    invocation_id, name, version, input_arg=input_arg, child_policy=child_policy,
                    lambda_role=lambda_role, task_list=task_list,
                    execution_start_to_close_timeout=execution_start_to_close_timeout)
        elif invocation_state.done:
            self._decision_helper.set_invocation_result(invocation_state, out_fut)

        return out_fut

    def sleep(self, seconds):
        """
        Blocks for the specified amount of time.

        :param seconds: Number of seconds to sleep
        """
        invocation_id = self._next_invocation_id('sleep')
        invocation_state = self._workflow_state.get_invocation_state(invocation_id)

        if invocation_state.state == ws.InvocationState.NOT_STARTED:
            invocation_state.state = ws.InvocationState.HANDLED

            if not self._is_replaying:
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

