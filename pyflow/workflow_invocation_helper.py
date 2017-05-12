import traceback

from pyflow import exceptions
from pyflow import future
from pyflow import workflow_state as ws


class InvokeOnceFuture(future.WrappedFuture):
    """A Future subclass returned by invoke_once"""
    def result(self):
        base_result = super(InvokeOnceFuture, self).result()
        if base_result['succeeded']:
            return base_result['result']
        else:
            raise exceptions.InvocationFailedException(base_result['reason'], base_result['details'])


class WorkflowInvocationHelper(object):
    # Possible arguments for the child_policy parameter of invoke_child_workflow
    CHILD_POLICY_TERMINATE = 'TERMINATE'
    CHILD_POLICY_REQUEST_CANCEL = 'REQUEST_CANCEL'
    CHILD_POLICY_ABANDON = 'ABANDON'

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

    @property
    def lambda_role(self):
        """The lambdaRole of the currently running workflow"""
        return self._workflow_state.lambda_role

    @property
    def is_replaying(self):
        """True if the current statement is being replayed"""
        return self._decision_helper.is_replaying

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
        out_fut = future.InvocationFuture(invocation_state, self._decision_helper)

        if invocation_state.state == ws.InvocationState.NOT_STARTED:
            invocation_state.update_state(ws.InvocationState.HANDLED)

            if not self._decision_helper.is_replaying:
                self._decision_helper.schedule_lambda_invocation(invocation_id, function_name, input_arg, timeout)

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
        out_fut = future.InvocationFuture(invocation_state, self._decision_helper)

        if invocation_state.state == ws.InvocationState.NOT_STARTED:
            invocation_state.update_state(ws.InvocationState.HANDLED)

            if not self._decision_helper.is_replaying:
                self._decision_helper.schedule_activity_invocation(invocation_id, name, version, input_arg,
                                                                   timeout=timeout, task_list=task_list)

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
        out_fut = future.InvocationFuture(invocation_state, self._decision_helper)

        if invocation_state.state == ws.InvocationState.NOT_STARTED:
            invocation_state.update_state(ws.InvocationState.HANDLED)

            if not self._decision_helper.is_replaying:
                self._decision_helper.schedule_child_workflow_invocation(
                    invocation_id, name, version, input_arg=input_arg, child_policy=child_policy,
                    lambda_role=lambda_role, task_list=task_list,
                    execution_start_to_close_timeout=execution_start_to_close_timeout)

        return out_fut

    def invoke_once(self, callable, *args, **kwargs):
        invocation_id = self._next_invocation_id('invoke_once')
        signal_id = self._next_invocation_id('invoke_once_signal')
        invocation_state = self._workflow_state.get_invocation_state(invocation_id)
        marker_fut = future.InvocationFuture(invocation_state, self._decision_helper)
        out_fut = InvokeOnceFuture(marker_fut)

        if invocation_state.state == ws.InvocationState.NOT_STARTED:
            invocation_state.update_state(ws.InvocationState.HANDLED)

            if not self._decision_helper.is_replaying:
                try:
                    result = callable(*args, **kwargs)
                except Exception:
                    marker_val = {'succeeded': False,
                                  'reason': 'Exception raised while invoking callable',
                                  'details': traceback.format_exc()}
                else:
                    marker_val = {'succeeded': True, 'result': result}

                self._decision_helper.set_marker(invocation_id, marker_val)
                self._decision_helper.schedule_signal(signal_id, self._decision_helper.workflow_id,
                                                      run_id=self._decision_helper.run_id)

        return out_fut

    def start_timer(self, seconds):
        """Returns a Future which will be done in the given number of seconds.

        :param seconds: Number of seconds until timer will be done
        :return: A Future which yields None when the timer is done.
        """
        invocation_id = self._next_invocation_id('sleep')
        invocation_state = self._workflow_state.get_invocation_state(invocation_id)
        out_fut = future.InvocationFuture(invocation_state, self._decision_helper)

        if invocation_state.state == ws.InvocationState.NOT_STARTED:
            invocation_state.update_state(ws.InvocationState.HANDLED)

            if not self._decision_helper.is_replaying:
                self._decision_helper.start_timer(invocation_id, seconds)

        return out_fut

    def sleep(self, seconds):
        """
        Blocks for the specified amount of time.

        :param seconds: Number of seconds to sleep
        """
        return self.start_timer(seconds).result()

    def wait_for_all(self, *futures):
        """
        Waits for all the futures to be done.

        :param futures: One or more futures.  If any arguments are a list, they are assumed to be a list of futures.
        :return: A list containing the input futures, in the order that they finished.
        """
        return self._wait_for_condition(lambda fs: all(f.done for f in fs), *futures)

    def wait_for_any(self, *futures):
        """
        Waits for any of the futures to be done.

        :param futures: One or more futures.  If any arguments are a list, they are assumed to be a list of futures.
        :return: A list containing the futures that finished, in the order that they finished.
        """
        return self._wait_for_condition(lambda fs: any(f.done for f in fs), *futures)

    def timed_wait_for_all(self, timeout, *futures):
        """
        Wait for all futures to be done, with a timeout.

        :param timeout: Timeout in seconds to wait
        :param futures: One or more futures.  If any arguments are a list, they are assumed to be a list of futures.
        :return: A list containing the futures that finished, in the order that they finished.
        :raises WaitTimedOutException: If all futures are not done within timeout seconds.
        """
        return self._timed_wait_for_condition(lambda fs: all(f.done for f in fs), timeout, *futures)

    def timed_wait_for_any(self, timeout, *futures):
        """
        Wait for any futures to be done, with a timeout.

        :param timeout: Timeout in seconds to wait
        :param futures: One or more futures.  If any arguments are a list, they are assumed to be a list of futures.
        :return: A list containing the futures that finished, in the order that they finished.
        :raises WaitTimedOutException: If no futures are done within timeout seconds.
        """
        return self._timed_wait_for_condition(lambda fs: any(f.done for f in fs), timeout, *futures)

    def _wait_for_condition(self, predicate, *futures):
        """
        Waits for a condition to become true on a list of predicates.  This is a helper for wait_for_all and
        wait_for_any.

        :param predicate: A callable which will be called with the input list of futures before each round of
          event processing.  It should return true when the condition is satisfied which should cause the function to
          return.
        :param futures: One or more futures.  If any arguments are a list, they are assumed to be a list of futures.
        :return: A list of the input futures which were done when the predicate returned True, in the order that they
          finished.
        """
        if any(isinstance(f, list) for f in futures):
            flattened = []
            for f in futures:
                if isinstance(f, list):
                    flattened.extend(f)
                else:
                    flattened.append(f)
            futures = flattened

        not_done = list(futures)
        done = []
        while True:
            done.extend(f for f in not_done if f.done)
            not_done = [f for f in not_done if not f.done]
            if predicate(futures):
                return done
            elif self._decision_helper.process_next_decision_task() is None:
                raise exceptions.WorkflowBlockedException()

    def _timed_wait_for_condition(self, predicate, timeout, *futures):
        """
        Waits for a condition to become true on a list of predicates, with a timeout.

        :param predicate: Same meaning as for _wait_for_condition
        :param timeout: Timeout in seconds to wait
        :param futures: One or more futures.  If any arguments are a list, they are assumed to be a list of futures.
        :return: A list containing the futures that finished, in the order that they finished.
        :raises WaitTimedOutException: If no futures are done within timeout seconds.
        """
        timeout_fut = self.start_timer(timeout)

        done_futures = self._wait_for_condition(lambda futs: predicate(futs) or timeout_fut.done,
                                                *futures)
        if predicate(futures):
            return done_futures
        else:
            raise exceptions.WaitTimedOutException()

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

