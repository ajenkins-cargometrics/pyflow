import attr


@attr.s
class InvocationState(object):
    """
    Encapsulates the state of an invocation, such as a lambda invocation, activity, timer, etc.
    """

    NOT_STARTED = 0
    HANDLED = 1
    STARTED = 2
    FAILED = 3
    TIMED_OUT = 4
    CANCELED = 5
    SUCCEEDED = 6

    DONE_STATES = (FAILED, TIMED_OUT, CANCELED, SUCCEEDED)

    invocation_id = attr.ib()

    invocation_args = attr.ib(default=None)

    state = attr.ib(default=NOT_STARTED)

    retries_left = attr.ib(default=0)

    result = attr.ib(default=None)

    failure_reason = attr.ib(default=None)
    failure_details = attr.ib(default=None)
    
    @property
    def done(self):
        return self.state in self.DONE_STATES

    def update_state(self, state=None, result=None, failure_reason=None, failure_details=None):
        if state is not None:
            self.state = state
        if result is not None:
            self.result = result
        if failure_reason is not None:
            self.failure_reason = failure_reason
        if failure_details is not None:
            self.failure_details = failure_details


@attr.s
class WorkflowState(object):
    """Encapsulates the state of a workflow instance"""

    # Identifies the workflow instance
    workflow_id = attr.ib()
    run_id = attr.ib()

    # The ARN of the lambda role specified when starting this workflow
    lambda_role = attr.ib(default=None)

    # A datetime object identifying when this workflow instance started
    workflow_start_time = attr.ib(default=None)

    # Input to the workflow when it was started
    input = attr.ib(default=None)

    # True if this workflow has completed, whether successfully or not.
    completed = attr.ib(default=False)

    # A dictionary of invocation states.  Keys are invocation ids, and values are InvocationState objects.
    invocation_states = attr.ib(default=attr.Factory(dict))  # type: dict[str, InvocationState]

    # The id of the last event added to the state
    last_seen_event_id = attr.ib(default=None)

    def get_invocation_state(self, invocation_id, initial_state=InvocationState.NOT_STARTED, num_retries=0,
                             invocation_args=None):
        """
        Gets the invocation state for an invocation_id, creating a new state if none exists

        :param invocation_id: The invocation id of the state to fetch
        :param initial_state: The initial value to set the state property to if a new InvocationState is created
        :param num_retries: Number of retries this invocation should be created with
        :param invocation_args: Arguments used to initiate this invocation
        :return: The InvocationState object for invocation_id
        """
        invocation_state = self.invocation_states.get(invocation_id)
        if invocation_state is None:
            invocation_state = self.invocation_states[invocation_id] = InvocationState(
                invocation_id=invocation_id, state=initial_state, retries_left=num_retries,
                invocation_args=invocation_args)
        return invocation_state
