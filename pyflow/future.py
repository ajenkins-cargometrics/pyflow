from pyflow import exceptions
from pyflow import workflow_state as ws


class Future(object):
    """
    Represents the result of an asynchronous invocation.
    """

    def __init__(self, invocation, decision_helper):
        self._invocation = invocation
        self._decision_helper = decision_helper

    @property
    def done(self):
        """Return true if the invocation represented by this future is complete, successfully or not."""
        return self._invocation.done

    @property
    def succeeded(self):
        """Return true if the invocation represented by this future completed successfully."""
        return self._invocation.state == ws.InvocationState.SUCCEEDED

    @property
    def failed(self):
        """Return true if the invocation represented by this future completed with an exception being thrown."""
        return self.done and not self.succeeded

    @property
    def exception(self):
        """If this future is done, and failed, return the Exception thrown, else return None"""
        if self.failed:
            state = self._invocation.state
            if state == ws.InvocationState.TIMED_OUT:
                exception = exceptions.InvocationTimedOutException('InvocationStep {!r} timed out'.format(
                    self._invocation.invocation_id))
            elif state == ws.InvocationState.CANCELED:
                exception = exceptions.InvocationCanceledException(
                    self._invocation.failure_reason, self._invocation.failure_details)
            elif state == ws.InvocationState.FAILED:
                exception = exceptions.InvocationFailedException(
                    self._invocation.failure_reason, self._invocation.failure_details)
            else:
                exception = exceptions.DeciderException('Unexpected done state: {!r}'.format(state))
            return exception
        else:
            return None

    @property
    def invocation_id(self):
        """
        Return the id of the invocation this is waiting for.  Mainly useful for debugging and testing.
        """
        return self._invocation.invocation_id

    def result(self):
        """
        Blocks until either the invocation is complete, or runs out of events to process.  If invocation is complete,
        returns the result of the invocation if it succeeded, or raise exception if invocation failed.  If runs out
        events while waiting, raises WorkflowBlockedException.
        """
        while True:
            if self.done:
                if self.succeeded:
                    return self._invocation.result
                else:
                    raise self.exception

            if self._decision_helper.process_next_decision_task() is None:
                raise exceptions.WorkflowBlockedException()
