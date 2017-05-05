class PyflowException(Exception):
    """Base class of exceptions raised by Pyflow"""
    pass


class DeciderException(PyflowException):
    """Exception raised for errors in the decider"""
    pass


# This class is intentionally not derived from PyflowException, so that workflow functions can catch PyflowException
# for error handling without interfering with pyflow.
class WorkflowBlockedException(Exception):
    """Exception thrown from a wait_for* method to exit to the event loop when a workflow execution is blocked."""
    pass


class WorkflowFailedException(PyflowException):
    """
    Exception which can be thrown by a workflow function to explicitly request that the workflow be marked as failed,
    with a given reason and details string.
    """
    def __init__(self, reason, details):
        super(WorkflowFailedException, self).__init__(reason, details)

    @property
    def reason(self):
        return self.args[0]

    @property
    def details(self):
        return self.args[1]


class InvocationException(PyflowException):
    """Base class of exceptions related to invocations failing."""
    pass


class InvocationTimedOutException(InvocationException):
    """Exception thrown to indicate a timeout occurred"""
    pass


class InvocationFailedException(InvocationException):
    """Exception thrown to indicate an invocation failed"""
    def __init__(self, reason=None, details=None):
        msg = reason or ''

        if details:
            if msg:
                msg += ': ' + details
            else:
                msg = details

        super(InvocationFailedException, self).__init__(msg)


class InvocationCanceledException(InvocationException):
    """Exception thrown to indicate an invocation was canceled"""

    def __init__(self, reason=None, details=None):
        msg = reason or ''

        if details:
            if msg:
                msg += ': ' + details
            else:
                msg = details

        super(InvocationFailedException, self).__init__(msg)


class WaitTimedOutException(PyflowException):
    """Exception thrown to indicate a timeout occurred while waiting for a result"""
    pass
