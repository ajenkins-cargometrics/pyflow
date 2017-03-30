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


class TimedOutException(PyflowException):
    """Exception thrown to indicate a timeout occurred"""
    pass


class InvocationFailedException(PyflowException):
    """Exception thrown to indicate an invocation failed"""
    def __init__(self, reason=None, details=None):
        msg = reason or ''

        if details:
            if msg:
                msg += ': ' + details
            else:
                msg = details

        super(InvocationFailedException, self).__init__(msg)


class InvocationCanceledException(PyflowException):
    """Exception thrown to indicate an invocation was canceled"""

    def __init__(self, reason=None, details=None):
        msg = reason or ''

        if details:
            if msg:
                msg += ': ' + details
            else:
                msg = details

        super(InvocationFailedException, self).__init__(msg)
