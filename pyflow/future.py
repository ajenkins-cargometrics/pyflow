from pyflow import exceptions


class Future(object):
    """
    Represents the result of an asynchronous invocation.
    """

    def __init__(self, invocation_id):
        self._done = False
        self._succeeded = False
        self._result = None
        self._invocation_id = invocation_id

    @property
    def done(self):
        """Return true if the invocation represented by this future is complete, successfully or not."""
        return self._done

    @property
    def succeeded(self):
        """Return true if the invocation represented by this future completed successfully."""
        return self._succeeded

    @property
    def failed(self):
        """Return true if the invocation represented by this future completed with an exception being thrown."""
        return not self.succeeded

    @property
    def exception(self):
        """If this future is done, and failed, return the Exception thrown, else return None"""
        if self.done and self.failed:
            return self._result
        else:
            return None

    @property
    def invocation_id(self):
        """
        Return the id of the invocation this is waiting for.  Mainly useful for debugging and testing.
        """
        return self._invocation_id

    def result(self):
        """
        If this future completed successfully, return the result.  If it completed with an exception, the exception will
        be raised.  Else raise WorkflowBlockedException.
        """
        if self.done:
            if self.succeeded:
                return self._result
            else:
                raise self._result
        else:
            raise exceptions.WorkflowBlockedException()

    def set_result(self, result):
        """
        Marks this future as done, with succeeded equal true, and sets the result
        :param result: The result of the invocation
        """
        self._done = True
        self._succeeded = True
        self._result = result

    def set_exception(self, exception):
        """
        Marks this future as done, with succeeded equal false, and sets the exception.
        :param exception: Exception object
        """
        self._done = True
        self._succeeded = False
        self._result = exception
