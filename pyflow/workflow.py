from pyflow import exceptions


class Workflow(object):
    """An instance of this class represents a workflow type.  Instances
    of this workflow can be executed.
    """

    # Workflow subclasses must override this
    NAME = None

    VERSION = '1.0'

    # default workflow options
    DEFAULT_OPTIONS = dict(
        defaultChildPolicy='TERMINATE',
        defaultTaskStartToCloseTimeout=3600,
        defaultExecutionStartToCloseTimeout=24 * 3600)

    # Workflow subclasses can redefine this to override default workflow options
    OPTIONS = {}

    def __init__(self):
        if not (self.NAME and self.VERSION):
            raise exceptions.PyflowException('Workflow classes must define the NAME and VERSION properties')

    @property
    def name(self):
        """The name of this workflow"""
        return self.NAME

    @property
    def version(self):
        """The version string of this workflow"""
        return self.VERSION

    @property
    def options(self):
        """Additional options to pass to SWF when creating the workflow type"""
        options = self.DEFAULT_OPTIONS.copy()
        options.update(self.OPTIONS)
        return options

    def run(self, swf, workflow_input):
        """Subclasses override this method to implement the workflow behavior"""
        raise NotImplementedError()
