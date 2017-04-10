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
        # default timeout in seconds for a task in this workflow to complete
        defaultTaskStartToCloseTimeout=600,
        # default timeout in seconds for an instance of this workflow to complete
        defaultExecutionStartToCloseTimeout=3600)

    # Workflow subclasses can redefine this to override default workflow options
    OPTIONS = {}

    def __init__(self, name=None, version=None, options=None):
        if name is None:
            name = self.NAME
        if version is None:
            version = self.VERSION
        if options is None:
            options = self.OPTIONS

        if not (name and version):
            raise exceptions.PyflowException('Workflow classes must define the NAME and VERSION properties, or '
                                             'pass name and version to the constructor')

        self._name = name
        self._version = version
        self._options = options

    @property
    def name(self):
        """The name of this workflow"""
        return self._name

    @property
    def version(self):
        """The version string of this workflow"""
        return self._version

    @property
    def options(self):
        """Additional options to pass to SWF when creating the workflow type"""
        options = self.DEFAULT_OPTIONS.copy()
        options.update(self._options)
        return options

    def run(self, swf, workflow_input):
        """Subclasses override this method to implement the workflow behavior"""
        raise NotImplementedError()
