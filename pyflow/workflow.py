class Workflow(object):
    """An instance of this class represents a workflow type.  Instances
    of this workflow can be executed.
    """

    NAME = None
    VERSION = '1.0'
    OPTIONS = {}

    def __init__(self, name=None, version=None, **options):
        self._name = name or self.NAME
        self._version = version or self.VERSION
        self._options = self.OPTIONS.copy()
        self._options.update(options)

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
        return self._options

    def run(self, swf, workflow_input):
        """Subclasses override this method to implement the workflow behavior"""
        raise NotImplementedError()
