import tasks

__all__ = ['Workflow']


class InvalidWorkflowError(Exception):
    pass


class StartTask(tasks.Task):
    """A pseudo-task class which represents the workflow starting. Its
    output is whatever input was passed when starting the workflow.
    Other tasks can declare a dependency on the start task in order to
    access the workflow input.

    """
    def __init__(self, name):
        super(StartTask, self).__init__(name)

    @property
    def has_output(self):
        return True


class Workflow(object):
    """An instance of this class represents a workflow type.  Instances
    of this workflow can be executed.

    """

    START_TASK_NAME = 'WorkflowStartTask'

    def __init__(self, name, version, result_task_name=None):
        self._name = name
        self._version = version
        self._tasks = {self.START_TASK_NAME: StartTask(self.START_TASK_NAME)}
        self._result_task_name = result_task_name

    @property
    def name(self):
        """The name of this workflow"""
        return self._name

    @property
    def version(self):
        """The version string of this workflow"""
        return self._version

    @property
    def tasks(self):
        """
        A iterable of the tasks in this workflow
        """
        return self._tasks.itervalues()

    @property
    def start_task(self):
        """Returns the start task of this workflow"""
        return self.get_task(self.START_TASK_NAME)

    @property
    def result_task(self):
        """Returns the result task of this workflow, or None if no result task was specified"""
        if self._result_task_name is None:
            return None
        else:
            return self.get_task(self._result_task_name)

    def add(self, task):
        """Add a new Task to this workflow.

        :param task: A Task object.  Its name must be unique in this workflow.
        :return: The task object given as the input argument.

        :raises ValueError: if the workflow already contains a task
        with the same name.
        """
        if task.name in self._tasks:
            raise ValueError('A task with name already exists: {!r}'.format(
                task.name))

        self._tasks[task.name] = task
        return task

    def has_task(self, name):
        """
        Return True if the workflow contains a task with the given name.
        """
        return name in self._tasks

    def get_task(self, name):
        """
        Gets a task by name from this workflow.

        :param name: The name of the task to fetch.
        :return: The Task object with name.
        :raises KeyError: if no task with name exists in this workflow.
        """
        return self._tasks[name]

    def validate(self):
        """
        Checks a workflow definition for certain errors.

        :raises InvalidWorkflowError if an error is found
        """

        if self._result_task_name is not None and self._result_task_name not in self._tasks:
            raise InvalidWorkflowError('Result task {!r} does not exist'.format(self._result_task_name))

        if len(self._tasks) == 0:
            return

        # check for cycles

        # Construct a map from tasks to their children
        tree = {name: [] for name in self._tasks}
        for task in self._tasks.itervalues():
            for parent in task.depends_on:
                tree[parent.name].append(task.name)

        def check_for_cycles(task, seen):
            if task.name in seen:
                raise InvalidWorkflowError('Found cycle in workflow involving task {!r}'.format(task.name))
            seen.add(task.name)
            for child_name in tree[task.name]:
                check_for_cycles(self._tasks[child_name], seen)
            seen.remove(task.name)

        # for each root, do a depth-first traversal to look for cycles
        for root in self._tasks.itervalues():
            check_for_cycles(root, set())
