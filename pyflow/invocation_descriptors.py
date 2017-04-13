import functools

import attr

from pyflow import exceptions

__all__ = ['LambdaDescriptor', 'ActivityDescriptor', 'ChildWorkflowDescriptor']


@attr.s
class LambdaDescriptor(object):
    """
    Used to define a shortcut for invoking a lambda function in a workflow definition.

        LambdaDescriptor(function_name, timeout=None)

    Usage::

        class MyWorkflow(pyflow.Workflow):
            # The lambda function name is a required parameter
            my_function = pyflow.LambdaDescriptor('my_function')
            # Can also optionally specify a timeout
            other_function = pyflow.LambdaDescriptor('other_lambda_func', timeout=120)

            def run(self, input):
                # these two calls are equivalent
                fut1 = self.my_function(input)
                fut2 = self.swf.invoke_lambda('my_function', input)

                # these calls are equivalent
                fut3 = self.other_function(fut2.result())
                fut4 = self.invoke_lambda('other_lambda_func', fut2.result(), timeout=120)
    """
    function_name = attr.ib()
    timeout = attr.ib(default=None)

    def __get__(self, instance, owner):
        return functools.partial(self._invoke, instance)

    def _invoke(self, instance, *args, **kwargs):
        if (args and kwargs) or len(args) > 1:
            raise exceptions.InvocationException('Lambda must be invoked with either a single positional argument, '
                                                 'or any number of keyword arguments, but not both.')

        if args:
            input_arg = args[0]
        elif kwargs:
            input_arg = kwargs
        else:
            input_arg = None

        return instance.swf.invoke_lambda(self.function_name, input_arg, timeout=self.timeout)


@attr.s
class ActivityDescriptor(object):
    """
    Used to define a shortcut for calling an SWF activity.

        ActivityDescriptor(activity_name, activity_version, timeout=None, task_list=None)

    Usage::

        class MyWorkflow(pyflow.Workflow):
            activity1 = pyflow.ActivityDescriptor('activity_name1', 'v1.0')
            activity2 = pyflow.ActivityDescriptor('activity_name2', 'v1.0', timeout=120, task_list='some-task-list')

            def run(self, input):
                # these calls are equivalent
                fut1 = self.activity1(input)
                fut2 = self.invoke_activity('activity_name1', 'v1.0', input)

                # these are equivalent
                self.activity2(input)
                self.invoke_activity('activity_name2', 'v1.0', input,
                    timeout=120, task_list='some-task-list')
    """
    activity_name = attr.ib()
    activity_version = attr.ib()
    timeout = attr.ib(default=None)
    task_list = attr.ib(default=None)

    def __get__(self, instance, owner):
        return functools.partial(self._invoke, instance)

    def _invoke(self, instance, *args, **kwargs):
        if (args and kwargs) or len(args) > 1:
            raise exceptions.InvocationException('Activity must be invoked with either a single positional argument, '
                                                 'or any number of keyword arguments, but not both.')

        if args:
            input_arg = args[0]
        elif kwargs:
            input_arg = kwargs
        else:
            input_arg = None

        return instance.swf.invoke_activity(self.activity_name, self.activity_version, input_arg=input_arg,
                                            timeout=self.timeout, task_list=self.task_list)


@attr.s
class ChildWorkflowDescriptor(object):
    """
    Used to define a shortcut for invoking a child workflow.

        ChildWorkflowDescriptor(workflow_name, workflow_version, child_policy=None, lambda_role=None,
            task_list=None, execution_start_to_close_timeout=None)

    Usage::

        class MyWorkflow(pyflow.Workflow):
            some_workflow = pyflow.ChildWorkflowDescriptor('SomeWorkflow', '1.0', task_list='some_task_list')

            def run(self, input)
                # these are equivalent
                fut = self.some_workflow(input)
                fut2 = self.swf.invoke_child_workflow('SomeWorkflow', '1.0', input, task_list='some_task_list'
    """
    workflow_name = attr.ib()
    workflow_version = attr.ib()
    child_policy = attr.ib(default=None)
    lambda_role = attr.ib(default=None)
    task_list = attr.ib(default=None)
    execution_start_to_close_timeout = attr.ib(default=None)

    def __get__(self, instance, owner):
        return functools.partial(self._invoke, instance)

    def _invoke(self, instance, *args, **kwargs):
        if (args and kwargs) or len(args) > 1:
            raise exceptions.InvocationException('Activity must be invoked with either a single positional argument, '
                                                 'or any number of keyword arguments, but not both.')

        if args:
            input_arg = args[0]
        elif kwargs:
            input_arg = kwargs
        else:
            input_arg = None

        return instance.swf.invoke_child_workflow(
            self.workflow_name, self.workflow_version, input_arg=input_arg,
            child_policy=self.child_policy, lambda_role=self.lambda_role,
            task_list=self.task_list,
            execution_start_to_close_timeout=self.execution_start_to_close_timeout)
