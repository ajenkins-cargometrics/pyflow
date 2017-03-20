import attr


@attr.s(frozen=True)
class Task(object):
    name = attr.ib()
    version = attr.ib(default='v1')
    depends_on = attr.ib(default=attr.Factory(list))
    options = attr.ib(default=attr.Factory(dict))
    transform_input = attr.ib(default=None)

    @property
    def num_retries(self):
        return 0

    @property
    def has_output(self):
        """True if this type of task produces output"""
        return False


@attr.s(frozen=True)
class ExecutableTask(Task):
    """
    Base class of tasks execute an activity
    """

    num_retries = attr.ib(default=0)
    retry_interval = attr.ib(default=30)
    param = attr.ib(default=None)

    @property
    def has_output(self):
        return True


def _not_none_validator(instance, attribute, value):
    assert value is not None, "Value for attribute {!r} must not be None".format(attribute.name)


@attr.s(frozen=True)
class LambdaTask(ExecutableTask):
    lambda_function_name = attr.ib(default=None, validator=_not_none_validator)


@attr.s(frozen=True)
class TimerTask(Task):
    # TODO: remove default
    duration = attr.ib(default=None, validator=_not_none_validator)
