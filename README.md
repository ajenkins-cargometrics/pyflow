# pyflow

A Python workflow framework based on the AWS [Simple Workflow Service][SWF].

[SWF]: https://aws.amazon.com/documentation/swf/

## Summary

Pyflow is a Python library which supports defining distributed
asynchronous workflow applications using ordinary procedural python
code.  It is implemented using [SWF].  Workflow components can can be
implemented as AWS Lambda functions, or activity functions implemented
in Python, Ruby or Java which run on any computing resource capable of
connecting to SWF.

Pyflow is heavily inspired by the AWS [Flow Framework for Java][Java Flow]
and [Flow Framework for Ruby][Ruby Flow], but makes no attempt to be compatible
with either of those frameworks.

[Java Flow]: http://docs.aws.amazon.com/amazonswf/latest/awsflowguide/welcome.html
[Ruby Flow]: http://docs.aws.amazon.com/amazonswf/latest/awsrbflowguide/

## Programming Model

This page gives a good overview of the concepts used in a pyflow
application: [AWS Flow Framework Basic Concepts][]. It's
about [Java Flow][] but the concepts are the same for pyflow.  The
diagram on that page doesn't mention Lambda functions, but if they
were added to the diagram, the Lambda service would be another
activity worker, with Lambda functions being the activity methods.

[AWS Flow Framework Basic Concepts]: http://docs.aws.amazon.com/amazonswf/latest/awsflowguide/awsflow-basics-application-structure.html


### Implementing a Workflow

To implement a workflow, you subclass the `pyflow.Workflow` class, and
implement the `run` method to define the workflow's behavior.

``` python
import pyflow

class MyWorkflow(pyflow.Workflow):
    NAME = 'MyWorkflow'
    VERSION = '1.0'
    
    some_func = pyflow.LambdaDescriptor('some_lambda_func')
    other_func = pyflow.LambdaDescriptor('other_lambda_func')
    
    def run(self, input_arg):
       if input_arg == 'bad_input':
           raise pyflow.WorkflowFailedException('BAD_INPUT', 'Received bad input')
           
       future1 = self.some_func(input_arg)
       
       x = future1.result() + 2
       
       future2 = self.other_func(x)
       
       return future2.result()
```

The `input_arg` argument to the `run` method is an arbitrary value
that can be passed to the workflow when invoking it.  The workflow
instance has an `swf` attribute, which is
a [WorkflowInvocationHelper](./pyflow/workflow_invocation_helper.py)
object that provides the interface for invoking remote tasks and
retrieving information about the workflow execution context.

The example above demonstrates invoking lambda functions.  You first
define a class attribute for each lambda function you want to invoke,
and then use it like a method inside the `run` method.  There are
similar descriptor classes for invoking SWF activities, and other SWF
workflows.  These methods are asynchronous.  They immediately return a
`Future` object, which can be used to retrieve the result of the
invocation when it is done.  Calling the `result()` method on a future
"blocks" until the result is ready.  If the invocation succeeded, its
result will be returned.  If the invocation failed, the `result`
method will raise an `InvocationException`.

Blocking methods such as `Future.result()` don't actually block the
python process, but rather allow control to transfer back to the
Workflow Worker process so it can process other SWF events.  I'll
explain later in this document how the context switching is
implemented, as well as some rules you need to follow in your workflow
implementation code as a result of this implementation.

The code above also demonstrates how to signal a workflow failure.
Raise the `WorkflowFailedException` with two arguments.  The first is
a short `reason` string, and the second is a longer `details` string.


### Executing a Workflow

To execute a worklow you need to do two things.  First you need to
start a Workflow Worker process, which will manage the execution of
one or more workflow definitions, and then you need to tell SWF to
invoke a workflow.  Here is how to create a workflow worker with SWF.

``` python
domain = 'SWFSampleDomain'
task_list = 'my-workflow-tasklist'


# Will poll indefinitely for events
pyflow.poll_for_executions([MyWorkflow()], domain=domain, task_list=task_list,
    identity='My Workflow Worker')
```

Executing the above will first ensure that the workflow type is
registered with SWF, and then enter an endless loop waiting to receive
events from the SWF service and executing workflow instances.

Optionally, a `max_time` parameter can be passed to
`poll_for_executions` to make it only perform `max_time` seconds
before returning.  The workflow runner is stateless; execution state
of workflow instances is maintained by the SWF service.  This means
it's possible to have `poll_for_executions` process several events,
then exit the python process and start it again with the same
arguments, and have it pick up workflow executions where it left off.

To actually start a workflow instance, you can run code like this:

``` python
domain = 'SWFSampleDomain'
task_list = 'my-workflow-tasklist'
workflow_name = 'MyWorkflow'
workflow_version = '1.0'
lambda_role='arn:aws:iam::528461152743:role/swf-lambda'

workflow_id = pyflow.start_workflow(
    domain=domain,
    workflow_name=workflow_name,
    workflow_version=workflow_version,
    task_list=task_list,
    lambda_role=lambda_role,
    input='"Hello"')

print "Workflow started with workflow_id {}".format(workflow_id)
```

Or using the AWS CLI:

```
aws swf start-workflow-execution --domain SWFSampleDomain \
    --workflow-id my-unique-workflow-id \
    --workflow-type name=MyWorkflow,version=1.0 \
    --task-list string-transformer-decider \
    --lambda-role arn:aws:iam::528461152743:role/swf-lambda \
    --input '"Hello"'
```
