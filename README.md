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

### Defining a Workflow

To define a workflow, you subclass the `pyflow.Workflow` class, and
implement the `run` method to define the workflow's behavior.  The
'run' method will be passed two arguments -- a
`WorkflowInvocationHelper` object which provides the interface for
interacting with SWF, and an input argument, which is an arbitrary
value that can be passed to the workflow when invoking it.  Here is an
example:

``` python
import pyflow

class MyWorkflow(pyflow.Workflow):
    NAME = 'MyWorkflow'
    VERSION = '1.0'
    
    def run(self, swf, arg):
        
```

### Executing a Workflow

To execute the example workflow defined above, use code like this:

``` python
domain = 'SWFSampleDomain'
task_list = 'string-transformer-decider'

pyflow.ensure_workflow_registered(
    workflow,
    domain='SWFSampleDomain')

# Will poll indefinitely for events
pyflow.poll_for_executions(workflow, domain=domain, task_list=task_list)
```

Executing the above will first ensure that the workflow type is
registered with SWF, and then enter an endless loop waiting to receive
events from the SWF service and executing workflow instances.

Optionally, a `num_iterations` parameter can be passed to
`poll_for_executions` to make it only perform `num_iterations` polls
before returning.  The workflow runner is stateless; execution state
of workflow instances is maintained by the SWF service.  This means
it's possible to have `poll_for_executions` process several events,
then exit the python process and start it again with the same
arguments, and have it pick up the workflow execution where it left
off.

To actually start a workflow instance, you can run code like this:

``` python
domain = 'SWFSampleDomain'
task_list = 'string-transformer-decider'
workflow_name = 'StringTransformer'
workflow_version = '1.0'
lambda_role='arn:aws:iam::528461152743:role/swf-lambda'

workflow_id = pyflow.start_workflow(
    domain=domain,
    workflow_name=workflow_name,
    workflow_version=workflow_version,
    task_list=task_list,
    lambda_role=lambda_role,
    input='World')

print "Workflow started with workflow_id {}".format(workflow_id)
```

Or using the AWS CLI:

```
aws swf start-workflow-execution --domain SWFSampleDomain \
    --workflow-id my-unique-workflow-id \
    --workflow-type StringTransformer \
    --task-list string-transformer-decider \
    --lambda-role arn:aws:iam::528461152743:role/swf-lambda \
    --input World
```

## Task Types

The following types of tasks can be used in a workflow.

**ActivityTask**: Executes a task on any computer that can
communicate with the SWF service, such as an EC2 machine, or a
machine outside of AWS. The machine which executes the task will need
to run an activity worker process which polls the SWF service for
tasks to run.

**LambdaTask**: Executes an AWS Lambda function.  The lambda
function's output is the task's output.

**WorkflowTask**: Executes another SWF workflow as a child
workflow. The child workflow can, but is not required to be, defined
using pyflow.  This allows composing workflows.  A `WorkflowTask` has
the same inputs and outputs as an `ActivityTask`.
  
**SignalTask**: This task type doesn't actually execute anything, but
instead waits for a signal from an external source.  This would be
useful as an alternative to using a LambdaTask or ActivityTask to poll
for some external condition to be fulfilled, such as a file being
downloaded.  The external process would need to know the workflow id
to signal.  I still need to work out the details of how the external
process would get hold of the workflow id.
  
**TimerTask**: This task doesn't actually execute anything or produce
output.  It is used to pause the workflow for a given amount of time.
This could be useful if you know that the next task in a workflow
can't succeed until a certain amount of time has passed.
  
**StartTask**: This is a pseudo-task which represents the start of a
workflow execution.  It exists to make workflow inputs available to
other tasks. You can think of it as executing at the start of a
workflow before any other tasks.  Its output is set to any input
passed to the workflow.  Other tasks can declare the workflow's start
task as a dependency in order to receive the workflow's input
parameter as in input.
