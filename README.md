# pyflow

A Python workflow framework based on the AWS
[Simple Workflow Service](https://aws.amazon.com/documentation/swf/) (SWF).

## Summary

Pyflow is a Python library which supports defining workflows,
consisting of a network of tasks, which can execute across distributed
computing resources.  It is implemented using AWS's SWF service.

Pyflow allows defining a workflow declaratively, as a dependency graph
of tasks.  Tasks can be dependent on the successful completion of
previous tasks, and can receive the output of previous tasks as
inputs.  Tasks will be executed concurrently when possible. The
framework supports retrying tasks and error handling.

Future versions of the framework may support looping and conditional
execution of tasks.

Executable tasks can be either AWS Lambda functions, or activities
written in Python or Ruby which run on any computer which can
communicate with the SWF service.

## Programming Model

### Defining a Workflow

A workflow consists of a collection of nodes representing tasks.  A
task can have inputs and an output.  Tasks can declare dependencies on
other tasks, in which case the ouputs of the dependencies will be
provided as inputs to the task.

A workflow itself can accept inputs, and specify an output.  This
allows parameters to be passed to a workflow.  It also allows
workflows to be used as tasks in another workflow.

This example creates a workflow which pipes a string through 3 lambda
functions.  First the string-upcase function converts the string to
uppercase.  Then the string-reverse function reverses the string,
finally the string-concat function concatenates a fixed prefix onto
the reversed string.


```python
import pyflow

workflow = pyflow.Workflow('StringTransformer', version='1.0')

upper_caser_task = workflow.add(pyflow.LambdaTask(
    name='UpperCaser',
    lambda_function='string-upcase',
    depends_on=[workflow.start_task]))

reverser_task = workflow.add(pyflow.LambdaTask(
    name='Reverser',
    lambda_function='string-reverse',
    depends_on=[upper_caser_task]))

sleep_task = workflow.add(pyflow.TimerTask(
    name='Sleep',
    duration=30,
    depends_on=[reverser_task]))

workflow.add(pyflow.LambdaTask(
    name='Concat',
    lambda_function='string-concat',
    param='Hello',
    depends_on=[sleep_task, reverser_task],
    transform_input=lambda inp: [inp['param'], ' ', inp['inputs']['Reverser']]))

```

See the [Task Types](#task-types) section for a full list of the
supported task types.

Every task has an `name` property, which is a user-provided string that
is required to be unique within a workflow.

The `depends_on` property specifies a list of tasks which must
complete before a task can run.

The `num_retries` and `time_between_retries` properties allow
specifying retry behavior when task fails. 

Tasks take two kinds of inputs.  The `param` property allows
specifying any JSON-serializable object in the workflow definition
which will always be passed to the task.  Additionally, the outputs of
any tasks in the `depends_on` property are passed to the task.  The
inputs are combined into a dictionary with the following format:

```python
# Input to the Concat task shown above
{
 'param': 'Hello',
 'inputs': {'Reverser': 'DLROW'}
}
```

The `param` entry contains the value of the `param` property if
given.  The `inputs` entry contains the outputs of any tasks this task
depends on, keyed by the task ids. Note that there is no input from
the Sleep task, because `TimerTask` doesn't produce an output.

As a special case, if a task contains only a single input, either a
`param` property, or a `depends_on` with a single dependency, then the
value is passed directly, instead of wrapping it in a dict.  For
example, if the workflow is called with 'World' as the input, then the
input to the `UpperCaser` task in the above example will be just:

``` python
'World'
```

To support invoking lambdas or other tasks which expect input in some
other format, provide the `transform_input` property, which should be
a Python callable which will be passed the input in the format shown
above, and returns the transformed input that will be passed to the
task.  The `Concat` task in the above example demonstrates this.  The
`string-concat` lambda expects a list of strings as input, so the
input transformer converts the adapts the input to expected format.

Finally, in the example above, notice that the `UpperCaser` task
declares a dependency on `workflow.start_task`.  The start task of a
workflow is a pseudo-task which represents the start of the workflow.
The output of the start task is whatever input was passed to the
workflow when starting it.  A task which wants to access the workflow
input paramaters can declare `start_task` as a dependency, so that it
will receive the workflow input as an input.

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
communcicate with the SWF service, such as an EC2 machine, or a
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
