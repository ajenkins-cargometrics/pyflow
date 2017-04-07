# New Features

* **Run Once**: When a workflow invokes a lambda or activity, pyflow
  ensures that it only runs once during the life of the workflow.  In
  some cases a user may want to run something directly in the workflow
  function rather than having to create a lambda or activity for it,
  but still have it run only once, just as if it was a lambda.  Usage
  would be like: `future = swf.run_once(some_callable, *args,
  **kwargs)`.  The `run_once` method would take a python callable, and
  arguments to call it with, and return a `Future` which will yield
  the result, or an `InvocationException` if it failed.  The function
  would run at most once in one decider instance.
  
* **Timeouts for waiting for futures**: Currently calling `result()`
  on a future blocks indefinitely.  It should be possible to specify
  an optional timeout.
  
* **Error Handler**: Allow a workflow definition to specify an action
  to take if a workflow fails.  This could be an option to just
  specify a SNS topic ARN to publish the failure to.  Could also have
  a `on_error` method on `pyflow.Workflow` that can be overridden to
  specify arbitrary error handling code.
  
* **Support for signaling workflows**: SWF supports an external
  process sending a signal to a workflow.  This could be used to have
  a workflow wait for a signal from some external process, as an
  alternative to polling.
 
