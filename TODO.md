# New Features
  
* **Error Handler**: Allow a workflow definition to specify an action
  to take if a workflow fails.  This could be an option to just
  specify a SNS topic ARN to publish the failure to.  Could also have
  a `on_error` method on `pyflow.Workflow` that can be overridden to
  specify arbitrary error handling code.
  
* **Support for signaling workflows**: SWF supports an external
  process sending a signal to a workflow.  This could be used to have
  a workflow wait for a signal from some external process, as an
  alternative to polling.
 
