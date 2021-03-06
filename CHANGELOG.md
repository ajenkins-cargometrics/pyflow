# 2.0.0

* Converted to python 3.x, dropped 2.x compatibility.

# 1.6.2

* Added support for retrying lambda invocations which fail due to internal errors

# 1.6.0
* Added additional parameters to `start_workflow()`

# 1.5.0
* Added `workflow_time()` method that returns a consistent datetime.
* Fixed a problem where too large lambda input would cause decider to crash

# 1.4.1
* Fixed problem with `invoke_once` hanging.

# 1.4.0

* Added `timed_wait_for_all` and `timed_wait_for_any`, which raise a `WaitTimedOutException` if the requested condition
  isn't satisfied after a given time.
* Added `invoke_once` method, to support invoking a callable only once in a workflow, but propagate the result to 
  other decider instances.

# 1.3.0

* Rewrote the way workflow functions execute, to make replay behavior more consistent.
* Added `is_replaying` property to `WorkflowInvocationHelper`
* Added `wait_for_any` method, and made `wait_for_all` return a list of the futures in the order they finished.
* Added `start_timer` method which returns a Future which completes after a given time.
