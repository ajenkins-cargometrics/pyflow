# 1.4.0

* Added `timed_wait_for_all` and `timed_wait_for_any`, which raise a `WaitTimedOutException` if the requested condition
  isn't satisfied after a given time.

# 1.3.0

* Rewrote the way workflow functions execute, to make replay behavior more consistent.
* Added `is_replaying` property to `WorkflowInvocationHelper`
* Added `wait_for_any` method, and made `wait_for_all` return a list of the futures in the order they finished.
* Added `start_timer` method which returns a Future which completes after a given time.