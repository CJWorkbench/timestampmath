2020-09-29.01
-------------

* Initial release: `difference`, `minimum` and `maximum`.

`difference` uses floating-point math, not integer math. In FP mode, it
probably shouldn't support month/year differences: that would be non-standard
math. But someday, we may consider an integer mode to allow Gregorian-calendar
math in the user's timezone.
