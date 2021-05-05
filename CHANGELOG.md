2021-05-05.01
-------------

* Add "startof" for rounding hour/minute/second/millisecond/microsecond.

2020-10-02.01
-------------

* (internal) rename "datetime" to "timestamp"

2020-09-30.01
-------------

* Fix error calculating difference in zero-row tables.

2020-09-29.01
-------------

* Initial release: `difference`, `minimum` and `maximum`.

`difference` uses floating-point math, not integer math. In FP mode, it
probably shouldn't support month/year differences: that would be non-standard
math. But someday, we may consider an integer mode to allow Gregorian-calendar
math in the user's timezone.
