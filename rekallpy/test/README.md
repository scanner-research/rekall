This directory contains tests for `rekall` using the Python3
[unittest](http://docs.python.org/3/library/unittest.html) framework.

To add a test, add a new Python script and create classes that inherit from
`unittest.TestCase`. These classes should have functions that start with
`test`.

To run the tests:

From the `rekall` directory, run `python3 -m unittest discover test`.
