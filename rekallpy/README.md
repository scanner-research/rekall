## Overview of Module

### `rekall`
The base module provides the `Interval`, `IntervalSet`, and
`IntervalSetMapping` types and provides a set of operations on them.

An `Interval` is a spatiotemporal volume defined by a bounds and a payload of
arbitrary type. An `IntervalSet` is a set of such Intervals.

One can perform unary operations such as `map`, `filter` on `IntervalSet` as
well as binary operations between sets such as `join`, `union` and `minus`.

`IntervalSetMapping` is a collection of `IntervalSet`s organized by some key.
It exposes the same interface as `IntervalSet` and executes operations on the
underlying `IntervalSet`s. It performs binary operations between `IntervalSet`s
of the same key.

### `rekall.bounds`
This submodule provides the `Bounds` abstraction and two default `Bounds`
implementations, a one-dimensional `Bounds1D` and a three-dimensional
`Bounds3D`. `Bounds1D` and `Bounds3D` both come with some useful functions on
their co-ordinate systems.

### `rekall.predicates`
This submodule provides a number of useful one-dimensional and two-dimensional
predicate functions. These functions are often used to filter pairs of
`Intervals` when joining two `IntervalSet`s or `IntervalSetMapping`s.

### `rekall.stdlib`
This submodule provides a number of useful functions that are not core to
Rekall but that we have nevertheless found to be useful. `rekall.stdlib.ingest`
in particular provides a number of useful functions for reading from various
data sources into an `IntervalSetMapping`.

### `rekall.runtime`
This module provides a library for efficiently executing rekall queries. Given
a function that operates on a batch of inputs, the runtime divides the long
list of inputs into chunks and run each chunk potentially in parallel, and can
combine the results of each chunk at the end.
