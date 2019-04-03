## Overview of Modules

### `rekall.interval_set_3d`
This module provides the `Interval3D` and `IntervalSet3D` types and defines the
set of operations on them.

An Interval3D is a spatiotemporal volume defined by bounds on each of the three
dimensions along with a payload of arbitrary type. An IntervalSet3D is a set of
such 3D volumes.

One can perform unary operations such as `map`, `filter` on IntervalSet3D as
well as binary operations between sets such as `join`, `union` and `minus`.

### `rekall.domain_interval_collection`
This module provides `DomainIntervalCollection` type which is a collection of
`IntervalSet3D`s organized by some domain key. It exposes the same interface as
`IntervalSet3D` and executes operations on the underlying `IntervalSet3D`s.
It performs binary operations between IntervalSet3Ds of the same key.

### `rekall.bbox_predicates`
This module defines a standard library of common predicates on bounding boxes.
One can use these predicates on the XY-projection of an Interval3D by using the
`XY` macro in `rekall.interval_set_3d_utils`.

### `rekall.temporal_predicates`
This module provides a standard library of common predicates on 1-dimensional
intervals. One can use these predicates on the projection of an Interval3D onto
any of the temporal or spatial dimension by using the `T`, `X` or `Y` macro in
`rekall.interval_set_3d_utils`.

### `rekall.runtime`
This module provides a library for efficiently executing rekall queries. Given
a function that operates on a batch of inputs, the runtime divides the long
list of inputs into chunks and run each chunk potentially in parallel, and can
combine the results of each chunk at the end.
