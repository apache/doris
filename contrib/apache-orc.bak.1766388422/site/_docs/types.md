---
layout: docs
title: Types
permalink: /docs/types.html
---

ORC files are completely self-describing and do not depend on the Hive
Metastore or any other external metadata. The file includes all of the
type and encoding information for the objects stored in the file. Because the
file is self-contained, it does not depend on the user's environment to
correctly interpret the file's contents.

ORC provides a rich set of scalar and compound types:

* Integer
  * boolean (1 bit)
  * tinyint (8 bit)
  * smallint (16 bit)
  * int (32 bit)
  * bigint (64 bit)
* Floating point
  * float
  * double
* String types
  * string
  * char
  * varchar
* Binary blobs
  * binary
* Decimal type
  * decimal
* Date/time
  * timestamp
  * timestamp with local time zone
  * date
* Compound types
  * struct
  * list
  * map
  * union

All ORC file are logically sequences of identically typed objects. Hive
always uses a struct with a field for each of the top-level columns as
the root object type, but that is not required. All types in ORC can take
null values including the compound types.

Compound types have children columns that hold the values for their
sub-elements. For example, a struct column has one child column for
each field of the struct. Lists always have a single child column for
the element values and maps always have two child columns. Union
columns have one child column for each of the variants.

Given the following definition of the table Foobar, the columns in the
file would form the given tree.

```
create table Foobar (
 myInt int,
 myMap map<string,
 struct<myString : string,
 myDouble: double>>,
 myTime timestamp
);
```

![ORC column structure](/img/TreeWriters.png)

# Timestamps

ORC includes two different forms of timestamps from the SQL world:

* **Timestamp** is a date and time without a time zone, which does not change based on the time zone of the reader.
* **Timestamp with local time zone** is a fixed instant in time, which does change based on the time zone of the reader.

Unless your application uses UTC consistently, **timestamp with
local time zone** is strongly preferred over **timestamp** for most
use cases. When users say an event is at 10:00, it is always in
reference to a certain timezone and means a point in time, rather than
10:00 in an arbitrary time zone.

| Type        | Value in America/Los_Angeles | Value in America/New_York |
| ----------- | ---------------------------- | ------------------------- |
| **timestamp** | 2014-12-12 6:00:00           | 2014-12-12 6:00:00        |
| **timestamp with local time zone** | 2014-12-12 9:00:00 | 2014-12-12 6:00:00 |