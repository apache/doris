---
layout: page
title: ORC Specification
---

There have been two released ORC file versions:

* [ORC v0](ORCv0) was released in Hive 0.11.
* [ORC v1](ORCv1) was released in Hive 0.12 and ORC 1.x.

Each version of the library will detect the format version and use
the appropriate reader. The library can also write the older versions
of the file format to ensure that users can write files that all of their
clusters can read correctly.

We are working on a new version of the file format:

* [ORC v2](ORCv2) is a work in progress and is rapidly evolving.
