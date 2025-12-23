---
layout: docs
title: Background
permalink: /docs/index.html
---

Back in January 2013, we created ORC files as part of the initiative
to massively speed up Apache Hive and improve the storage efficiency
of data stored in Apache Hadoop. The focus was on enabling high speed
processing and reducing file sizes.

ORC is a self-describing type-aware columnar file format designed for 
Hadoop workloads. It is optimized for large streaming reads, but with
integrated support for finding required rows quickly. Storing data in
a columnar format lets the reader read, decompress, and process only
the values that are required for the current query. Because ORC files
are type-aware, the writer chooses the most appropriate encoding for
the type and builds an internal index as the file is written.

Predicate pushdown uses those indexes to determine which stripes in a
file need to be read for a particular query and the row indexes can
narrow the search to a particular set of 10,000 rows. ORC supports the
complete set of types in Hive, including the complex types: structs,
lists, maps, and unions.

Many large Hadoop users have adopted ORC. For instance, Facebook uses
ORC to [save tens of petabytes](https://s.apache.org/fb-scaling-300-pb)
in their data warehouse and demonstrated that ORC is [significantly
faster](https://s.apache.org/presto-orc) than RC File or Parquet. Yahoo
uses ORC to store their production data and has released some of their
[benchmark results](https://s.apache.org/yahoo-orc).

ORC files are divided in to *stripes* that are roughly 64MB by
default. The stripes in a file are independent of each other and form
the natural unit of distributed work. Within each stripe, the columns
are separated from each other so the reader can read just the columns
that are required.

For details on the specifics of the ORC format, please see the [ORC
format specification](/specification/).