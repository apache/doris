---
layout: docs
title: Indexes
permalink: /docs/indexes.html
---

ORC provides three level of indexes within each file:

* file level - statistics about the values in each column across the entire 
  file
* stripe level - statistics about the values in each column for each stripe
* row level - statistics about the values in each column for each set of
  10,000 rows within a stripe

The file and stripe level column statistics are in the file footer so
that they are easy to access to determine if the rest of the file
needs to be read at all. Row level indexes include both the column
statistics for each row group and the position for seeking to the
start of the row group.

Column statistics always contain the count of values and whether there
are null values present. Most other primitive types include the
minimum and maximum values and for numeric types the sum. As of Hive
1.2, the indexes can include bloom filters, which provide a much more
selective filter.

The indexes at all levels are used by the reader using Search
ARGuments or SARGs, which are simplified expressions that restrict the
rows that are of interest. For example, if a query was looking for
people older than 100 years old, the SARG would be "age > 100" and
only files, stripes, or row groups that had people over 100 years old
would be read.