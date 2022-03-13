# 1.0.0-preview

## New Features

1. Support for vectorized execution engine (Experimental)

    Enabled by `set enable_vectorized_engine=true`. In most cases, this can significantly improve query performance.

    http://doris.incubator.apache.org/administrator-guide/vectorized-execution-engine.html

2. Support for Lateral View syntax (Exprimental)

    This syntax allows us to expand a bitmap, String or Json Array from one column into multiple rows, and then further processing of the expanded data (e.g. Filter, Join, etc.) can be performed.

    http://doris.incubator.apache.org/sql-reference/sql-statements/Data%20Manipulation/lateral-view.html

3. Support for Hive tables (Exprimental)

    Support for users to create Hive Exprimental Tables and perform queries. This feature will extend Doris' federated query capabilities. You can use this feature to access and analyze data stored in hive directly or import hive data into Doris with insert into select statements.

    http://doris.incubator.apache.org/extending-doris/hive-of-doris.html

4. Support for the Apache SeaTunnel (Incubating) Plugin

    Users can contact SeaTunnel for pass-through and ETL between multiple data sources.

    https://github.com/apache/incubator-seatunnel

5. Support for Z-Order data sorting format.

    Store data in Z-Order sorting to speed up filtering performance on non-prefixed column conditions.

6. Support for more bitmap functions

    ```
    bitmap_max
    bitmap_and_not
    bitmap_and_not_count
    bitmap_has_all
    bitmap_and_count
    bitmap_or_count
    bitmap_xor_count
    bitmap_subset_limit
    sub_bitmap
    ```
    For details, please check the function manual.

7. Support SM3/SM4 national security algorithm.

## Important Bug Fixes

* Fix bug that memory usage too high when executing `insert into select` statement.
* Fix some query error problems.
* Fix some scheduling logic problems of broker load.
* Fix the problem that metadata cannot be loaded due to STREAM keyword.
* Repair the problem that Decommission cannot be executed correctly.

## Optimization

* Reduce the number of Segment files generated when importing large batches to reduce the pressure of Compaction.
* Transfer data via attachment function of BRPC to reduce serialization and deserialization overhead during query.
* Support direct return of HLL/BITMAP type binary data for business parsing on the outside.
* Optimize and reduce the probability of OVERCROWDED and NOT_CONNECTED errors in BRPC to enhance system stability.
* Enhance the fault tolerance of import.
* Support synchronous update and deletion of data via Flink CDC.
* Support adaptive Runtime Filter.

## Ease of use

* Routine Load supports displaying the status of the current offset lag, etc.
* Add the statistics of peak memory usage in FE audit log.
* Compaction URL adds missing version information to the result to facilitate troubleshooting.
* Support marking BE as unqueryable or unimportable to quickly block problem nodes.

## Others

*  Add Minidump function.
