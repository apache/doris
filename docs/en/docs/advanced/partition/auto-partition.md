---
{
    "title": "Auto Partition",
    "language": "en"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# AUTO PARTITION

<version since="2.1">

</version>

The Auto Partitioning feature supports automatic detection of whether the corresponding partition exists during the data import process. If it does not exist, the partition will be created automatically and imported normally.

## Usage Scenarios

The auto partition function mainly solves the problem that the user expects to partition the table based on a certain column, but the data distribution of the column is scattered or unpredictable, so it is difficult to accurately create the required partitions when building or adjusting the structure of the table, or the number of partitions is so large that it is too cumbersome to create them manually.

Take the time type partition column as an example, in the [Dynamic Partition](./dynamic-partition) function, we support the automatic creation of new partitions to accommodate real-time data at specific time periods. For real-time user behaviour logs and other scenarios, this feature basically meets the requirements. However, in more complex scenarios, such as dealing with non-real-time data, the partition column is independent of the current system time and contains a large number of discrete values. At this time to improve efficiency we want to partition the data based on this column, but the data may actually involve the partition can not be grasped in advance, or the expected number of required partitions is too large. In this case, dynamic partitioning or manually created partitions can not meet our needs, automatic partitioning function is very good to cover such needs.

Suppose our table DDL is as follows:

```sql
CREATE TABLE `DAILY_TRADE_VALUE`
(
    `TRADE_DATE`              datev2 NULL COMMENT '交易日期',
    `TRADE_ID`                varchar(40) NULL COMMENT '交易编号',
    ......
)
UNIQUE KEY(`TRADE_DATE`, `TRADE_ID`)
PARTITION BY RANGE(`TRADE_DATE`)
(
    PARTITION p_2000 VALUES [('2000-01-01'), ('2001-01-01')),
    PARTITION p_2001 VALUES [('2001-01-01'), ('2002-01-01')),
    PARTITION p_2002 VALUES [('2002-01-01'), ('2003-01-01')),
    PARTITION p_2003 VALUES [('2003-01-01'), ('2004-01-01')),
    PARTITION p_2004 VALUES [('2004-01-01'), ('2005-01-01')),
    PARTITION p_2005 VALUES [('2005-01-01'), ('2006-01-01')),
    PARTITION p_2006 VALUES [('2006-01-01'), ('2007-01-01')),
    PARTITION p_2007 VALUES [('2007-01-01'), ('2008-01-01')),
    PARTITION p_2008 VALUES [('2008-01-01'), ('2009-01-01')),
    PARTITION p_2009 VALUES [('2009-01-01'), ('2010-01-01')),
    PARTITION p_2010 VALUES [('2010-01-01'), ('2011-01-01')),
    PARTITION p_2011 VALUES [('2011-01-01'), ('2012-01-01')),
    PARTITION p_2012 VALUES [('2012-01-01'), ('2013-01-01')),
    PARTITION p_2013 VALUES [('2013-01-01'), ('2014-01-01')),
    PARTITION p_2014 VALUES [('2014-01-01'), ('2015-01-01')),
    PARTITION p_2015 VALUES [('2015-01-01'), ('2016-01-01')),
    PARTITION p_2016 VALUES [('2016-01-01'), ('2017-01-01')),
    PARTITION p_2017 VALUES [('2017-01-01'), ('2018-01-01')),
    PARTITION p_2018 VALUES [('2018-01-01'), ('2019-01-01')),
    PARTITION p_2019 VALUES [('2019-01-01'), ('2020-01-01')),
    PARTITION p_2020 VALUES [('2020-01-01'), ('2021-01-01')),
    PARTITION p_2021 VALUES [('2021-01-01'), ('2022-01-01'))
)
DISTRIBUTED BY HASH(`TRADE_DATE`) BUCKETS 10
PROPERTIES (
  "replication_num" = "1"
);
```

The table stores a large amount of business history data, partitioned based on the date the transaction occurred. As you can see when building the table, we need to manually create the partitions in advance. If the data range of the partitioned columns changes, for example, 2022 is added to the above table, we need to create a partition by [ALTER-TABLE-PARTITION](../../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-TABLE-PARTITION) to make changes to the table partition. If such partitions need to be changed, or subdivided at a finer level of granularity, it is very tedious to modify them. At this point we can rewrite the table DDL using AUTO PARTITION.

## Grammar

When building a table, use the following syntax to populate [CREATE-TABLE](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE) with the `partition_info` section:

1. AUTO RANGE PARTITION:

  ```sql
  AUTO PARTITION BY RANGE FUNC_CALL_EXPR
  (
  )
  ```
  where
  ```sql
  FUNC_CALL_EXPR ::= date_trunc ( <partition_column>, '<interval>' )
  ```

2. AUTO LIST PARTITION:

  ```sql
  AUTO PARTITION BY LIST(`partition_col`)
  (
  )
  ```

### Usage example

1. AUTO RANGE PARTITION

  ```sql
  CREATE TABLE `${tblDate}` (
      `TIME_STAMP` datev2 NOT NULL COMMENT 'Date of collection'
  ) ENGINE=OLAP
  DUPLICATE KEY(`TIME_STAMP`)
  AUTO PARTITION BY RANGE date_trunc(`TIME_STAMP`, 'month')
  (
  )
  DISTRIBUTED BY HASH(`TIME_STAMP`) BUCKETS 10
  PROPERTIES (
  "replication_allocation" = "tag.location.default: 1"
  );
  ```

2. AUTO LIST PARTITION

  ```sql
  CREATE TABLE `${tblName1}` (
      `str` varchar not null
  ) ENGINE=OLAP
  DUPLICATE KEY(`str`)
  AUTO PARTITION BY LIST (`str`)
  (
  )
  DISTRIBUTED BY HASH(`str`) BUCKETS 10
  PROPERTIES (
  "replication_allocation" = "tag.location.default: 1"
  );
  ```

### Using constraints

1. Currently the AUTO RANGE PARTITION function supports only one partition column;
2. In AUTO RANGE PARTITION, the partition function supports only `date_trunc` and the partition column supports only `DATEV2` or `DATETIMEV2` format;
3. In AUTO LIST PARTITION, function calls are not supported. Partitioned columns support `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `LARGEINT`, `DATE`, `DATETIME`, `CHAR`, `VARCHAR` data-types, and partitioned values are enum values.
4. In AUTO LIST PARTITION, a separate new PARTITION is created for each fetch of a partition column for which the corresponding partition does not currently exist.

## Sample Scenarios

In the example in the Usage Scenarios section, the table DDL can be rewritten after using AUTO PARTITION:

```sql
CREATE TABLE `DAILY_TRADE_VALUE`
(
    `TRADE_DATE`              datev2 NULL,
    `TRADE_ID`                varchar(40) NULL,
    ......
)
UNIQUE KEY(`TRADE_DATE`, `TRADE_ID`)
AUTO PARTITION BY RANGE date_trunc(`TRADE_DATE`, 'year')
(
)
DISTRIBUTED BY HASH(`TRADE_DATE`) BUCKETS 10
PROPERTIES (
  "replication_num" = "1"
);
```

At this point the new table does not have a default partition:
```sql
mysql> show partitions from `DAILY_TRADE_VALUE`;
Empty set (0.12 sec)
```

After inserting the data and then viewing it again, we could found that the table has been created with corresponding partitions:
```sql
mysql> insert into `DAILY_TRADE_VALUE` values ('2012-12-13', 1), ('2008-02-03', 2), ('2014-11-11', 3);
Query OK, 3 rows affected (0.88 sec)

mysql> show partitions from `DAILY_TRADE_VALUE`;
+-------------+-----------------+----------------+---------------------+--------+--------------+--------------------------------------------------------------------------------+-----------------+---------+----------------+---------------+---------------------+---------------------+--------------------------+----------+------------+-------------------------+-----------+
| PartitionId | PartitionName   | VisibleVersion | VisibleVersionTime  | State  | PartitionKey | Range                                                                          | DistributionKey | Buckets | ReplicationNum | StorageMedium | CooldownTime        | RemoteStoragePolicy | LastConsistencyCheckTime | DataSize | IsInMemory | ReplicaAllocation       | IsMutable |
+-------------+-----------------+----------------+---------------------+--------+--------------+--------------------------------------------------------------------------------+-----------------+---------+----------------+---------------+---------------------+---------------------+--------------------------+----------+------------+-------------------------+-----------+
| 180060      | p20080101000000 | 2              | 2023-09-18 21:49:29 | NORMAL | TRADE_DATE   | [types: [DATEV2]; keys: [2008-01-01]; ..types: [DATEV2]; keys: [2009-01-01]; ) | TRADE_DATE      | 10      | 1              | HDD           | 9999-12-31 23:59:59 |                     | NULL                     | 0.000    | false      | tag.location.default: 1 | true      |
| 180039      | p20120101000000 | 2              | 2023-09-18 21:49:29 | NORMAL | TRADE_DATE   | [types: [DATEV2]; keys: [2012-01-01]; ..types: [DATEV2]; keys: [2013-01-01]; ) | TRADE_DATE      | 10      | 1              | HDD           | 9999-12-31 23:59:59 |                     | NULL                     | 0.000    | false      | tag.location.default: 1 | true      |
| 180018      | p20140101000000 | 2              | 2023-09-18 21:49:29 | NORMAL | TRADE_DATE   | [types: [DATEV2]; keys: [2014-01-01]; ..types: [DATEV2]; keys: [2015-01-01]; ) | TRADE_DATE      | 10      | 1              | HDD           | 9999-12-31 23:59:59 |                     | NULL                     | 0.000    | false      | tag.location.default: 1 | true      |
+-------------+-----------------+----------------+---------------------+--------+--------------+--------------------------------------------------------------------------------+-----------------+---------+----------------+---------------+---------------------+---------------------+--------------------------+----------+------------+-------------------------+-----------+
3 rows in set (0.12 sec)
```

A partition created by the AUTO PARTITION function has the exact same functional properties as a manually created partition.

## caveat

- If a partition is created during the insertion or import of data and the entire import process does not complete (fails or is cancelled), the created partition is not automatically deleted.
- Tables that use AUTO PARTITION only have their partitions created automatically instead of manually. The original use of the table and the partitions it creates is the same as for non-AUTO PARTITION tables or partitions.
- To prevent accidental creation of too many partitions, we use the [FE Configuration](../../admin-manual/config/fe-config) `max_auto_partition_num` controls the maximum number of partitions an AUTO PARTITION table can hold. This value can be adjusted if necessary
- When importing data to a table with AUTO PARTITION enabled, the polling interval for data sent by the Coordinator is different from that of a normal table. For details, see `olap_table_sink_send_interval_auto_partition_factor` in [BE Configuration](../../admin-manual/config/be-config).
- In the case of inserting data with [insert-overwrite](../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/INSERT-OVERWRITE) command, if an overwrite partition is specified, the AUTO PARTITION table behaves as a normal table during this process and no new partitions will be created.
