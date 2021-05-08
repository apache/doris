---
{
    "title": "Dynamic Partition",
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

# Dynamic Partition

Dynamic partition is a new feature introduced in Doris version 0.12. It's designed to manage partition's Time-to-Life (TTL), reducing the burden on users.

At present, the functions of dynamically adding partitions and dynamically deleting partitions are realized.

Dynamic partitioning is only supported for Range partitions.

## Noun Interpretation

* FE: Frontend, the front-end node of Doris. Responsible for metadata management and request access.
* BE: Backend, Doris's back-end node. Responsible for query execution and data storage.

## Principle

In some usage scenarios, the user will partition the table according to the day and perform routine tasks regularly every day. At this time, the user needs to manually manage the partition. Otherwise, the data load may fail because the user does not create a partition. This brings additional maintenance costs to the user.

Through the dynamic partitioning feature, users can set the rules of dynamic partitioning when building tables. FE will start a background thread to create or delete partitions according to the rules specified by the user. Users can also change existing rules at runtime.

## Usage

### Establishment of tables

The rules for dynamic partitioning can be specified when the table is created or modified at runtime. Currently,dynamic partition rules can only be set for partition tables with single partition columns.    

* Specified when creating table

    ```
    CREATE TABLE tbl1
    (...)
    PROPERTIES
    (
        "dynamic_partition.prop1" = "value1",
        "dynamic_partition.prop2" = "value2",
        ...
    )
    ```
    
* Modify at runtime

    ```
    ALTER TABLE tbl1 SET
    (
        "dynamic_partition.prop1" = "value1",
        "dynamic_partition.prop2" = "value2",
        ...
    )
    ```
    
### Dynamic partition rule parameters

The rules of dynamic partition are prefixed with `dynamic_partition.`:

* `dynamic_partition.enable`

    Whether to enable the dynamic partition feature. Can be specified as `TRUE` or` FALSE`. If not filled, the default is `TRUE`. If it is `FALSE`, Doris will ignore the dynamic partitioning rules of the table.

* `dynamic_partition.time_unit`

    The unit for dynamic partition scheduling. Can be specified as `HOUR`,`DAY`,` WEEK`, and `MONTH`, means to create or delete partitions by hour, day, week, and month, respectively.

    When specified as `HOUR`, the suffix format of the dynamically created partition name is `yyyyMMddHH`, for example, `2020032501`. *When the time unit is HOUR, the data type of partition column cannot be DATE.*

    When specified as `DAY`, the suffix format of the dynamically created partition name is `yyyyMMdd`, for example, `20200325`.

    When specified as `WEEK`, the suffix format of the dynamically created partition name is `yyyy_ww`. That is, the week of the year of current date. For example, the suffix of the partition created for `2020-03-25` is `2020_13`, indicating that it is currently the 13th week of 2020.

    When specified as `MONTH`, the suffix format of the dynamically created partition name is `yyyyMM`, for example, `202003`.

* `dynamic_partition.time_zone`

    The time zone of the dynamic partition, if not filled in, defaults to the time zone of the current machine's system, such as `Asia/Shanghai`, if you want to know the supported TimeZone, you can found in `https://en.wikipedia.org/wiki/List_of_tz_database_time_zones`.

* `dynamic_partition.start`

    The starting offset of the dynamic partition, usually a negative number. Depending on the `time_unit` attribute, based on the current day (week / month), the partitions with a partition range before this offset will be deleted. If not filled, the default is `-2147483648`, that is, the history partition will not be  deleted.
    
* `dynamic_partition.end`

    The end offset of the dynamic partition, usually a positive number. According to the difference of the `time_unit` attribute, the partition of the corresponding range is created in advance based on the current day (week / month).

* `dynamic_partition.prefix`

    The dynamically created partition name prefix.

* `dynamic_partition.buckets`

    The number of buckets corresponding to the dynamically created partitions.

* `dynamic_partition.replication_num`

    The replication number of dynamic partition.If not filled in, defaults to the number of table's replication number.    

* `dynamic_partition.start_day_of_week`

    When `time_unit` is` WEEK`, this parameter is used to specify the starting point of the week. The value ranges from 1 to 7. Where 1 is Monday and 7 is Sunday. The default is 1, which means that every week starts on Monday.
    
* `dynamic_partition.start_day_of_month`

    When `time_unit` is` MONTH`, this parameter is used to specify the start date of each month. The value ranges from 1 to 28. 1 means the 1st of every month, and 28 means the 28th of every month. The default is 1, which means that every month starts at 1st. The 29, 30 and 31 are not supported at the moment to avoid ambiguity caused by lunar years or months.

* `dynamic_partition.create_history_partition`

    The default is false. When set to true, Doris will automatically create all partitions from start to end. At the same time, the parameter `max_dynamic_partition_num` of FE will limit the total number of partitions to avoid creating too many partitions at once. If (end - start) is larger than `max_dynamic_partition_num`, the operation will fail.

    When the `start` attribute is not specified, this parameter has no effect.

### Notice

If some partitions between `dynamic_partition.start` and `dynamic_partition.end` are lost due to some unexpected circumstances when using dynamic partition, the lost partitions between the current time and `dynamic_partition.end` will be recreated, but the lost partitions between `dynamic_partition.start` and the current time will not be recreated.

### Example

1. Table `tbl1` partition column k1, type is DATE, create a dynamic partition rule. By day partition, only the partitions of the last 7 days are kept, and the partitions of the next 3 days are created in advance.

    ```
    CREATE TABLE tbl1
    (
        k1 DATE,
        ...
    )
    PARTITION BY RANGE(k1) ()
    DISTRIBUTED BY HASH(k1)
    PROPERTIES
    (
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "DAY",
        "dynamic_partition.start" = "-7",
        "dynamic_partition.end" = "3",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.buckets" = "32"
    );
    ```

    Suppose the current date is 2020-05-29. According to the above rules, tbl1 will produce the following partitions:
    
    ```
    p20200529: ["2020-05-29", "2020-05-30")
    p20200530: ["2020-05-30", "2020-05-31")
    p20200531: ["2020-05-31", "2020-06-01")
    p20200601: ["2020-06-01", "2020-06-02")
    ```

    On the next day, 2020-05-30, a new partition will be created `p20200602: [" 2020-06-02 "," 2020-06-03 ")`
    
    On 2020-06-06, because `dynamic_partition.start` is set to 7, the partition 7 days ago will be deleted, that is, the partition `p20200529` will be deleted.
    
2. Table tbl1 partition column k1, type is DATETIME, create a dynamic partition rule. Partition by week, only keep the partition of the last 2 weeks, and create the partition of the next 2 weeks in advance.

    ```
    CREATE TABLE tbl1
    (
        k1 DATETIME,
        ...
    )
    PARTITION BY RANGE(k1) ()
    DISTRIBUTED BY HASH(k1)
    PROPERTIES
    (
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "WEEK",
        "dynamic_partition.start" = "-2",
        "dynamic_partition.end" = "2",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.buckets" = "8"
    );
    ```

    Suppose the current date is 2020-05-29, which is the 22nd week of 2020. The default week starts on Monday. Based on the above rules, tbl1 will produce the following partitions:
    
    ```
    p2020_22: ["2020-05-25 00:00:00", "2020-06-01 00:00:00")
    p2020_23: ["2020-06-01 00:00:00", "2020-06-08 00:00:00")
    p2020_24: ["2020-06-08 00:00:00", "2020-06-15 00:00:00")
    ```
    
    The start date of each partition is Monday of the week. At the same time, because the type of the partition column k1 is DATETIME, the partition value will fill the hour, minute and second fields, and all are 0.

    On 2020-06-15, the 25th week, the partition 2 weeks ago will be deleted, ie `p2020_22` will be deleted.

    In the above example, suppose the user specified the start day of the week as `"dynamic_partition.start_day_of_week" = "3"`, that is, set Wednesday as the start of week. The partition is as follows:
    
    ```
    p2020_22: ["2020-05-27 00:00:00", "2020-06-03 00:00:00")
    p2020_23: ["2020-06-03 00:00:00", "2020-06-10 00:00:00")
    p2020_24: ["2020-06-10 00:00:00", "2020-06-17 00:00:00")
    ```
    
    That is, the partition ranges from Wednesday of the current week to Tuesday of the next week.
    
    * Note: 2019-12-31 and 2020-01-01 are in same week, if the starting date of the partition is 2019-12-31, the partition name is `p2019_53`, if the starting date of the partition is 2020-01 -01, the partition name is `p2020_01`.

3. Table tbl1 partition column k1, type is DATE, create a dynamic partition rule. Partition by month without deleting historical partitions, and create partitions for the next 2 months in advance. At the same time, set the starting date on the 3rd of each month.

    ```
    CREATE TABLE tbl1
    (
        k1 DATE,
        ...
    )
    PARTITION BY RANGE(k1) ()
    DISTRIBUTED BY HASH(k1)
    PROPERTIES
    (
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "MONTH",
        "dynamic_partition.end" = "2",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.buckets" = "8",
        "dynamic_partition.start_day_of_month" = "3"
    );
    ```
    
    Suppose the current date is 2020-05-29. Based on the above rules, tbl1 will produce the following partitions:
    
    ```
    p202005: ["2020-05-03", "2020-06-03")
    p202006: ["2020-06-03", "2020-07-03")
    p202007: ["2020-07-03", "2020-08-03")
    ```
    
    Because `dynamic_partition.start` is not set, the historical partition will not be deleted.

    Assuming that today is 2020-05-20, and set 28th as the start of each month, the partition range is:

    ```
    p202004: ["2020-04-28", "2020-05-28")
    p202005: ["2020-05-28", "2020-06-28")
    p202006: ["2020-06-28", "2020-07-28")
    ```

### Modify Dynamic Partition Properties

You can modify the properties of the dynamic partition with the following command

```
ALTER TABLE tbl1 SET
(
    "dynamic_partition.prop1" = "value1",
    ...
);
```

The modification of certain attributes may cause conflicts. Assume that the partition granularity was DAY and the following partitions have been created:

```
p20200519: ["2020-05-19", "2020-05-20")
p20200520: ["2020-05-20", "2020-05-21")
p20200521: ["2020-05-21", "2020-05-22")
```

If the partition granularity is changed to MONTH at this time, the system will try to create a partition with the range `["2020-05-01", "2020-06-01")`, and this range conflicts with the existing partition. So it cannot be created. And the partition with the range `["2020-06-01", "2020-07-01")` can be created normally. Therefore, the partition between 2020-05-22 and 2020-05-30 needs to be filled manually.

### Check Dynamic Partition Table Scheduling Status

You can further view the scheduling of dynamic partitioned tables by using the following command:

```    
mysql> SHOW DYNAMIC PARTITION TABLES;
+-----------+--------+----------+-------------+------+--------+---------+-----------+----------------+---------------------+--------+------------------------+----------------------+
| TableName | Enable | TimeUnit | Start       | End  | Prefix | Buckets | StartOf   | LastUpdateTime | LastSchedulerTime   | State  | LastCreatePartitionMsg | LastDropPartitionMsg |
+-----------+--------+----------+-------------+------+--------+---------+-----------+----------------+---------------------+--------+------------------------+----------------------+
| d3        | true   | WEEK     | -3          | 3    | p      | 1       | MONDAY    | N/A            | 2020-05-25 14:29:24 | NORMAL | N/A                    | N/A                  |
| d5        | true   | DAY      | -7          | 3    | p      | 32      | N/A       | N/A            | 2020-05-25 14:29:24 | NORMAL | N/A                    | N/A                  |
| d4        | true   | WEEK     | -3          | 3    | p      | 1       | WEDNESDAY | N/A            | 2020-05-25 14:29:24 | NORMAL | N/A                    | N/A                  |
| d6        | true   | MONTH    | -2147483648 | 2    | p      | 8       | 3rd       | N/A            | 2020-05-25 14:29:24 | NORMAL | N/A                    | N/A                  |
| d2        | true   | DAY      | -3          | 3    | p      | 32      | N/A       | N/A            | 2020-05-25 14:29:24 | NORMAL | N/A                    | N/A                  |
| d7        | true   | MONTH    | -2147483648 | 5    | p      | 8       | 24th      | N/A            | 2020-05-25 14:29:24 | NORMAL | N/A                    | N/A                  |
+-----------+--------+----------+-------------+------+--------+---------+-----------+----------------+---------------------+--------+------------------------+----------------------+
7 rows in set (0.02 sec)
```
    
* LastUpdateTime: The last time of modifying dynamic partition properties 
* LastSchedulerTime: The last time of performing dynamic partition scheduling
* State: The state of the last execution of dynamic partition scheduling
* LastCreatePartitionMsg: Error message of the last time to dynamically add partition scheduling
* LastDropPartitionMsg: Error message of the last execution of dynamic deletion partition scheduling

## Advanced Operation

### FE Configuration Item

* dynamic\_partition\_enable

    Whether to enable Doris's dynamic partition feature. The default value is false, which is off. This parameter only affects the partitioning operation of dynamic partition tables, not normal tables. You can modify the parameters in `fe.conf` and restart FE to take effect. You can also execute the following commands at runtime to take effect:
    
    MySQL protocol:
    
    `ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true")`
    
    HTTP protocol:
    
    `curl --location-trusted -u username:password -XGET http://fe_host:fe_http_port/api/_set_config?dynamic_partition_enable=true`
    
    To turn off dynamic partitioning globally, set this parameter to false.
    
* dynamic\_partition\_check\_interval\_seconds

    The execution frequency of dynamic partition threads defaults to 3600 (1 hour), that is, scheduling is performed every 1 hour. You can modify the parameters in `fe.conf` and restart FE to take effect. You can also modify the following commands at runtime:
    
    MySQL protocol:

    `ADMIN SET FRONTEND CONFIG ("dynamic_partition_check_interval_seconds" = "7200")`
    
    HTTP protocol:
    
    `curl --location-trusted -u username:password -XGET http://fe_host:fe_http_port/api/_set_config?dynamic_partition_check_interval_seconds=432000`
    
### Converting dynamic and manual partition tables to each other

For a table, dynamic and manual partitioning can be freely converted, but they cannot exist at the same time, there is and only one state.

#### Converting Manual Partitioning to Dynamic Partitioning

If a table is not dynamically partitioned when it is created, it can be converted to dynamic partitioning at runtime by modifying the dynamic partitioning properties with `ALTER TABLE`, an example of which can be seen with `HELP ALTER TABLE`.

When dynamic partitioning feature is enabled, Doris will no longer allow users to manage partitions manually, but will automatically manage partitions based on dynamic partition properties.

**NOTICE**: If `dynamic_partition.start` is set, historical partitions with a partition range before the start offset of the dynamic partition will be deleted.

#### Converting Dynamic Partitioning to Manual Partitioning

The dynamic partitioning feature can be disabled by executing `ALTER TABLE tbl_name SET ("dynamic_partition.enable" = "false") ` and converting it to a manual partition table.

When dynamic partitioning feature is disabled, Doris will no longer manage partitions automatically, and users will have to create or delete partitions manually by using `ALTER TABLE`.

## Common problem

1. After creating the dynamic partition table, it prompts ```Could not create table with dynamic partition when fe config dynamic_partition_enable is false```

         Because the main switch of dynamic partition, that is, the configuration of FE ```dynamic_partition_enable``` is false, the dynamic partition table cannot be created.

         At this time, please modify the FE configuration file, add a line ```dynamic_partition_enable=true```, and restart FE. Or execute the command ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true") to turn on the dynamic partition switch.
