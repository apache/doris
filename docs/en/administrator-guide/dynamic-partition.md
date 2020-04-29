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

Dynamic partition is a new feature introduced in Doris verion 0.12. It's designed to manage partition's Time-to-Life (TTL), reducing the burden on users.

The original design, implementation and effect can be referred to [ISSUE 2262](https://github.com/apache/incubator-doris/issues/2262)。

Currently, the function of adding partitions dynamically is implemented, and the next version will support removing partitions dynamically.

## Noun Interpretation

* FE: Frontend, the front-end node of Doris. Responsible for metadata management and request access.
* BE: Backend, Doris's back-end node. Responsible for query execution and data storage.

## Principle

In some scenarios, the user will create partitions for the table according to the day and perform routine tasks regularly every day. In this case, the user needs to manually manage the partition, otherwise the data import may fail because the partition is forgot to create, which brings additional maintenance costs to the user.

The design of implementation is that FE will starts a background thread that determines whether or not to start the thread and the scheduling frequency of the thread based on the parameters `dynamic_partition_enable` and `dynamic_partition_check_interval_seconds` in `fe.conf`.

When create a olap table, the `dynamic_partition` properties will be assigned. FE will parse `dynamic_partition` properties and check the legitimacy of the input parameters firstly, and then persist the properties to FE metadata, register the table to the list of dynamic partition at the same time. Daemon thread will scan the dynamic partition list periodically according to the configuration parameters,
read dynamic partition properties of the table, and doing the task of adding partitions. The scheduling information of each time will be kept in the memory of FE. You can check whether the scheduling task is successful through `SHOW DYNAMIC PARTITION TABLES`.

## Usage

### Establishment of tables

When creating a table, you can specify the attribute `dynamic_partition` in `PROPERTIES`, which means that the table is a dynamic partition table.
    
Examples:

```
CREATE TABLE example_db.dynamic_partition
(
k1 DATE,
k2 INT,
k3 SMALLINT,
v1 VARCHAR(2048),
v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1)
(
PARTITION p1 VALUES LESS THAN ("2014-01-01"),
PARTITION p2 VALUES LESS THAN ("2014-06-01"),
PARTITION p3 VALUES LESS THAN ("2014-12-01")
)
DISTRIBUTED BY HASH(k2) BUCKETS 32
PROPERTIES(
"storage_medium" = "SSD",
"dynamic_partition.enable" = "true"
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.buckets" = "32"
 );
```
Create a dynamic partition table, specify enable dynamic partition features, take today is 2020-01-08 for example, at every time of scheduling, will create today and after 3 days in advance of four partitions 
(if the partition is existed, the task will be ignored), partition name respectively according to the specified prefix `p20200108` `p20200109` `p20200110` `p20200111`, each partition to 32 the number of points barrels, each partition scope is as follows:
```
[types: [DATE]; keys: [2020-01-08]; ‥types: [DATE]; keys: [2020-01-09]; )
[types: [DATE]; keys: [2020-01-09]; ‥types: [DATE]; keys: [2020-01-10]; )
[types: [DATE]; keys: [2020-01-10]; ‥types: [DATE]; keys: [2020-01-11]; )
[types: [DATE]; keys: [2020-01-11]; ‥types: [DATE]; keys: [2020-01-12]; )
```
    
### Enable Dynamic Partition Feature

1. First of all, `dynamic_partition_enable=true` needs to be set in fe.conf, which can be specified by modifying the configuration file when the cluster starts up, or dynamically modified by HTTP interface at run time

2. If you need to add dynamic partitioning properties to a table prior to version 0.12, you need to modify the properties of the table with the following command

```
ALTER TABLE dynamic_partition set ("dynamic_partition.enable" = "true", "dynamic_partition.time_unit" = "DAY", "dynamic_partition.end" = "3", "dynamic_partition.prefix" = "p", "dynamic_partition.buckets" = "32");
```

### Disable Dynamic Partition Feature

If you need to stop dynamic partitioning for all dynamic partitioning tables in the cluster, you need to set 'dynamic_partition_enable=true' in fe.conf

If you need to stop dynamic partitioning for a specified table, you can modify the properties of the table with the following command

```
ALTER TABLE dynamic_partition set ("dynamic_partition.enable" = "false")
```

### Modify Dynamic Partition Properties

You can modify the properties of the dynamic partition with the following command

```
ALTER TABLE dynamic_partition set("key" = "value")
```

### Check Dynamic Partition Table Scheduling Status

You can further view the scheduling of dynamic partitioned tables by using the following command:

```    
SHOW DYNAMIC PARTITION TABLES;

+-------------------+--------+----------+------+--------+---------+---------------------+---------------------+--------+------+
| TableName         | Enable | TimeUnit | End  | Prefix | Buckets | LastUpdateTime      | LastSchedulerTime   | State  | Msg  |
+-------------------+--------+----------+------+--------+---------+---------------------+---------------------+--------+------+
| dynamic_partition | true   | DAY      | 3    | p      | 32      | 2020-01-08 20:19:09 | 2020-01-08 20:19:34 | NORMAL | N/A  |
+-------------------+--------+----------+------+--------+---------+---------------------+---------------------+--------+------+
1 row in set (0.00 sec)

```
    
* LastUpdateTime: The last time of modifying dynamic partition properties 
* LastSchedulerTime:   The last time of performing dynamic partition scheduling
* State:    The state of the last execution of dynamic partition scheduling
* Msg:  Error message for the last time dynamic partition scheduling was performed 

## Advanced Operation

### FE Configuration Item

* dynamic\_partition\_enable

    Whether to enable Doris's dynamic partition feature. The default value is false, which is off. This parameter only affects the partitioning operation of dynamic partition tables, not normal tables.
    
* dynamic\_partition\_check\_interval\_seconds

    The execution frequency of dynamically partitioned threads, by default 3600(1 hour), which means scheduled every 1 hour.
    
### HTTP Restful API

Doris provides an HTTP Restful API for modifying dynamic partition configuration parameters at run time.

The API is implemented in FE, user can access it by `fe_host:fe_http_port`.The operation needs admin privilege.

1. Set dynamic_partition_enable to true or false
    
    * Set to true
    
        ```
        GET /api/_set_config?dynamic_partition_enable=true
        
        For example: curl --location-trusted -u username:password -XGET http://fe_host:fe_http_port/api/_set_config?dynamic_partition_enable=true
        
        Return Code：200
        ```
        
    * Set to false
    
        ```
        GET /api/_set_config?dynamic_partition_enable=false
        
        For example: curl --location-trusted -u username:password -XGET http://fe_host:fe_http_port/api/_set_config?dynamic_partition_enable=false
        
        Return Code：200
        ```
    
2.  Set the scheduling frequency for dynamic partition 
    
    * Set schedule frequency to 12 hours.
        
        ```
        GET /api/_set_config?dynamic_partition_check_interval_seconds=432000
        
        For example: curl --location-trusted -u username:password -XGET http://fe_host:fe_http_port/api/_set_config?dynamic_partition_check_interval_seconds=432000
        
        Return Code：200
        ```
