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

# 动态分区

动态分区是在 Doris 0.12 版本中引入的新功能。旨在对表级别的分区实现生命周期管理(TTL)，减少用户的使用负担。

最初的设计、实现和效果可以参阅 [ISSUE 2262](https://github.com/apache/incubator-doris/issues/2262)。

目前实现了动态添加分区及动态删除分区的功能。

## 名词解释

* FE：Frontend，Doris 的前端节点。负责元数据管理和请求接入。
* BE：Backend，Doris 的后端节点。负责查询执行和数据存储。

## 原理

在某些使用场景下，用户会将表按照天进行分区划分，每天定时执行例行任务，这时需要使用方手动管理分区，否则可能由于使用方没有创建分区导致数据导入失败，这给使用方带来了额外的维护成本。

在实现方式上, FE会启动一个后台线程，根据fe.conf中`dynamic_partition_enable` 及 `dynamic_partition_check_interval_seconds`参数决定该线程是否启动以及该线程的调度频率。每次调度时，会在注册表中读取动态分区表的属性，并根据动态分区属性动态添加及删除分区。

建表时，在properties中指定dynamic_partition属性，FE首先对动态分区属性进行解析，校验输入参数的合法性，然后将对应的属性持久化到FE的元数据中，并将该表注册到动态分区列表中，后台线程会根据配置参数定期对动态分区列表进行扫描，读取表的动态分区属性，执行添加分区及删除分区的任务，每次的调度信息会保留在FE的内存中（重启后则丢失），可以通过`SHOW DYNAMIC PARTITION TABLES`查看调度任务是否成功，如果存在分区创建或删除失败，会将失败信息输出。

## 使用方式

### 动态分区属性参数说明:

`dynamic_partition.enable`: 是否开启动态分区特性，可指定为 `TRUE` 或 `FALSE`。如果不填写，默认为 `TRUE`。


`dynamic_partition.time_unit`: 动态分区调度的单位，可指定为 `DAY` `WEEK` `MONTH`，当指定为 `DAY` 时，动态创建的分区名后缀格式为`yyyyMMdd`，例如`20200325`。当指定为 `WEEK` 时，动态创建的分区名后缀格式为`yyyy_ww`即当前日期属于这一年的第几周，例如 `2020-03-25` 创建的分区名后缀为 `2020_13`, 表明目前为2020年第13周。当指定为 `MONTH` 时，动态创建的分区名后缀格式为 `yyyyMM`，例如 `202003`。

`dynamic_partition.start`: 动态分区的开始时间, 以当天为基准，超过该时间范围的分区将会被删除。如果不填写，则默认为`Integer.MIN_VALUE` 即 `-2147483648`。


`dynamic_partition.end`: 动态分区的结束时间, 以当天为基准，会提前创建N个单位的分区范围。

`dynamic_partition.prefix`: 动态创建的分区名前缀。

`dynamic_partition.buckets`: 动态创建的分区所对应的分桶数量。
    
### 建表

建表时，可以在 `PROPERTIES` 中指定以下`dynamic_partition`属性，表示这个表是一个动态分区表。

示例：

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
PARTITION p20200321 VALUES LESS THAN ("2020-03-22"),
PARTITION p20200322 VALUES LESS THAN ("2020-03-23"),
PARTITION p20200323 VALUES LESS THAN ("2020-03-24"),
PARTITION p20200324 VALUES LESS THAN ("2020-03-25")
)
DISTRIBUTED BY HASH(k2) BUCKETS 32
PROPERTIES(
"storage_medium" = "SSD",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-3",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.buckets" = "32"
 );
```
创建一张动态分区表，指定开启动态分区特性，以当天为2020-03-25为例，在每次调度时，会删除分区上界小于 `2020-03-22` 的分区，为了避免删除非动态创建的分区，动态删除分区只会删除分区名符合动态创建分区规则的分区，例如分区名为a1, 则即使分区范围在待删除的分区范围内，也不会被删除。同时在调度时会提前创建今天以及以后3天（总共4天）的分区(若分区已存在则会忽略)，分区名根据指定前缀分别为`p20200325` `p20200326` `p20200327` `p20200328`,每个分区的分桶数量为32。同时会删除 `p20200321` 的分区，最终的分区范围如下:
```
[types: [DATE]; keys: [2020-03-22]; ‥types: [DATE]; keys: [2020-03-23]; )
[types: [DATE]; keys: [2020-03-23]; ‥types: [DATE]; keys: [2020-03-24]; )
[types: [DATE]; keys: [2020-03-24]; ‥types: [DATE]; keys: [2020-03-25]; )
[types: [DATE]; keys: [2020-03-25]; ‥types: [DATE]; keys: [2020-03-26]; )
[types: [DATE]; keys: [2020-03-26]; ‥types: [DATE]; keys: [2020-03-27]; )
[types: [DATE]; keys: [2020-03-27]; ‥types: [DATE]; keys: [2020-03-28]; )
[types: [DATE]; keys: [2020-03-28]; ‥types: [DATE]; keys: [2020-03-29]; )
```
    
### 开启动态分区功能
1. 首先需要在fe.conf中设置`dynamic_partition_enable=true`，可以在集群启动时通过修改配置文件指定，或者通过MySQL连接后使用命令行 `ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true")`修改，也可以在运行时通过http接口动态修改,修改方法查看高级操作部分

2. 如果需要对0.12版本之前的表添加动态分区属性，则需要通过以下命令修改表的属性
```
ALTER TABLE dynamic_partition set ("dynamic_partition.enable" = "true", "dynamic_partition.time_unit" = "DAY", "dynamic_partition.end" = "3", "dynamic_partition.prefix" = "p", "dynamic_partition.buckets" = "32");
```

### 停止动态分区功能

如果需要对集群中所有动态分区表停止动态分区功能，则需要在fe.conf中设置`dynamic_partition_enable=false`

如果需要对指定表停止动态分区功能，则可以通过以下命令修改表的属性
```
ALTER TABLE dynamic_partition SET ("dynamic_partition.enable" = "false")
```

### 修改动态分区属性

通过如下命令可以修改动态分区的属性
```
ALTER TABLE dynamic_partition SET ("key" = "value")
```

### 查看动态分区表调度情况

通过以下命令可以进一步查看动态分区表的调度情况：

```    
SHOW DYNAMIC PARTITION TABLES;

+-------------------+--------+----------+-------+------+--------+---------+---------------------+---------------------+--------+------------------------+----------------------+
| TableName         | Enable | TimeUnit | Start | End  | Prefix | Buckets | LastUpdateTime      | LastSchedulerTime   | State  | LastCreatePartitionMsg | LastDropPartitionMsg |
+-------------------+--------+----------+-------+------+--------+---------+---------------------+---------------------+--------+------------------------+----------------------+
| dynamic_partition | true   | DAY      | -3    | 3    | p      | 32      | 2020-03-12 17:25:47 | 2020-03-12 17:25:52 | NORMAL | N/A                    | N/A                  |
+-------------------+--------+----------+-------+------+--------+---------+---------------------+---------------------+--------+------------------------+----------------------+
1 row in set (0.00 sec)

```
    
* LastUpdateTime: 最后一次修改动态分区属性的时间 
* LastSchedulerTime:   最后一次执行动态分区调度的时间
* State:    最后一次执行动态分区调度的状态
* LastCreatePartitionMsg:  最后一次执行动态添加分区调度的错误信息 
* LastDropPartitionMsg:  最后一次执行动态删除分区调度的错误信息 

## 高级操作

### FE 配置项

* dynamic\_partition\_enable

    是否开启 Doris 的动态分区功能。默认为 false，即关闭。该参数只影响动态分区表的分区操作，不影响普通表。
    
* dynamic\_partition\_check\_interval\_seconds

    动态分区线程的执行频率，默认为3600(1个小时)，即每1个小时进行一次调度
    
### HTTP Restful API

Doris 提供了修改动态分区配置参数的 HTTP Restful API，用于运行时修改动态分区配置参数。

该 API 实现在 FE 端，使用 `fe_host:fe_http_port` 进行访问。需要 ADMIN 权限。

1. 将 dynamic_partition_enable 设置为 true 或 false
    
    * 标记为 true
    
        ```
        GET /api/_set_config?dynamic_partition_enable=true
        
        例如: curl --location-trusted -u username:password -XGET http://fe_host:fe_http_port/api/_set_config?dynamic_partition_enable=true
        
        返回：200
        ```
        
    * 标记为 false
    
        ```
        GET /api/_set_config?dynamic_partition_enable=false
        
        例如: curl --location-trusted -u username:password -XGET http://fe_host:fe_http_port/api/_set_config?dynamic_partition_enable=false
        
        返回：200
        ```
    
2. 设置 dynamic partition 的调度频率
    
    * 设置调度时间为12小时调度一次
        
        ```
        GET /api/_set_config?dynamic_partition_check_interval_seconds=432000
        
        例如: curl --location-trusted -u username:password -XGET http://fe_host:fe_http_port/api/_set_config?dynamic_partition_check_interval_seconds=432000
        
        返回：200
        ```
