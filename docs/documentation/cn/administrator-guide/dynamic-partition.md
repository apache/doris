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

目前实现了动态添加分区的功能，下一个版本会支持动态删除分区的功能。

## 名词解释

* FE：Frontend，Doris 的前端节点。负责元数据管理和请求接入。
* BE：Backend，Doris 的后端节点。负责查询执行和数据存储。

## 原理

在某些使用场景下，用户会将表按照天进行分区划分，每天定时执行例行任务，这时需要使用方手动管理分区，否则可能由于使用方没有创建分区导致数据导入失败，这给使用方带来了额外的维护成本。

在实现方式上, FE会启动一个后台线程，根据fe.conf中`dynamic_partition_enable` 及 `dynamic_partition_check_interval_seconds`参数决定该线程是否启动以及该线程的调度频率.

建表时，在properties中指定dynamic_partition属性，FE首先对动态分区属性进行解析，校验输入参数的合法性，然后将对应的属性持久化到FE的元数据中，并将该表注册到动态分区列表中，后台线程会根据配置参数定期对动态分区列表进行扫描，读取表的动态分区属性，执行添加分区的任务，每次的调度信息会保留在FE的内存中，可以通过`SHOW DYNAMIC PARTITION TABLES`查看调度任务是否成功，如果存在分区创建失败，会将失败信息输出。

## 使用方式

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
创建一张动态分区表，指定开启动态分区特性，以当天为2020-01-08为例，在每次调度时，会提前创建今天以及以后3天的4个分区(若分区已存在则会忽略)，分区名根据指定前缀分别为`p20200108` `p20200109` `p20200110` `p20200111`,每个分区的分桶数量为32，每个分区的范围如下:
```
[types: [DATE]; keys: [2020-01-08]; ‥types: [DATE]; keys: [2020-01-09]; )
[types: [DATE]; keys: [2020-01-09]; ‥types: [DATE]; keys: [2020-01-10]; )
[types: [DATE]; keys: [2020-01-10]; ‥types: [DATE]; keys: [2020-01-11]; )
[types: [DATE]; keys: [2020-01-11]; ‥types: [DATE]; keys: [2020-01-12]; )
```
    
### 开启动态分区功能
1. 首先需要在fe.conf中设置`dynamic_partition_enable=true`，可以在集群启动时通过修改配置文件指定，也可以在运行时通过http接口动态修改,修改方法查看高级操作部分

2. 如果需要对0.12版本之前的表添加动态分区属性，则需要通过以下命令修改表的属性
```
ALTER TABLE dynamic_partition set ("dynamic_partition.enable" = "true", "dynamic_partition.time_unit" = "DAY", "dynamic_partition.end" = "3", "dynamic_partition.prefix" = "p", "dynamic_partition.buckets" = "32");
```

### 停止动态分区功能

如果需要对集群中所有动态分区表停止动态分区功能，则需要在fe.conf中设置`dynamic_partition_enable=true`

如果需要对指定表停止动态分区功能，则可以通过以下命令修改表的属性
```
ALTER TABLE dynamic_partition set ("dynamic_partition.enable" = "false")
```

### 修改动态分区属性

通过如下命令可以修改动态分区的属性
```
ALTER TABLE dynamic_partition set("key" = "value")
```

### 查看动态分区表调度情况

通过以下命令可以进一步查看动态分区表的调度情况：

```    
SHOW DYNAMIC PARTITION TABLES;

+-------------------+--------+----------+------+--------+---------+---------------------+---------------------+--------+------+
| TableName         | Enable | TimeUnit | End  | Prefix | Buckets | LastUpdateTime      | LastSchedulerTime   | State  | Msg  |
+-------------------+--------+----------+------+--------+---------+---------------------+---------------------+--------+------+
| dynamic_partition | true   | DAY      | 3    | p      | 32      | 2020-01-08 20:19:09 | 2020-01-08 20:19:34 | NORMAL | N/A  |
+-------------------+--------+----------+------+--------+---------+---------------------+---------------------+--------+------+
1 row in set (0.00 sec)

```
    
* LastUpdateTime: 最后一次修改动态分区属性的时间 
* LastSchedulerTime:   最后一次执行动态分区调度的时间
* State:    最后一次执行动态分区调度的状态
* Msg:  最后一次执行动态分区调度的错误信息 

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
