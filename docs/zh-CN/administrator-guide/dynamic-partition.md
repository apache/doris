---
{
    "title": "动态分区",
    "language": "zh-CN"
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

# 动态分区

动态分区是在 Doris 0.12 版本中引入的新功能。旨在对表级别的分区实现生命周期管理(TTL)，减少用户的使用负担。

目前实现了动态添加分区及动态删除分区的功能。

## 名词解释

* FE：Frontend，Doris 的前端节点。负责元数据管理和请求接入。
* BE：Backend，Doris 的后端节点。负责查询执行和数据存储。

## 原理

在某些使用场景下，用户会将表按照天进行分区划分，每天定时执行例行任务，这时需要使用方手动管理分区，否则可能由于使用方没有创建分区导致数据导入失败，这给使用方带来了额外的维护成本。

通过动态分区功能，用户可以在建表时设定动态分区的规则。FE 会启动一个后台线程，根据用户指定的规则创建或删除分区。用户也可以在运行时对现有规则进行变更。

## 使用方式

动态分区的规则可以在建表时指定，或者在运行时进行修改。当前仅支持对单分区列的分区表设定动态分区规则。

* 建表时指定：

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

* 运行时修改

    ```
    ALTER TABLE tbl1 SET
    (
        "dynamic_partition.prop1" = "value1",
        "dynamic_partition.prop2" = "value2",
        ...
    )
    ```

### 动态分区规则参数

动态分区的规则参数都以 `dynamic_partition.` 为前缀：

* `dynamic_partition.enable`

    是否开启动态分区特性。可指定为 `TRUE` 或 `FALSE`。如果不填写，默认为 `TRUE`。如果为 `FALSE`，则 Doris 会忽略该表的动态分区规则。

* `dynamic_partition.time_unit`

    动态分区调度的单位。可指定为 `HOUR`、`DAY`、`WEEK`、`MONTH`。分别表示按天、按星期、按月进行分区创建或删除。
    
    当指定为 `HOUR` 时，动态创建的分区名后缀格式为 `yyyyMMddHH`，例如`2020032501`。小时为单位的分区列数据类型不能为 DATE。

    当指定为 `DAY` 时，动态创建的分区名后缀格式为 `yyyyMMdd`，例如`20200325`。
    
    当指定为 `WEEK` 时，动态创建的分区名后缀格式为`yyyy_ww`。即当前日期属于这一年的第几周，例如 `2020-03-25` 创建的分区名后缀为 `2020_13`, 表明目前为2020年第13周。
    
    当指定为 `MONTH` 时，动态创建的分区名后缀格式为 `yyyyMM`，例如 `202003`。

* `dynamic_partition.time_zone`

    动态分区的时区，如果不填写，则默认为当前机器的系统的时区，例如 `Asia/Shanghai`，如果想获取当前支持的时区设置，可以参考 `https://en.wikipedia.org/wiki/List_of_tz_database_time_zones`。

* `dynamic_partition.start`

    动态分区的起始偏移，为负数。根据 `time_unit` 属性的不同，以当天（星期/月）为基准，分区范围在此偏移之前的分区将会被删除。如果不填写，则默认为 `-2147483648`，即不删除历史分区。
    
* `dynamic_partition.end`

    动态分区的结束偏移，为正数。根据 `time_unit` 属性的不同，以当天（星期/月）为基准，提前创建对应范围的分区。

* `dynamic_partition.prefix`

    动态创建的分区名前缀。

* `dynamic_partition.buckets`

    动态创建的分区所对应的分桶数量。
  
* `dynamic_partition.replication_num`

    动态创建的分区所对应的副本数量，如果不填写，则默认为该表创建时指定的副本数量。

* `dynamic_partition.start_day_of_week`

    当 `time_unit` 为 `WEEK` 时，该参数用于指定每周的起始点。取值为 1 到 7。其中 1 表示周一，7 表示周日。默认为 1，即表示每周以周一为起始点。
    
* `dynamic_partition.start_day_of_month`

    当 `time_unit` 为 `MONTH` 时，该参数用于指定每月的起始日期。取值为 1 到 28。其中 1 表示每月1号，28 表示每月28号。默认为 1，即表示每月以1号位起始点。暂不支持以29、30、31号为起始日，以避免因闰年或闰月带来的歧义。
  
### 注意事项 
 
动态分区使用过程中，如果因为一些意外情况导致 `dynamic_partition.start` 和 `dynamic_partition.end` 之间的某些分区丢失，那么当前时间与 `dynamic_partition.end` 之间的丢失分区会被重新创建，`dynamic_partition.start`与当前时间之间的丢失分区不会重新创建。
    
## 示例

1. 表 tbl1 分区列 k1 类型为 DATE，创建一个动态分区规则。按天分区，只保留最近7天的分区，并且预先创建未来3天的分区。

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
    
    假设当前日期为 2020-05-29。则根据以上规则，tbl1 会产生以下分区：
    
    ```
    p20200529: ["2020-05-29", "2020-05-30")
    p20200530: ["2020-05-30", "2020-05-31")
    p20200531: ["2020-05-31", "2020-06-01")
    p20200601: ["2020-06-01", "2020-06-02")
    ```
    
    在第二天，即 2020-05-30，会创建新的分区 `p20200602: ["2020-06-02", "2020-06-03")`
    
    在 2020-06-06 时，因为 `dynamic_partition.start` 设置为 7，则将删除7天前的分区，即删除分区 `p20200529`。
    
2. 表 tbl1 分区列 k1 类型为 DATETIME，创建一个动态分区规则。按星期分区，只保留最近2个星期的分区，并且预先创建未来2个星期的分区。

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

    假设当前日期为 2020-05-29，是 2020 年的第 22 周。默认每周起始为星期一。则根于以上规则，tbl1 会产生以下分区：
    
    ```
    p2020_22: ["2020-05-25 00:00:00", "2020-06-01 00:00:00")
    p2020_23: ["2020-06-01 00:00:00", "2020-06-08 00:00:00")
    p2020_24: ["2020-06-08 00:00:00", "2020-06-15 00:00:00")
    ```
    
    其中每个分区的起始日期为当周的周一。同时，因为分区列 k1 的类型为 DATETIME，则分区值会补全时分秒部分，且皆为 0。
    
    在 2020-06-15，即第25周时，会删除2周前的分区，即删除 `p2020_22`。
    
    在上面的例子中，假设用户指定了周起始日为 `"dynamic_partition.start_day_of_week" = "3"`，即以每周三为起始日。则分区如下：
    
    ```
    p2020_22: ["2020-05-27 00:00:00", "2020-06-03 00:00:00")
    p2020_23: ["2020-06-03 00:00:00", "2020-06-10 00:00:00")
    p2020_24: ["2020-06-10 00:00:00", "2020-06-17 00:00:00")
    ```
    
    即分区范围为当周的周三到下周的周二。
    
    * 注：2019-12-31 和 2020-01-01 在同一周内，如果分区的起始日期为 2019-12-31，则分区名为 `p2019_53`，如果分区的起始日期为 2020-01-01，则分区名为 `p2020_01`。
    
3. 表 tbl1 分区列 k1 类型为 DATE，创建一个动态分区规则。按月分区，不删除历史分区，并且预先创建未来2个月的分区。同时设定以每月3号为起始日。

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
    
    假设当前日期为 2020-05-29。则根于以上规则，tbl1 会产生以下分区：
    
    ```
    p202005: ["2020-05-03", "2020-06-03")
    p202006: ["2020-06-03", "2020-07-03")
    p202007: ["2020-07-03", "2020-08-03")
    ```

    因为没有设置 `dynamic_partition.start`，则不会删除历史分区。
    
    假设今天为 2020-05-20，并设置以每月28号为起始日，则分区范围为：
    
    ```
    p202004: ["2020-04-28", "2020-05-28")
    p202005: ["2020-05-28", "2020-06-28")
    p202006: ["2020-06-28", "2020-07-28")
    ```
    
## 修改动态分区属性

通过如下命令可以修改动态分区的属性：

```
ALTER TABLE tbl1 SET
(
    "dynamic_partition.prop1" = "value1",
    ...
);
```

某些属性的修改可能会可能会产生冲突。假设之前分区粒度为 DAY，并且已经创建了如下分区：

```
p20200519: ["2020-05-19", "2020-05-20")
p20200520: ["2020-05-20", "2020-05-21")
p20200521: ["2020-05-21", "2020-05-22")
```

如果此时将分区粒度改为 MONTH，则系统会尝试创建范围为 `["2020-05-01", "2020-06-01")` 的分区，而该分区的分区范围和已有分区冲突，所以无法创建。而范围为 `["2020-06-01", "2020-07-01")` 的分区可以正常创建。因此，2020-05-22 到 2020-05-30 时间段的分区，需要自行填补。

### 查看动态分区表调度情况

通过以下命令可以进一步查看当前数据库下，所有动态分区表的调度情况：

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
    
* LastUpdateTime: 最后一次修改动态分区属性的时间 
* LastSchedulerTime:   最后一次执行动态分区调度的时间
* State: 最后一次执行动态分区调度的状态
* LastCreatePartitionMsg:  最后一次执行动态添加分区调度的错误信息 
* LastDropPartitionMsg:  最后一次执行动态删除分区调度的错误信息 

## 高级操作

### FE 配置项

* dynamic\_partition\_enable

    是否开启 Doris 的动态分区功能。默认为 false，即关闭。该参数只影响动态分区表的分区操作，不影响普通表。可以通过修改 fe.conf 中的参数并重启 FE 生效。也可以在运行时执行以下命令生效：
    
    MySQL 协议：
    
    `ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true")`
    
    HTTP 协议：
    
    `curl --location-trusted -u username:password -XGET http://fe_host:fe_http_port/api/_set_config?dynamic_partition_enable=true`
    
    若要全局关闭动态分区，则设置此参数为 false 即可。
    
* dynamic\_partition\_check\_interval\_seconds

    动态分区线程的执行频率，默认为3600(1个小时)，即每1个小时进行一次调度。可以通过修改 fe.conf 中的参数并重启 FE 生效。也可以在运行时执行以下命令修改：
    
    MySQL 协议：

    `ADMIN SET FRONTEND CONFIG ("dynamic_partition_check_interval_seconds" = "7200")`
    
    HTTP 协议：
    
    `curl --location-trusted -u username:password -XGET http://fe_host:fe_http_port/api/_set_config?dynamic_partition_check_interval_seconds=432000`

### 动态分区表与手动分区表相互转换

对于一个表来说，动态分区和手动分区可以自由转换，但二者不能同时存在，有且只有一种状态。

#### 手动分区转换为动态分区

如果一个表在创建时未指定动态分区，可以通过 `ALTER TABLE` 在运行时修改动态分区相关属性来转化为动态分区，具体示例可以通过 `HELP ALTER TABLE` 查看。

开启动态分区功能后，Doris 将不再允许用户手动管理分区，会根据动态分区属性来自动管理分区。

**注意**：如果已设定 `dynamic_partition.start`，分区范围在动态分区起始偏移之前的历史分区将会被删除。

#### 动态分区转换为手动分区

通过执行 `ALTER TABLE tbl_name SET ("dynamic_partition.enable" = "false")` 即可关闭动态分区功能，将其转换为手动分区表。

关闭动态分区功能后，Doris 将不再自动管理分区，需要用户手动通过 `ALTER TABLE` 的方式创建或删除分区。

## 常见问题

1. 创建动态分区表后提示 ```Could not create table with dynamic partition when fe config dynamic_partition_enable is false```

	由于动态分区的总开关，也就是 FE 的配置 ```dynamic_partition_enable``` 为 false，导致无法创建动态分区表。

        这时候请修改 FE 的配置文件，增加一行 ```dynamic_partition_enable=true```，并重启 FE。或者执行命令 ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true") 将动态分区开关打开即可。
