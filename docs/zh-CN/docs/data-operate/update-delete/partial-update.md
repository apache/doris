---
{
    "title": "部分列更新",
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

# 部分列更新 

Doris默认的数据写入语义是整行Upsert，在2.0版本之前，用户想要更新某些行的一部分字段，只能通过`UPDATE`命令，但是`UPDATE`命令由于读写事务的锁粒度原因，并不适合高频的数据写入场景。因此我们在2.0版本引入了Unique Key模型的部分列更新支持。

> 注意：
>
> 1. 2.0.0版本仅在Unique Key的Merge-on-Write实现中支持了部分列更新能力
> 3. 2.0.2版本支持使用`INSERT INTO`进行部分列更新
> 3. 2.1.0版本将支持Unique Key模型Merge-on-Read的部分列更新
> 3. 2.1.0版本讲支持更为灵活的列更新，见下文“使用限制”部分的说明

## 适用场景

- 实时的动态列更新，需要在表中实时的高频更新某些字段值。例如T+1生成的用户标签表中有一些关于用户最新行为信息的字段需要实时的更新，以实现广告/推荐等系统能够据其进行实时的分析和决策
- 将多张源表拼接成一张大宽表
- 数据修正

## 基本原理

### Merge-on-Write实现

用户通过正常的导入方式将一部分列的数据写入Doris的Memtable，此时Memtable中并没有整行数据，在Memtable下刷的时候，会查找历史数据，用历史数据补齐一整行，并写入数据文件中，同时将历史数据文件中相同key的数据行标记删除。

当出现并发导入时，Doris会利用MVCC机制来保证数据的正确性。如果两批数据导入都更新了一个相同key的不同列，则其中系统版本较高的导入任务会在版本较低的导入任务成功后，使用版本较低的导入任务写入的相同key的数据行重新进行补齐。

### Merge-on-Read实现

数据在写入过程中不做任何处理，在数据读取时通过REPLACE_IF_NOT_NULL聚合函数对不同数据文件的数据进行聚合。

## 并发写入和数据可见性

部分列更新支持高频的并发写入，写入成功后数据即可见，系统自动通过MVCC机制来保证并发写入的数据正确性

## 性能

使用建议：

1. 对写入性能要求较高，查询性能要求较低的用户，建议使用merge-on-read实现
2. 对查询性能要求较高，对写入性能要求不高（例如数据的写入和更新基本都在凌晨低峰期完成），或者写入频率不高的用户，建议使用merge-on-write实现

### Merge-on-Write实现

由于Merge-on-Write实现需要在数据写入的时候，进行整行数据的补齐，以保证最优的查询性能，因此使用Merge-on-Write实现进行部分列更新会有较为明显的导入性能下降。

写入性能优化建议：

1. 使用配备了NVMe的SSD，或者极速SSD云盘。因为补齐数据时会大量的读取历史数据，产生较高的读IOPS，以及读吞吐
2. 开启行存将能够大大减少补齐数据时产生的IOPS，导入性能提升明显，用户可以在建表时通过如下property来开启行存：

```
"store_row_column" = "true"
```

### Merge-on-Read实现

Merge-on-Read实现在写入过程中不做任何额外处理，所以写入性能不受影响，与普通的数据导入相同。但是在查询时进行聚合的代价较大，典型的聚合查询性能相比Merge-on-Write实现会有5-10倍的下降。

## 使用方式

由于实现方式差异较大，Merge-on-Write实现和Merge-on-Read实现无法在使用方式上实现完全的统一，用户需要根据自己的需求选择不同的实现和使用方式。

> 注：本文档目前仅介绍Merge-on-Write的部分列更新的使用示例。待2.1.0版本正式发布后，将更新Merge-on-Read的使用示例。

### Merge-on-Write实现

#### StreamLoad/BrokerLoad/RoutineLoad

如果使用的是StreamLoad/BrokerLoad/RoutineLoad，在导入时添加如下header

```
partial_columns:true
```

同时在`columns`中指定要导入的列（必须包含所有key列，不然无法更新）

#### INSERT INTO

在所有的数据模型中，`INSERT INTO` 给定一部分列时默认行为都是整行写入，为了防止误用，在Merge-on-Write实现中，`INSERT INTO`默认仍然保持整行UPSERT的语意，如果需要开启部分列更新的语意，需要设置如下 session variable

```
set enable_unique_key_partial_update=true
```

## 使用示例

假设 Doris 中存在一张订单表order_tbl，其中 订单id 是 Key 列，订单状态，订单金额是 Value 列。数据状态如下：

| 订单id | 订单金额 | 订单状态 |
| ------ | -------- | -------- |
| 1      | 100      | 待付款   |

```sql
+----------+--------------+--------------+
| order_id | order_amount | order_status |
+----------+--------------+--------------+
| 1        |          100 | 待付款        |
+----------+--------------+--------------+
1 row in set (0.01 sec)
```

这时候，用户点击付款后，Doris 系统需要将订单id 为 '1' 的订单状态变更为 '待发货'。

若使用StreamLoad可以通过如下方式进行更新：

```sql
$cat update.csv
1,待发货

$ curl  --location-trusted -u root: -H "partial_columns:true" -H "column_separator:," -H "columns:order_id,order_status" -T /tmp/update.csv http://127.0.0.1:48037/api/db1/order_tbl/_stream_load
```

若使用`INSRT INTO`可以通过如下方式进行更新：

```
set enable_unique_key_partial_update=true;
INSERT INTO order_tbl (order_id, order_status) values (1,'待付款');
```

更新后结果如下

```sql
+----------+--------------+--------------+
| order_id | order_amount | order_status |
+----------+--------------+--------------+
| 1        |          100 | 待发货       |
+----------+--------------+--------------+
1 row in set (0.01 sec)
```

## 使用限制

在2.0版本中，同一批次数据写入任务（无论是导入任务还是`INSERT INTO`）的所有行只能更新相同的列，如果需要更新不同列的数据，则需要分不同的批次进行写入

在2.1版本中，我们将支持灵活的列更新，用户可以在同一批导入中，每一行更新不同的列

## 更多帮助

关于Unique Key的Merge-on-Read实现和Merge-on-Write实现，请参考[数据模型](../../data-table/data-model.md)的介绍

