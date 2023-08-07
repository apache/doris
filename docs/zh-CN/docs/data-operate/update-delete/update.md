---
{
    "title": "数据更新",
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

# 数据更新 

本文主要讲述如果我们需要修改或更新Doris中的数据，如何使用UPDATE命令来操作。数据更新对Doris的版本有限制，只能在Doris **Version 0.15.x +**  才可以使用。

## 适用场景

- 对满足某些条件的行，修改其取值；
- 点更新，小范围更新，待更新的行最好是整个表的非常小的一部分；
- update 命令只能在 Unique 数据模型的表中执行。

## 基本原理

利用查询引擎自身的 where 过滤逻辑，从待更新表中筛选出需要被更新的行。再利用 Unique 模型自带的 Value 列新数据替换旧数据的逻辑，将待更新的行变更后，再重新插入到表中，从而实现行级别更新。

### 同步

Update 语法在Doris中是一个同步语法，即 Update 语句执行成功，更新操作也就完成了，数据是可见的。

### 性能

Update 语句的性能和待更新的行数以及 condition 的检索效率密切相关。

- 待更新的行数：待更新的行数越多，Update 语句的速度就会越慢。这和导入的原理是一致的。 Doris 的更新比较合适偶发更新的场景，比如修改个别行的值。 Doris 并不适合大批量的修改数据。大批量修改会使得 Update 语句运行时间很久。
- condition 的检索效率：Doris 的 Update 实现原理是先将满足 condition 的行读取处理，所以如果 condition 的检索效率高，则 Update 的速度也会快。 condition 列最好能命中索引或者分区分桶裁剪，这样 Doris 就不需要扫全表，可以快速定位到需要更新的行，从而提升更新效率。 **强烈不推荐 condition 列中包含 UNIQUE 模型的 value 列**。

### 并发控制

默认情况下，并不允许同一时间对同一张表并发进行多个 Update 操作。

主要原因是，Doris 目前支持的是行更新，这意味着，即使用户声明的是 `SET v2 = 1`，实际上，其他所有的 Value 列也会被覆盖一遍（尽管值没有变化）。

这就会存在一个问题，如果同时有两个 Update 操作对同一行进行更新，那么其行为可能是不确定的，也就是可能存在脏数据。

但在实际应用中，如果用户自己可以保证即使并发更新，也不会同时对同一行进行操作的话，就可以手动打开并发限制。通过修改 FE 配置 `enable_concurrent_update`，当配置值为 true 时，则对更新并发无限制。
>注：开启配置后，会有一定的性能风险，可参考上面的性能小节部分，提升更新效率。

## 使用风险

由于 Doris 目前支持的是行更新，并且采用的是读取后再写入的两步操作，则如果 Update 语句和其他导入或 Delete 语句刚好修改的是同一行时，存在不确定的数据结果。

所以用户在使用的时候，一定要注意**用户侧自己**进行 Update 语句和其他 DML 语句的并发控制。

## 使用示例

假设 Doris 中存在一张订单表，其中 订单id 是 Key 列，订单状态，订单金额是 Value 列。数据状态如下：

| 订单id | 订单金额 | 订单状态 |
| ------ | -------- | -------- |
| 1      | 100      | 待付款   |

```sql
+----------+--------------+--------------+
| order_id | order_amount | order_status |
+----------+--------------+--------------+
| 1        |          100 | 待付款       |
+----------+--------------+--------------+
1 row in set (0.01 sec)
```

这时候，用户点击付款后，Doris 系统需要将订单id 为 '1' 的订单状态变更为 '待发货'，就需要用到 Update 功能。

```sql
mysql> UPDATE test_order SET order_status = '待发货' WHERE order_id = 1;
Query OK, 1 row affected (0.11 sec)
{'label':'update_20ae22daf0354fe0-b5aceeaaddc666c5', 'status':'VISIBLE', 'txnId':'33', 'queryId':'20ae22daf0354fe0-b5aceeaaddc666c5'}
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

用户执行 UPDATE 命令后，系统会进行如下三步：

- 第一步：读取满足 WHERE 订单id=1 的行 （1，100，'待付款'）

- 第二步：变更该行的订单状态，从'待付款'改为'待发货' （1，100，'待发货'）

-  第三步：将更新后的行再插入回表中，从而达到更新的效果。 

  |订单id | 订单金额| 订单状态| 
  |---|---|---| 
  | 1 | 100| 待付款 | 
  | 1 | 100 | 待发货 | 
  
由于表 test_order 是 UNIQUE 模型，所以相同 Key 的行，之后后者才会生效，所以最终效果如下： 
  
  |订单id | 订单金额| 订单状态| 
  |---|---|---| 
  | 1 | 100 | 待发货 |

## 更新Key列
目前Update操作只支持更新Value列，Key列的更新可参考[使用FlinkCDC更新Key列](../../ecosystem/flink-doris-connector.md#使用FlinkCDC更新Key列)

## 更多帮助

关于 **数据更新** 使用的更多详细语法，请参阅 [update](../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/UPDATE.md) 命令手册，也可以在Mysql客户端命令行下输入 `HELP UPDATE` 获取更多帮助信息。

