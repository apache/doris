---
{
    "title": "导入严格模式",
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


# 导入严格模式

严格模式（strict_mode）为导入操作中的一个参数配置。该参数会影响某些数值的导入行为和最终导入的数据。

本文档主要说明如何设置严格模式，以及严格模式产生的影响。

## 如何设置

严格模式默认情况下都为 False，即关闭状态。

不同的导入方式设置严格模式的方式不尽相同。

1. [BROKER LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.md)

   ```sql
   LOAD LABEL example_db.label1
   (
       DATA INFILE("bos://my_bucket/input/file.txt")
       INTO TABLE `my_table`
       COLUMNS TERMINATED BY ","
   )
   WITH BROKER bos
   (
       "bos_endpoint" = "http://bj.bcebos.com",
       "bos_accesskey" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
       "bos_secret_accesskey"="yyyyyyyyyyyyyyyyyyyyyyyyyy"
   )
   PROPERTIES
   (
       "strict_mode" = "true"
   )
   ```

2. [STREAM LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD.md)

   ```bash
   curl --location-trusted -u user:passwd \
   -H "strict_mode: true" \
   -T 1.txt \
   http://host:port/api/example_db/my_table/_stream_load
   ```

3. [ROUTINE LOAD](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Load/CREATE-ROUTINE-LOAD.md)

   ```sql
   CREATE ROUTINE LOAD example_db.test_job ON my_table
   PROPERTIES
   (
       "strict_mode" = "true"
   ) 
   FROM KAFKA
   (
       "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
       "kafka_topic" = "my_topic"
   );
   ```

4. [INSERT](../../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/INSERT.md)

   通过[会话变量](../../../advanced/variables.md)设置：

   ```sql
   SET enable_insert_strict = true;
   INSERT INTO my_table ...;
   ```

## 严格模式的作用

严格模式的意思是，对于导入过程中的列类型转换进行严格过滤。

严格过滤的策略如下：

对于列类型转换来说，如果开启严格模式，则错误的数据将被过滤。这里的错误数据是指：原始数据并不为 `null`，而在进行列类型转换后结果为 `null` 的这一类数据。

这里说指的 `列类型转换`，并不包括用函数计算得出的 `null` 值。

对于导入的某列类型包含范围限制的，如果原始数据能正常通过类型转换，但无法通过范围限制的，严格模式对其也不产生影响。例如：如果类型是 `decimal(1,0)`, 原始数据为 10，则属于可以通过类型转换但不在列声明的范围内。这种数据 strict 对其不产生影响。

1. 以列类型为 TinyInt 来举例：

   | 原始数据类型 | 原始数据举例  | 转换为 TinyInt 后的值 | 严格模式   | 结果             |
   | ------------ | ------------- | --------------------- | ---------- | ---------------- |
   | 空值         | \N            | NULL                  | 开启或关闭 | NULL             |
   | 非空值       | "abc" or 2000 | NULL                  | 开启       | 非法值（被过滤） |
   | 非空值       | "abc"         | NULL                  | 关闭       | NULL             |
   | 非空值       | 1             | 1                     | 开启或关闭 | 正确导入         |

   > 说明：
   >
   > 1. 表中的列允许导入空值
   > 2. `abc` 及 `2000` 在转换为 TinyInt 后，会因类型或精度问题变为 NULL。在严格模式开启的情况下，这类数据将会被过滤。而如果是关闭状态，则会导入 `null`。

2. 以列类型为 Decimal(1,0) 举例

   | 原始数据类型 | 原始数据举例 | 转换为 Decimal 后的值 | 严格模式   | 结果             |
   | ------------ | ------------ | --------------------- | ---------- | ---------------- |
   | 空值         | \N           | null                  | 开启或关闭 | NULL             |
   | 非空值       | aaa          | NULL                  | 开启       | 非法值（被过滤） |
   | 非空值       | aaa          | NULL                  | 关闭       | NULL             |
   | 非空值       | 1 or 10      | 1 or 10               | 开启或关闭 | 正确导入         |

   > 说明：
   >
   > 1. 表中的列允许导入空值
   > 2. `abc` 在转换为 Decimal 后，会因类型问题变为 NULL。在严格模式开启的情况下，这类数据将会被过滤。而如果是关闭状态，则会导入 `null`。
   > 3. `10` 虽然是一个超过范围的值，但是因为其类型符合 decimal 的要求，所以严格模式对其不产生影响。`10` 最后会在其他导入处理流程中被过滤。但不会被严格模式过滤。
