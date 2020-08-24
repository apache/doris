---
{
    "title": "Segment V2 升级手册",
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

# Segment V2 升级手册

## 背景

Doris 0.12 版本中实现了新的存储格式：Segment V2，引入词典压缩、bitmap索引、page cache等优化，能够提升系统性能。

0.12 版本会同时支持读写原有的 Segment V1（以下简称V1） 和新的 Segment V2（以下简称V2） 两种格式。如果原有数据想使用 V2 相关特性，需通过命令将 V1 转换成 V2 格式。

本文档主要介绍从 0.11 版本升级至 0.12 版本后，如何转换和使用 V2 格式。

V2 格式的表可以支持以下新的特性：

1. bitmap 索引
2. 内存表
3. page cache
4. 字典压缩
5. 延迟物化（Lazy Materialization）

**从 0.13 版本开始，新建表的默认存储格式将为 Segment V2**

## 集群升级

0.12 版本仅支持从 0.11 版本升级，不支持从 0.11 之前的版本升级。请先确保升级的前的 Doris 集群版本为 0.11。

0.12 版本有两个 V2 相关的重要参数：

* `default_rowset_type`：FE 一个全局变量（Global Variable）设置，默认为 "alpha"，即 V1 版本。
* `default_rowset_type`：BE 的一个配置项，默认为 "ALPHA"，即 V1 版本。

保持上述配置默认的话，按常规步骤对集群升级后，原有集群数据的存储格式不会变更，即依然为 V1 格式。如果对 V2 格式没有需求，则继续正常使用集群即可，无需做任何额外操作。所有原有数据、以及新导入的数据，都依然是 V1 版本。

## V2 格式转换

### 已有表数据转换成 V2

对于已有表数据的格式转换，Doris 提供两种方式：

1. 创建一个 V2 格式的特殊 Rollup

    该方式会针对指定表，创建一个 V2 格式的特殊 Rollup。创建完成后，新的 V2 格式的 Rollup 会和原有表格式数据并存。用户可以指定对 V2 格式的 Rollup 进行查询验证。
    
    该方式主要用于对 V2 格式的验证，因为不会修改原有表数据，因此可以安全的进行 V2 格式的数据验证，而不用担心表数据因格式转换而损坏。通常先使用这个方式对数据进行校验，之后再使用方法2对整个表进行数据格式转换。
    
    操作步骤如下：
    
    ```
    ## 创建 V2 格式的 Rollup

    ALTER TABLE table_name ADD ROLLUP table_name (columns) PROPERTIES ("storage_format" = "v2");
    ```

    其中， Rollup 的名称必须为表名。columns 字段可以任意填写，系统不会检查该字段的合法性。该语句会自动生成一个名为 `__V2_table_name` 的 Rollup，并且该 Rollup 列包含表的全部列。
    
    通过以下语句查看创建进度：
    
    ```
    SHOW ALTER TABLE ROLLUP;
    ```
    
    创建完成后，可以通过 `DESC table_name ALL;` 查看到名为 `__v2_table_name` 的 Rollup。
    
    之后，通过如下命令，切换到 V2 格式查询：

    ```
    set use_v2_rollup = true;
    select * from table_name limit 10;
    ```
    
    `use_V2_Rollup` 这个变量会强制查询名为 `__V2_table_name` 的 Rollup，并且不会考虑其他 Rollup 的命中条件。所以该参数仅用于对 V2 格式数据进行验证。

2. 转换现有表数据格式

    该方式相当于给指定的表发送一个 schema change 作业，作业完成后，表的所有数据会被转换成 V2 格式。该方法不会保留原有 v1 格式，所以请先使用方法1进行格式验证。
    
    ```
    ALTER TABLE table_name SET ("storage_format" = "v2");
    ```

    之后通过如下命令查看作业进度：
    
    ```
    SHOW ALTER TABLE COLUMN;
    ```
    
    作业完成后，该表的所有数据（包括Rollup）都转换为了 V2。且 V1 版本的数据已被删除。如果该表是分区表，则之后创建的分区也都是 V2 格式。
    
    **V2 格式的表不能重新转换为 V1**
    
### 创建新的 V2 格式的表

在不改变默认配置参数的情况下，用户可以创建 V2 格式的表：

```
CREATE TABLE tbl_name
(
    k1 INT,
    k2 INT
)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES
(
    "storage_format" = "v2"
);
```

在 `properties` 中指定 `"storage_format" = "v2"` 后，该表将使用 V2 格式创建。如果是分区表，则之后创建的分区也都是 V2 格式。

### 全量格式转换(试验功能，不推荐)

通过以下方式可以开启整个集群的全量数据格式转换（V1 -> V2）。全量数据转换是通过 BE 后台的数据 compaction 过程异步进行的。
**该功能目前并没有很好的方式查看或控制转换进度，并且无法保证数据能够转换完成。可能导致同一张表长期处于同时包含两种数据格式的状态。因此建议使用 ALTER TABLE 针对性的转换。**

1. 从 BE 开启全量格式转换

    在 `be.conf` 中添加变量 `default_rowset_type=BETA` 并重启 BE 节点。在之后的 compaction 流程中，数据会自动从 V1 转换成 V2。
    
2. 从 FE 开启全量格式转换

    通过 mysql 客户端连接 Doris 后，执行如下语句：
    
    `SET GLOBAL default_rowset_type = beta;`

    执行完成后，FE 会通过心跳将信息发送给 BE，之后 BE 的 compaction 流程中，数据会自动从 V1 转换成 V2。
    
    FE 的配置参数优先级高于 BE 的配置。即使 BE 中的配置 `default_rowset_type` 为 ALPHA，如果 FE 配置为 beta 后，则 BE 依然开始进行 V1 到 V2 的数据格式转换。

    **建议先通过对单独表的数据格式转换验证后，再进行全量转换。全量转换的时间比较长，且进度依赖于 compaction 的进度。**可能出现 compaction 无法完成的情况，因此需要通过显式的执行 `ALTER TABLE` 操作进行个别表的数据格式转换。

3. 查看全量转换进度

    全量转换进度须通过脚本查看。脚本位置为代码库的 `tools/show_segment_status/` 目录。请参阅其中的 `README` 文档查看使用帮助。
