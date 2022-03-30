---
{
    "title": "最佳实践",
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

# 最佳实践

## 1 建表

### 1.1 数据模型选择

Doris 数据模型上目前分为三类: AGGREGATE KEY, UNIQUE KEY, DUPLICATE KEY。三种模型中数据都是按KEY进行排序。

1.1.1 AGGREGATE KEY

    AGGREGATE KEY相同时，新旧记录进行聚合，目前支持的聚合函数有SUM, MIN, MAX, REPLACE。
    
    AGGREGATE KEY模型可以提前聚合数据, 适合报表和多维分析业务。
    
    ```
    CREATE TABLE site_visit
    (
        siteid      INT,
        city        SMALLINT,
        username    VARCHAR(32),
        pv BIGINT   SUM DEFAULT '0'
    )
    AGGREGATE KEY(siteid, city, username)
    DISTRIBUTED BY HASH(siteid) BUCKETS 10;
    ```
    
1.1.2. UNIQUE KEY

    UNIQUE KEY 相同时，新记录覆盖旧记录。目前 UNIQUE KEY 实现上和 AGGREGATE KEY 的 REPLACE 聚合方法一样，二者本质上相同。适用于有更新需求的分析业务。
    
    ```
    CREATE TABLE sales_order
    (
        orderid     BIGINT,
        status      TINYINT,
        username    VARCHAR(32),
        amount      BIGINT DEFAULT '0'
    )
    UNIQUE KEY(orderid)
    DISTRIBUTED BY HASH(orderid) BUCKETS 10;
    ```
    
1.1.3. DUPLICATE KEY

    只指定排序列，相同的行不会合并。适用于数据无需提前聚合的分析业务。
    
    ```
    CREATE TABLE session_data
    (
        visitorid   SMALLINT,
        sessionid   BIGINT,
        visittime   DATETIME,
        city        CHAR(20),
        province    CHAR(20),
        ip          varchar(32),
        brower      CHAR(20),
        url         VARCHAR(1024)
    )
    DUPLICATE KEY(visitorid, sessionid)
    DISTRIBUTED BY HASH(sessionid, visitorid) BUCKETS 10;
    ```

### 1.2 大宽表与 Star Schema

业务方建表时, 为了和前端业务适配, 往往不对维度信息和指标信息加以区分, 而将 Schema 定义成大宽表。对于 Doris 而言, 这类大宽表往往性能不尽如人意: 

* Schema 中字段数比较多, 聚合模型中可能 key 列比较多, 导入过程中需要排序的列会增加。
* 维度信息更新会反应到整张表中，而更新的频率直接影响查询的效率。

使用过程中，建议用户尽量使用 Star Schema 区分维度表和指标表。频繁更新的维度表也可以放在 MySQL 外部表中。而如果只有少量更新, 可以直接放在 Doris 中。在 Doris 中存储维度表时，可对维度表设置更多的副本，提升 Join 的性能。
 
### 1.3 分区和分桶

Doris 支持两级分区存储, 第一层为分区(partition)，目前支持 RANGE 分区和 LIST 分区两种类型, 第二层为 HASH 分桶(bucket)。

1.3.1. 分区(partition)

    分区用于将数据划分成不同区间, 逻辑上可以理解为将原始表划分成了多个子表。可以方便的按分区对数据进行管理，例如，删除数据时，更加迅速。

1.3.1.1. RANGE分区

    业务上，多数用户会选择采用按时间进行partition, 让时间进行partition有以下好处：
    
    * 可区分冷热数据
    * 可用上Doris分级存储(SSD + SATA)的功能

1.3.1.2. LIST分区

    业务上，用户可以选择城市或者其他枚举值进行partition。


1.3.2. HASH分桶(bucket)

    根据hash值将数据划分成不同的 bucket。
    
    * 建议采用区分度大的列做分桶, 避免出现数据倾斜
    * 为方便数据恢复, 建议单个 bucket 的 size 不要太大, 保持在 10GB 以内, 所以建表或增加 partition 时请合理考虑 bucket 数目, 其中不同 partition 可指定不同的 buckets 数。

### 1.4 稀疏索引和 Bloom Filter

Doris对数据进行有序存储, 在数据有序的基础上为其建立稀疏索引,索引粒度为 block(1024行)。

稀疏索引选取 schema 中固定长度的前缀作为索引内容, 目前 Doris 选取 36 个字节的前缀作为索引。

* 建表时建议将查询中常见的过滤字段放在 Schema 的前面, 区分度越大，频次越高的查询字段越往前放。
* 这其中有一个特殊的地方,就是 varchar 类型的字段。varchar 类型字段只能作为稀疏索引的最后一个字段。索引会在 varchar 处截断, 因此 varchar 如果出现在前面，可能索引的长度可能不足 36 个字节。具体可以参阅 [数据模型、ROLLUP 及前缀索引](./data-model-rollup.md)。
* 除稀疏索引之外, Doris还提供bloomfilter索引, bloomfilter索引对区分度比较大的列过滤效果明显。 如果考虑到varchar不能放在稀疏索引中, 可以建立bloomfilter索引。

### 1.5 物化视图(rollup)

Rollup 本质上可以理解为原始表(Base Table)的一个物化索引。建立 Rollup 时可只选取 Base Table 中的部分列作为 Schema。Schema 中的字段顺序也可与 Base Table 不同。

下列情形可以考虑建立 Rollup：

1.5.1. Base Table 中数据聚合度不高。

这一般是因 Base Table 有区分度比较大的字段而导致。此时可以考虑选取部分列，建立 Rollup。
    
如对于 `site_visit` 表：

```
site_visit(siteid, city, username, pv)
```

siteid 可能导致数据聚合度不高，如果业务方经常根据城市统计pv需求，可以建立一个只有 city, pv 的 Rollup：

```
ALTER TABLE site_visit ADD ROLLUP rollup_city(city, pv);
```
    
1.5.2. Base Table 中的前缀索引无法命中

这一般是 Base Table 的建表方式无法覆盖所有的查询模式。此时可以考虑调整列顺序，建立 Rollup。

如对于 session_data 表：

```
session_data(visitorid, sessionid, visittime, city, province, ip, brower, url)
```

如果除了通过 visitorid 分析访问情况外，还有通过 brower, province 分析的情形，可以单独建立 Rollup。

```
ALTER TABLE session_data ADD ROLLUP rollup_brower(brower,province,ip,url) DUPLICATE KEY(brower,province);
```

## 2 Schema Change

Doris中目前进行 Schema Change 的方式有三种：Sorted Schema Change，Direct Schema Change, Linked Schema Change。

2.1. Sorted Schema Change

    改变了列的排序方式，需对数据进行重新排序。例如删除排序列中的一列, 字段重排序。
    
    ```
    ALTER TABLE site_visit DROP COLUMN city;
    ```
    
2.2. Direct Schema Change: 无需重新排序，但是需要对数据做一次转换。例如修改列的类型，在稀疏索引中加一列等。

    ```
    ALTER TABLE site_visit MODIFY COLUMN username varchar(64);
    ```
    
2.3. Linked Schema Change: 无需转换数据，直接完成。例如加列操作。
    
    ```
    ALTER TABLE site_visit ADD COLUMN click bigint SUM default '0';
    ```
    
建表时建议考虑好 Schema，这样在进行 Schema Change 时可以加快速度。
