---
{
    "title": "使用 HLL 近似去重",
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

## HLL 近似去重

在实际的业务场景中，随着业务数据量越来越大，对数据去重的压力也越来越大，当数据达到一定规模之后，使用精准去重的成本也越来越高，在业务可以接受的情况下，通过近似算法来实现快速去重降低计算压力是一个非常好的方式，本文主要介绍 Doris 提供的 HyperLogLog（简称 HLL）是一种近似去重算法。

HLL 的特点是具有非常优异的空间复杂度 O(mloglogn) , 时间复杂度为 O(n),  并且计算结果的误差可控制在 1%—2% 左右，误差与数据集大小以及所采用的哈希函数有关。

## 什么是 HyperLogLog

它是 LogLog 算法的升级版，作用是能够提供不精确的去重计数。其数学基础为**伯努利试验**。

假设硬币拥有正反两面，一次的上抛至落下，最终出现正反面的概率都是50%。一直抛硬币，直到它出现正面为止，我们记录为一次完整的试验。

那么对于多次的伯努利试验，假设这个多次为n次。就意味着出现了n次的正面。假设每次伯努利试验所经历了的抛掷次数为k。第一次伯努利试验，次数设为k1，以此类推，第n次对应的是kn。

其中，对于这n次伯努利试验中，必然会有一个最大的抛掷次数k，例如抛了12次才出现正面，那么称这个为k_max，代表抛了最多的次数。

伯努利试验容易得出有以下结论：

- n 次伯努利过程的投掷次数都不大于 k_max。
- n 次伯努利过程，至少有一次投掷次数等于 k_max

最终结合极大似然估算的方法，发现在n和k_max中存在估算关联：n = 2 ^ k_max。**当我们只记录了k_max时，即可估算总共有多少条数据，也就是基数。**

假设试验结果如下：

- 第1次试验: 抛了3次才出现正面，此时 k=3，n=1
- 第2次试验: 抛了2次才出现正面，此时 k=2，n=2
- 第3次试验: 抛了6次才出现正面，此时 k=6，n=3
- 第n次试验：抛了12次才出现正面，此时我们估算， n = 2^12

取上面例子中前三组试验，那么 k_max = 6，最终 n=3，我们放进估算公式中去，明显： 3 ≠ 2^6 。也即是说，当试验次数很小的时候，这种估算方法的误差是很大的。

这三组试验，我们称为一轮的估算。如果只是进行一轮的话，当 n 足够大的时候，估算的误差率会相对减少，但仍然不够小。

## Doris HLL 函数

HLL 是基于 HyperLogLog 算法的工程实现，用于保存 HyperLogLog 计算过程的中间结果，它只能作为表的 value 列类型、通过聚合来不断的减少数据量，以此

来实现加快查询的目的，基于它得到的是一个估算结果，误差大概在1%左右，hll 列是通过其它列或者导入数据里面的数据生成的，导入的时候通过 hll_hash 函数

来指定数据中哪一列用于生成 hll 列，它常用于替代 count distinct，通过结合 rollup 在业务上用于快速计算uv等

**HLL_UNION_AGG(hll)**

此函数为聚合函数，用于计算满足条件的所有数据的基数估算。

**HLL_CARDINALITY(hll)**

此函数用于计算单条hll列的基数估算

**HLL_HASH(column_name)**

生成HLL列类型，用于insert或导入的时候，导入的使用见相关说明

## 如何使用 Doris HLL

1. 使用 HLL 去重的时候，需要在建表语句中将目标列类型设置成HLL，聚合函数设置成HLL_UNION
2. HLL类型的列不能作为 Key 列使用
3. 用户不需要指定长度及默认值，长度根据数据聚合程度系统内控制

### 创建一张含有 hll 列的表

```sql
create table test_hll(
	dt date,
	id int,
	name char(10),
	province char(10),
	os char(10),
	pv hll hll_union
)
Aggregate KEY (dt,id,name,province,os)
distributed by hash(id) buckets 10
PROPERTIES(
	"replication_num" = "1",
	"in_memory"="false"
);
```

### 导入数据

1. Stream load 导入

   ```
   curl --location-trusted -u root: -H "label:label_test_hll_load" \
       -H "column_separator:," \
       -H "columns:dt,id,name,province,os, pv=hll_hash(id)" -T test_hll.csv http://fe_IP:8030/api/demo/test_hll/_stream_load
   ```

   示例数据如下（test_hll.csv）：

   ```
   2022-05-05,10001,测试01,北京,windows
   2022-05-05,10002,测试01,北京,linux
   2022-05-05,10003,测试01,北京,macos
   2022-05-05,10004,测试01,河北,windows
   2022-05-06,10001,测试01,上海,windows
   2022-05-06,10002,测试01,上海,linux
   2022-05-06,10003,测试01,江苏,macos
   2022-05-06,10004,测试01,陕西,windows
   ```

   导入结果如下

   ```
   # curl --location-trusted -u root: -H "label:label_test_hll_load"     -H "column_separator:,"     -H "columns:dt,id,name,province,os, pv=hll_hash(id)" -T test_hll.csv http://127.0.0.1:8030/api/demo/test_hll/_stream_load
   
   {
       "TxnId": 693,
       "Label": "label_test_hll_load",
       "TwoPhaseCommit": "false",
       "Status": "Success",
       "Message": "OK",
       "NumberTotalRows": 8,
       "NumberLoadedRows": 8,
       "NumberFilteredRows": 0,
       "NumberUnselectedRows": 0,
       "LoadBytes": 320,
       "LoadTimeMs": 23,
       "BeginTxnTimeMs": 0,
       "StreamLoadPutTimeMs": 1,
       "ReadDataTimeMs": 0,
       "WriteDataTimeMs": 9,
       "CommitAndPublishTimeMs": 11
   }
   ```

2. Broker Load

```
LOAD LABEL demo.test_hlllabel
 (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/doris_test_hll/data/input/file")
    INTO TABLE `test_hll`
    COLUMNS TERMINATED BY ","
    (dt,id,name,province,os)
    SET (
      pv = HLL_HASH(id)
    )
 );
```

## 查询数据

HLL 列不允许直接查询原始值，只能通过 HLL 的聚合函数进行查询。

1. 求总的PV

   ```sql
   mysql> select HLL_UNION_AGG(pv) from test_hll;
   +---------------------+
   | hll_union_agg(`pv`) |
   +---------------------+
   |                   4 |
   +---------------------+
   1 row in set (0.00 sec)
   ```

   等价于：

   ```sql
   mysql> SELECT COUNT(DISTINCT pv) FROM test_hll;
   +----------------------+
   | count(DISTINCT `pv`) |
   +----------------------+
   |                    4 |
   +----------------------+
   1 row in set (0.01 sec)
   ```

2. 求每一天的PV

   ```sql
   mysql> select HLL_UNION_AGG(pv) from test_hll group by dt;
   +---------------------+
   | hll_union_agg(`pv`) |
   +---------------------+
   |                   4 |
   |                   4 |
   +---------------------+
   2 rows in set (0.01 sec)
   ```