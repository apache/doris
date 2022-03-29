---
{
    "title": "Runtime Filter",
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

# Runtime Filter

Runtime Filter 是在 Doris 0.15 版本中正式加入的新功能。旨在为某些 Join 查询在运行时动态生成过滤条件，来减少扫描的数据量，避免不必要的I/O和网络传输，从而加速查询。

它的设计、实现和效果可以参阅 [ISSUE 6116](https://github.com/apache/incubator-doris/issues/6116)。

## 名词解释

* FE：Frontend，Doris 的前端节点。负责元数据管理和请求接入。
* BE：Backend，Doris 的后端节点。负责查询执行和数据存储。
* 左表：Join查询时，左边的表。进行Probe操作。可被Join Reorder调整顺序。
* 右表：Join查询时，右边的表。进行Build操作。可被Join Reorder调整顺序。
* Fragment：FE会将具体的SQL语句的执行转化为对应的Fragment并下发到BE进行执行。BE上执行对应Fragment，并将结果汇聚返回给FE。
* Join on clause: `A join B on A.a=B.b`中的`A.a=B.b`，在查询规划时基于此生成join conjuncts，包含join Build和Probe使用的expr，其中Build expr在Runtime Filter中称为src expr，Probe expr在Runtime Filter中称为target expr。

## 原理

Runtime Filter在查询规划时生成，在HashJoinNode中构建，在ScanNode中应用。

举个例子，当前存在T1表与T2表的Join查询，它的Join方式为HashJoin，T1是一张事实表，数据行数为100000，T2是一张维度表，数据行数为2000，Doris join的实际情况是:
```
|          >      HashJoinNode     <
|         |                         |
|         | 100000                  | 2000
|         |                         |
|   OlapScanNode              OlapScanNode
|         ^                         ^   
|         | 100000                  | 2000
|        T1                        T2
|
```
显而易见对T2扫描数据要远远快于T1，如果我们主动等待一段时间再扫描T1，等T2将扫描的数据记录交给HashJoinNode后，HashJoinNode根据T2的数据计算出一个过滤条件，比如T2数据的最大和最小值，或者构建一个Bloom Filter，接着将这个过滤条件发给等待扫描T1的ScanNode，后者应用这个过滤条件，将过滤后的数据交给HashJoinNode，从而减少probe hash table的次数和网络开销，这个过滤条件就是Runtime Filter，效果如下:
```
|          >      HashJoinNode     <
|         |                         |
|         | 6000                    | 2000
|         |                         |
|   OlapScanNode              OlapScanNode
|         ^                         ^   
|         | 100000                  | 2000
|        T1                        T2
|
```
如果能将过滤条件（Runtime Filter）下推到存储引擎，则某些情况下可以利用索引来直接减少扫描的数据量，从而大大减少扫描耗时，效果如下:
```
|          >      HashJoinNode     <
|         |                         |
|         | 6000                    | 2000
|         |                         |
|   OlapScanNode              OlapScanNode
|         ^                         ^   
|         | 6000                    | 2000
|        T1                        T2
|
```
可见，和谓词下推、分区裁剪不同，Runtime Filter是在运行时动态生成的过滤条件，即在查询运行时解析join on clause确定过滤表达式，并将表达式广播给正在读取左表的ScanNode，从而减少扫描的数据量，进而减少probe hash table的次数，避免不必要的I/O和网络传输。

Runtime Filter主要用于优化针对大表的join，如果左表的数据量太小，或者右表的数据量太大，则Runtime Filter可能不会取得预期效果。

## 使用方式

### Runtime Filter查询选项

与Runtime Filter相关的查询选项信息，请参阅以下部分:

- 第一个查询选项是调整使用的Runtime Filter类型，大多数情况下，您只需要调整这一个选项，其他选项保持默认即可。

  - `runtime_filter_type`: 包括Bloom Filter、MinMax Filter、IN predicate、IN Or Bloom Filter，默认会使用IN Or Bloom Filter，部分情况下同时使用Bloom Filter、MinMax Filter、IN predicate时性能更高。

- 其他查询选项通常仅在某些特定场景下，才需进一步调整以达到最优效果。通常只在性能测试后，针对资源密集型、运行耗时足够长且频率足够高的查询进行优化。

  - `runtime_filter_mode`: 用于调整Runtime Filter的下推策略，包括OFF、LOCAL、GLOBAL三种策略，默认设置为GLOBAL策略

  - `runtime_filter_wait_time_ms`: 左表的ScanNode等待每个Runtime Filter的时间，默认1000ms

  - `runtime_filters_max_num`: 每个查询可应用的Runtime Filter中Bloom Filter的最大数量，默认10

  - `runtime_bloom_filter_min_size`: Runtime Filter中Bloom Filter的最小长度，默认1048576（1M）

  - `runtime_bloom_filter_max_size`: Runtime Filter中Bloom Filter的最大长度，默认16777216（16M）

  - `runtime_bloom_filter_size`: Runtime Filter中Bloom Filter的默认长度，默认2097152（2M）

  - `runtime_filter_max_in_num`: 如果join右表数据行数大于这个值，我们将不生成IN predicate，默认1024

下面对查询选项做进一步说明。

#### 1.runtime_filter_type
使用的Runtime Filter类型。

**类型**: 数字(1, 2, 4, 8)或者相对应的助记符字符串(IN, BLOOM_FILTER, MIN_MAX, ```IN_OR_BLOOM_FILTER```)，默认8(```IN_OR_BLOOM_FILTER```)，使用多个时用逗号分隔，注意需要加引号，或者将任意多个类型的数字相加，例如:
```
set runtime_filter_type="BLOOM_FILTER,IN,MIN_MAX";
```
等价于:
```
set runtime_filter_type=7;
```

**使用注意事项**

- **IN or Bloom Filter**: 根据右表在执行过程中的真实行数，由系统自动判断使用 IN predicate 还是 Bloom Filter
  - 默认在右表数据行数少于1024时会使用IN predicate（可通过session变量中的`runtime_filter_max_in_num`调整，否则使用Bloom filter。
- **Bloom Filter**: 有一定的误判率，导致过滤的数据比预期少一点，但不会导致最终结果不准确，在大部分情况下Bloom Filter都可以提升性能或对性能没有显著影响，但在部分情况下会导致性能降低。
    - Bloom Filter构建和应用的开销较高，所以当过滤率较低时，或者左表数据量较少时，Bloom Filter可能会导致性能降低。
    - 目前只有左表的Key列应用Bloom Filter才能下推到存储引擎，而测试结果显示Bloom Filter不下推到存储引擎时往往会导致性能降低。
    - 目前Bloom Filter仅在ScanNode上使用表达式过滤时有短路(short-circuit)逻辑，即当假阳性率过高时，不继续使用Bloom Filter，但当Bloom Filter下推到存储引擎后没有短路逻辑，所以当过滤率较低时可能导致性能降低。

- **MinMax Filter**: 包含最大值和最小值，从而过滤小于最小值和大于最大值的数据，MinMax Filter的过滤效果与join on clause中Key列的类型和左右表数据分布有关。
    - 当join on clause中Key列的类型为int/bigint/double等时，极端情况下，如果左右表的最大最小值相同则没有效果，反之右表最大值小于左表最小值，或右表最小值大于左表最大值，则效果最好。
    - 当join on clause中Key列的类型为varchar等时，应用MinMax Filter往往会导致性能降低。

- **IN predicate**: 根据join on clause中Key列在右表上的所有值构建IN predicate，使用构建的IN predicate在左表上过滤，相比Bloom Filter构建和应用的开销更低，在右表数据量较少时往往性能更高。
    - 默认只有右表数据行数少于1024才会下推（可通过session变量中的`runtime_filter_max_in_num`调整）。
    - 目前IN predicate已实现合并方法。
    - 当同时指定In predicate和其他filter，并且in的过滤数值没达到runtime_filter_max_in_num时，会尝试把其他filter去除掉。原因是In predicate是精确的过滤条件，即使没有其他filter也可以高效过滤，如果同时使用则其他filter会做无用功。目前仅在Runtime filter的生产者和消费者处于同一个fragment时才会有去除非in filter的逻辑。

#### 2.runtime_filter_mode
用于控制Runtime Filter在instance之间传输的范围。

**类型**: 数字(0, 1, 2)或者相对应的助记符字符串(OFF, LOCAL, GLOBAL)，默认2(GLOBAL)。

**使用注意事项**

LOCAL：相对保守，构建的Runtime Filter只能在同一个instance（查询执行的最小单元）上同一个Fragment中使用，即Runtime Filter生产者（构建Filter的HashJoinNode）和消费者（使用RuntimeFilter的ScanNode）在同一个Fragment，比如broadcast join的一般场景；

GLOBAL：相对激进，除满足LOCAL策略的场景外，还可以将Runtime Filter合并后通过网络传输到不同instance上的不同Fragment中使用，比如Runtime Filter生产者和消费者在不同Fragment，比如shuffle join。

大多数情况下GLOBAL策略可以在更广泛的场景对查询进行优化，但在有些shuffle join中生成和合并Runtime Filter的开销超过给查询带来的性能优势，可以考虑更改为LOCAL策略。

如果集群中涉及的join查询不会因为Runtime Filter而提高性能，您可以将设置更改为OFF，从而完全关闭该功能。

在不同Fragment上构建和应用Runtime Filter时，需要合并Runtime Filter的原因和策略可参阅 [ISSUE 6116](https://github.com/apache/incubator-doris/issues/6116)

#### 3.runtime_filter_wait_time_ms
Runtime Filter的等待耗时。

**类型**: 整数，默认1000，单位ms

**使用注意事项**

在开启Runtime Filter后，左表的ScanNode会为每一个分配给自己的Runtime Filter等待一段时间再扫描数据，即如果ScanNode被分配了3个Runtime Filter，那么它最多会等待3000ms。

因为Runtime Filter的构建和合并均需要时间，ScanNode会尝试将等待时间内到达的Runtime Filter下推到存储引擎，如果超过等待时间后，ScanNode会使用已经到达的Runtime Filter直接开始扫描数据。

如果Runtime Filter在ScanNode开始扫描之后到达，则ScanNode不会将该Runtime Filter下推到存储引擎，而是对已经从存储引擎扫描上来的数据，在ScanNode上基于该Runtime Filter使用表达式过滤，之前已经扫描的数据则不会应用该Runtime Filter，这样得到的中间数据规模会大于最优解，但可以避免严重的裂化。

如果集群比较繁忙，并且集群上有许多资源密集型或长耗时的查询，可以考虑增加等待时间，以避免复杂查询错过优化机会。如果集群负载较轻，并且集群上有许多只需要几秒的小查询，可以考虑减少等待时间，以避免每个查询增加1s的延迟。

#### 4.runtime_filters_max_num
每个查询生成的Runtime Filter中Bloom Filter数量的上限。

**类型**: 整数，默认10

**使用注意事项**
目前仅对Bloom Filter的数量进行限制，因为相比MinMax Filter和IN predicate，Bloom Filter构建和应用的代价更高。

如果生成的Bloom Filter超过允许的最大数量，则保留选择性大的Bloom Filter，选择性大意味着预期可以过滤更多的行。这个设置可以防止Bloom Filter耗费过多的内存开销而导致潜在的问题。
```
选择性=(HashJoinNode Cardinality / HashJoinNode left child Cardinality)
-- 因为目前FE拿到Cardinality不准，所以这里Bloom Filter计算的选择性与实际不准，因此最终可能只是随机保留了部分Bloom Filter。
```
仅在对涉及大表间join的某些长耗时查询进行调优时，才需要调整此查询选项。

#### 5.Bloom Filter长度相关参数
包括`runtime_bloom_filter_min_size`、`runtime_bloom_filter_max_size`、`runtime_bloom_filter_size`，用于确定Runtime Filter使用的Bloom Filter数据结构的大小（以字节为单位）。

**类型**: 整数

**使用注意事项**
因为需要保证每个HashJoinNode构建的Bloom Filter长度相同才能合并，所以目前在FE查询规划时计算Bloom Filter的长度。

如果能拿到join右表统计信息中的数据行数(Cardinality)，会尝试根据Cardinality估计Bloom Filter的最佳大小，并四舍五入到最接近的2的幂(以2为底的log值)。如果无法拿到右表的Cardinality，则会使用默认的Bloom Filter长度`runtime_bloom_filter_size`。`runtime_bloom_filter_min_size`和`runtime_bloom_filter_max_size`用于限制最终使用的Bloom Filter长度最小和最大值。

更大的Bloom Filter在处理高基数的输入集时更有效，但需要消耗更多的内存。假如查询中需要过滤高基数列（比如含有数百万个不同的取值），可以考虑增加`runtime_bloom_filter_size`的值进行一些基准测试，这有助于使Bloom Filter过滤的更加精准，从而获得预期的性能提升。

Bloom Filter的有效性取决于查询的数据分布，因此通常仅对一些特定查询额外调整其Bloom Filter长度，而不是全局修改，一般仅在对涉及大表间join的某些长耗时查询进行调优时，才需要调整此查询选项。

### 查看query生成的Runtime Filter

`explain`命令可以显示的查询计划中包括每个Fragment使用的join on clause信息，以及Fragment生成和使用Runtime Filter的注释，从而确认是否将Runtime Filter应用到了期望的join on clause上。
- 生成Runtime Filter的Fragment包含的注释例如`runtime filters: filter_id[type] <- table.column`。
- 使用Runtime Filter的Fragment包含的注释例如`runtime filters: filter_id[type] -> table.column`。

下面例子中的查询使用了一个ID为RF000的Runtime Filter。
```
CREATE TABLE test (t1 INT) DISTRIBUTED BY HASH (t1) BUCKETS 2 PROPERTIES("replication_num" = "1");
INSERT INTO test VALUES (1), (2), (3), (4);

CREATE TABLE test2 (t2 INT) DISTRIBUTED BY HASH (t2) BUCKETS 2 PROPERTIES("replication_num" = "1");
INSERT INTO test2 VALUES (3), (4), (5);

EXPLAIN SELECT t1 FROM test JOIN test2 where test.t1 = test2.t2;
+-------------------------------------------------------------------+
| Explain String                                                    |
+-------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                   |
|  OUTPUT EXPRS:`t1`                                                |
|                                                                   |
|   4:EXCHANGE                                                      |
|                                                                   |
| PLAN FRAGMENT 1                                                   |
|  OUTPUT EXPRS:                                                    |
|   PARTITION: HASH_PARTITIONED: `default_cluster:ssb`.`test`.`t1`  |
|                                                                   |
|   2:HASH JOIN                                                     |
|   |  join op: INNER JOIN (BUCKET_SHUFFLE)                         |
|   |  equal join conjunct: `test`.`t1` = `test2`.`t2`              |
|   |  runtime filters: RF000[in] <- `test2`.`t2`                   |
|   |                                                               |
|   |----3:EXCHANGE                                                 |
|   |                                                               |
|   0:OlapScanNode                                                  |
|      TABLE: test                                                  |
|      runtime filters: RF000[in] -> `test`.`t1`                    |
|                                                                   |
| PLAN FRAGMENT 2                                                   |
|  OUTPUT EXPRS:                                                    |
|   PARTITION: HASH_PARTITIONED: `default_cluster:ssb`.`test2`.`t2` |
|                                                                   |
|   1:OlapScanNode                                                  |
|      TABLE: test2                                                 |
+-------------------------------------------------------------------+
-- 上面`runtime filters`的行显示了`PLAN FRAGMENT 1`的`2:HASH JOIN`生成了ID为RF000的IN predicate，
-- 其中`test2`.`t2`的key values仅在运行时可知，
-- 在`0:OlapScanNode`使用了该IN predicate用于在读取`test`.`t1`时过滤不必要的数据。

SELECT t1 FROM test JOIN test2 where test.t1 = test2.t2; 
-- 返回2行结果[3, 4];

-- 通过query的profile（set enable_profile=true;）可以查看查询内部工作的详细信息，
-- 包括每个Runtime Filter是否下推、等待耗时、以及OLAP_SCAN_NODE从prepare到接收到Runtime Filter的总时长。
RuntimeFilter:in:
    -  HasPushDownToEngine:  true
    -  AWaitTimeCost:  0ns
    -  EffectTimeCost:  2.76ms

-- 此外，在profile的OLAP_SCAN_NODE中还可以查看Runtime Filter下推后的过滤效果和耗时。
    -  RowsVectorPredFiltered:  9.320008M  (9320008)
    -  VectorPredEvalTime:  364.39ms
```

## Runtime Filter的规划规则
1. 只支持对join on clause中的等值条件生成Runtime Filter，不包括Null-safe条件，因为其可能会过滤掉join左表的null值。
2. 不支持将Runtime Filter下推到left outer、full outer、anti join的左表；
3. 不支持src expr或target expr是常量；
4. 不支持src expr和target expr相等；
5. 不支持src expr的类型等于`HLL`或者`BITMAP`；
6. 目前仅支持将Runtime Filter下推给OlapScanNode；
7. 不支持target expr包含NULL-checking表达式，比如`COALESCE/IFNULL/CASE`，因为当outer join上层其他join的join on clause包含NULL-checking表达式并生成Runtime Filter时，将这个Runtime Filter下推到outer join的左表时可能导致结果不正确；
8. 不支持target expr中的列（slot）无法在原始表中找到某个等价列；
9. 不支持列传导，这包含两种情况：
    - 一是例如join on clause包含A.k = B.k and B.k = C.k时，目前C.k只可以下推给B.k，而不可以下推给A.k；
    - 二是例如join on clause包含A.a + B.b = C.c，如果A.a可以列传导到B.a，即A.a和B.a是等价的列，那么可以用B.a替换A.a，然后可以尝试将Runtime Filter下推给B（如果A.a和B.a不是等价列，则不能下推给B，因为target expr必须与唯一一个join左表绑定）；
10. Target expr和src expr的类型必须相等，因为Bloom Filter基于hash，若类型不等则会尝试将target expr的类型转换为src expr的类型；
11. 不支持`PlanNode.Conjuncts`生成的Runtime Filter下推，与HashJoinNode的`eqJoinConjuncts`和`otherJoinConjuncts`不同，`PlanNode.Conjuncts`生成的Runtime Filter在测试中发现可能会导致错误的结果，例如`IN`子查询转换为join时，自动生成的join on clause将保存在`PlanNode.Conjuncts`中，此时应用Runtime Filter可能会导致结果缺少一些行。
