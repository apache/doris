---
{
    "title": "Hive Bitmap UDF",
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

# Hive UDF

 Hive Bitmap UDF 提供了在 hive 表中生成 bitmap 、bitmap 运算等 UDF，Hive 中的 bitmap 与 Doris bitmap 完全一致 ，Hive 中的 bitmap 可以通过 spark bitmap load 导入 doris

 主要目的：
  1. 减少数据导入 doris 时间 , 除去了构建字典、bitmap 预聚合等流程；
  2. 节省 hive 存储 ，使用 bitmap 对数据压缩 ，减少了存储成本；
  3. 提供在 hive 中 bitmap 的灵活运算 ，比如：交集、并集、差集运算 ，计算后的 bitmap 也可以直接导入 doris；

## 使用方法

### 在 Hive 中创建 Bitmap 类型表

```sql

-- 例子：创建 Hive Bitmap 表
CREATE TABLE IF NOT EXISTS `hive_bitmap_table`(
  `k1`   int       COMMENT '',
  `k2`   String    COMMENT '',
  `k3`   String    COMMENT '',
  `uuid` binary    COMMENT 'bitmap'
) comment  'comment'

-- 例子：创建普通 Hive 表
CREATE TABLE IF NOT EXISTS `hive_table`(
    `k1`   int       COMMENT '',
    `k2`   String    COMMENT '',
    `k3`   String    COMMENT '',
    `uuid` int       COMMENT ''
) comment  'comment'
```

### Hive Bitmap UDF 使用：

Hive Bitmap UDF 需要在 Hive/Spark 中使用，首先需要编译fe得到hive-udf-jar-with-dependencies.jar。
编译准备工作：如果进行过ldb源码编译可直接编译fe，如果没有进行过ldb源码编译，则需要手动安装thrift，可参考：[FE开发环境搭建](/community/developer-guide/fe-idea-dev.md) 中的编译与安装

```sql
--clone doris源码
git clone https://github.com/apache/doris.git
cd doris
git submodule update --init --recursive
--安装thrift
--进入fe目录
cd fe
--执行maven打包命令（fe的子module会全部打包）
mvn package -Dmaven.test.skip=true
--也可以只打hive-udf module
mvn package -pl hive-udf -am -Dmaven.test.skip=true
```
打包编译完成进入hive-udf目录会有target目录，里面就会有打包完成的hive-udf.jar包

```sql

-- 加载hive bitmap udf jar包  (需要将编译好的 hive-udf jar 包上传至 HDFS)
add jar hdfs://node:9001/hive-udf-jar-with-dependencies.jar;

-- 创建UDAF函数
create temporary function to_bitmap as 'org.apache.doris.udf.ToBitmapUDAF' USING JAR 'hdfs://node:9001/hive-udf-jar-with-dependencies.jar';
create temporary function bitmap_union as 'org.apache.doris.udf.BitmapUnionUDAF' USING JAR 'hdfs://node:9001/hive-udf-jar-with-dependencies.jar';

-- 创建UDF函数
create temporary function bitmap_count as 'org.apache.doris.udf.BitmapCountUDF' USING JAR 'hdfs://node:9001/hive-udf-jar-with-dependencies.jar';
create temporary function bitmap_and as 'org.apache.doris.udf.BitmapAndUDF' USING JAR 'hdfs://node:9001/hive-udf-jar-with-dependencies.jar';
create temporary function bitmap_or as 'org.apache.doris.udf.BitmapOrUDF' USING JAR 'hdfs://node:9001/hive-udf-jar-with-dependencies.jar';
create temporary function bitmap_xor as 'org.apache.doris.udf.BitmapXorUDF' USING JAR 'hdfs://node:9001/hive-udf-jar-with-dependencies.jar';

-- 例子：通过 to_bitmap 生成 bitmap 写入 Hive Bitmap 表
insert into hive_bitmap_table
select 
    k1,
    k2,
    k3,
    to_bitmap(uuid) as uuid
from 
    hive_table
group by 
    k1,
    k2,
    k3

-- 例子：bitmap_count 计算 bitmap 中元素个数
select k1,k2,k3,bitmap_count(uuid) from hive_bitmap_table

-- 例子：bitmap_union 用于计算分组后的 bitmap 并集
select k1,bitmap_union(uuid) from hive_bitmap_table group by k1

```

###  Hive Bitmap UDF  说明

## Hive bitmap 导入 doris

<version since="2.0.2">

### 方法一：Catalog （推荐）

</version>

创建 Hive 表指定为 TEXT 格式，此时，对于 Binary 类型，Hive 会以 bash64 编码的字符串形式保存，此时可以通过 Hive Catalog 的形式，直接将位图数据通过 bitmap_from_bash64 函数插入到 Doris 内部。

以下是一个完整的例子：

1. 在 Hive 中创建 Hive 表

```sql
CREATE TABLE IF NOT EXISTS `test`.`hive_bitmap_table`(
`k1`   int       COMMENT '',
`k2`   String    COMMENT '',
`k3`   String    COMMENT '',
`uuid` binary    COMMENT 'bitmap'
) stored as textfile 
```

2. [在 Doris 中创建 Catalog](../lakehouse/multi-catalog/hive)

```sql
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://127.0.0.1:9083'
);
```

3. 创建 Doris 内表

```sql
CREATE TABLE IF NOT EXISTS `test`.`doris_bitmap_table`(
    `k1`   int                   COMMENT '',
    `k2`   String                COMMENT '',
    `k3`   String                COMMENT '',
    `uuid` BITMAP  BITMAP_UNION  COMMENT 'bitmap'
)
AGGREGATE KEY(k1, k2, k3)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);
```

4. 从 Hive 插入数据到 Doris 中

```sql
insert into doris_bitmap_table select k1, k2, k3, bitmap_from_base64(uuid) from hive.test.hive_bitmap_table;
```

### 方法二：Spark Load

 详见: [Spark Load](../data-operate/import/import-way/spark-load-manual.md) -> 基本操作  -> 创建导入 (示例3：上游数据源是hive binary类型情况)
