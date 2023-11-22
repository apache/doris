---
{
    "title": "倒排索引",
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

# [Experimental] 倒排索引

<version since="2.0.0">
 
</version>

从2.0.0版本开始，Doris支持倒排索引，可以用来进行文本类型的全文检索、普通数值日期类型的等值范围查询，快速从海量数据中过滤出满足条件的行。本文档主要介绍如何倒排索引的创建、删除、查询等使用方式。


## 名词解释

- [inverted index](https://en.wikipedia.org/wiki/Inverted_index)：[倒排索引](https://zh.wikipedia.org/wiki/%E5%80%92%E6%8E%92%E7%B4%A2%E5%BC%95)，是信息检索领域常用的索引技术，将文本分割成一个个词，构建 词 -> 文档编号 的索引，可以快速查找一个词在哪些文档出现。


## 原理介绍

在Doris的倒排索引实现中，table的一行对应一个文档、一列对应文档中的一个字段，因此利用倒排索引可以根据关键词快速定位包含它的行，达到WHERE子句加速的目的。

与Doris中其他索引不同的是，在存储层倒排索引使用独立的文件，跟segment文件有逻辑对应关系、但存储的文件相互独立。这样的好处是可以做到创建、删除索引不用重写tablet和segment文件，大幅降低处理开销。


## 功能介绍

Doris倒排索引的功能简要介绍如下：

- 增加了字符串类型的全文检索
  - 支持字符串全文检索，包括同时匹配多个关键字MATCH_ALL、匹配任意一个关键字MATCH_ANY、匹配短语词组MATCH_PHRASE
  - 支持字符串数组类型的全文检索
  - 支持英文、中文以及Unicode多语言分词
- 加速普通等值、范围查询，覆盖bitmap索引的功能，未来会代替bitmap索引
  - 支持字符串、数值、日期时间类型的 =, !=, >, >=, <, <= 快速过滤
  - 支持字符串、数字、日期时间数组类型的 =, !=, >, >=, <, <=
- 支持完善的逻辑组合
  - 新增索引对OR NOT逻辑的下推
  - 支持多个条件的任意AND OR NOT组合
- 灵活、快速的索引管理
  - 支持在创建表上定义倒排索引
  - 支持在已有的表上增加倒排索引，而且支持增量构建倒排索引，无需重写表中的已有数据
  - 支持删除已有表上的倒排索引，无需重写表中的已有数据

## 语法

- 建表时定义倒排索引，语法说明如下
  - USING INVERTED 是必须的，用于指定索引类型是倒排索引
  - PROPERTIES 是可选的，用于指定倒排索引的额外属性，目前有三个属性
    - parser指定分词器
      - 默认不指定代表不分词
      - english是英文分词，适合被索引列是英文的情况，用空格和标点符号分词，性能高
      - chinese是中文分词，适合被索引列主要是中文的情况，性能比english分词低
      - unicode是多语言混合类型分词，适用于中英文混合、多语言混合的情况。它能够对邮箱前缀和后缀、IP地址以及字符数字混合进行分词，并且可以对中文按字符分词。
    - parser_mode用于指定分词的模式，目前parser = chinese时支持如下几种模式：
      - fine_grained：细粒度模式，倾向于分出比较短的词，比如 '武汉市长江大桥' 会分成 '武汉', '武汉市', '市长', '长江', '长江大桥', '大桥' 6个词
      - coarse_grained：粗粒度模式，倾向于分出比较长的词，，比如 '武汉市长江大桥' 会分成 '武汉市' '长江大桥' 2个词
      - 默认coarse_grained
    - support_phrase用于指定索引是否支持MATCH_PHRASE短语查询加速
      - true为支持，但是索引需要更多的存储空间
      - false为不支持，更省存储空间，可以用MATCH_ALL查询多个关键字
      - 默认false
    - char_filter：功能主要在分词前对字符串提前处理
      - char_filter_type：指定使用不同功能的char_filter（目前仅支持char_replace）
        - char_replace 将pattern中每个char替换为一个replacement中的char
          - char_filter_pattern：需要被替换掉的字符数组
          - char_filter_replacement：替换后的字符数组，可以不用配置，默认为一个空格字符
  - COMMENT 是可选的，用于指定注释

```sql
CREATE TABLE table_name
(
  columns_difinition,
  INDEX idx_name1(column_name1) USING INVERTED [PROPERTIES("parser" = "english|unicode|chinese")] [COMMENT 'your comment']
  INDEX idx_name2(column_name2) USING INVERTED [PROPERTIES("parser" = "english|unicode|chinese")] [COMMENT 'your comment']
  INDEX idx_name3(column_name3) USING INVERTED [PROPERTIES("parser" = "chinese", "parser_mode" = "fine_grained|coarse_grained")] [COMMENT 'your comment']
  INDEX idx_name4(column_name4) USING INVERTED [PROPERTIES("parser" = "english|unicode|chinese", "support_phrase" = "true|false")] [COMMENT 'your comment']
  INDEX idx_name5(column_name4) USING INVERTED [PROPERTIES("char_filter_type" = "char_replace", "char_filter_pattern" = "._"), "char_filter_replacement" = " "] [COMMENT 'your comment']
  INDEX idx_name5(column_name4) USING INVERTED [PROPERTIES("char_filter_type" = "char_replace", "char_filter_pattern" = "._")] [COMMENT 'your comment']
)
table_properties;
```

:::tip

倒排索引在不同数据模型中有不同的使用限制：
- Aggregate 模型：只能为 Key 列建立倒排索引。
- Unique 模型：需要开启 merge on write 特性，开启后，可以为任意列建立倒排索引。
- Duplicate 模型：可以为任意列建立倒排索引。

:::

- 已有表增加倒排索引

**2.0-beta版本之前：**
```sql
-- 语法1
CREATE INDEX idx_name ON table_name(column_name) USING INVERTED [PROPERTIES("parser" = "english|unicode|chinese")] [COMMENT 'your comment'];
-- 语法2
ALTER TABLE table_name ADD INDEX idx_name(column_name) USING INVERTED [PROPERTIES("parser" = "english|unicode|chinese")] [COMMENT 'your comment'];
```

**2.0-beta版本（含2.0-beta）之后：**

上述`create/add index`操作只对增量数据生成倒排索引，增加了BUILD INDEX的语法用于对存量数据加倒排索引：
```sql
-- 语法1，默认给全表的存量数据加上倒排索引
BUILD INDEX index_name ON table_name;
-- 语法2，可指定partition，可指定一个或多个
BUILD INDEX index_name ON table_name PARTITIONS(partition_name1, partition_name2);
```
(**在执行BUILD INDEX之前需要已经执行了以上`create/add index`的操作**)

查看`BUILD INDEX`进展，可通过以下语句进行查看：
```sql
SHOW BUILD INDEX [FROM db_name];
-- 示例1，查看所有的BUILD INDEX任务进展
SHOW BUILD INDEX;
-- 示例2，查看指定table的BUILD INDEX任务进展
SHOW BUILD INDEX where TableName = "table1";
```

取消 `BUILD INDEX`, 可通过以下语句进行
```sql
CANCEL BUILD INDEX ON table_name;
CANCEL BUILD INDEX ON table_name (job_id1,jobid_2,...);
```

- 删除倒排索引
```sql
-- 语法1
DROP INDEX idx_name ON table_name;
-- 语法2
ALTER TABLE table_name DROP INDEX idx_name;
```

- 利用倒排索引加速查询
```sql
-- 1. 全文检索关键词匹配，通过MATCH_ANY MATCH_ALL完成
SELECT * FROM table_name WHERE column_name MATCH_ANY | MATCH_ALL 'keyword1 ...';

-- 1.1 logmsg中包含keyword1的行
SELECT * FROM table_name WHERE logmsg MATCH_ANY 'keyword1';

-- 1.2 logmsg中包含keyword1或者keyword2的行，后面还可以添加多个keyword
SELECT * FROM table_name WHERE logmsg MATCH_ANY 'keyword1 keyword2';

-- 1.3 logmsg中同时包含keyword1和keyword2的行，后面还可以添加多个keyword
SELECT * FROM table_name WHERE logmsg MATCH_ALL 'keyword1 keyword2';

-- 1.4 logmsg中同时包含keyword1和keyword2的行，并且按照keyword1在前，keyword2在后的顺序
SELECT * FROM table_name WHERE logmsg MATCH_PHRASE 'keyword1 keyword2';


-- 2. 普通等值、范围、IN、NOT IN，正常的SQL语句即可，例如
SELECT * FROM table_name WHERE id = 123;
SELECT * FROM table_name WHERE ts > '2023-01-01 00:00:00';
SELECT * FROM table_name WHERE op_type IN ('add', 'delete');
```

- 分词函数

如果想检查分词实际效果或者对一段文本进行分词的话，可以使用tokenize函数
```sql
mysql> SELECT TOKENIZE('武汉长江大桥','"parser"="chinese","parser_mode"="fine_grained"');
+-----------------------------------------------------------------------------------+
| tokenize('武汉长江大桥', '"parser"="chinese","parser_mode"="fine_grained"')       |
+-----------------------------------------------------------------------------------+
| ["武汉", "武汉长江大桥", "长江", "长江大桥", "大桥"]                              |
+-----------------------------------------------------------------------------------+
1 row in set (0.02 sec)

mysql> SELECT TOKENIZE('武汉市长江大桥','"parser"="chinese","parser_mode"="fine_grained"');
+--------------------------------------------------------------------------------------+
| tokenize('武汉市长江大桥', '"parser"="chinese","parser_mode"="fine_grained"')        |
+--------------------------------------------------------------------------------------+
| ["武汉", "武汉市", "市长", "长江", "长江大桥", "大桥"]                               |
+--------------------------------------------------------------------------------------+
1 row in set (0.02 sec)

mysql> SELECT TOKENIZE('武汉市长江大桥','"parser"="chinese","parser_mode"="coarse_grained"');
+----------------------------------------------------------------------------------------+
| tokenize('武汉市长江大桥', '"parser"="chinese","parser_mode"="coarse_grained"')        |
+----------------------------------------------------------------------------------------+
| ["武汉市", "长江大桥"]                                                                 |
+----------------------------------------------------------------------------------------+
1 row in set (0.02 sec)

mysql> SELECT TOKENIZE('I love CHINA','"parser"="english"');
+------------------------------------------------+
| tokenize('I love CHINA', '"parser"="english"') |
+------------------------------------------------+
| ["i", "love", "china"]                         |
+------------------------------------------------+
1 row in set (0.02 sec)

mysql> SELECT TOKENIZE('I love CHINA 我爱我的祖国','"parser"="unicode"');
+-------------------------------------------------------------------+
| tokenize('I love CHINA 我爱我的祖国', '"parser"="unicode"')       |
+-------------------------------------------------------------------+
| ["i", "love", "china", "我", "爱", "我", "的", "祖", "国"]        |
+-------------------------------------------------------------------+
1 row in set (0.02 sec)
```

## 使用示例

用hackernews 100万条数据展示倒排索引的创建、全文检索、普通查询，包括跟无索引的查询性能进行简单对比。

### 建表

```sql

CREATE DATABASE test_inverted_index;

USE test_inverted_index;

-- 创建表的同时创建了comment的倒排索引idx_comment
--   USING INVERTED 指定索引类型是倒排索引
--   PROPERTIES("parser" = "english") 指定采用english分词，还支持"chinese"中文分词和"unicode"中英文多语言混合分词，如果不指定"parser"参数表示不分词
CREATE TABLE hackernews_1m
(
    `id` BIGINT,
    `deleted` TINYINT,
    `type` String,
    `author` String,
    `timestamp` DateTimeV2,
    `comment` String,
    `dead` TINYINT,
    `parent` BIGINT,
    `poll` BIGINT,
    `children` Array<BIGINT>,
    `url` String,
    `score` INT,
    `title` String,
    `parts` Array<INT>,
    `descendants` INT,
    INDEX idx_comment (`comment`) USING INVERTED PROPERTIES("parser" = "english") COMMENT 'inverted index for comment'
)
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES ("replication_num" = "1");

```


### 导入数据

- 通过stream load导入数据

```

wget https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression/index/hacknernews_1m.csv.gz

curl --location-trusted -u root: -H "compress_type:gz" -T hacknernews_1m.csv.gz  http://127.0.0.1:8030/api/test_inverted_index/hackernews_1m/_stream_load
{
    "TxnId": 2,
    "Label": "a8a3e802-2329-49e8-912b-04c800a461a6",
    "TwoPhaseCommit": "false",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 1000000,
    "NumberLoadedRows": 1000000,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 130618406,
    "LoadTimeMs": 8988,
    "BeginTxnTimeMs": 23,
    "StreamLoadPutTimeMs": 113,
    "ReadDataTimeMs": 4788,
    "WriteDataTimeMs": 8811,
    "CommitAndPublishTimeMs": 38
}
```

- SQL运行count()确认导入数据成功

```sql
mysql> SELECT count() FROM hackernews_1m;
+---------+
| count() |
+---------+
| 1000000 |
+---------+
1 row in set (0.02 sec)
```

### 查询

#### 全文检索

- 用LIKE匹配计算comment中含有'OLAP'的行数，耗时0.18s
```sql
mysql> SELECT count() FROM hackernews_1m WHERE comment LIKE '%OLAP%';
+---------+
| count() |
+---------+
|      34 |
+---------+
1 row in set (0.18 sec)
```

- 用基于倒排索引的全文检索MATCH_ANY计算comment中含有'OLAP'的行数，耗时0.02s，加速9倍，在更大的数据集上效果会更加明显
  - 这里结果条数的差异，是因为倒排索引对comment分词后，还会对词进行进行统一成小写等归一化处理，因此MATCH_ANY比LIKE的结果多一些
```sql
mysql> SELECT count() FROM hackernews_1m WHERE comment MATCH_ANY 'OLAP';
+---------+
| count() |
+---------+
|      35 |
+---------+
1 row in set (0.02 sec)
```

- 同样的对比统计'OLTP'出现次数的性能，0.07s vs 0.01s，由于缓存的原因LIKE和MATCH_ANY都有提升，倒排索引仍然有7倍加速
```sql
mysql> SELECT count() FROM hackernews_1m WHERE comment LIKE '%OLTP%';
+---------+
| count() |
+---------+
|      48 |
+---------+
1 row in set (0.07 sec)

mysql> SELECT count() FROM hackernews_1m WHERE comment MATCH_ANY 'OLTP';
+---------+
| count() |
+---------+
|      51 |
+---------+
1 row in set (0.01 sec)
```

- 同时出现'OLAP'和'OLTP'两个词，0.13s vs 0.01s，13倍加速
  - 要求多个词同时出现时（AND关系）使用 MATCH_ALL 'keyword1 keyword2 ...'
```sql
mysql> SELECT count() FROM hackernews_1m WHERE comment LIKE '%OLAP%' AND comment LIKE '%OLTP%';
+---------+
| count() |
+---------+
|      14 |
+---------+
1 row in set (0.13 sec)

mysql> SELECT count() FROM hackernews_1m WHERE comment MATCH_ALL 'OLAP OLTP';
+---------+
| count() |
+---------+
|      15 |
+---------+
1 row in set (0.01 sec)
```

- 任意出现'OLAP'和'OLTP'其中一个词，0.12s vs 0.01s，12倍加速
  - 只要求多个词任意一个或多个出现时（OR关系）使用 MATCH_ANY 'keyword1 keyword2 ...'
```sql
mysql> SELECT count() FROM hackernews_1m WHERE comment LIKE '%OLAP%' OR comment LIKE '%OLTP%';
+---------+
| count() |
+---------+
|      68 |
+---------+
1 row in set (0.12 sec)

mysql> SELECT count() FROM hackernews_1m WHERE comment MATCH_ANY 'OLAP OLTP';
+---------+
| count() |
+---------+
|      71 |
+---------+
1 row in set (0.01 sec)
```


#### 普通等值、范围查询

- DataTime类型的列范围查询
```sql
mysql> SELECT count() FROM hackernews_1m WHERE timestamp > '2007-08-23 04:17:00';
+---------+
| count() |
+---------+
|  999081 |
+---------+
1 row in set (0.03 sec)
```

- 为timestamp列增加一个倒排索引
```sql
-- 对于日期时间类型USING INVERTED，不用指定分词
-- CREATE INDEX 是第一种建索引的语法，另外一种在后面展示
mysql> CREATE INDEX idx_timestamp ON hackernews_1m(timestamp) USING INVERTED;
Query OK, 0 rows affected (0.03 sec)
```
  **2.0-beta(含2.0-beta)后，需要再执行`BUILD INDEX`才能给存量数据加上倒排索引：**
```sql
mysql> BUILD INDEX idx_timestamp ON hackernews_1m;
Query OK, 0 rows affected (0.01 sec)
```

- 查看索引创建进度，通过FinishTime和CreateTime的差值，可以看到100万条数据对timestamp列建倒排索引只用了1s
```sql
mysql> SHOW ALTER TABLE COLUMN;
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| JobId | TableName     | CreateTime              | FinishTime              | IndexName     | IndexId | OriginIndexId | SchemaVersion | TransactionId | State    | Msg  | Progress | Timeout |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| 10030 | hackernews_1m | 2023-02-10 19:44:12.929 | 2023-02-10 19:44:13.938 | hackernews_1m | 10031   | 10008         | 1:1994690496  | 3             | FINISHED |      | NULL     | 2592000 |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
1 row in set (0.00 sec)
```

**2.0-beta(含2.0-beta)后，可通过`show builde index`来查看存量数据创建索引进展：**
```sql
-- 若table没有分区，PartitionName默认就是TableName
mysql> SHOW BUILD INDEX;
+-------+---------------+---------------+----------------------------------------------------------+-------------------------+-------------------------+---------------+----------+------+----------+
| JobId | TableName     | PartitionName | AlterInvertedIndexes                                     | CreateTime              | FinishTime              | TransactionId | State    | Msg  | Progress |
+-------+---------------+---------------+----------------------------------------------------------+-------------------------+-------------------------+---------------+----------+------+----------+
| 10191 | hackernews_1m | hackernews_1m | [ADD INDEX idx_timestamp (`timestamp`) USING INVERTED],  | 2023-06-26 15:32:33.894 | 2023-06-26 15:32:34.847 | 3             | FINISHED |      | NULL     |
+-------+---------------+---------------+----------------------------------------------------------+-------------------------+-------------------------+---------------+----------+------+----------+
1 row in set (0.04 sec)
```

- 索引创建后，范围查询用同样的查询方式，Doris会自动识别索引进行优化，但是这里由于数据量小性能差别不大
```sql
mysql> SELECT count() FROM hackernews_1m WHERE timestamp > '2007-08-23 04:17:00';
+---------+
| count() |
+---------+
|  999081 |
+---------+
1 row in set (0.01 sec)
```

- 在数值类型的列parent进行类似timestamp的操作，这里查询使用等值匹配
```sql
mysql> SELECT count() FROM hackernews_1m WHERE parent = 11189;
+---------+
| count() |
+---------+
|       2 |
+---------+
1 row in set (0.01 sec)

-- 对于数值类型USING INVERTED，不用指定分词
-- ALTER TABLE t ADD INDEX 是第二种建索引的语法
mysql> ALTER TABLE hackernews_1m ADD INDEX idx_parent(parent) USING INVERTED;
Query OK, 0 rows affected (0.01 sec)

-- 2.0-beta(含2.0-beta)后，需要再执行BUILD INDEX才能给存量数据加上倒排索引：
mysql> BUILD INDEX idx_parent ON hackernews_1m;
Query OK, 0 rows affected (0.01 sec)

mysql> SHOW ALTER TABLE COLUMN;
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| JobId | TableName     | CreateTime              | FinishTime              | IndexName     | IndexId | OriginIndexId | SchemaVersion | TransactionId | State    | Msg  | Progress | Timeout |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| 10030 | hackernews_1m | 2023-02-10 19:44:12.929 | 2023-02-10 19:44:13.938 | hackernews_1m | 10031   | 10008         | 1:1994690496  | 3             | FINISHED |      | NULL     | 2592000 |
| 10053 | hackernews_1m | 2023-02-10 19:49:32.893 | 2023-02-10 19:49:33.982 | hackernews_1m | 10054   | 10008         | 1:378856428   | 4             | FINISHED |      | NULL     | 2592000 |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+

mysql> SHOW BUILD INDEX;
+-------+---------------+---------------+----------------------------------------------------+-------------------------+-------------------------+---------------+----------+------+----------+
| JobId | TableName     | PartitionName | AlterInvertedIndexes                               | CreateTime              | FinishTime              | TransactionId | State    | Msg  | Progress |
+-------+---------------+---------------+----------------------------------------------------+-------------------------+-------------------------+---------------+----------+------+----------+
| 11005 | hackernews_1m | hackernews_1m | [ADD INDEX idx_parent (`parent`) USING INVERTED],  | 2023-06-26 16:25:10.167 | 2023-06-26 16:25:10.838 | 1002          | FINISHED |      | NULL     |
+-------+---------------+---------------+----------------------------------------------------+-------------------------+-------------------------+---------------+----------+------+----------+
1 row in set (0.01 sec)

mysql> SELECT count() FROM hackernews_1m WHERE parent = 11189;
+---------+
| count() |
+---------+
|       2 |
+---------+
1 row in set (0.01 sec)
```

- 对字符串类型的author建立不分词的倒排索引，等值查询也可以利用索引加速
```sql
mysql> SELECT count() FROM hackernews_1m WHERE author = 'faster';
+---------+
| count() |
+---------+
|      20 |
+---------+
1 row in set (0.03 sec)

-- 这里只用了USING INVERTED，不对author分词，整个当做一个词处理
mysql> ALTER TABLE hackernews_1m ADD INDEX idx_author(author) USING INVERTED;
Query OK, 0 rows affected (0.01 sec)

-- 2.0-beta(含2.0-beta)后，需要再执行BUILD INDEX才能给存量数据加上倒排索引：
mysql> BUILD INDEX idx_author ON hackernews_1m;
Query OK, 0 rows affected (0.01 sec)

-- 100万条author数据增量建索引仅消耗1.5s
mysql> SHOW ALTER TABLE COLUMN;
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| JobId | TableName     | CreateTime              | FinishTime              | IndexName     | IndexId | OriginIndexId | SchemaVersion | TransactionId | State    | Msg  | Progress | Timeout |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| 10030 | hackernews_1m | 2023-02-10 19:44:12.929 | 2023-02-10 19:44:13.938 | hackernews_1m | 10031   | 10008         | 1:1994690496  | 3             | FINISHED |      | NULL     | 2592000 |
| 10053 | hackernews_1m | 2023-02-10 19:49:32.893 | 2023-02-10 19:49:33.982 | hackernews_1m | 10054   | 10008         | 1:378856428   | 4             | FINISHED |      | NULL     | 2592000 |
| 10076 | hackernews_1m | 2023-02-10 19:54:20.046 | 2023-02-10 19:54:21.521 | hackernews_1m | 10077   | 10008         | 1:1335127701  | 5             | FINISHED |      | NULL     | 2592000 |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+

mysql> SHOW BUILD INDEX order by CreateTime desc limit 1;
+-------+---------------+---------------+----------------------------------------------------+-------------------------+-------------------------+---------------+----------+------+----------+
| JobId | TableName     | PartitionName | AlterInvertedIndexes                               | CreateTime              | FinishTime              | TransactionId | State    | Msg  | Progress |
+-------+---------------+---------------+----------------------------------------------------+-------------------------+-------------------------+---------------+----------+------+----------+
| 13006 | hackernews_1m | hackernews_1m | [ADD INDEX idx_author (`author`) USING INVERTED],  | 2023-06-26 17:23:02.610 | 2023-06-26 17:23:03.755 | 3004          | FINISHED |      | NULL     |
+-------+---------------+---------------+----------------------------------------------------+-------------------------+-------------------------+---------------+----------+------+----------+
1 row in set (0.01 sec)

-- 创建索引后，字符串等值匹配也有明显加速
mysql> SELECT count() FROM hackernews_1m WHERE author = 'faster';
+---------+
| count() |
+---------+
|      20 |
+---------+
1 row in set (0.01 sec)

```
