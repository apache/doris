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

# 倒排索引

倒排索引可以用来加速查询，快速从海量数据中过滤出满足条件的行。本文档主要介绍如何创建 index 作业，以及创建 index 的一些注意事项和常见问题。



## 名词解释

- [inverted index](https://en.wikipedia.org/wiki/Inverted_index)：[倒排索引](https://zh.wikipedia.org/wiki/%E5%80%92%E6%8E%92%E7%B4%A2%E5%BC%95)，是信息检索领域常用的索引技术。

## 功能介绍

- 支持字符串全文检索，包括同时匹配多个关键字MATCH_ALL、匹配任意一个关键字MATCH_ANY
- 支持字符串英文、中文
- 支持字符串、数值、日期时间类型的 =, !=, >, >=, <, <= 快速过滤
- 支持数组，包括字符串全文检索和数字、日期时间类型的 =, !=, >, >=, <, <=
- 支持多个条件的任意AND OR NOT组合
- 支持在创建表上定义倒排索引
- 支持在已有的表上增加倒排，而且支持倒排索引增量构建、避免重写表中的已有数据

## 原理介绍



## 语法

### 建表

```sql

CREATE DATABASE test_inverted_index;

USE test_inverted_index;

-- 创建表的同时创建了comment的倒排索引idx_comment
--   USING INVERTED 指定索引类型是倒排索引
--   PROPERTIES("parser" = "english") 指定采用english分词，还支持"chinese"中文分词，如果不指定"parser"参数表示不分词
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

wget https://justtmp-bj-1308700295.cos.ap-beijing.myqcloud.com/tmp/hacknernews_1m.csv.gz

curl --output - --location-trusted -u root: -H "compress_type:gz" -T hacknernews_1m.csv.gz  http://127.0.0.1:8030/api/test_inverted_index/hackernews_1m/_stream_load
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
mysql> CREATE INDEX idx_timestamp on hackernews_1m(timestamp) USING INVERTED;
Query OK, 0 rows affected (0.03 sec)
```

- 查看索引创建进度，100万条数据只用了1s
```sql
mysql> SHOW ALTER TABLE COLUMN;
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| JobId | TableName     | CreateTime              | FinishTime              | IndexName     | IndexId | OriginIndexId | SchemaVersion | TransactionId | State    | Msg  | Progress | Timeout |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| 10030 | hackernews_1m | 2023-02-10 19:44:12.929 | 2023-02-10 19:44:13.938 | hackernews_1m | 10031   | 10008         | 1:1994690496  | 3             | FINISHED |      | NULL     | 2592000 |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
1 row in set (0.00 sec)
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

mysql> SHOW ALTER TABLE COLUMN;
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| JobId | TableName     | CreateTime              | FinishTime              | IndexName     | IndexId | OriginIndexId | SchemaVersion | TransactionId | State    | Msg  | Progress | Timeout |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| 10030 | hackernews_1m | 2023-02-10 19:44:12.929 | 2023-02-10 19:44:13.938 | hackernews_1m | 10031   | 10008         | 1:1994690496  | 3             | FINISHED |      | NULL     | 2592000 |
| 10053 | hackernews_1m | 2023-02-10 19:49:32.893 | 2023-02-10 19:49:33.982 | hackernews_1m | 10054   | 10008         | 1:378856428   | 4             | FINISHED |      | NULL     | 2592000 |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+

mysql> SELECT count() FROM hackernews_1m WHERE parent = 11189;
+---------+
| count() |
+---------+
|       2 |
+---------+
1 row in set (0.01 sec)
```

- 对字符串类型的author建立部分词的倒排索引，等值查询也可以利用索引加速
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

-- 100万条author数据增量建索引仅消耗1.5s
mysql> SHOW ALTER TABLE COLUMN;
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| JobId | TableName     | CreateTime              | FinishTime              | IndexName     | IndexId | OriginIndexId | SchemaVersion | TransactionId | State    | Msg  | Progress | Timeout |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| 10030 | hackernews_1m | 2023-02-10 19:44:12.929 | 2023-02-10 19:44:13.938 | hackernews_1m | 10031   | 10008         | 1:1994690496  | 3             | FINISHED |      | NULL     | 2592000 |
| 10053 | hackernews_1m | 2023-02-10 19:49:32.893 | 2023-02-10 19:49:33.982 | hackernews_1m | 10054   | 10008         | 1:378856428   | 4             | FINISHED |      | NULL     | 2592000 |
| 10076 | hackernews_1m | 2023-02-10 19:54:20.046 | 2023-02-10 19:54:21.521 | hackernews_1m | 10077   | 10008         | 1:1335127701  | 5             | FINISHED |      | NULL     | 2592000 |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+

-- 创建索引后，字符串等值匹配也有明显加速
mysql> SELECT count() FROM hackernews_1m WHERE author = 'faster';
+---------+
| count() |
+---------+
|      20 |
+---------+
1 row in set (0.01 sec)

```
