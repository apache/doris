---
{
    "title": "Inverted index",
    "language": "en"
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

# [Experimental] Inverted Index

<version since="2.0.0">
 
</version>

From version 2.0.0, Doris implemented inverted index to support fulltext search on text field, normal eq and range filter on text, numeric, datetime field. This doc introduce inverted index usage, including create, drop and query.


## Glossary

- [inverted index](https://en.wikipedia.org/wiki/Inverted_index) is a index techlogy used in information retirval commonly. It split text into word terms and construct a term to doc index. This index is called inverted index and can be used to find the docs where a specific term appears.


## Basic Principles

Doris use [CLucene](https://clucene.sourceforge.net/) as its underlying lib for inverted index. CLucene is a high performance and robust implementation of the famous Lucene inverted index library written in C++. Doris optimize CLucene to be more simple, fast and suitable for a database.

In the inverted index of Doris, a row in a table corresponds to a doc in CLucene, a column corresponds to a field in doc. So using inverted index, doris can get the rows that meet the filter of SQL WHERE clause, and then get the rows quickly without reading other unrelated rows.

Doris use a seperate file to store inverted index. It's related to segment file in logic, but iosolated with each other. The advantange is that, create and drop inverted index does not need to rewrite tablet and segment file, which is very heavy work.


## Features

The features for inverted index is as follows:

- add fulltext search on text(string, varchar, char) field
  - MATCH_ALL matches all keywords, MATCH_ANY matches any keywords
  - support fulltext on array of text field
  - support english and chinese word parser
- accelerate normal equal, range query, replacing bitmap index in the future
  - suport =, !=, >, >=, <, <= on text, numeric, datetime types
  - suport =, !=, >, >=, <, <= on array of text, numeric, datetime types
- complete suport for logic combination
  - add index filter push down for OR, NOT
  - support combination of AND, OR, NOT
- flexiable and fast index management
  - support inverted index definition on table creation
  - support add inverted index on existed table, without rewrite data
  - support delete inverted index on existed table, without rewrite data


## Syntax

- The inverted index definition syntax on table creation is as follows
  - USING INVERTED is mandatory, it specify index type to be inverted index
  - PROPERTIES is optional, it allows user to specify additional properties for index, "parser" is for type of word tokenizor/parser
    - missing stands for no parser, the whole field is considered to be a term
    - "english" stands for english parser
    - "chinese" stands for chinese parser
  - COMMENT is optional

```sql
CREATE TABLE table_name
(
  columns_difinition,
  INDEX idx_name1(column_name1) USING INVERTED [PROPERTIES("parser" = "english|chinese")] [COMMENT 'your comment']
  INDEX idx_name2(column_name2) USING INVERTED [PROPERTIES("parser" = "english|chinese")] [COMMENT 'your comment']
)
table_properties;
```

- add an inverted index to existed table
```sql
-- syntax 1
CREATE INDEX idx_name ON table_name(column_name) USING INVERTED [PROPERTIES("parser" = "english|chinese")] [COMMENT 'your comment'];
-- syntax 2
ALTER TABLE table_name ADD INDEX idx_name(column_name) USING INVERTED [PROPERTIES("parser" = "english|chinese")] [COMMENT 'your comment'];
```

- drop an inverted index
```sql
-- syntax 1
DROP INDEX idx_name ON table_name;
-- syntax 2
ALTER TABLE table_name DROP INDEX idx_name;
```

- speed up query using inverted index
```sql
-- 1. fulltext search using MATCH_ANY OR MATCH_ALL
SELECT * FROM table_name WHERE column_name MATCH_ANY | MATCH_ALL 'keyword1 ...';

-- 1.1 find rows that logmsg contains keyword1
SELECT * FROM table_name WHERE logmsg MATCH_ANY 'keyword1';

-- 1.2 find rows that logmsg contains keyword1 or keyword2 or more keywords
SELECT * FROM table_name WHERE logmsg MATCH_ANY 'keyword2 keyword2';

-- 1.3 find rows that logmsg contains both keyword1 and keyword2 and more keywords
SELECT * FROM table_name WHERE logmsg MATCH_ALL 'keyword2 keyword2';


-- 2. normal equal, range query
SELECT * FROM table_name WHERE id = 123;
SELECT * FROM table_name WHERE ts > '2023-01-01 00:00:00';
SELECT * FROM table_name WHERE op_type IN ('add', 'delete');
```

## Example

This example will demostrate inverted index creation, fulltext query, normal query using a hackernews dataset with 1 million rows. The performanc comparation between using  and without inverted index will also be showed.

### Create table

```sql

CREATE DATABASE test_inverted_index;

USE test_inverted_index;

-- define inverted index idx_comment for comment column on table creation
--   USING INVERTED specify using inverted index
--   PROPERTIES("parser" = "english") specify english word parser
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


### Load data

- load data by stream load

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

- check loaded data by SQL count()

```sql
mysql> SELECT count() FROM hackernews_1m;
+---------+
| count() |
+---------+
| 1000000 |
+---------+
1 row in set (0.02 sec)
```

### Query

#### Fulltext search query

- count the rows that comment contains 'OLAP' using LIKE, cost 0.18s
```sql
mysql> SELECT count() FROM hackernews_1m WHERE comment LIKE '%OLAP%';
+---------+
| count() |
+---------+
|      34 |
+---------+
1 row in set (0.18 sec)
```

- count the rows that comment contains 'OLAP' using MATCH_ANY fulltext search based on inverted index , cost 0.02s and 9x speedup, the speedup will be even larger on larger dataset
  - the difference of count is due to feature of fulltext. Word parser will not only split text to words, but also do some normalization such transform to lower case letters. So the result of MATCH_ANY will be a little more.
```sql
mysql> SELECT count() FROM hackernews_1m WHERE comment MATCH_ANY 'OLAP';
+---------+
| count() |
+---------+
|      35 |
+---------+
1 row in set (0.02 sec)
```

- Semilarly, count on 'OLTP' shows 0.07s vs 0.01s. Due to the cache in Doris, both LIKE and MATCH_ANY is faster, but there is still 7x speedup.
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


- search for both 'OLAP' and 'OLTP', 0.13s vs 0.01s，13x speedup
  - using MATCH_ALL if you need the keywords all appears
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

- search for at least one of 'OLAP' or 'OLTP', 0.12s vs 0.01s，12x speedup
  - using MATCH_ALL if you only need at least one of the keywords appears
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


#### normal equal, range query

- range query on DateTime column
```sql
mysql> SELECT count() FROM hackernews_1m WHERE timestamp > '2007-08-23 04:17:00';
+---------+
| count() |
+---------+
|  999081 |
+---------+
1 row in set (0.03 sec)
```

- add inverted index for timestamp column
```sql
-- for timestamp column, there is no need for word parser, so just USING INVERTED without PROPERTIES
-- this is the first syntax for CREATE INDEX
mysql> CREATE INDEX idx_timestamp ON hackernews_1m(timestamp) USING INVERTED;
Query OK, 0 rows affected (0.03 sec)
```

- progress of building index can be view by SQL. It just costs 1s (compare FinishTime and CreateTime) to build index for timestamp column with 1 million rows.
```sql
mysql> SHOW ALTER TABLE COLUMN;
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| JobId | TableName     | CreateTime              | FinishTime              | IndexName     | IndexId | OriginIndexId | SchemaVersion | TransactionId | State    | Msg  | Progress | Timeout |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| 10030 | hackernews_1m | 2023-02-10 19:44:12.929 | 2023-02-10 19:44:13.938 | hackernews_1m | 10031   | 10008         | 1:1994690496  | 3             | FINISHED |      | NULL     | 2592000 |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
1 row in set (0.00 sec)
```

- after the index is build, Doris will automaticaly use index for range query, but the performance is almost the same since it's already fast on the small dataset
```sql
mysql> SELECT count() FROM hackernews_1m WHERE timestamp > '2007-08-23 04:17:00';
+---------+
| count() |
+---------+
|  999081 |
+---------+
1 row in set (0.01 sec)
```

- similary test for parent column with numeric type, using equal query
```sql
mysql> SELECT count() FROM hackernews_1m WHERE parent = 11189;
+---------+
| count() |
+---------+
|       2 |
+---------+
1 row in set (0.01 sec)

-- do not use word parser for numeric type USING INVERTED
-- use the second syntax ALTER TABLE
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

- for text column author, inverted index can also be used to speedup equal query
```sql
mysql> SELECT count() FROM hackernews_1m WHERE author = 'faster';
+---------+
| count() |
+---------+
|      20 |
+---------+
1 row in set (0.03 sec)

-- do not use any word parser for author to treat it as a whole
mysql> ALTER TABLE hackernews_1m ADD INDEX idx_author(author) USING INVERTED;
Query OK, 0 rows affected (0.01 sec)

-- costs 1.5s to build index for author column with 1 million rows.
mysql> SHOW ALTER TABLE COLUMN;
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| JobId | TableName     | CreateTime              | FinishTime              | IndexName     | IndexId | OriginIndexId | SchemaVersion | TransactionId | State    | Msg  | Progress | Timeout |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| 10030 | hackernews_1m | 2023-02-10 19:44:12.929 | 2023-02-10 19:44:13.938 | hackernews_1m | 10031   | 10008         | 1:1994690496  | 3             | FINISHED |      | NULL     | 2592000 |
| 10053 | hackernews_1m | 2023-02-10 19:49:32.893 | 2023-02-10 19:49:33.982 | hackernews_1m | 10054   | 10008         | 1:378856428   | 4             | FINISHED |      | NULL     | 2592000 |
| 10076 | hackernews_1m | 2023-02-10 19:54:20.046 | 2023-02-10 19:54:21.521 | hackernews_1m | 10077   | 10008         | 1:1335127701  | 5             | FINISHED |      | NULL     | 2592000 |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+

-- equal qury on text field autor get 3x speedup
mysql> SELECT count() FROM hackernews_1m WHERE author = 'faster';
+---------+
| count() |
+---------+
|      20 |
+---------+
1 row in set (0.01 sec)

```
