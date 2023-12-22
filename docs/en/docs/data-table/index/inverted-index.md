---
{
    "title": "Inverted Index",
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

In the inverted index of Doris, a row in a table corresponds to a doc in CLucene, a column corresponds to a field in doc. So using inverted index, doris can get the rows that meet the filter of SQL WHERE clause, and then get the rows quickly without reading other unrelated rows.

Doris use a seperate file to store inverted index. It's related to segment file in logic, but iosolated with each other. The advantange is that, create and drop inverted index does not need to rewrite tablet and segment file, which is very heavy work.


## Features

The features for inverted index is as follows:

- add fulltext search on text(string, varchar, char) field
  - MATCH_ALL matches all keywords, MATCH_ANY matches any keywords
  - support fulltext on array of text field
  - support english, chinese and mixed unicode word parser
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
  - PROPERTIES is optional, it allows user to specify additional properties for index. Currently, there are three types of properties available.
    - "parser" is utilized to set the type of tokenizer/parser
      - missing stands for no parser, the whole field is considered to be a term
      - "english" stands for english parser
      - "chinese" stands for chinese parser
      - "unicode" stands for muti-language mixed word segmentation suitable for situations with a mix of Chinese and English. It can segment email prefixes and suffixes, IP addresses, and mixed characters and numbers, and can also segment Chinese characters one by one.

    - "parser_mode" is utilized to set the tokenizer/parser type for Chinese word segmentation.
      - in "fine_grained" mode, the system tend to generate short words, eg. 6 words '武汉' '武汉市' '市长' '长江' '长江大桥' '大桥' for '武汉长江大桥'.
      - in "coarse_grained" mode, the system tend to generate long words, eg. 2 words '武汉市' '市长' '长江大桥' for '武汉长江大桥'.
      - default mode is "coarse_grained".
    - "support_phrase" is utilized to specify if the index requires support for phrase mode query MATCH_PHRASE
      - "true" indicates that support is needed, but needs more storage for index.
      - "false" indicates that support is not needed, and less storage for index. MATCH_ALL can be used for matching multi words without order.
      - default mode is "false".
    - char_filter: the main function is to pre-process the string before word segmentation
      - char_filter_type: specify char_filters with different functions (currently only char_replace is supported)
        - char_replace: replace each char in the pattern with a char in the replacement
          - char_filter_pattern: character array to be replaced
          - char_filter_replacement: replaced character array, can be left unset, defaults to a space character
    - ignore_above: Controls whether strings are indexed.
      - Strings longer than the ignore_above setting will not be indexed. For arrays of strings, ignore_above will be applied for each array element separately and string elements longer than ignore_above will not be indexed.
      - default value is 256 bytes.
    - lower_case: Whether to convert tokens to lowercase, thereby achieving case-insensitive matching.
      - true: Convert to lowercase
      - false: Do not convert to lowercase 
  - COMMENT is optional

```sql
CREATE TABLE table_name
(
  columns_difinition,
  INDEX idx_name1(column_name1) USING INVERTED [PROPERTIES("parser" = "english|chinese|unicode")] [COMMENT 'your comment']
  INDEX idx_name2(column_name2) USING INVERTED [PROPERTIES("parser" = "english|chinese|unicode")] [COMMENT 'your comment']
  INDEX idx_name3(column_name3) USING INVERTED [PROPERTIES("parser" = "chinese", "parser_mode" = "fine_grained|coarse_grained")] [COMMENT 'your comment']
  INDEX idx_name4(column_name4) USING INVERTED [PROPERTIES("parser" = "english|chinese|unicode", "support_phrase" = "true|false")] [COMMENT 'your comment']
  INDEX idx_name5(column_name4) USING INVERTED [PROPERTIES("char_filter_type" = "char_replace", "char_filter_pattern" = "._"), "char_filter_replacement" = " "] [COMMENT 'your comment']
  INDEX idx_name5(column_name4) USING INVERTED [PROPERTIES("char_filter_type" = "char_replace", "char_filter_pattern" = "._")] [COMMENT 'your comment']
)
table_properties;
```

:::tip

Inverted indexes have different limitations in different data models:
- Aggregate model: Inverted indexes can only be created for the Key column.
- Unique model: The merge on write feature needs to be enabled. After enabling it, an inverted index can be created for any column.
- Duplicate model: An inverted index can be created for any column.

:::

- add an inverted index to existed table

**Before version 2.0-beta:**
```sql
-- syntax 1
CREATE INDEX idx_name ON table_name(column_name) USING INVERTED [PROPERTIES("parser" = "english|chinese|unicode")] [COMMENT 'your comment'];
-- syntax 2
ALTER TABLE table_name ADD INDEX idx_name(column_name) USING INVERTED [PROPERTIES("parser" = "english|chinese|unicode")] [COMMENT 'your comment'];
```

**After version 2.0-beta (including 2.0-beta):**

The above 'create/add index' operation only generates inverted index for incremental data. The syntax of BUILD INDEX is added to add inverted index to stock data:
```sql
-- syntax 1, add inverted index to the stock data of the whole table by default
BUILD INDEX index_name ON table_name;
-- syntax 2, partition can be specified, and one or more can be specified
BUILD INDEX index_name ON table_name PARTITIONS(partition_name1, partition_name2);
```
(**The above 'create/add index' operation needs to be executed before executing the BUILD INDEX**)

To view the progress of the `BUILD INDEX`, you can run the following statement
```sql
SHOW BUILD INDEX [FROM db_name];
-- Example 1: Viewing the progress of all BUILD INDEX tasks
SHOW BUILD INDEX;
-- Example 2: Viewing the progress of the BUILD INDEX task for a specified table
SHOW BUILD INDEX where TableName = "table1";
```

To cancel `BUILD INDEX`, you can run the following statement
```sql
CANCEL BUILD INDEX ON table_name;
CANCEL BUILD INDEX ON table_name (job_id1,jobid_2,...);
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
SELECT * FROM table_name WHERE logmsg MATCH_ANY 'keyword1 keyword2';

-- 1.3 find rows that logmsg contains both keyword1 and keyword2 and more keywords
SELECT * FROM table_name WHERE logmsg MATCH_ALL 'keyword1 keyword2';

-- 1.4 find rows that logmsg contains both keyword1 and keyword2, and in the order of keyword1 appearing first and keyword2 appearing later.
SELECT * FROM table_name WHERE logmsg MATCH_PHRASE 'keyword1 keyword2';

-- 2. normal equal, range query
SELECT * FROM table_name WHERE id = 123;
SELECT * FROM table_name WHERE ts > '2023-01-01 00:00:00';
SELECT * FROM table_name WHERE op_type IN ('add', 'delete');
```

- Tokenization Function

To evaluate the actual effects of tokenization or to tokenize a block of text, the `tokenize` function can be utilized.
```sql
mysql> SELECT TOKENIZE('武汉长江大桥','"parser"="chinese","parser_mode"="fine_grained");
+-----------------------------------------------------------------------------------+
| tokenize('武汉长江大桥', '"parser"="chinese","parser_mode"="fine_grained"')       |
+-----------------------------------------------------------------------------------+
| ["武汉", "武汉长江大桥", "长江", "长江大桥", "大桥"]                              |
+-----------------------------------------------------------------------------------+
1 row in set (0.02 sec)

mysql> SELECT TOKENIZE('武汉市长江大桥','"parser"="chinese","parser_mode"="fine_grained");
+--------------------------------------------------------------------------------------+
| tokenize('武汉市长江大桥', '"parser"="chinese","parser_mode"="fine_grained"')        |
+--------------------------------------------------------------------------------------+
| ["武汉", "武汉市", "市长", "长江", "长江大桥", "大桥"]                               |
+--------------------------------------------------------------------------------------+
1 row in set (0.02 sec)

mysql> SELECT TOKENIZE('武汉市长江大桥','"parser"="chinese","parser_mode"="coarse_grained");
+----------------------------------------------------------------------------------------+
| tokenize('武汉市长江大桥', '"parser"="chinese","parser_mode"="coarse_grained"')        |
+----------------------------------------------------------------------------------------+
| ["武汉市", "长江大桥"]                                                                 |
+----------------------------------------------------------------------------------------+
1 row in set (0.02 sec)

mysql> SELECT TOKENIZE('I love CHINA','"parser"="english");
+------------------------------------------------+
| tokenize('I love CHINA', '"parser"="english"') |
+------------------------------------------------+
| ["i", "love", "china"]                         |
+------------------------------------------------+
1 row in set (0.02 sec)

mysql> SELECT TOKENIZE('I love CHINA 我爱我的祖国','"parser"="unicode");
+-------------------------------------------------------------------+
| tokenize('I love CHINA 我爱我的祖国', '"parser"="unicode"')       |
+-------------------------------------------------------------------+
| ["i", "love", "china", "我", "爱", "我", "的", "祖", "国"]        |
+-------------------------------------------------------------------+
1 row in set (0.02 sec)
```

## Examples

This example will demostrate inverted index creation, fulltext query, normal query using a hackernews dataset with 1 million rows. The performanc comparation between using  and without inverted index will also be showed.

### Create Table

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


### Load Data

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

#### Fulltext Search Query

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


#### Normal Equal, Range Query

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
**After 2.0-beta (including 2.0-beta), you need to execute `BUILD INDEX` to add inverted index to the stock data:**
```sql
mysql> BUILD INDEX idx_timestamp ON hackernews_1m;
Query OK, 0 rows affected (0.01 sec)
```

- progress of building index can be view by SQL. It just costs 1s (compare FinishTime and CreateTime) to BUILD INDEX for timestamp column with 1 million rows.
```sql
mysql> SHOW ALTER TABLE COLUMN;
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| JobId | TableName     | CreateTime              | FinishTime              | IndexName     | IndexId | OriginIndexId | SchemaVersion | TransactionId | State    | Msg  | Progress | Timeout |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
| 10030 | hackernews_1m | 2023-02-10 19:44:12.929 | 2023-02-10 19:44:13.938 | hackernews_1m | 10031   | 10008         | 1:1994690496  | 3             | FINISHED |      | NULL     | 2592000 |
+-------+---------------+-------------------------+-------------------------+---------------+---------+---------------+---------------+---------------+----------+------+----------+---------+
1 row in set (0.00 sec)
```

**After 2.0-beta (including 2.0-beta), you can view the progress of stock data creating index by `SHOW BUILD INDEX`:**
```sql
-- If the table has no partitions, the PartitionName defaults to TableName
mysql> SHOW BUILD INDEX;
+-------+---------------+---------------+----------------------------------------------------------+-------------------------+-------------------------+---------------+----------+------+----------+
| JobId | TableName     | PartitionName | AlterInvertedIndexes                                     | CreateTime              | FinishTime              | TransactionId | State    | Msg  | Progress |
+-------+---------------+---------------+----------------------------------------------------------+-------------------------+-------------------------+---------------+----------+------+----------+
| 10191 | hackernews_1m | hackernews_1m | [ADD INDEX idx_timestamp (`timestamp`) USING INVERTED],  | 2023-06-26 15:32:33.894 | 2023-06-26 15:32:34.847 | 3             | FINISHED |      | NULL     |
+-------+---------------+---------------+----------------------------------------------------------+-------------------------+-------------------------+---------------+----------+------+----------+
1 row in set (0.04 sec)
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

-- After 2.0-beta (including 2.0-beta), you need to execute `BUILD INDEX` to add inverted index to the stock data:
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

-- After 2.0-beta (including 2.0-beta), you need to execute `BUILD INDEX` to add inverted index to the stock data:
mysql> BUILD INDEX idx_author ON hackernews_1m;
Query OK, 0 rows affected (0.01 sec)

-- costs 1.5s to BUILD INDEX for author column with 1 million rows.
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

-- equal qury on text field autor get 3x speedup
mysql> SELECT count() FROM hackernews_1m WHERE author = 'faster';
+---------+
| count() |
+---------+
|      20 |
+---------+
1 row in set (0.01 sec)

```
