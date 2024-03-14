---
{
    "title": "VARIANT",
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

## VARIANT

<version since="2.1.0">

</version>

### Description

VARIANT Type
Introduced a new data type VARIANT in Doris 2.1, which can store semi-structured JSON data. It allows storing complex data structures containing different data types (such as integers, strings, boolean values, etc.) without the need to define specific columns in the table structure beforehand. The VARIANT type is particularly useful for handling complex nested structures that may change at any time. During the writing process, this type can automatically infer column information based on the structure and types of the columns, dynamicly merge written schemas. It stores JSON keys and their corresponding values as columns and dynamic sub-columns.

### Note

Advantages over JSON Type:

1. Different storage methods: The JSON type is stored in binary JSONB format, and the entire JSON is stored row by row in segment files. In contrast, the VARIANT type infers types during writing and stores the written JSON columns. It has a higher compression ratio compared to the JSON type, providing better storage efficiency.
2. Query: Querying does not require parsing. VARIANT fully utilizes columnar storage, vectorized engines, optimizers, and other components in Doris, providing users with extremely high query performance.
Below are test results based on clickbench data:

|    | Storage Space |
|--------------|------------|
| Predefined Static Columns | 12.618 GB  |
| VARIANT Type    | 12.718 GB  |
| JSON Type             | 35.711 GB   |

**Saves approximately 65% storage capacity**

| Query Counts        | Predefined Static Columns | VARIANT Type | JSON Type        |
|---------------------|---------------------------|--------------|-----------------|
| First Query (cold)  | 233.79s                   | 248.66s        | **Most queries timeout**  |
| Second Query (hot) | 86.02s                     | 94.82s          | 789.24s         |
| Third Query (hot)   | 83.03s                     | 92.29s          | 743.69s         |

[test case](https://github.com/ClickHouse/ClickBench/blob/main/doris/queries.sql) contains 43 queries 

**8x faster query, query performance comparable to static columns**

### Example

Demonstrate the functionality and usage of VARIANT with an example covering table creation, data import, and query cycle.

**Table Creation Syntax**
Create a table, using the `variant` keyword in the syntax.

``` sql
-- Without index
CREATE TABLE IF NOT EXISTS ${table_name} (
    k BIGINT,
    v VARIANT
)
table_properties;

-- Create an index on the v column, optionally specify the tokenize method, default is untokenized 
CREATE TABLE IF NOT EXISTS ${table_name} (
    k BIGINT,
    v VARIANT,
    INDEX idx_var(v) USING INVERTED [PROPERTIES("parser" = "english|unicode|chinese")] [COMMENT 'your comment']
)
table_properties;

-- Create an bloom filter on v column, to enhance query seed on sub columns
CREATE TABLE IF NOT EXISTS ${table_name} (
    k BIGINT,
    v VARIANT
)
...
properties("replication_num" = "1", "bloom_filter_columns" = "v");

```

**Query Syntax**

``` sql
-- use v['a']['b'] format for example, v['properties']['title'] type is Variant
SELECT v['properties']['title'] from ${table_name}

```

**Example based on the GitHub events dataset**

Here, github events data is used to demonstrate the table creation, data import, and query using variant.
The below is a formatted line of data:

``` json
{
  "id": "14186154924",
  "type": "PushEvent",
  "actor": {
    "id": 282080,
    "login": "brianchandotcom",
    "display_login": "brianchandotcom",
    "gravatar_id": "",
    "url": "https://api.github.com/users/brianchandotcom",
    "avatar_url": "https://avatars.githubusercontent.com/u/282080?"
  },
  "repo": {
    "id": 1920851,
    "name": "brianchandotcom/liferay-portal",
    "url": "https://api.github.com/repos/brianchandotcom/liferay-portal"
  },
  "payload": {
    "push_id": 6027092734,
    "size": 4,
    "distinct_size": 4,
    "ref": "refs/heads/master",
    "head": "91edd3c8c98c214155191feb852831ec535580ba",
    "before": "abb58cc0db673a0bd5190000d2ff9c53bb51d04d",
    "commits": [""]
  },
  "public": true,
  "created_at": "2020-11-13T18:00:00Z"
}
```

**Table Creation**

- Created three columns of VARIANT type: `actor`, `repo`, and `payload`.
- Simultaneously created an inverted index, `idx_payload`, for the `payload` column while creating the table.
- Specified the index type as inverted using `USING INVERTED`, aimed at accelerating conditional filtering of sub-columns.
- `PROPERTIES("parser" = "english")` specified the adoption of English tokenization.

``` sql
CREATE DATABASE test_variant;
USE test_variant;
CREATE TABLE IF NOT EXISTS github_events (
    id BIGINT NOT NULL,
    type VARCHAR(30) NULL,
    actor VARIANT NULL,
    repo VARIANT NULL,
    payload VARIANT NULL,
    public BOOLEAN NULL,
    created_at DATETIME NULL,
    INDEX idx_payload (`payload`) USING INVERTED PROPERTIES("parser" = "english") COMMENT 'inverted index for payload'
)
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(id) BUCKETS 10
properties("replication_num" = "1");
```

:::tip

1. Creating an index on VARIANT columns, such as when there are numerous sub-columns in payload, might lead to an excessive number of index columns, impacting write performance.
2. The tokenization properties for the same VARIANT column are uniform. If you have varied tokenization requirements, consider creating multiple VARIANT columns and specifying index properties separately for each.

:::


**Using Streamload for Import**

Importing gh_2022-11-07-3.json, which contains one hour's worth of GitHub events data.

``` shell
wget http://doris-build-hk-1308700295.cos.ap-hongkong.myqcloud.com/regression/variant/gh_2022-11-07-3.json

curl --location-trusted -u root:  -T gh_2022-11-07-3.json -H "read_json_by_line:true" -H "format:json"  http://127.0.0.1:18148/api/test_variant/github_events/_strea
m_load

{
    "TxnId": 2,
    "Label": "086fd46a-20e6-4487-becc-9b6ca80281bf",
    "Comment": "",
    "TwoPhaseCommit": "false",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 139325,
    "NumberLoadedRows": 139325,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 633782875,
    "LoadTimeMs": 7870,
    "BeginTxnTimeMs": 19,
    "StreamLoadPutTimeMs": 162,
    "ReadDataTimeMs": 2416,
    "WriteDataTimeMs": 7634,
    "CommitAndPublishTimeMs": 55
}
```

Confirm the successful import.

``` sql
-- View the number of rows.
mysql> select count() from github_events;
+----------+
| count(*) |
+----------+
|   139325 |
+----------+
1 row in set (0.25 sec)

-- Random select one row
mysql> select * from github_events limit 1;
+-------------+-----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+---------------------+
| id          | type      | actor                                                                                                                                                                                                                       | repo                                                                                                                                                     | payload                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | public | created_at          |
+-------------+-----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+---------------------+
| 25061821748 | PushEvent | {"gravatar_id":"","display_login":"jfrog-pipelie-intg","url":"https://api.github.com/users/jfrog-pipelie-intg","id":98024358,"login":"jfrog-pipelie-intg","avatar_url":"https://avatars.githubusercontent.com/u/98024358?"} | {"url":"https://api.github.com/repos/jfrog-pipelie-intg/jfinte2e_1667789956723_16","id":562683829,"name":"jfrog-pipelie-intg/jfinte2e_1667789956723_16"} | {"commits":[{"sha":"334433de436baa198024ef9f55f0647721bcd750","author":{"email":"98024358+jfrog-pipelie-intg@users.noreply.github.com","name":"jfrog-pipelie-intg"},"message":"commit message 10238493157623136117","distinct":true,"url":"https://api.github.com/repos/jfrog-pipelie-intg/jfinte2e_1667789956723_16/commits/334433de436baa198024ef9f55f0647721bcd750"}],"before":"f84a26792f44d54305ddd41b7e3a79d25b1a9568","head":"334433de436baa198024ef9f55f0647721bcd750","size":1,"push_id":11572649828,"ref":"refs/heads/test-notification-sent-branch-10238493157623136113","distinct_size":1} |      1 | 2022-11-07 11:00:00 |
+-------------+-----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+---------------------+
1 row in set (0.23 sec)
```
Running desc command to view schema information, sub-columns will automatically expand at the storage layer and undergo type inference.

``` sql
mysql> desc github_events;
+------------------------------------------------------------+------------+------+-------+---------+-------+
| Field                                                      | Type       | Null | Key   | Default | Extra |
+------------------------------------------------------------+------------+------+-------+---------+-------+
| id                                                         | BIGINT     | No   | true  | NULL    |       |
| type                                                       | VARCHAR(*) | Yes  | false | NULL    | NONE  |
| actor                                                      | VARIANT    | Yes  | false | NULL    | NONE  |
| created_at                                                 | DATETIME   | Yes  | false | NULL    | NONE  |
| payload                                                    | VARIANT    | Yes  | false | NULL    | NONE  |
| public                                                     | BOOLEAN    | Yes  | false | NULL    | NONE  |
+------------------------------------------------------------+------------+------+-------+---------+-------+
6 rows in set (0.07 sec)

mysql> set describe_extend_variant_column = true;
Query OK, 0 rows affected (0.01 sec)

mysql> desc github_events;
+------------------------------------------------------------+------------+------+-------+---------+-------+
| Field                                                      | Type       | Null | Key   | Default | Extra |
+------------------------------------------------------------+------------+------+-------+---------+-------+
| id                                                         | BIGINT     | No   | true  | NULL    |       |
| type                                                       | VARCHAR(*) | Yes  | false | NULL    | NONE  |
| actor                                                      | VARIANT    | Yes  | false | NULL    | NONE  |
| actor.avatar_url                                           | TEXT       | Yes  | false | NULL    | NONE  |
| actor.display_login                                        | TEXT       | Yes  | false | NULL    | NONE  |
| actor.id                                                   | INT        | Yes  | false | NULL    | NONE  |
| actor.login                                                | TEXT       | Yes  | false | NULL    | NONE  |
| actor.url                                                  | TEXT       | Yes  | false | NULL    | NONE  |
| created_at                                                 | DATETIME   | Yes  | false | NULL    | NONE  |
| payload                                                    | VARIANT    | Yes  | false | NULL    | NONE  |
| payload.action                                             | TEXT       | Yes  | false | NULL    | NONE  |
| payload.before                                             | TEXT       | Yes  | false | NULL    | NONE  |
| payload.comment.author_association                         | TEXT       | Yes  | false | NULL    | NONE  |
| payload.comment.body                                       | TEXT       | Yes  | false | NULL    | NONE  |
....
+------------------------------------------------------------+------------+------+-------+---------+-------+
406 rows in set (0.07 sec)
```
DESC can be used to specify partition and view the schema of a particular partition. The syntax is as follows:

``` sql
DESCRIBE ${table_name} PARTITION ($partition_name);
```

**Querying**

:::tip

When utilizing filtering and aggregation functionalities to query sub-columns, additional casting operations need to be performed on sub-columns (because the storage types are not necessarily fixed and require a unified SQL type).
For instance, `SELECT * FROM tbl where CAST(var['titile'] as text) MATCH "hello world"`
The simplified examples below illustrate how to use VARIANT for querying:
The following are three typical query scenarios

:::

1. Retrieve the top 5 repositories based on star count from the `github_events` table.

``` sql
mysql> SELECT
    ->     cast(repo['name'] as text) as repo_name, count() AS stars
    -> FROM github_events
    -> WHERE type = 'WatchEvent'
    -> GROUP BY 
    -> ORDER BY stars DESC LIMIT 5;
+--------------------------+-------+
| repo_name                | stars |
+--------------------------+-------+
| aplus-framework/app      |    78 |
| lensterxyz/lenster       |    77 |
| aplus-framework/database |    46 |
| stashapp/stash           |    42 |
| aplus-framework/image    |    34 |
+--------------------------+-------+
5 rows in set (0.03 sec)
```

2. Retrieve the count of comments containing "doris".

``` sql
mysql> SELECT
    ->     count() FROM github_events
    ->     WHERE cast(payload['comment']['body'] as text) MATCH 'doris';
+---------+
| count() |
+---------+
|       3 |
+---------+
1 row in set (0.04 sec)
```

3. Query the issue number with the highest number of comments along with its corresponding repository.

``` sql
mysql> SELECT 
    ->   cast(repo['name'] as string) as repo_name, 
    ->   cast(payload['issue']['number'] as int) as issue_number, 
    ->   count() AS comments, 
    ->   count(
    ->     distinct cast(actor['login'] as string)
    ->   ) AS authors 
    -> FROM  github_events 
    -> WHERE type = 'IssueCommentEvent' AND (cast(payload["action"] as string) = 'created') AND (cast(payload["issue"]["number"] as int) > 10) 
    -> GROUP BY repo_name, issue_number 
    -> HAVING authors >= 4
    -> ORDER BY comments DESC, repo_name 
    -> LIMIT 50;
+--------------------------------------+--------------+----------+---------+
| repo_name                            | issue_number | comments | authors |
+--------------------------------------+--------------+----------+---------+
| facebook/react-native                |        35228 |        5 |       4 |
| swsnu/swppfall2022-team4             |           27 |        5 |       4 |
| belgattitude/nextjs-monorepo-example |         2865 |        4 |       4 |
+--------------------------------------+--------------+----------+---------+
3 rows in set (0.03 sec)
```

### Usage Restrictions and Best Practices

**There are several limitations when using the VARIANT type:**
Dynamic columns of VARIANT are nearly as efficient as predefined static columns. When dealing with data like logs, where fields are often added dynamically (such as container labels in Kubernetes), parsing JSON and inferring types can generate additional costs during write operations. Therefore, it's recommended to keep the number of columns for a single import below 1000.

Ensure consistency in types whenever possible. Doris automatically performs compatible type conversions. When a field cannot undergo compatible type conversion, it is uniformly converted to JSONB type. The performance of JSONB columns may degrade compared to columns like int or text.

1. tinyint -> smallint -> int -> bigint, integer types can be promoted following the direction of the arrows.
2. float -> double, floating-point numbers can be promoted following the direction of the arrow.
3. text, string type.
4. JSON, binary JSON type.

When the above types cannot be compatible, they will be transformed into JSON type to prevent loss of type information. If you need to set a strict schema in VARIANT, the VARIANT MAPPING mechanism will be introduced soon.

**Other limitations include:**

- Aggregate models are currently not supported.
- VARIANT columns can only create inverted indexes or bloom filter to speed up query.
- Using the **RANDOM** mode or [group commit](https://doris.apache.org/docs/dev/data-operate/import/import-way/group-commit-manual/) mode is recommended for higher write performance.
- Non-standard JSON types such as date and decimal should ideally use static types for better performance, since these types are infered to text type.
- Arrays with dimensions of 2 or higher will be stored as JSONB encoding, which might perform less efficiently than native arrays.
- Not supported as primary or sort keys.
- Queries with filters or aggregations require casting. The storage layer eliminates cast operations based on storage type and the target type of the cast, speeding up queries. 

### Keywords

    VARIANT