---
{
    "title": "NGram BloomFilter Index",
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

# [Experimental] NGram BloomFilter Index

<version since="2.0.0">
</version>

In order to improve the like query performance, the NGram BloomFilter index was implemented, which referenced to the ClickHouse's ngrambf skip indices。
NGram BloomFilter can accelerate the calculation of like query, equals query, and in query.

## Create Column With NGram BloomFilter Index

During create table：

```sql
CREATE TABLE `table3` (
  `siteid` int(11) NULL DEFAULT "10" COMMENT "",
  `citycode` smallint(6) NULL COMMENT "",
  `username` varchar(100) NULL DEFAULT "" COMMENT "",
  INDEX idx_ngrambf (`username`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256") COMMENT 'username ngram_bf index'
) ENGINE=OLAP
AGGREGATE KEY(`siteid`, `citycode`, `username`) COMMENT "OLAP"
DISTRIBUTED BY HASH(`siteid`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);

-- PROPERTIES("gram_size"="3", "bf_size"="1024")，indicate the number of gram and bytes of bloom filter respectively.
-- the gram size set to same as the like query pattern string length. and the suitable bytes of bloom filter can be get by test, more larger more better, 256 maybe is a good start.
-- Usually, if the data's cardinality is small, you can increase the bytes of bloom filter to improve the efficiency.
-- the value range of gram_size is [1, 256], and the value range of bf_size is [64, 65536].
```

## Show NGram BloomFilter Index

```sql
show index from example_db.table3;
```

## Drop NGram BloomFilter Index


```sql
alter table example_db.table3 drop index idx_ngrambf;
```

## Add NGram BloomFilter Index

Add NGram BloomFilter Index for old column:

```sql
alter table example_db.table3 add index idx_ngrambf(username) using NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="512")comment 'username ngram_bf index' 
```

## Query Example

Use clickbench 20 million pieces of data to display equivalent query, in query, and like query under ngram index, including a simple comparison with query performance without index.

### Equivalent Query
- Equivalent query with index takes 0.08s, which is 3.5 times that of equivalent query without index.
```sql
MySQL [clickbench]> select count(*)  from hits_url4 where url_ngram6 = 'http://lk.wildberries.ru/with_video';
+----------+
| count(*) |
+----------+
|      525 |
+----------+
1 row in set (0.08 sec)
```

- Equivalent query without index takes 0.28s
```sql
MySQL [clickbench]> select count(*)  from hits_url4 where url = 'http://lk.wildberries.ru/with_video';
+----------+
| count(*) |
+----------+
|      525 |
+----------+
1 row in set (0.28 sec)
```

### In Query
- In query with index takes 0.08s, which is 3.5 times that of in query without index
```sql
MySQL [clickbench]> select count(*)  from hits_url4 where url_ngram6 in ('http://lk.wildberries.ru/with_video');
+----------+
| count(*) |
+----------+
|      525 |
+----------+
1 row in set (0.08 sec)
```

- In query without index takes 0.29s
```sql
MySQL [clickbench]> select count(*)  from hits_url4 where url in ('http://lk.wildberries.ru/with_video');
+----------+
| count(*) |
+----------+
|      525 |
+----------+
1 row in set (0.29 sec)
```

### Like Query
- Like query with index takes 0.10s, which is 8.3 times that of like query without index, and can support case sensitivity.
```sql
MySQL [clickbench]> select count(*) from hits_url4 where url_ngram3 like '%google%';
+----------+
| count(*) |
+----------+
|     1278 |
+----------+
1 row in set (0.10 sec)
```

- Like query without index takes 0.83s
```sql
MySQL [clickbench]> select count(*) from hits_url4 where url like '%google%';
+----------+
| count(*) |
+----------+
|     1278 |
+----------+
1 row in set (0.83 sec)
```

## **Some notes about Doris NGram BloomFilter**

1. NGram BloomFilter only support CHAR/VARCHAR/String column.
2. NGram BloomFilter index and BloomFilter index should be exclusive on same column
3. The gram number and bytes of BloomFilter can be adjust and optimize. Like if gram is too small, you can increase the bytes of BloomFilter.
4. To find some query whether use the NGram BloomFilter index, you can check the query profile.
