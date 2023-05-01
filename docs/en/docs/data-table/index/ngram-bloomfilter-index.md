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
CREATE TABLE `hits_url` (
  `UserID` bigint(20) NOT NULL,
  `url` text NULL DEFAULT "",
  `url_ngram3` text NULL DEFAULT "",
  `url_ngram6` text NULL DEFAULT "",
  `url_inverted` text NULL DEFAULT "",
  INDEX idx_ngrambf (`url_ngram3`) USING NGRAM_BF PROPERTIES("gram_size" = "3", "bf_size" = "2048") COMMENT 'url_ngram ngram_bf index',
  INDEX idx_ngrambf2 (`url_ngram6`) USING NGRAM_BF PROPERTIES("gram_size" = "6", "bf_size" = "2048") COMMENT 'url_ngram ngram_bf index',
  INDEX idx_inverted (`url_inverted`) USING INVERTED PROPERTIES("parser" = "english") COMMENT 'url_inverted index'
) ENGINE=OLAP
DUPLICATE KEY(`UserID`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`UserID`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"storage_format" = "V2",
"light_schema_change" = "true"
);

-- PROPERTIES("gram_size"="3", "bf_size"="2048")，indicate the number of gram and bytes of bloom filter respectively.
-- the gram size set to same as the like query pattern string length. and the suitable bytes of bloom filter can be get by test, more larger more better, 256 maybe is a good start.
-- Usually, if the data's cardinality is small, you can increase the bytes of bloom filter to improve the efficiency.
-- the value range of gram_size is [1, 256], and the value range of bf_size is [64, 65536].
```

## Show NGram BloomFilter Index

```sql
show index from clickbench.hits_url;
```

## Drop NGram BloomFilter Index


```sql
alter table clickbench.hits_url drop index idx_ngrambf;
```

## Add NGram BloomFilter Index

Add NGram BloomFilter Index for old column:

```sql
alter table clickbench.hits_url add index idx_ngrambf(`url_ngram3`) using NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="2048")comment 'url_ngram3 ngram_bf index' 
```

## Query Example

Use clickbench 20 million pieces of data to display equivalent query, in query, and like query under ngram index, including a simple comparison with query performance without index.

### Equivalent Query
- Equivalent query with index takes 0.08s, which is 3.5 times that of equivalent query without index.
```sql
MySQL [clickbench]> select count(*)  from hits_url where url_ngram6 = 'http://lk.wildberries.ru/with_video';
+----------+
| count(*) |
+----------+
|      525 |
+----------+
1 row in set (0.08 sec)
```

- Equivalent query without index takes 0.28s
```sql
MySQL [clickbench]> select count(*)  from hits_url where url = 'http://lk.wildberries.ru/with_video';
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
MySQL [clickbench]> select count(*)  from hits_url where url_ngram6 in ('http://lk.wildberries.ru/with_video');
+----------+
| count(*) |
+----------+
|      525 |
+----------+
1 row in set (0.08 sec)
```

- In query without index takes 0.29s
```sql
MySQL [clickbench]> select count(*)  from hits_url where url in ('http://lk.wildberries.ru/with_video');
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
MySQL [clickbench]> select count(*) from hits_url where url_ngram3 like '%google%';
+----------+
| count(*) |
+----------+
|     1278 |
+----------+
1 row in set (0.10 sec)
```

- Like query without index takes 0.83s
```sql
MySQL [clickbench]> select count(*) from hits_url where url like '%google%';
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
