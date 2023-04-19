---
{
    "title": "Token BloomFilter Index",
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

# [Experimental] Token BloomFilter Index

<version since="2.0.0">
</version>

In order to improve the query performance of the text, the Token BloomFilter index is added, and its implementation mainly refers to the tokenbf of ClickHouse. Token BloomFilter splits tokens by non-digits and non-characters, so the main scenario is applied to the query of English document words.

## Create Column With Token BloomFilter Index

During create table：

```sql
CREATE TABLE `table3` (
  `siteid` int(11) NULL DEFAULT "10" COMMENT "",
  `citycode` smallint(6) NULL COMMENT "",
  `username` varchar(100) NULL DEFAULT "" COMMENT "",
  INDEX idx_tokenbf (`username`) USING TOKEN_BF PROPERTIES("gram_size"="3", "bf_size"="256") COMMENT 'username token_bf index'
) ENGINE=OLAP
AGGREGATE KEY(`siteid`, `citycode`, `username`) COMMENT "OLAP"
DISTRIBUTED BY HASH(`siteid`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);

-- PROPERTIES("gram_size"="3", "bf_size"="1024")，indicate the number of gram and bytes of bloom filter respectively.
-- the gram size set to same as the like query pattern string length. and the suitable bytes of bloom filter can be get by test, more larger more better, 256 maybe is a good start.
-- Usually, if the data's cardinality is small, you can increase the bytes of bloom filter to improve the efficiency.
```

## Show Token BloomFilter Index

```sql
show index from example_db.table3;
```

## Drop Token BloomFilter Index


```sql
alter table example_db.table3 drop index idx_tokenbf;
```

## Add Token BloomFilter Index

Add Token BloomFilter Index for old column:

```sql
alter table example_db.table3 add index idx_tokenbf(username) using Token_BF PROPERTIES("bf_size"="512")comment 'username token_bf index' 
```

## **Some notes about Doris Token BloomFilter**

1. Token BloomFilter only support CHAR/VARCHAR/String column.
2. Token BloomFilter index, NGram BloomFilter index and BloomFilter index should be exclusive on same column
3. The gram number and bytes of BloomFilter can be adjust and optimize. Like if gram is too small, you can increase the bytes of BloomFilter.
4. To find some query whether use the Token BloomFilter index, you can check the query profile.
