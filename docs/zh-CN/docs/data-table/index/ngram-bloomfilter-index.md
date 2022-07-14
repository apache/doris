---
{
    "title": "NGram BloomFilter索引",
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

# Doris NGram BloomFilter索引及使用使用场景

为了提升like的查询性能，增加了NGram BloomFilter索引，其实现主要参照了ClickHouse的ngrambf。

## NGram BloomFilter创建

表创建时指定：

```sql
CREATE TABLE `table3` (
  `siteid` int(11) NULL DEFAULT "10" COMMENT "",
  `citycode` smallint(6) NULL COMMENT "",
  `username` varchar(32) NULL DEFAULT "" COMMENT "",
  INDEX idx_ngrambf (`username`) USING NGRAM_BF (3,256) COMMENT 'username ngram_bf index'
) ENGINE=OLAP
AGGREGATE KEY(`siteid`, `citycode`, `username`) COMMENT "OLAP"
DISTRIBUTED BY HASH(`siteid`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);

-- 其中(3,256)，分别表示ngram的个数和bloomfilter的字节数。
```

## 查看NGram BloomFilter索引

查看我们在表上建立的NGram BloomFilter索引是使用:

```sql
show index from example_db.table3;
```

## 删除NGram BloomFilter索引


```sql
alter table example_db.table3 drop index idx_ngrambf;
```

## 修改NGram BloomFilter索引

为已有列新增NGram BloomFilter索引：

```sql
alter table example_db.table3 add index idx_ngrambf(username) using NGRAM_BF(3, 256) comment 'username ngram_bf index' 
```

## **Doris NGram BloomFilter使用注意事项**

1. NGram BloomFilter只支持字符串列
2. NGram BloomFilter索引和BloomFilter索引为互斥关系，即同一个列只能设置两者中的一个
3. NGram大小和BloomFilter的字节数，可以根据实际情况调优，如果NGram比较小，可以适当增加BloomFilter大小
4. 如果要查看某个查询是否命中了NGram Bloom Filter索引，可以通过查询的Profile信息查看
