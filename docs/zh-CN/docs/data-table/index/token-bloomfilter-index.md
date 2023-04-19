---
{
    "title": "Token BloomFilter索引",
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

# [Experimental] Token BloomFilter索引及使用使用场景

<version since="2.0.0">
</version>

为了提升文本的查询性能，增加了Token BloomFilter索引，其实现主要参照了ClickHouse的tokenbf。Token BloomFilter按非数字非字符进行分割token，因此主要场景应用于对英文文档单词的查询。

## Token BloomFilter创建

表创建时指定：

```sql
CREATE TABLE `table3` (
  `siteid` int(11) NULL DEFAULT "10" COMMENT "",
  `citycode` smallint(6) NULL COMMENT "",
  `username` varchar(32) NULL DEFAULT "" COMMENT "",
  INDEX idx_tokenbf (`username`) USING TOKEN_BF PROPERTIES("bf_size"="256") COMMENT 'username token_bf index'
) ENGINE=OLAP
AGGREGATE KEY(`siteid`, `citycode`, `username`) COMMENT "OLAP"
DISTRIBUTED BY HASH(`siteid`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);

-- PROPERTIES("bf_size"="256")，分别表示bloom filter的字节数。
-- bloom filter字节数，可以通过测试得出，通常越大过滤效果越好，可以从256开始进行验证测试看看效果。当然字节数越大也会带来索引存储、内存cost上升。
-- 如果数据基数比较高，字节数可以不用设置过大，如果基数不是很高，可以通过增加字节数来提升过滤效果。
```

## 查看Token BloomFilter索引

查看我们在表上建立的Token BloomFilter索引是使用:

```sql
show index from example_db.table3;
```

## 删除Token BloomFilter索引


```sql
alter table example_db.table3 drop index idx_tokenbf;
```

## 修改Token BloomFilter索引

为已有列新增Token BloomFilter索引：

```sql
alter table example_db.table3 add index idx_tokenbf(username) using TOKEN_BF PROPERTIES("bf_size"="512")comment 'username token_bf index' 
```

## **Doris Token BloomFilter使用注意事项**

1. Token BloomFilter只支持字符串列
2. Token BloomFilter索引、BloomFilter索引、NGram BloomFilter为互斥关系，即同一个列只能设置两者中的一个
3. BloomFilter的字节数，可以根据实际情况调优，可以适当增加BloomFilter大小
4. 如果要查看某个查询是否命中了Token Bloom Filter索引，可以通过查询的Profile信息查看
