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

# [Experimental] NGram BloomFilter索引及使用使用场景

<version since="2.0.0">
</version>

为了提升like的查询性能，增加了NGram BloomFilter索引，其实现主要参照了ClickHouse的ngrambf。
NGram BloomFilter能够加速like、equals、in的计算。

## NGram BloomFilter创建

表创建时指定：

```sql
CREATE TABLE `table3` (
  `siteid` int(11) NULL DEFAULT "10" COMMENT "",
  `citycode` smallint(6) NULL COMMENT "",
  `username` varchar(32) NULL DEFAULT "" COMMENT "",
  INDEX idx_ngrambf (`username`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256") COMMENT 'username ngram_bf index'
) ENGINE=OLAP
AGGREGATE KEY(`siteid`, `citycode`, `username`) COMMENT "OLAP"
DISTRIBUTED BY HASH(`siteid`) BUCKETS 10
PROPERTIES (
"replication_num" = "1"
);

-- PROPERTIES("gram_size"="3", "bf_size"="256")，分别表示gram的个数和bloom filter的字节数。
-- gram的个数跟实际查询场景相关，通常设置为大部分查询字符串的长度，bloom filter字节数，可以通过测试得出，通常越大过滤效果越好，可以从256开始进行验证测试看看效果。当然字节数越大也会带来索引存储、内存cost上升。
-- 如果数据基数比较高，字节数可以不用设置过大，如果基数不是很高，可以通过增加字节数来提升过滤效果。
-- gram_size的取值范围在[1, 256], bf_size的取值范围在[64,65536]。
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
alter table example_db.table3 add index idx_ngrambf(username) using NGRAM_BF PROPERTIES("gram_size"="2", "bf_size"="512")comment 'username ngram_bf index' 
```

## 查询示例
用clickbench 2000万条数据展示ngram索引下等值查询、in查询、like查询，包括跟无索引的查询性能进行简单对比。

### 等值查询
- 有索引等值查询，耗时0.08s，是无索引等值查询的3.5倍
```sql
MySQL [clickbench]> select count(*)  from hits_url4 where url_ngram6 = 'http://lk.wildberries.ru/with_video';
+----------+
| count(*) |
+----------+
|      525 |
+----------+
1 row in set (0.08 sec)
```

- 无索引等值查询，耗时0.28s
```sql
MySQL [clickbench]> select count(*)  from hits_url4 where url = 'http://lk.wildberries.ru/with_video';
+----------+
| count(*) |
+----------+
|      525 |
+----------+
1 row in set (0.28 sec)
```

### in查询
- 有索引in查询，耗时0.08s，是无索引in查询的3.5倍
```sql
MySQL [clickbench]> select count(*)  from hits_url4 where url_ngram6 in ('http://lk.wildberries.ru/with_video');
+----------+
| count(*) |
+----------+
|      525 |
+----------+
1 row in set (0.08 sec)
```

- 无索引in查询，耗时0.28s
```sql
MySQL [clickbench]> select count(*)  from hits_url4 where url in ('http://lk.wildberries.ru/with_video');
+----------+
| count(*) |
+----------+
|      525 |
+----------+
1 row in set (0.29 sec)
```

### like查询
- 有索引like查询，耗时0.10s，是无索引like查询的8.3倍, 能够支持大小写敏感。
```sql
MySQL [clickbench]> select count(*) from hits_url4 where url_ngram3 like '%google%';
+----------+
| count(*) |
+----------+
|     1278 |
+----------+
1 row in set (0.10 sec)
```

- 无索引like查询，耗时0.83s
```sql
MySQL [clickbench]> select count(*) from hits_url4 where url like '%google%';
+----------+
| count(*) |
+----------+
|     1278 |
+----------+
1 row in set (0.83 sec)
```

## **Doris NGram BloomFilter使用注意事项**

1. NGram BloomFilter只支持字符串列
2. NGram BloomFilter索引和BloomFilter索引为互斥关系，即同一个列只能设置两者中的一个
3. NGram大小和BloomFilter的字节数，可以根据实际情况调优，如果NGram比较小，可以适当增加BloomFilter大小
4. 如果要查看某个查询是否命中了NGram Bloom Filter索引，可以通过查询的Profile信息查看
