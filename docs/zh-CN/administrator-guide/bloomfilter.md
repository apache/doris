---
{
    "title": "BloomFilter索引",
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

# BloomFilter索引

BloomFilter是由Bloom在1970年提出的一种多哈希函数映射的快速查找算法。通常应用在一些需要快速判断某个元素是否属于集合，但是并不严格要求100%正确的场合，BloomFilter有以下特点：

- 空间效率高的概率型数据结构，用来检查一个元素是否在一个集合中。
- 对于一个元素检测是否存在的调用，BloomFilter会告诉调用者两个结果之一：可能存在或者一定不存在。
-  缺点是存在误判，告诉你可能存在，不一定真实存在。

布隆过滤器实际上是由一个超长的二进制位数组和一系列的哈希函数组成。二进制位数组初始全部为0，当给定一个待查询的元素时，这个元素会被一系列哈希函数计算映射出一系列的值，所有的值在位数组的偏移量处置为1。

下图所示出一个 m=18, k=3 （m是该Bit数组的大小，k是Hash函数的个数）的Bloom Filter示例。集合中的 x、y、z 三个元素通过 3 个不同的哈希函数散列到位数组中。当查询元素w时，通过Hash函数计算之后因为有一个比特为0，因此w不在该集合中。

![Bloom_filter.svg](/images/Bloom_filter.svg.png)

那么怎么判断谋和元素是否在集合中呢？同样是这个元素经过哈希函数计算后得到所有的偏移位置，若这些位置全都为1，则判断这个元素在这个集合中，若有一个不为1，则判断这个元素不在这个集合中。就是这么简单！

## Doris BloomFilter索引及使用使用场景

我们在使用HBase的时候，知道Hbase数据块索引提供了一个有效的方法，在访问一个特定的行时用来查找应该读取的HFile的数据块。但是它的效用是有限的。HFile数据块的默认大小是64KB，这个大小不能调整太多。

如果你要查找一个短行，只在整个数据块的起始行键上建立索引无法给你细粒度的索引信息。例如，如果你的行占用100字节存储空间，一个64KB的数据块包含(64 * 1024)/100 = 655.53 = ~700行，而你只能把起始行放在索引位上。你要查找的行可能落在特定数据块上的行区间里，但也不是肯定存放在那个数据块上。这有多种情况的可能，或者该行在表里不存在，或者存放在另一个HFile里，甚至在MemStore里。这些情况下，从硬盘读取数据块会带来IO开销，也会滥用数据块缓存。这会影响性能，尤其是当你面对一个巨大的数据集并且有很多并发读用户时。

所以HBase提供了布隆过滤器允许你对存储在每个数据块的数据做一个反向测试。当某行被请求时，先检查布隆过滤器看看该行是否不在这个数据块。布隆过滤器要么确定回答该行不在，要么回答它不知道。这就是为什么我们称它是反向测试。布隆过滤器也可以应用到行里的单元上。当访问某列标识符时先使用同样的反向测试。

布隆过滤器也不是没有代价。存储这个额外的索引层次占用额外的空间。布隆过滤器随着它们的索引对象数据增长而增长，所以行级布隆过滤器比列标识符级布隆过滤器占用空间要少。当空间不是问题时，它们可以帮助你榨干系统的性能潜力。

Doris的BloomFilter索引是从通过建表的时候指定，或者通过表的ALTER操作来完成。Bloom Filter本质上是一种位图结构，用于快速的判断一个给定的值是否在一个集合中。这种判断会产生小概率的误判。即如果返回false，则一定不在这个集合内。而如果范围true，则有可能在这个集合内。

BloomFilter索引也是以Block为粒度创建的。每个Block中，指定列的值作为一个集合生成一个BloomFilter索引条目，用于在查询是快速过滤不满足条件的数据。

下面我们通过实例来看看Doris怎么创建BloomFilter索引。

### 创建BloomFilter索引

Doris BloomFilter索引的创建是通过在建表语句的PROPERTIES里加上"bloom_filter_columns"="k1,k2,k3",这个属性，k1,k2,k3是你要创建的BloomFilter索引的Key列名称，例如下面我们对表里的saler_id,category_id创建了BloomFilter索引。

```sql
CREATE TABLE IF NOT EXISTS sale_detail_bloom  (
    sale_date date NOT NULL COMMENT "销售时间",
    customer_id int NOT NULL COMMENT "客户编号",
    saler_id int NOT NULL COMMENT "销售员",
    sku_id int NOT NULL COMMENT "商品编号",
    category_id int NOT NULL COMMENT "商品分类",
    sale_count int NOT NULL COMMENT "销售数量",
    sale_price DECIMAL(12,2) NOT NULL COMMENT "单价",
    sale_amt DECIMAL(20,2)  COMMENT "销售总金额"
)
Duplicate  KEY(sale_date, customer_id,saler_id,sku_id,category_id)
PARTITION BY RANGE(sale_date)
(
PARTITION P_202111 VALUES [('2021-11-01'), ('2021-12-01'))
)
DISTRIBUTED BY HASH(saler_id) BUCKETS 10
PROPERTIES (
"replication_num" = "3",
"bloom_filter_columns"="saler_id,category_id",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "MONTH",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"dynamic_partition.start" = "-2147483648",
"dynamic_partition.end" = "2",
"dynamic_partition.prefix" = "P_",
"dynamic_partition.replication_num" = "3",
"dynamic_partition.buckets" = "3"
);
```

### 查看BloomFilter索引

查看我们在表上建立的BloomFilter索引是使用:

```
SHOW CREATE TABLE <table_name>
```

### 删除BloomFilter索引

删除索引即为将索引列从bloom_filter_columns属性中移除：

```
ALTER TABLE <db.table_name> SET ("bloom_filter_columns" = "");
```

### 修改BloomFilter索引

修改索引即为修改表的bloom_filter_columns属性：

```
ALTER TABLE <db.table_name> SET ("bloom_filter_columns" = "k1,k3");
```

### **Doris BloomFilter使用场景**

满足以下几个条件时可以考虑对某列建立Bloom Filter 索引：

1. 首先BloomFilter适用于非前缀过滤.

2. 查询会根据该列高频过滤，而且查询条件大多是in和 = 过滤.

3. 不同于Bitmap, BloomFilter适用于高基数列。比如UserID。因为如果创建在低基数的列上，比如”性别“列，则每个Block几乎都会包含所有取值，导致BloomFilter索引失去意义

### **Doris BloomFilter使用注意事项**

1. 不支持对Tinyint、Float、Double 类型的列建Bloom Filter索引。

2. Bloom Filter索引只对in和 = 过滤查询有加速效果。
3. 如果要查看某个查询是否命中了Bloom Filter索引，可以通过查询的Profile信息查看
