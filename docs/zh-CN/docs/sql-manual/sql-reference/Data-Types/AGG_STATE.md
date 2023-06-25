---
{
    "title": "AGG_STATE",
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

## AGG_STATE
### description
    AGG_STATE
    AGG_STATE不能作为key列使用，建表时需要同时声明聚合函数的签名。
    用户不需要指定长度和默认值。实际存储的数据大小与函数实现有关。
    
  AGG_STATE只能配合[state](../../sql-functions/combinators/state.md)
    /[merge](../../sql-functions/combinators/merge.md)/[union](../..//sql-functions/combinators/union.md)函数组合器使用。
    
  需要注意的是，聚合函数的签名也是类型的一部分，不同签名的agg_state无法混合使用。比如如果建表声明的签名为`max_by(int,int)`,那就无法插入`max_by(bigint,int)`或者`group_concat(varchar)`。
  此处nullable属性也是签名的一部分，如果能确定不会输入null值，可以将参数声明为not null，这样可以获得更小的存储大小和减少序列化/反序列化开销。

### example

建表示例如下：

      create table a_table(
          k1 int null,
          k2 agg_state max_by(int not null,int)
      )
      aggregate key (k1)
      distributed BY hash(k1) buckets 3
      properties("replication_num" = "1");

插入数据示例：

    insert into a_table values(1,max_by_state(3,1));

查询数据示例：

    select k1,max_by_merge(k2) from a_table group by k1 order by k1;

### keywords

    AGG_STATE
