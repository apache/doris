---
{
    "title": "ALTER-TABLE-PROPERTY",
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

## ALTER-TABLE-PROPERTY

### Name

ALTER TABLE PROPERTY

### Description

该语句用于对已有 table 的 property 进行修改操作。这个操作是同步的，命令返回表示执行完毕。

语法：

```sql
ALTER TABLE [database.]table alter_clause;
```

property 的 alter_clause 支持如下几种修改方式

1. 修改表的 bloom filter 列

```sql
ALTER TABLE example_db.my_table SET ("bloom_filter_columns"="k1,k2,k3");
```

也可以合并到上面的 schema change 操作中（注意多子句的语法有少许区别）

```sql
ALTER TABLE example_db.my_table
DROP COLUMN col2
PROPERTIES ("bloom_filter_columns"="k1,k2,k3");
```

2. 修改表的Colocate 属性

```sql
ALTER TABLE example_db.my_table set ("colocate_with" = "t1");
```

3. 将表的分桶方式由 Hash Distribution 改为 Random Distribution

```sql
ALTER TABLE example_db.my_table set ("distribution_type" = "random");
```

4. 修改表的动态分区属性(支持未添加动态分区属性的表添加动态分区属性)

```sql
ALTER TABLE example_db.my_table set ("dynamic_partition.enable" = "false");
```

如果需要在未添加动态分区属性的表中添加动态分区属性，则需要指定所有的动态分区属性
   (注:非分区表不支持添加动态分区属性)

```sql
ALTER TABLE example_db.my_table set (
  "dynamic_partition.enable" = "true", 
  "dynamic_partition.time_unit" = "DAY", 
  "dynamic_partition.end" = "3", 
  "dynamic_partition.prefix" = "p", 
  "dynamic_partition.buckets" = "32"
);
```

5. 修改表的 in_memory 属性，只支持修改为'false'

```sql
ALTER TABLE example_db.my_table set ("in_memory" = "false");
```

6. 启用 批量删除功能

```sql
ALTER TABLE example_db.my_table ENABLE FEATURE "BATCH_DELETE";
```

注意：

- 只能用在unique 表
- 用于旧表支持批量删除功能，新表创建时已经支持

7. 启用按照sequence column的值来保证导入顺序的功能

```sql
ALTER TABLE example_db.my_table ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES (
  "function_column.sequence_type" = "Date"
);
```

注意：

- 只能用在unique 表
- sequence_type用来指定sequence列的类型，可以为整型和时间类型
- 只支持新导入数据的有序性，历史数据无法更改

8. 将表的默认分桶数改为50

```sql
ALTER TABLE example_db.my_table MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 50;
```

注意：

- 只能用在分区类型为RANGE，采用哈希分桶的非colocate表

9. 修改表注释

```sql
ALTER TABLE example_db.my_table MODIFY COMMENT "new comment";
```

10. 修改列注释

```sql
ALTER TABLE example_db.my_table MODIFY COLUMN k1 COMMENT "k1", MODIFY COLUMN k2 COMMENT "k2";
```

11. 修改引擎类型

    仅支持将 MySQL 类型修改为 ODBC 类型。driver 的值为 odbc.init 配置中的 driver 名称。

```sql
ALTER TABLE example_db.mysql_table MODIFY ENGINE TO odbc PROPERTIES("driver" = "MySQL");
```

12. 修改副本数

```sql
ALTER TABLE example_db.mysql_table SET ("replication_num" = "2");
ALTER TABLE example_db.mysql_table SET ("default.replication_num" = "2");
ALTER TABLE example_db.mysql_table SET ("replication_allocation" = "tag.location.default: 1");
ALTER TABLE example_db.mysql_table SET ("default.replication_allocation" = "tag.location.default: 1");
```

注：
1. default 前缀的属性表示修改表的默认副本分布。这种修改不会修改表的当前实际副本分布，而只影响分区表上新建分区的副本分布。
2. 对于非分区表，修改不带 default 前缀的副本分布属性，会同时修改表的默认副本分布和实际副本分布。即修改后，通过 `show create table` 和 `show partitions from tbl` 语句可以看到副本分布数据都被修改了。
3. 对于分区表，表的实际副本分布是分区级别的，即每个分区有自己的副本分布，可以通过 `show partitions from tbl` 语句查看。如果想修改实际副本分布，请参阅 `ALTER TABLE PARTITION`。

13\. **[Experimental]** 打开`light_schema_change`

  对于建表时未开启light_schema_change的表，可以通过如下方式打开。

```sql
ALTER TABLE example_db.mysql_table SET ("light_schema_change" = "true");
```

### Example

1. 修改表的 bloom filter 列

```sql
ALTER TABLE example_db.my_table SET (
  "bloom_filter_columns"="k1,k2,k3"
);
```

也可以合并到上面的 schema change 操作中（注意多子句的语法有少许区别）

```sql
ALTER TABLE example_db.my_table
DROP COLUMN col2
PROPERTIES (
  "bloom_filter_columns"="k1,k2,k3"
);
```

2. 修改表的Colocate 属性

```sql
ALTER TABLE example_db.my_table set ("colocate_with" = "t1");
```

3. 将表的分桶方式由 Hash Distribution 改为 Random Distribution

```sql
ALTER TABLE example_db.my_table set (
  "distribution_type" = "random"
);
```

4. 修改表的动态分区属性(支持未添加动态分区属性的表添加动态分区属性)

```sql
ALTER TABLE example_db.my_table set (
  "dynamic_partition.enable" = "false"
);
```

如果需要在未添加动态分区属性的表中添加动态分区属性，则需要指定所有的动态分区属性
   (注:非分区表不支持添加动态分区属性)

```sql
ALTER TABLE example_db.my_table set (
  "dynamic_partition.enable" = "true", 
  "dynamic_partition.time_unit" = "DAY", 
  "dynamic_partition.end" = "3", 
  "dynamic_partition.prefix" = "p", 
  "dynamic_partition.buckets" = "32"
);
```

5. 修改表的 in_memory 属性，只支持修改为'false'

```sql
ALTER TABLE example_db.my_table set ("in_memory" = "false");
```

6. 启用 批量删除功能

```sql
ALTER TABLE example_db.my_table ENABLE FEATURE "BATCH_DELETE";
```

7. 启用按照sequence column的值来保证导入顺序的功能

```sql
ALTER TABLE example_db.my_table ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES (
  "function_column.sequence_type" = "Date"
);
```

8. 将表的默认分桶数改为50

```sql
ALTER TABLE example_db.my_table MODIFY DISTRIBUTION DISTRIBUTED BY HASH(k1) BUCKETS 50;
```

9. 修改表注释

```sql
ALTER TABLE example_db.my_table MODIFY COMMENT "new comment";
```

10. 修改列注释

```sql
ALTER TABLE example_db.my_table MODIFY COLUMN k1 COMMENT "k1", MODIFY COLUMN k2 COMMENT "k2";
```

11. 修改引擎类型

```sql
ALTER TABLE example_db.mysql_table MODIFY ENGINE TO odbc PROPERTIES("driver" = "MySQL");
```

12. 给表添加冷热分层数据迁移策略
```sql
 ALTER TABLE create_table_not_have_policy set ("storage_policy" = "created_create_table_alter_policy");
```
注：表没有关联过storage policy，才能被添加成功，一个表只能添加一个storage policy

13. 给表的partition添加冷热分层数据迁移策略
```sql
ALTER TABLE create_table_partition MODIFY PARTITION (*) SET("storage_policy"="created_create_table_partition_alter_policy");
```
注：表的partition没有关联过storage policy，才能被添加成功，一个表只能添加一个storage policy
### Keywords

```text
ALTER, TABLE, PROPERTY, ALTER TABLE
```

### Best Practice

