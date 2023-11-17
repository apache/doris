---
{
    "title": "Min Load Replica Num",
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

# Min Load Replica Num

Importing data requires more than half of the replicas to be written successfully. However, it is not flexible enough and may cause inconvenience in some scenarios.

For example, in the case of two replicas, to import data, both replicas need to be written successfully. This means that no replica is allowed to be unavailable during the data import process. This greatly affects the availability of the cluster.

In order to solve the above problems, Doris allows users to set the minimum number of write replicas. For the task of importing data, when the number of replicas it successfully writes is greater than or equal to the minimum number of replicas written, the import is successful.

## Usage

### Min load replica num for single table

You can set the table property `min_load_replica_num` for a single olap table. The valid value of this property must be greater than 0 and not exceed `replication_num`（the number of replicas of the table）. Its default value is -1, indicating that the property is not enabled.

The `min_load_replica_num` of the table can be set when creating the table.

```sql
CREATE TABLE test_table1
(
    k1 INT,
    k2 INT
)
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 5
PROPERTIES
(
    'replication_num' = '2',
    'min_load_replica_num' = '1'
);
```

For an existing table, you can use `ALTER TABLE` to modify its `min_load_replica_num`.

```sql
ALTER TABLE test_table1
SET ( 'min_load_replica_num' = '1');
```

You can use `SHOW CREATE TABLE` to view the table property `min_load_replica_num`.

```SQL
SHOW CREATE TABLE test_table1;
```

The PROPERTIES of the output will contain `min_load_replica_num`. e.g.

```text
Create Table: CREATE TABLE `test_table1` (
  `k1` int(11) NULL,
  `k2` int(11) NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`k1`) BUCKETS 5
PROPERTIES (
"replication_allocation" = "tag.location.default: 2",
"min_load_replica_num" = "1",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false"
);
```

### Global min load replica num for all tables

You can set FE configuration item `min_load_replica_num` for all olap tables. The valid value of this configuration item must be greater than 0. Its default value is -1, which means that the global minimum number of load replicas is not enabled.

For a table, if the table property `min_load_replica_num` is valid (>0), then the table will ignore the global configuration `min_load_replica_num`. Otherwise, if the global configuration `min_load_replica_num` is valid (>0), then the minimum number of load replicas for the table will be equal to `min(FE.conf.min_load_replica_num, table.replication_num/2 + 1)`.

For viewing and modification of FE configuration items, you can refer to [here](../../../admin-manual/config/fe-config.md).

### Other cases

If the table property `min_load_replica_num` is not enabled (<=0), and the global configuration `min_load_replica_num` is not enabled(<=0), then the data import still needs to be successfully written to the majority replica. At this point, the minimum number of write replicas for the table is equal to `table.replication_num/2 + 1`.
