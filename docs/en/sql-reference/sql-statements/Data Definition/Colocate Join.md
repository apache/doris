---
{
    "title": "Colocate Join",
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

# Colocate Join
## Description
Colocate/Local Join means that when multiple nodes are Join, there is no data movement and network transmission, and each node is only Join locally.
The premise of Join locally is to import data from the same Join Key into a fixed node according to the same rules.

1 How To Use:

Simply add the property colocate_with when building a table. The value of colocate_with can be set to any one of the same set of colocate tables.
However, you need to ensure that tables in the colocate_with attribute are created first.

If you need to Colocate Join table t1 and t2, you can build tables according to the following statements:

CREATE TABLE `t1` (
`id` int(11) COMMENT "",
'value ` varchar (8) COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"colocate_with" = "t1"
);

CREATE TABLE `t2` (
`id` int(11) COMMENT "",
'value ` varchar (8) COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"colocate_with" = "t1"
);

2 Colocate Join 目前的限制:

1. Colcoate Table must be an OLAP-type table
2. The BUCKET number of tables with the same colocate_with attribute must be the same
3. The number of copies of tables with the same colocate_with attribute must be the same
4. Data types of DISTRIBUTED Columns for tables with the same colocate_with attribute must be the same

3 Colocate Join's applicable scenario:

Colocate Join is well suited for scenarios where tables are bucketed according to the same field and high frequency according to the same field Join.

4 FAQ:

Q: 支持多张表进行Colocate Join 吗?

A: 25903;. 25345

Q: Do you support Colocate table and normal table Join?

A: 25903;. 25345

Q: Does the Colocate table support Join with non-bucket Key?

A: Support: Join that does not meet Colocate Join criteria will use Shuffle Join or Broadcast Join

Q: How do you determine that Join is executed according to Colocate Join?

A: The child node of Hash Join in the result of explain is Colocate Join if it is OlapScanNode directly without Exchange Node.

Q: How to modify the colocate_with attribute?

A: ALTER TABLE example_db.my_table set ("colocate_with"="target_table");

Q: 229144; colcoate join?

A: set disable_colocate_join = true; 就可以禁用Colocate Join, 查询时就会使用Shuffle Join 和Broadcast Join

## keyword

COLOCATE, JOIN, CREATE TABLE
