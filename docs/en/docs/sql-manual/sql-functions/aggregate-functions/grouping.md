---
{
    "title": "GROUPING",
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

## GROUPING

### Name

GROUPING

### Description

Indicates whether a specified column expression in a `GROUP BY` list is aggregated or not. `GROUPING` returns 1 for aggregated or 0 for not aggregated in the result set. `GROUPING` can be used only in the `SELECT <select> list`, `HAVING`, and `ORDER BY` clauses when `GROUP BY` is specified.

`GROUPING` is used to distinguish the null values that are returned by `ROLLUP`, `CUBE` or `GROUPING SETS` from standard null values. The `NULL` returned as the result of a `ROLLUP`, `CUBE` or `GROUPING SETS` operation is a special use of `NULL`. This acts as a column placeholder in the result set and means all.

```sql
GROUPING( <column_expression> )
```

`<column_expression>`
Is a column or an expression that contains a column in a `GROUP BY` clause.

Return Types: BIGINT

### Example

The following example groups `camp` and aggregates `occupation` amounts in the database. The `GROUPING` function is applied to the `camp` column.

```sql
CREATE TABLE `roles` (
  role_id       INT,
  occupation    VARCHAR(32),
  camp          VARCHAR(32),
  register_time DATE
)
UNIQUE KEY(role_id)
DISTRIBUTED BY HASH(role_id) BUCKETS 1
PROPERTIES (
  "replication_allocation" = "tag.location.default: 1"
);

INSERT INTO `roles` VALUES
(0, 'who am I', NULL, NULL),
(1, 'mage', 'alliance', '2018-12-03 16:11:28'),
(2, 'paladin', 'alliance', '2018-11-30 16:11:28'),
(3, 'rogue', 'horde', '2018-12-01 16:11:28'),
(4, 'priest', 'alliance', '2018-12-02 16:11:28'),
(5, 'shaman', 'horde', NULL),
(6, 'warrior', 'alliance', NULL),
(7, 'warlock', 'horde', '2018-12-04 16:11:28'),
(8, 'hunter', 'horde', NULL);

SELECT 
  camp, 
  COUNT(occupation) AS 'occ_cnt',
  GROUPING(camp)    AS 'grouping'
FROM
   `roles`
GROUP BY
  ROLLUP(camp); -- CUBE(camp) and GROUPING SETS((camp)) also can work;
```

The result set shows two null value under `camp`. The first NULL is in the summary row added by the `ROLLUP` operation. The summary row shows the occupation counts for all `camp` groups and is indicated by 1 in the Grouping column. The second NULL represents the group of null values from this column in the table.

Here is the result set.

```log
+----------+---------+----------+
| camp     | occ_cnt | grouping |
+----------+---------+----------+
| NULL     |       9 |        1 |
| NULL     |       1 |        0 |
| alliance |       4 |        0 |
| horde    |       4 |        0 |
+----------+---------+----------+
4 rows in set (0.01 sec)
```

### Keywords

GROUPING

### Best Practice

See also [GROUPING_ID](./grouping_id.md)
