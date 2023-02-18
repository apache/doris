---
{
    "title": "GROUPING",
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

## GROUPING

### Name

GROUPING

### Description

用在含有 CUBE、ROLLUP 或 GROUPING SETS 的 SQL 语句中，用于表示进行 CUBE、ROLLUP 或 GROUPING SETS 操作的列是否汇总。当结果集中的数据行是 CUBE、ROLLUP 或 GROUPING SETS 操作产生的汇总结果时，该函数返回 1，否则返回 0。GROUPING 函数可以在 `SELECT`、`HAVING` 和 `ORDER BY` 子句当中使用。

`ROLLUP`、`CUBE` 或 `GROUPING SETS` 操作返回的汇总结果，会用 NULL 充当被分组的字段的值。因此，`GROUPING` 通常用于区分 `ROLLUP`、`CUBE` 或 `GROUPING SETS` 返回的空值与表中的空值。

```sql
GROUPING( <column_expression> )
```

`<column_expression>`
是在 `GROUP BY` 子句中包含的列或表达式。

返回值：BIGINT

### Example

下面的例子使用 `camp` 列进行分组操作，并对 `occupation` 的数量进行汇总，`GROUPING` 函数作用于 `camp` 列。

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
  ROLLUP(camp); -- CUBE(camp) 和 GROUPING SETS((camp)) 同样也有效;
```

结果集在 `camp` 列下有两个 NULL 值，第一个 NULL 值表示 `ROLLUP` 操作的列的汇总结果，这一行的 `occ_cnt` 列表示所有 `camp` 的 `occupation` 的计数结果，在 `grouping` 函数中返回 1。第二个 NULL 表示 `camp` 列中本来就存在的 NULL 值。

结果集如下：

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

还可参阅 [GROUPING_ID](./grouping_id.md)
