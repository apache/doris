---
{
    "title": "RETENTION_COUNT",
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

## RETENTION_COUNT

<version since="1.2.0">

RETENTION_INFO

</version>

### description
#### Syntax

`retention_count(varchar retention) RETURNS varchar`

对全量用户事件进行留存分析。

#### Arguments

`retention` — 每一个用户的留存事件数据，`retention_info()`函数的输出结果。该函数需要和`retention_info()`函数配合使用

##### Returned value

对全量用户事件进行留存分析的结果。`m:n:count` 表示查询开始时间之后的第m天发生了首次事件，同时在该首次事件发生之后的第n天发生了留存事件的用户数量为count。其中，`m:-1:count` 表示查询开始时间之后的第m天发生首次事件的用户数量为count。`m:0:count` 表示查询开始时间之后的第m天发生首次事件，同时当天发生了留存事件的用户数量为count（留存事件发生的时间在当天发生首次事件之后）。

### example

```sql
 CREATE TABLE `retention_analysis_test` (
  `distinct_id` int(11) NULL,
  `event_name` varchar(64) NULL,
  `timestamp` bigint(20) NULL
) ENGINE=OLAP
DUPLICATE KEY(`distinct_id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`distinct_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"storage_format" = "V2",
); 

insert into retention_analysis_test values (1, "view", 1685584800000), (2, "view", 1685584800000), (3, "view", 1685584800000), (4, "view", 1685584800000), (5, "view", 1685584800000), (6, "view", 1685584800000), (7, "view", 1685584800000), (8, "view", 1685584800000), (9, "view", 1685584800000), (10, "view", 1685584800000);
insert into retention_analysis_test values (1, "download", 1685602800000), (2, "download", 1685602800000), (3, "download", 1685602800000), (4, "download", 1685602800000), (5, "download", 1685602800000), (6, "download", 1685602800000);
insert into retention_analysis_test values (1, "view", 1685671200000), (2, "view", 1685671200000), (3, "view", 1685671200000), (4, "view", 1685671200000), (5, "view", 1685671200000), (6, "view", 1685671200000), (7, "view", 1685671200000), (8, "view", 1685671200000);
insert into retention_analysis_test values (1, "download", 1685689200000), (2, "download", 1685689200000), (3, "download", 1685689200000), (4, "download", 1685689200000), (5, "download", 1685689200000);
insert into retention_analysis_test values (1, "view", 1685757600000), (2, "view", 1685757600000), (3, "view", 1685757600000), (4, "view", 1685757600000), (5, "view", 1685757600000), (6, "view", 1685757600000), (7, "view", 1685757600000), (8, "view", 1685757600000), (9, "view", 1685757600000), (10, "view", 1685757600000), (11, "view", 1685757600000);
insert into retention_analysis_test values (1, "download", 1685775600000), (2, "download", 1685775600000), (3, "download", 1685775600000);

SELECT retention_count(c.retention_info)
FROM (
	SELECT distinct_id
		, retention_info(1685548800000, "day", timestamp, CASE 
			WHEN event_name = "view" THEN 1
			ELSE 0
		END | CASE 
			WHEN event_name = "download" THEN 2
			ELSE 0
		END) AS retention_info
	FROM retention_analysis_test
	WHERE timestamp >= 1685548800000
	GROUP BY distinct_id
) c;
```
查询结果
```
+-------------------------------------------------------------+
| retention_count(`c`.`retention_info`)                       |
+-------------------------------------------------------------+
| 0:-1:10;0:0:6;0:1:5;0:2:3;1:-1:8;1:0:5;1:1:3;2:-1:11;2:0:3; |
+-------------------------------------------------------------+
```
`0:-1:10`表示第0天（留存分析起始时间当天）发生了初次事件的用户数为10。

`0:0:6`表示第0天发生了初次事件的用户中，之后第0天（当日）发生了留存事件的用户数为6。

`0:1:5`表示第0天发生了初次事件的用户中，之后第1天发生了留存事件的用户数为5。

`0:2:3`表示第0天发生了初次事件的用户中，之后第2天发生了留存事件的用户数为3。

`1:-1:8`表示第1天（留存分析起始时间之后的第1天）发生了初次事件的用户数为8。

`1:0:5`表示第1天发生了初次事件的用户中，之后第0天（当日）发生了留存事件的用户数为5。

`1:1:3`表示第1天发生了初次事件的用户中，之后第1天发生了留存事件的用户数为3。

`2:-1:11`表示第2天（留存分析起始时间之后的第2天）发生了初次事件的用户数为1。

`2:0:3`表示第2天发生了初次事件的用户中，之后第0天（当日）发生了留存事件的用户数为3。

### keywords

RETENTION_COUNT
