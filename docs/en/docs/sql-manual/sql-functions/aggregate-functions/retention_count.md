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

Retention analysis for all user events.

#### Arguments

Retention event data for each user, output of the `retention_info()` function. This function needs to be used in conjunction with the `retention_info()` function

##### Returned value

Result of retention analysis for all user events.`m:n:count` indicates that the first event occurred on the `m-th` day after the start time, and the number of users who had a retention event on the `n-th` day after the first event occurred is `count`. In addition, `m:-1:count` indicates that the number of users whose first event occurred on the `m-th` day after the start time is `count`, `m:0:count` indicates that the first event occurred on the `m-th` day after the start time, and the number of users with a retention event on that day is `count` (the retention event occurred on the same day with the first event).

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

the result of query:
```
+-------------------------------------------------------------+
| retention_count(`c`.`retention_info`)                       |
+-------------------------------------------------------------+
| 0:-1:10;0:0:6;0:1:5;0:2:3;1:-1:8;1:0:5;1:1:3;2:-1:11;2:0:3; |
+-------------------------------------------------------------+
```
`0:-1:10` means that the number of users whose first event occurred on the 0-th day after the start time is 10.

`0:0:6` means that the first event occurred on the 0-th day after the start time, and the number of users with a retention event on that day is 6.

`0:1:5` means that the first event occurred on the 0-th day after the start time, and the number of users who had a retention event on the 1-th day after the first event occurred is 5.

`0:2:3` means that the first event occurred on the 0-th day after the start time, and the number of users who had a retention event on the 2-th day after the first event occurred is 3.

`1:-1:8` means that the number of users whose first event occurred on the 1-th day after the start time is 8.

`1:0:5` means that the first event occurred on the 1-th day after the start time, and the number of users with a retention event on that day is 5.

`1:1:3` means that the first event occurred on the 1-th day after the start time, and the number of users who had a retention event on the 1-th day after the first event occurred is 3.

`2:-1:11` means that the number of users whose first event occurred on the 2-th day after the start time is 11.

`2:0:3` means that the first event occurred on the 2-th day after the start time, and the number of users with a retention event on that day is 3.

### keywords

RETENTION_COUNT
