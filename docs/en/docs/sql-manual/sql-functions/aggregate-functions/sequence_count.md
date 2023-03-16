---
{
    "title": "SEQUENCE-COUNT",
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

## SEQUENCE-COUNT
### Description
#### Syntax

`sequence_count(pattern, timestamp, cond1, cond2, ...);`

Counts the number of event chains that matched the pattern. The function searches event chains that do not overlap. It starts to search for the next chain after the current chain is matched.

**WARNING!** 

Events that occur at the same second may lay in the sequence in an undefined order affecting the result.

#### Arguments

`pattern` — Pattern string.

**Pattern syntax**

`(?N)` — Matches the condition argument at position N. Conditions are numbered in the `[1, 32]` range. For example, `(?1)` matches the argument passed to the `cond1` parameter.

`.*` — Matches any number of events. You do not need conditional arguments to count this element of the pattern.

`(?t operator value)` —  Sets the time in seconds that should separate two events.

We define `t` as the difference in seconds between two times,  For example, pattern `(?1)(?t>1800)(?2)` matches events that occur more than 1800 seconds from each other. pattern `(?1)(?t>10000)(?2)` matches events that occur more than 10000 seconds from each other. An arbitrary number of any events can lay between these events. You can use the `>=`, `>`, `<`, `<=`, `==` operators.

`timestamp` — Column considered to contain time data. Typical data types are `Date` and `DateTime`. You can also use any of the supported UInt data types.

`cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. You can pass up to 32 condition arguments. The function takes only the events described in these conditions into account. If the sequence contains data that isn’t described in a condition, the function skips them.

#### Returned value

Number of non-overlapping event chains that are matched.

### example

**count examples**

```sql
DROP TABLE IF EXISTS sequence_count_test2;

CREATE TABLE sequence_count_test2(
                `uid` int COMMENT 'user id',
                `date` datetime COMMENT 'date time', 
                `number` int NULL COMMENT 'number' 
                )
DUPLICATE KEY(uid) 
DISTRIBUTED BY HASH(uid) BUCKETS 3 
PROPERTIES ( 
    "replication_num" = "1"
); 

INSERT INTO sequence_count_test2(uid, date, number) values (1, '2022-11-02 10:41:00', 1),
                                                   (2, '2022-11-02 13:28:02', 2),
                                                   (3, '2022-11-02 16:15:01', 1),
                                                   (4, '2022-11-02 19:05:04', 2),
                                                   (5, '2022-11-02 20:08:44', 3); 

SELECT * FROM sequence_count_test2 ORDER BY date;

+------+---------------------+--------+
| uid  | date                | number |
+------+---------------------+--------+
|    1 | 2022-11-02 10:41:00 |      1 |
|    2 | 2022-11-02 13:28:02 |      2 |
|    3 | 2022-11-02 16:15:01 |      1 |
|    4 | 2022-11-02 19:05:04 |      2 |
|    5 | 2022-11-02 20:08:44 |      3 |
+------+---------------------+--------+

SELECT sequence_count('(?1)(?2)', date, number = 1, number = 3) FROM sequence_count_test2;

+----------------------------------------------------------------+
| sequence_count('(?1)(?2)', `date`, `number` = 1, `number` = 3) |
+----------------------------------------------------------------+
|                                                              1 |
+----------------------------------------------------------------+

SELECT sequence_count('(?1)(?2)', date, number = 1, number = 2) FROM sequence_count_test2;

+----------------------------------------------------------------+
| sequence_count('(?1)(?2)', `date`, `number` = 1, `number` = 2) |
+----------------------------------------------------------------+
|                                                              2 |
+----------------------------------------------------------------+

SELECT sequence_count('(?1)(?t>=10000)(?2)', date, number = 1, number = 2) FROM sequence_count_test1;

+---------------------------------------------------------------------------+
| sequence_count('(?1)(?t>=3600)(?2)', `date`, `number` = 1, `number` = 2) |
+---------------------------------------------------------------------------+
|                                                                         2 |
+---------------------------------------------------------------------------+
```

**not count examples**

```sql
DROP TABLE IF EXISTS sequence_count_test1;

CREATE TABLE sequence_count_test1(
                `uid` int COMMENT 'user id',
                `date` datetime COMMENT 'date time', 
                `number` int NULL COMMENT 'number' 
                )
DUPLICATE KEY(uid) 
DISTRIBUTED BY HASH(uid) BUCKETS 3 
PROPERTIES ( 
    "replication_num" = "1"
); 

INSERT INTO sequence_count_test1(uid, date, number) values (1, '2022-11-02 10:41:00', 1),
                                                   (2, '2022-11-02 11:41:00', 7),
                                                   (3, '2022-11-02 16:15:01', 3),
                                                   (4, '2022-11-02 19:05:04', 4),
                                                   (5, '2022-11-02 21:24:12', 5);

SELECT * FROM sequence_count_test1 ORDER BY date;

+------+---------------------+--------+
| uid  | date                | number |
+------+---------------------+--------+
|    1 | 2022-11-02 10:41:00 |      1 |
|    2 | 2022-11-02 11:41:00 |      7 |
|    3 | 2022-11-02 16:15:01 |      3 |
|    4 | 2022-11-02 19:05:04 |      4 |
|    5 | 2022-11-02 21:24:12 |      5 |
+------+---------------------+--------+

SELECT sequence_count('(?1)(?2)', date, number = 1, number = 2) FROM sequence_count_test1;

+----------------------------------------------------------------+
| sequence_count('(?1)(?2)', `date`, `number` = 1, `number` = 2) |
+----------------------------------------------------------------+
|                                                              0 |
+----------------------------------------------------------------+

SELECT sequence_count('(?1)(?2).*', date, number = 1, number = 2) FROM sequence_count_test1;

+------------------------------------------------------------------+
| sequence_count('(?1)(?2).*', `date`, `number` = 1, `number` = 2) |
+------------------------------------------------------------------+
|                                                                0 |
+------------------------------------------------------------------+

SELECT sequence_count('(?1)(?t>3600)(?2)', date, number = 1, number = 7) FROM sequence_count_test1;

+--------------------------------------------------------------------------+
| sequence_count('(?1)(?t>3600)(?2)', `date`, `number` = 1, `number` = 7) |
+--------------------------------------------------------------------------+
|                                                                        0 |
+--------------------------------------------------------------------------+
```

**special examples**

```sql
DROP TABLE IF EXISTS sequence_count_test3;

CREATE TABLE sequence_count_test3(
                `uid` int COMMENT 'user id',
                `date` datetime COMMENT 'date time', 
                `number` int NULL COMMENT 'number' 
                )
DUPLICATE KEY(uid) 
DISTRIBUTED BY HASH(uid) BUCKETS 3 
PROPERTIES ( 
    "replication_num" = "1"
); 

INSERT INTO sequence_count_test3(uid, date, number) values (1, '2022-11-02 10:41:00', 1),
                                                   (2, '2022-11-02 11:41:00', 7),
                                                   (3, '2022-11-02 16:15:01', 3),
                                                   (4, '2022-11-02 19:05:04', 4),
                                                   (5, '2022-11-02 21:24:12', 5);

SELECT * FROM sequence_count_test3 ORDER BY date;

+------+---------------------+--------+
| uid  | date                | number |
+------+---------------------+--------+
|    1 | 2022-11-02 10:41:00 |      1 |
|    2 | 2022-11-02 11:41:00 |      7 |
|    3 | 2022-11-02 16:15:01 |      3 |
|    4 | 2022-11-02 19:05:04 |      4 |
|    5 | 2022-11-02 21:24:12 |      5 |
+------+---------------------+--------+
```

Perform the query:

```sql
SELECT sequence_count('(?1)(?2)', date, number = 1, number = 5) FROM sequence_count_test3;

+----------------------------------------------------------------+
| sequence_count('(?1)(?2)', `date`, `number` = 1, `number` = 5) |
+----------------------------------------------------------------+
|                                                              1 |
+----------------------------------------------------------------+
```

This is a very simple example. The function found the event chain where number 5 follows number 1. It skipped number 7,3,4 between them, because the number is not described as an event. If we want to take this number into account when searching for the event chain given in the example, we should make a condition for it.

Now, perform this query:

```sql
SELECT sequence_count('(?1)(?2)', date, number = 1, number = 5, number = 4) FROM sequence_count_test3;

+------------------------------------------------------------------------------+
| sequence_count('(?1)(?2)', `date`, `number` = 1, `number` = 5, `number` = 4) |
+------------------------------------------------------------------------------+
|                                                                            0 |
+------------------------------------------------------------------------------+
```

The result is kind of confusing. In this case, the function couldn’t find the event chain matching the pattern, because the event for number 4 occurred between 1 and 5. If in the same case we checked the condition for number 6, the sequence would count the pattern.

```sql
SELECT sequence_count('(?1)(?2)', date, number = 1, number = 5, number = 6) FROM sequence_count_test3;

+------------------------------------------------------------------------------+
| sequence_count('(?1)(?2)', `date`, `number` = 1, `number` = 5, `number` = 6) |
+------------------------------------------------------------------------------+
|                                                                            1 |
+------------------------------------------------------------------------------+
```

### keywords

SEQUENCE_COUNT