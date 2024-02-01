---
{
    "title": "WINDOW_FUNCTION_WINDOW_FUNNEL",
    "language": "en"
}
---

<!--  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

## WINDOW FUNCTION WINDOW_FUNNEL
### description

Searches the longest event chain happened in order (event1, event2, ... , eventN) along the timestamp_column with length of window.

- window is the length of time window in seconds.
- mode can be one of the followings:
    - "default": Defualt mode.
    - "deduplication": If the same event holds for the sequence of events, then such repeating event interrupts further processing. E.g. the array parameter is [event1='A', event2='B', event3='C', event4='D'], and the original event chain is "A-B-C-B-D". Since event B repeats, the filtered event chain can only be "A-B-C" and the max event level is 3.
    - "fixed": Don't allow interventions of other events. E.g. the array parameter is [event1='A', event2='B', event3='C', event4='D'], and the original event chain is A->B->D->C, it stops finding A->B->C at the D and the max event level is 2.
    - "increase": Apply conditions only to events with strictly increasing timestamps.
- timestamp_column specifies column of DATETIME type, sliding time window works on it.
- evnetN is boolean expression like eventID = 1004.

The function works according to the algorithm:

- The function searches for data that triggers the first condition in the chain and sets the event counter to 1. This is the moment when the sliding window starts.
- If events from the chain occur sequentially within the window, the counter is incremented. If the sequence of events is disrupted, the counter is not incremented.
- If the data has multiple event chains at varying points of completion, the function will only output the size of the longest chain.

```sql
window_funnel(window, mode, timestamp_column, event1, event2, ... , eventN)
```

### example

```sql
CREATE TABLE windowfunnel_test (
                `xwho` varchar(50) NULL COMMENT 'xwho',
                `xwhen` datetime COMMENT 'xwhen',
                `xwhat` int NULL COMMENT 'xwhat'
                )
DUPLICATE KEY(xwho)
DISTRIBUTED BY HASH(xwho) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

INSERT into windowfunnel_test (xwho, xwhen, xwhat) values ('1', '2022-03-12 10:41:00', 1),
                                                   ('1', '2022-03-12 13:28:02', 2),
                                                   ('1', '2022-03-12 16:15:01', 3),
                                                   ('1', '2022-03-12 19:05:04', 4);

select window_funnel(3600 * 3, 'default', t.xwhen, t.xwhat = 1, t.xwhat = 2 ) AS level from windowfunnel_test t;

| level |
|---|
| 2 |
```

### keywords

    WINDOW,FUNCTION,WINDOW_FUNNEL
