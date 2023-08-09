---
{
    "title": "WINDOW_FUNCTION_WINDOW_FUNNEL",
    "language": "zh-CN"
}
---

<!--  Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License. -->

## WINDOW FUNCTION WINDOW_FUNNEL
### description

漏斗分析函数搜索滑动时间窗口内最大的发生的最大事件序列长度。

- window ：滑动时间窗口大小，单位为秒。
- mode ：模式，共有四种模式
    - "default": 默认模式。
    - "deduplication": 当某个事件重复发生时，这个重复发生的事件会阻止后续的处理过程。如，指定事件链为[event1='A', event2='B', event3='C', event4='D']，原始事件链为"A-B-C-B-D"。由于B事件重复，最终的结果事件链为A-B-C，最大长度为3。
    - "fixed": 不允许事件的顺序发生交错，即事件发生的顺序必须和指定的事件链顺序一致。如，指定事件链为[event1='A', event2='B', event3='C', event4='D']，原始事件链为"A-B-D-C"，则结果事件链为A-B，最大长度为2
    - "increase": 选中的事件的时间戳必须按照指定事件链严格递增。
- timestamp_column ：指定时间列，类型为DATETIME, 滑动窗口沿着此列工作。
- eventN ：表示事件的布尔表达式。

漏斗分析函数按照如下算法工作：

- 搜索到满足满足条件的第一个事件，设置事件长度为1，此时开始滑动时间窗口计时。
- 如果事件在时间窗口内按照指定的顺序发生，时间长度累计增加。如果事件没有按照指定的顺序发生，时间长度不增加。
- 如果搜索到多个事件链，漏斗分析函数返回最大的长度。

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
