---
{
    "title": "ALTER-ROUTINE-LOAD",
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

## ALTER-ROUTINE-LOAD

### Name

ALTER ROUTINE LOAD

### Description

该语法用于修改已经创建的例行导入作业。

只能修改处于 PAUSED 状态的作业。

语法：

```sql
ALTER ROUTINE LOAD FOR [db.]job_name
[job_properties]
FROM data_source
[data_source_properties]
```

1. `[db.]job_name`

    指定要修改的作业名称。

2. `tbl_name`

    指定需要导入的表的名称。

3. `job_properties`

    指定需要修改的作业参数。目前仅支持如下参数的修改：

    1. `desired_concurrent_number`
    2. `max_error_number`
    3. `max_batch_interval`
    4. `max_batch_rows`
    5. `max_batch_size`
    6. `jsonpaths`
    7. `json_root`
    8. `strip_outer_array`
    9. `strict_mode`
    10. `timezone`
    11. `num_as_string`
    12. `fuzzy_parse`
    13. `partial_columns`
    14. `max_filter_ratio`


4. `data_source`

    数据源的类型。当前支持：

    KAFKA

5. `data_source_properties`

    数据源的相关属性。目前仅支持：

    1. `kafka_partitions`
    2. `kafka_offsets`
    3. `kafka_broker_list`
    4. `kafka_topic`
    5. 自定义 property，如 `property.group.id`

    注：

    1. `kafka_partitions` 和 `kafka_offsets` 用于修改待消费的 kafka partition 的offset，仅能修改当前已经消费的 partition。不能新增 partition。

### Example

1. 将 `desired_concurrent_number` 修改为 1

    ```sql
    ALTER ROUTINE LOAD FOR db1.label1
    PROPERTIES
    (
        "desired_concurrent_number" = "1"
    );
    ```

2.  将 `desired_concurrent_number` 修改为 10，修改 partition 的offset，修改 group id。

    ```sql
    ALTER ROUTINE LOAD FOR db1.label1
    PROPERTIES
    (
        "desired_concurrent_number" = "10"
    )
    FROM kafka
    (
        "kafka_partitions" = "0, 1, 2",
        "kafka_offsets" = "100, 200, 100",
        "property.group.id" = "new_group"
    );

### Keywords

    ALTER, ROUTINE, LOAD

### Best Practice

