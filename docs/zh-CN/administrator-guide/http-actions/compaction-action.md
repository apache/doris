---
{
    "title": "Compaction Action",
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

# Compaction Action

该 API 用于查看某个 BE 节点总体的 compaction 状态，或者指定 tablet 的 compaction 状态。也可以用于手动触发 Compaction。

## 查看 Compaction 状态

### 节点整体 compaction 状态

(TODO)

### 指定 tablet 的 compaction 状态

```
curl -X GET http://be_host:webserver_port/api/compaction/show?tablet_id=xxxx\&schema_hash=yyyy
```

若 tablet 不存在，返回 JSON 格式的错误：

```
{
    "status": "Fail",
    "msg": "Tablet not found"
}
```

若 tablet 存在，则返回 JSON 格式的结果:

```
{
    "cumulative point": 50,
    "last cumulative failure time": "2019-12-16 18:13:43.224",
    "last base failure time": "2019-12-16 18:13:23.320",
    "last cumu success time": "2019-12-16 18:12:15.110",
    "last base success time": "2019-12-16 18:11:50.780",
    "rowsets": [
        "[0-48] 10 DATA OVERLAPPING",
        "[49-49] 2 DATA OVERLAPPING",
        "[50-50] 0 DELETE NONOVERLAPPING",
        "[51-51] 5 DATA OVERLAPPING"
    ]
}
```

结果说明：

* cumulative point：base 和 cumulative compaction 的版本分界线。在 point（不含）之前的版本由 base compaction 处理。point（含）之后的版本由 cumulative compaction 处理。
* last cumulative failure time：上一次尝试 cumulative compaction 失败的时间。默认 10min 后才会再次尝试对该 tablet 做 cumulative compaction。
* last base failure time：上一次尝试 base compaction 失败的时间。默认 10min 后才会再次尝试对该 tablet 做 base compaction。
* rowsets：该 tablet 当前的 rowset 集合。如 [0-48] 表示 0-48 版本。第二位数字表示该版本中 segment 的数量。`DELETE` 表示 delete 版本。`DATA` 表示数据版本。`OVERLAPPING` 和 `NONOVERLAPPING` 表示segment数据是否重叠。

### 示例

```
curl -X GET http://192.168.10.24:8040/api/compaction/show?tablet_id=10015\&schema_hash=1294206575
```

## 手动触发 Compaction

(TODO)

