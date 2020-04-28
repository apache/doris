---
{
    "title": "Compaction Action",
    "language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Compaction Action

This API is used to view the overall compaction status of a BE node or the compaction status of a specified tablet. It can also be used to manually trigger Compaction.

## View Compaction status

### The overall compaction status of the node

(TODO)

### Specify the compaction status of the tablet

```
curl -X GET http://be_host:webserver_port/api/compaction/show?tablet_id=xxxx\&schema_hash=yyyy
```

If the tablet does not exist, an error in JSON format is returned:

```
{
    "status": "Fail",
    "msg": "Tablet not found"
}
```

If the tablet exists, the result is returned in JSON format:

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

Explanation of results:

* cumulative point: The version boundary between base and cumulative compaction. Versions before (excluding) points are handled by base compaction. Versions after (inclusive) are handled by cumulative compaction.
* last cumulative failure time: The time when the last cumulative compaction failed. After 10 minutes by default, cumulative compaction is attempted on the this tablet again.
* last base failure time: The time when the last base compaction failed. After 10 minutes by default, base compaction is attempted on the this tablet again.
* rowsets: The current rowsets collection of this tablet. [0-48] means a rowset with version 0-48. The second number is the number of segments in a rowset. The `DELETE` indicates the delete version. `OVERLAPPING` and `NONOVERLAPPING` indicates whether data between segments is overlap.

### Examples

```
curl -X GET http://192.168.10.24:8040/api/compaction/show?tablet_id=10015\&schema_hash=1294206575
```

## Manually trigger Compaction

(TODO)
