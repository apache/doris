---
{
    "title": "QUERY DETAIL",
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

# QUERY DETAIL
   
Collect the query details from FE. You should set the event_time.
FE will return the query detail after the event_time.
The unit of event_time is milliseconds.

```
curl -X GET http://fe_host:fe_http_port/api/query_detail?event_time=1592054515284
```

The query details will be be returned as JSON
```
[
  {
    "eventTime": 1592201405063,
    "queryId": "a0a9259df9844029-845331577440a3bd",
    "startTime": 1592201405055,
    "endTime": 1592201405063,
    "latency": 8,
    "state": "FINISHED",
    "database": "test",
    "sql": "select * from table1"
  }, 
  {
    "eventTime": 1592201420842,
    "queryId": "21cd79c3e1634e8a-bdac090c7e7bcc36",
    "startTime": 1592201420834,
    "endTime": 1592201420842,
    "latency": 8,
    "state": "FINISHED",
    "database": "test",
    "sql": "select * from table1"
  }
]
```
