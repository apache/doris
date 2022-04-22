---
{
    "title": "CONNECTION",
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

# CONNECTION

To get current query_id from connection

```
curl -X GET http://fe_host:fe_http_port/api/connection?connection_id=123
```

If connection_id does not exist, return 404 NOT FOUND ERROR

If connection_id exists, return last query_id belongs to connection_id
```
{
    "query_id" : 9133b7efa92a44c8-8ed4b44772ec2a0c
}
```
