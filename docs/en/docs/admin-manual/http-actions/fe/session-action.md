---
{
    "title": "Session Action",
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

# Session Action

## Request

`GET /rest/v1/session`

<version since="dev">

`GET /rest/v1/session/all`

</version>

## Description

Session Action is used to obtain the current session information.
    
## Path parameters

None

## Query parameters

None

## Request body

None

## Obtain the current session information

`GET /rest/v1/session`

## Response

```
{
	"msg": "success",
	"code": 0,
	"data": {
		"column_names": ["Id", "User", "Host", "Cluster", "Db", "Command", "Time", "State", "Info"],
		"rows": [{
			"User": "root",
			"Command": "Sleep",
			"State": "",
			"Cluster": "default_cluster",
			"Host": "10.81.85.89:31465",
			"Time": "230",
			"Id": "0",
			"Info": "",
			"Db": "db1"
		}]
	},
	"count": 2
}
```

## Obtain all FE session information

`GET /rest/v1/session/all`

## Response

```
{
	"msg": "success",
	"code": 0,
	"data": {
		"column_names": ["FE", "Id", "User", "Host", "Cluster", "Db", "Command", "Time", "State", "Info"],
		"rows": [{
		    "FE": "10.14.170.23",
			"User": "root",
			"Command": "Sleep",
			"State": "",
			"Cluster": "default_cluster",
			"Host": "10.81.85.89:31465",
			"Time": "230",
			"Id": "0",
			"Info": "",
			"Db": "db1"
		},
		{
            "FE": "10.14.170.24",
			"User": "root",
			"Command": "Sleep",
			"State": "",
			"Cluster": "default_cluster",
			"Host": "10.81.85.88:61465",
			"Time": "460",
			"Id": "1",
			"Info": "",
			"Db": "db1"
		}]
	},
	"count": 2
}
```
    
The returned result is the same as `System Action`. Is a description of the table.
