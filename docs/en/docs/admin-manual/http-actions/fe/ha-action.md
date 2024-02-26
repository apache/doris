---
{
    "title": "HA Action",
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

# HA Action

## Request

```
GET /rest/v1/ha
```

## Description

HA Action is used to obtain the high availability group information of the FE cluster.
    
## Path parameters

None

## Query parameters

None

## Request body

None

## Response

```
{
	"msg": "success",
	"code": 0,
	"data": {
		"Observernodes": [],
		"CurrentJournalId": [{
			"Value": 433648,
			"Name": "FrontendRole"
		}],
		"Electablenodes": [{
			"Value": "host1",
			"Name": "host1"
		}],
		"allowedFrontends": [{
			"Value": "name: 192.168.1.1_9213_1597652404352, role: FOLLOWER, 192.168.1.1:9213",
			"Name": "192.168.1.1_9213_1597652404352"
		}],
		"removedFrontends": [],
		"CanRead": [{
			"Value": true,
			"Name": "Status"
		}],
		"databaseNames": [{
			"Value": "433436 ",
			"Name": "DatabaseNames"
		}],
		"FrontendRole": [{
			"Value": "MASTER",
			"Name": "FrontendRole"
		}],
		"CheckpointInfo": [{
			"Value": 433435,
			"Name": "Version"
		}, {
			"Value": "2020-09-03T02:07:37.000+0000",
			"Name": "lastCheckPointTime"
		}]
	},
	"count": 0
}
```
    
