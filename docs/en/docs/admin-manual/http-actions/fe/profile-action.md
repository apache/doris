---
{
    "title": "Profile Action",
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

# Profile Action

## Request

`GET /api/profile`

## Description

Used to obtain the query profile of the specified query id.
    
## Path parameters

None

## Query parameters

* query_id

    Specify query id

## Request body

None

## Response

```
{
	"msg": "success",
	"code": 0,
	"data": {
		"profile": "query profile ..."
	},
	"count": 0
}
```
    
## Examples

1. Get the query profile of the specified query id

    ```
    GET /api/profile?query_id=f732084bc8e74f39-8313581c9c3c0b58
    
    Response:
    {
    	"msg": "success",
    	"code": 0,
    	"data": {
    		"profile": "query profile ..."
    	},
    	"count": 0
    }
    ```
