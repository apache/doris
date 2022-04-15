---
{
    "title": "Check Storage Type Action",
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

# Check Storage Type Action

## Request

`GET /api/_check_storagetype`

## Description

It is used to check whether the storage format of the table under the specified database is the row storage format. (The row format is deprecated)
    
## Path parameters

None

## Query parameters

* `db`

    Specify the database

## Request body

None

## Response

```
{
	"msg": "success",
	"code": 0,
	"data": {
		"tbl2": {},
		"tbl1": {}
	},
	"count": 0
}
```

If there is content after the table name, the base or rollup table whose storage format is row storage will be displayed.

## Examples

1. Check whether the storage format of the following table of the specified database is row format

    ```
    GET /api/_check_storagetype
    
    Response:
    {
    	"msg": "success",
    	"code": 0,
    	"data": {
    		"tbl2": {},
    		"tbl1": {}
    	},
    	"count": 0
    }
    ```