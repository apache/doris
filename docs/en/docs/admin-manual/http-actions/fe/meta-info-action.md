---
{
    "title": "Meta Info Action",
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

# Meta Action

Meta Info Action is used to obtain metadata information in the cluster. Such as database list, table structure, etc.

## List Database

### Request

```
GET /api/meta/namespaces/<ns_name>/databases
```

### Description

Get a list of all database names, arranged in alphabetical order.
    
### Path parameters

* `<ns_name>`

    Specify cluster name, default value is 'default_cluster'

### Query parameters

* `limit`

    Limit the number of result rows returned
    
* `offset`

    Pagination information, need to be used with `limit`
    
### Request body

None

### Response

```
{
	"msg": "OK",
	"code": 0,
	"data": [
	   "db1", "db2", "db3", ...  
	],
	"count": 3
}
```

* The data field returns a list of database names.

## List Table

### Request

```
GET /api/meta/namespaces/<ns_name>/databases/<db_name>/tables
```

### Description

Get a list of tables in the specified database, arranged in alphabetical order.
    
### Path parameters
* `<ns_name>`

  Specify cluster name, default value is 'default_cluster'

* `<db_name>`

    Specify database

### Query parameters

* `limit`

    Limit the number of result rows returned
    
* `offset`

    Pagination information, need to be used with `limit`

### Request body

None

### Response

```
{
	"msg": "OK",
	"code": 0,
	"data": [
	   "tbl1", "tbl2", "tbl3", ...  
	],
	"count": 0
}
```

* The data field returns a list of table names.

## Schema Info

### Request

```
GET /api/meta/namespaces/<ns_name>/databases/<db_name>/tables/<tbl_name>/schema
```

### Description

Get the table structure information of the specified table in the specified database.
    
### Path parameters
* `<ns_name>`

  Specify cluster name, default value is 'default_cluster'

* `<db_name>`

    Specify the database name
    
* `<tbl_name>`

    Specify table name

### Query parameters

* `with_mv`

    Optional. If not specified, the table structure of the base table is returned by default. If specified, all rollup index will also be returned.

### Request body

None

### Response

```
GET /api/meta/namespaces/default/databases/db1/tables/tbl1/schema

{
	"msg": "success",
	"code": 0,
	"data": {
		"tbl1": {
			"schema": [{
					"Field": "k1",
					"Type": "INT",
					"Null": "Yes",
					"Extra": "",
					"Default": null,
					"Key": "true"
				},
				{
					"Field": "k2",
					"Type": "INT",
					"Null": "Yes",
					"Extra": "",
					"Default": null,
					"Key": "true"
				}
			],
			"is_base": true
		}
	},
	"count": 0
}
```

```
GET /api/meta/namespaces/default/databases/db1/tables/tbl1/schema?with_mv?=1

{
	"msg": "success",
	"code": 0,
	"data": {
		"tbl1": {
			"schema": [{
					"Field": "k1",
					"Type": "INT",
					"Null": "Yes",
					"Extra": "",
					"Default": null,
					"Key": "true"
				},
				{
					"Field": "k2",
					"Type": "INT",
					"Null": "Yes",
					"Extra": "",
					"Default": null,
					"Key": "true"
				}
			],
			"is_base": true
		},
		"rollup1": {
			"schema": [{
				"Field": "k1",
				"Type": "INT",
				"Null": "Yes",
				"Extra": "",
				"Default": null,
				"Key": "true"
			}],
			"is_base": false
		}
	},
	"count": 0
}
```

* The data field returns the table structure information of the base table or rollup table.
