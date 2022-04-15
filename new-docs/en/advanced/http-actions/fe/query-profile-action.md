---
{
    "title": "Query Profile Action",
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

# Query Profile Action

## Request

```
GET /rest/v1/query_profile/<query_id>
```

## Description

The Query Profile Action is used to obtain the Query profile.
    
## Path parameters

* `<query_id>`

    Optional parameters. When not specified, the latest query list is returned. When specified, return the profile of the specified query.

## Query parameters

无

## Request body

无

## Response

* Not specify `<query_id>`

    ```
    GET /rest/v1/query_profile/
    {
    	"msg": "success",
    	"code": 0,
    	"data": {
    		"href_column": ["Query ID"],
    		"column_names": ["Query ID", "User", "Default Db", "Sql Statement", "Query Type", "Start Time", "End Time", "Total", "Query State"],
    		"rows": [{
    			"User": "root",
    			"__hrefPath": ["/query_profile/d73a8a0b004f4b2f-b4829306441913da"],
    			"Query Type": "Query",
    			"Total": "5ms",
    			"Default Db": "default_cluster:db1",
    			"Sql Statement": "select * from tbl1",
    			"Query ID": "d73a8a0b004f4b2f-b4829306441913da",
    			"Start Time": "2020-09-03 10:07:54",
    			"Query State": "EOF",
    			"End Time": "2020-09-03 10:07:54"
    		}, {
    			"User": "root",
    			"__hrefPath": ["/query_profile/fd706dd066824c21-9d1a63af9f5cb50c"],
    			"Query Type": "Query",
    			"Total": "6ms",
    			"Default Db": "default_cluster:db1",
    			"Sql Statement": "select * from tbl1",
    			"Query ID": "fd706dd066824c21-9d1a63af9f5cb50c",
    			"Start Time": "2020-09-03 10:07:54",
    			"Query State": "EOF",
    			"End Time": "2020-09-03 10:07:54"
    		}]
    	},
    	"count": 3
    }
    ```
    
    The returned result is the same as `System Action`, which is a table description.
    
* Specify `<query_id>`

    ```
    GET /rest/v1/query_profile/<query_id>

    {
    	"msg": "success",
    	"code": 0,
    	"data": "Query:</br>&nbsp;&nbsp;&nbsp;&nbsp;Summary:</br>...",
    	"count": 0
    }
    ```
    
    `data` is the text content of the profile.