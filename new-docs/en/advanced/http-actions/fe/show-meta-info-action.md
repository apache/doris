---
{
    "title": "Show Meta Info Action",
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

# Show Meta Info Action

## Request

`GET /api/show_meta_info`

## Description

Used to display some metadata information
    
## Path parameters

无

## Query parameters

* action

    Specify the type of metadata information to be obtained. Currently supports the following:
    
    * `SHOW_DB_SIZE`

        Get the data size of the specified database, in bytes.
        
    * `SHOW_HA`

        Obtain the playback status of FE metadata logs and the status of electable groups.

## Request body

None

## Response


* `SHOW_DB_SIZE`

    ```
    {
    	"msg": "success",
    	"code": 0,
    	"data": {
    		"default_cluster:information_schema": 0,
    		"default_cluster:db1": 381
    	},
    	"count": 0
    }
    ```
    
* `SHOW_HA`

    ```
    {
    	"msg": "success",
    	"code": 0,
    	"data": {
    		"can_read": "true",
    		"role": "MASTER",
    		"is_ready": "true",
    		"last_checkpoint_version": "1492",
    		"last_checkpoint_time": "1596465109000",
    		"current_journal_id": "1595",
    		"electable_nodes": "",
    		"observer_nodes": "",
    		"master": "10.81.85.89"
    	},
    	"count": 0
    }
    ```
    
## Examples

1. View the data size of each database in the cluster

    ```
    GET /api/show_meta_info?action=show_db_size
    
    Response:
    {
    	"msg": "success",
    	"code": 0,
    	"data": {
    		"default_cluster:information_schema": 0,
    		"default_cluster:db1": 381
    	},
    	"count": 0
    }
    ```
    
2. View the FE election group situation

    ```
    GET /api/show_meta_info?action=show_ha
    
    Response:
    {
    	"msg": "success",
    	"code": 0,
    	"data": {
    		"can_read": "true",
    		"role": "MASTER",
    		"is_ready": "true",
    		"last_checkpoint_version": "1492",
    		"last_checkpoint_time": "1596465109000",
    		"current_journal_id": "1595",
    		"electable_nodes": "",
    		"observer_nodes": "",
    		"master": "10.81.85.89"
    	},
    	"count": 0
    }
    ```
