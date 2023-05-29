---
{
    "title": "Statement Execution Action",
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

# Statement Execution Action


## Request

```
POST /api/query/<ns_name>/<db_name>
```

## Description

Statement Execution Action is used to execute a statement and return the result.
    
## Path parameters
* `<ns_name>`

    Specify cluster name, default value is 'default_cluster'

* `<db_name>`

    Specify the database name. This database will be regarded as the default database of the current session. If the table name in SQL does not qualify the database name, this database will be used.

## Query parameters

None

## Request body

```
{
    "stmt" : "select * from tbl1"
}
```

* sql 字段为具体的 SQL

### Response

* 返回结果集

    ```
    {
        "msg": "success",
        "code": 0,
        "data": {
            "type": "result_set",
            "data": [
                [1],
                [2]
            ],
            "meta": [{
                "name": "k1",
                "type": "INT"
            }],
            "status": {},
            "time": 10
        },
        "count": 0
    }
    ```

    * The type field is `result_set`, which means the result set is returned. The results need to be obtained and displayed based on the meta and data fields. The meta field describes the column information returned. The data field returns the result row. The column type in each row needs to be judged by the content of the meta field. The status field returns some information of MySQL, such as the number of alarm rows, status code, etc. The time field return the execution time, unit is millisecond.

* Return execution result

    ```
    {
        "msg": "success",
        "code": 0,
        "data": {
            "type": "exec_status",
            "status": {}
        },
        "count": 0,
        "time": 10
    }
    ```

    * The type field is `exec_status`, which means the execution result is returned. At present, if the return result is received, it means that the statement was executed successfully.
