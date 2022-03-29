---
{
    "title": "Get Load State",
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

# Get Load State

## Request

`GET /api/<db>/get_load_state`

## Description

Returns the status of the load transaction of the specified label
    
## Path parameters

* `<db>`

    Specify database

## Query parameters

* `label`

    Specify label

## Request body

None

## Response

```
{
	"msg": "success",
	"code": 0,
	"data": "VISIBLE",
	"count": 0
}
```

RETURN VALUES:

- `msg`: Error message. If there are no errors, it displays "success"

- `code`: Doris's internal error code. If there is no error, 0 is displayed.

- `data` for successful execution:
  - UNKNOWN: No corresponding Label was found
  - PREPARE: The corresponding transaction has been prepared, but not yet committed
  - PRECOMMITTED: The transaction has been prepared and can be rollback if failed to commit 
  - COMMITTED: The transaction has been committed and cannot be canceled
  - VISIBLE: Transaction submission, and data visible, cannot be canceled
  - ABORTED: The transaction has been ROLLBACK and the import has failed

- `count`: currently not used and always be 0

## Examples

1. Get the status of the load transaction of the specified label.

    ```
    GET /api/example_db/get_load_state?label=my_label
    
    {
    	"msg": "success",
    	"code": 0,
    	"data": "VISIBLE",
    	"count": 0
    }
    ```

2. Obtain the state with curl.

    ```
    curl -u <user[:password]> http://host:port/api/<db>/get_load_state?label=<label>
    ```
