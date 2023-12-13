---
{
    "title": "Debug Point",
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

# Debug Point

Debug point is used in code test, when enabled, it can run related code.

Both FE and BE support debug points.

## Code Example

FE example

```java
private Status foo() {
	// dbug_fe_foo_do_nothing is the debug point name.
	// When activated，DebugPointUtil.isEnable("dbug_fe_foo_do_nothing") will return true.
	if (DebugPointUtil.isEnable("dbug_fe_foo_do_nothing")) {
      	return Status.Nothing;
    }
      	
    do_foo_action();
    
    return Status.Ok;
}
```

BE example

```c++
void Status foo() {
     // dbug_be_foo_do_nothing is the debug point name.
     // When activated，DBUG_EXECUTE_IF will execute the code block.
     DBUG_EXECUTE_IF("dbug_be_foo_do_nothing",  { return Status.Nothing; });
   
     do_foo_action();
     
     return Status.Ok;
}
```

## Global config

To activate debug points, you need to set `enable_debug_points` to true.

`enable_debug_points` is located in FE's fe.conf and BE's be.conf。


## Enable Debug Point

### API

```
	POST /api/debug_point/add/{debug_point_name}[?timeout=<int>&execute=<int>]
```


### Query Parameters

* `debug_point_name`
    Debug point name. Required.

* `timeout`
    Timeout in seconds. When timeout, the debug point will be disable. Default is -1, never timeout. Optional.

* `execute`
    After activating, the max times the debug point can be executed. Default is -1,  unlimited times. Optional.  


### Request body

None

### Response

```
{
    msg: "OK",
    code: 0
}
```
    
### Examples


After enabling debug point `foo`, executed no more than five times.
	
	
```
curl -X POST "http://127.0.0.1:8030/api/debug_point/add/foo?execute=5"

```
NOTE: You need to send the http request first, then run the code containing the debug point.

## Passing custom parameters to debug point
Besides "timeout" and "execute" mentioned above, when activating debug point, you can also pass other parameters.

### API

```
POST /api/debug_point/add/{debug_point_name}[?key1=value1&key2=value2&key3=value3...]
```
* `key1=value1`
  Passing a key-value pairs to debug point, key1 is the parameter name and value1 is the parameter value,
  multiple key-value pairs are concatenated by '&'.
### Request body

无

### Response

```
{
    msg: "OK",
    code: 0
}
```


## Disable Debug Point

### API

```
	POST /api/debug_point/remove/{debug_point_name}
```


### Query Parameters

* `debug_point_name`
    Debug point name. Require.
    


### Request body

None

### Response

```
    {
        msg: "OK",
        code: 0
    }
```
    
### Examples


Disable debug point `foo`。
	
	
    ```
    curl -X POST "http://127.0.0.1:8030/api/debug_point/remove/foo"

    ```
    
## Clear Debug Points

### API

```
	POST /api/debug_point/clear
```



### Request body

None

### Response

    ```
    {
        msg: "OK",
        code: 0
    }
    ```
    
### Examples

	
    ```
    curl -X POST "http://127.0.0.1:8030/api/debug_point/clear"
    ```
