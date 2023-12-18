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

Debug point is a piece of code, inserted into FE or BE code, when program running into this code, 

it can change variables or behaviors of the program. 

It is mainly used for unit test or regression test when it is impossible to trigger some exceptions through normal means.

Each debug point has a name, we can name it whatever we want, there are swithes to enable and disable debug points, 

and we can also pass data to debug points.

Both FE and BE support debug point, and after inserting the debug point code, we need to recompile FE or BE.

## Code Example

FE example

```java
private Status foo() {
	// dbug_fe_foo_do_nothing is the debug point name.
	// When it's active，DebugPointUtil.isEnable("dbug_fe_foo_do_nothing") will return true.
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
     // When it's active，DBUG_EXECUTE_IF will execute the code block.
     DBUG_EXECUTE_IF("dbug_be_foo_do_nothing",  { return Status.Nothing; });
   
     do_foo_action();
     
     return Status.Ok;
}
```


## Global Config

To enable debug points globally, we need to set `enable_debug_points` to true.

`enable_debug_points` is located in FE's fe.conf and BE's be.conf。


## Activate Specified Debug Point

After debug points are enabled globally, 
we need to specify a debug point name we want to activate, by sending a http request to FE or BE node,
only after that, when program running into the specified debug point, related code can be executed.

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


After activating debug point `foo`, executed no more than five times.
	
	
```
curl -X POST "http://127.0.0.1:8030/api/debug_point/add/foo?execute=5"

```


## Pass Custom Parameters
When activating debug point, besides "timeout" and "execute" mentioned above, we can also pass our own parameters.

### API

```
POST /api/debug_point/add/{debug_point_name}[?k1=v1&k2=v2&k3=v3...]
```
* `k1=v1` <br>
  The parameters are passed in a key=value fashion, k1 is parameter name and v1 is parameter value, <br>
  multiple key-value pairs are concatenated by `&`.

  
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
Assuming a FE node with configuration http_port=8030 in fe.conf, the following http request activates a debug point named `foo` in FE node, and passes two parameters:
		
```
curl -u root: -X POST "http://127.0.0.1:8030/api/debug_point/add/foo?percent=0.5&duration=3"
```

```
NOTE:
1. Inside FE and BE code, names and values of parameters are taken as strings.
2. Parameter names and values in http request and FE/BE code are case sensitive.
3. FE and BE share the same url path, it's just their IPs and Ports are different.
```

### Using parameters in FE and BE code
Following request activates debug point `OlapTableSink.write_random_choose_sink` in FE and passes two parameters, `needCatchUp` and `sinkNum`: 
```
curl -u root: -X POST "http://127.0.0.1:8030/api/debug_point/add/OlapTableSink.write_random_choose_sink?needCatchUp=true&sinkNum=3"
```

The code in FE checks debug point `OlapTableSink.write_random_choose_sink` and gets values of parameter `needCatchUp` and `sinkNum`:
```java
private void debugWriteRandomChooseSink(Tablet tablet, long version, Multimap<Long, Long> bePathsMap) {
    DebugPoint debugPoint = DebugPointUtil.getDebugPoint("OlapTableSink.write_random_choose_sink");
    if (debugPoint == null) {
        return;
    }
    boolean needCatchup = debugPoint.param("needCatchUp", false);
    int sinkNum = debugPoint.param("sinkNum", 0);
    ...
}
```

Following request activates debug point `TxnManager.prepare_txn.random_failed` in BE and passes one parameter:
```
curl -X POST "http://127.0.0.1:8040/api/debug_point/add/TxnManager.prepare_txn.random_failed?percent=0.7
```

The code in BE checks debug point `TxnManager.prepare_txn.random_failed` and gets value of parameter `percent`:
```c++
DBUG_EXECUTE_IF("TxnManager.prepare_txn.random_failed",
		{if (rand() % 100 < (100 * dp->param("percent", 0.5))) {
		        LOG_WARNING("TxnManager.prepare_txn.random_failed random failed");
		        return Status::InternalError("debug prepare txn random failed");
		}}
);
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

## Debug Points in Regression Test

In regression test, the debug points are especially useful, and the framework provides methods to activate debug points.
We can also use debug points in regression tests, in the test suite context, we can use GetDebugPoint().enableDebugPointForAllBEs(), to activate debug points in BE and GetDebugPoint().enableDebugPointForAllFEs(), to activate debug points in FEs before executing the test action.

### Examples

```java
suite('debugpoint_action', 'nonConcurrent') {
    try {
        GetDebugPoint().enableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')
        GetDebugPoint().enableDebugPointForAllBEs('Tablet.build_tablet_report_info.version_miss')
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')
        GetDebugPoint().disableDebugPointForAllBEs('Tablet.build_tablet_report_info.version_miss')
    }
}
```
The enableDebugPointForAllFE/senableDebugPointForAllBEs is declared as :
def enableDebugPointForAllFEs(String name, Map<String, String> params = null)
def enableDebugPointForAllBEs(String name, Map<String, String> params = null)
where the first parameter is name of debug point to activate, the second parameter is a set of key-value pairs passed to the debug point.

```java
suite('debugpoint_action', 'nonConcurrent') {
    try {
        GetDebugPoint().enableDebugPointForAllFEs('PublishVersionDaemon.stop_publish', [timeout:1])
        GetDebugPoint().enableDebugPointForAllBEs('Tablet.build_tablet_report_info.version_miss', [tablet_id:'12345', version_miss:true, timeout:1])
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')
        GetDebugPoint().disableDebugPointForAllBEs('Tablet.build_tablet_report_info.version_miss')
    }
}
```





