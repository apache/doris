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

Each debug point has a name, the name can be whatever you want, there are swithes to enable and disable debug points, 

and you can also pass data to debug points.

Both FE and BE support debug point, and after inserting debug point code, recompilation of FE or BE is needed.

## Code Example

FE example

```java
private Status foo() {
	// dbug_fe_foo_do_nothing is the debug point name
	// when it's active, DebugPointUtil.isEnable("dbug_fe_foo_do_nothing") returns true
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
     // dbug_be_foo_do_nothing is the debug point name
     // when it's active, DBUG_EXECUTE_IF will execute the code block
     DBUG_EXECUTE_IF("dbug_be_foo_do_nothing",  { return Status.Nothing; });
   
     do_foo_action();
     
     return Status.Ok;
}
```


## Global Config

To enable debug points globally, we need to set `enable_debug_points` to true,

`enable_debug_points` is located in FE's fe.conf and BE's be.conf.


## Activate A Specified Debug Point

After debug points are enabled globally, a http request with a debug point name should be send to FE or BE node, <br/>
only after that, when the program running into the specified debug point, related code can be executed.

### API

```
POST /api/debug_point/add/{debug_point_name}[?timeout=<int>&execute=<int>]
```


### Query Parameters

* `debug_point_name`
    Debug point name. Required.

* `timeout`
    Timeout in seconds. When timeout, the debug point will be deactivated. Default is -1, never timeout. Optional.

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
When activating debug point, besides "timeout" and "execute" mentioned above, passing custom parameters is also allowed.<br/>
A parameter is a key-value pair in the form of "key=value" in url path, after debug point name glued by charactor '?'.<br/> 
See examples below.

### API

```
POST /api/debug_point/add/{debug_point_name}[?k1=v1&k2=v2&k3=v3...]
```
* `k1=v1` <br/>
  k1 is parameter name <br/>
  v1 is parameter value <br/>
  multiple key-value pairs are concatenated by `&` <br/>
  

  
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
Assuming a FE node with configuration http_port=8030 in fe.conf, <br/>
the following http request activates a debug point named `foo` in FE node and passe parameter `percent` and `duration`:
>NOTE: User name and password may be needed.
```
curl -u root: -X POST "http://127.0.0.1:8030/api/debug_point/add/foo?percent=0.5&duration=3"
```

```
NOTE:
1. Inside FE and BE code, names and values of parameters are taken as strings.
2. Parameter names and values are case sensitive in http request and FE/BE code.
3. FE and BE share same url paths of REST API, it's just their IPs and Ports are different.
```

### Use parameters in FE and BE code
Following request activates debug point `OlapTableSink.write_random_choose_sink` in FE and passes parameter `needCatchUp` and `sinkNum`: 
```
curl -u root: -X POST "http://127.0.0.1:8030/api/debug_point/add/OlapTableSink.write_random_choose_sink?needCatchUp=true&sinkNum=3"
```

The code in FE checks debug point `OlapTableSink.write_random_choose_sink` and gets parameter values:
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

Following request activates debug point `TxnManager.prepare_txn.random_failed` in BE and passes parameter `percent`:
```
curl -X POST "http://127.0.0.1:8040/api/debug_point/add/TxnManager.prepare_txn.random_failed?percent=0.7
```

The code in BE checks debug point `TxnManager.prepare_txn.random_failed` and gets parameter value:
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

>In community's CI system, `enable_debug_points` configuration of FE and BE are true by default.

The Regression test framework also provides methods to activate and deactivate a particular debug point, <br/>
they are declared as below:
```groovy
// "name" is the debug point to activate, "params" is a list of key-value pairs passed to debug point
def enableDebugPointForAllFEs(String name, Map<String, String> params = null);
def enableDebugPointForAllBEs(String name, Map<String, String> params = null);
// "name" is the debug point to deactivate
def disableDebugPointForAllFEs(String name);
def disableDebugPointForAllFEs(String name);
```
`enableDebugPointForAllFEs()` or `enableDebugPointForAllBEs()` needs to be called before the test actions you want to generate error, <br/>
and `disableDebugPointForAllFEs()` or `disableDebugPointForAllBEs()` needs to be called afterward.

### Concurrent Issue

Enabled debug points affects FE or BE globally, which could cause other concurrent tests to fail unexpectly in your pull request. <br/>
To avoid this, there's a convension that regression tests using debug points must be in directory regression-test/suites/fault_injection_p0, <br/>
and their group name must be "nonConcurrent", as these regression tests will be executed serially by pull request workflow. 

### Examples

```groovy
// .groovy file of the test case must be in regression-test/suites/fault_injection_p0
// and the group name must be 'nonConcurrent'
suite('debugpoint_action', 'nonConcurrent') {
    try {
        // Activate debug point named "PublishVersionDaemon.stop_publish" in all FE
        // and pass parameter "timeout"
        // "execute" and "timeout" are pre-existing parameters, usage is mentioned above
        GetDebugPoint().enableDebugPointForAllFEs('PublishVersionDaemon.stop_publish', [timeout:1])

        // Activate debug point named "Tablet.build_tablet_report_info.version_miss" in all BE
        // and pass parameter "tablet_id", "version_miss" and "timeout"
        GetDebugPoint().enableDebugPointForAllBEs('Tablet.build_tablet_report_info.version_miss',
                                                  [tablet_id:'12345', version_miss:true, timeout:1])

        // Test actions which will run into debug point and generate error
        sql """CREATE TABLE tbl_1 (k1 INT, k2 INT)
               DUPLICATE KEY (k1)
               DISTRIBUTED BY HASH(k1)
               BUCKETS 3
               PROPERTIES ("replication_allocation" = "tag.location.default: 1");
            """
        sql "INSERT INTO tbl_1 VALUES (1, 10)"
        sql "INSERT INTO tbl_1 VALUES (2, 20)"
        order_qt_select_1_1 'SELECT * FROM tbl_1'

    } finally {
        // Deactivate debug points
        GetDebugPoint().disableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')
        GetDebugPoint().disableDebugPointForAllBEs('Tablet.build_tablet_report_info.version_miss')
    }
}
```



