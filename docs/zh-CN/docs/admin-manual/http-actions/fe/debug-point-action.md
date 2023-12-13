---
{
    "title": "代码打桩",
    "language": "zh-CN"
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

# 代码打桩

代码打桩是代码测试使用的。激活木桩后，可以执行木桩代码。木桩的名字是任意取的。

FE 和 BE 都支持代码打桩。

## 木桩代码示例

FE 桩子示例代码

```java
private Status foo() {  
	// dbug_fe_foo_do_nothing 是一个木桩名字，
	// 打开这个木桩之后，DebugPointUtil.isEnable("dbug_fe_foo_do_nothing") 将会返回true
	if (DebugPointUtil.isEnable("dbug_fe_foo_do_nothing")) {
		return Status.Nothing;
	}
      	
     do_foo_action();
     
     return Status.Ok;
}
```

BE 桩子示例代码

```c++
void Status foo() {

     // dbug_be_foo_do_nothing 是一个木桩名字，
     // 打开这个木桩之后，DBUG_EXECUTE_IF 将会执行宏参数中的代码块
     DBUG_EXECUTE_IF("dbug_be_foo_do_nothing",  { return Status.Nothing; });
   
     do_foo_action();
     
     return Status.Ok;
}
```

## 总开关

需要把木桩总开关 `enable_debug_points` 打开之后，才能激活木桩。默认情况下，木桩总开关是关闭的。

总开关`enable_debug_points` 分别在 FE 的 fe.conf 和 BE 的 be.conf 中配置。


## 打开木桩

### API

```
POST /api/debug_point/add/{debug_point_name}[?timeout=<int>&execute=<int>]
```


### 参数

* `debug_point_name`
    木桩名字。必填。

* `timeout`
    超时时间，单位为秒。超时之后，木桩失活。默认值-1表示永远不超时。可选。

* `execute`
    木桩最大激活次数。默认值-1表示不限激活次数。可选。       


### Request body

无

### Response

```
{
    msg: "OK",
    code: 0
}
```
    
### Examples


打开木桩 `foo`，最多激活5次。
	
	
```
curl -X POST "http://127.0.0.1:8030/api/debug_point/add/foo?execute=5"

```
注意，要先激活木桩，然后再执行含有木桩的代码，代码中的木桩才会生效。
    
## 向木桩传递参数

除了 timeout 和 execute，激活木桩时，还可以传递其它自定义参数。

### API

```
POST /api/debug_point/add/{debug_point_name}[?key1=value1&key2=value2&key3=value3...]
```
* `key1=value1`
  向木桩传递自定义参数，名称为 key1，值为 value1，多个参数用&分隔，“&”前后没有空格。
### Request body

无

### Response

```
{
    msg: "OK",
    code: 0
}
```

### Examples

向 FE 或 BE 传递参数的 REST API 格式相同，只是 IP 地址和端口不同。

假设 FE 的 http_port=8030，则下面的请求激活 FE 中的木桩`foo`，并传递了两个参数：

一个名称为 percent 值为 0.5，另一个名称为 duration 值为 3。
		
```
curl -u root: -X POST "http://127.0.0.1:8030/api/debug_point/add/foo?percent=0.5&duration=3"
```

### 在FE、BE代码中使用参数
激活 FE 中的木桩并传递参数:
```
curl -u root: -X POST "http://127.0.0.1:8030/api/debug_point/add/OlapTableSink.write_random_choose_sink?needCatchUp=true&sinkNum=3"
```
在 FE 代码中使用木桩 OlapTableSink.write_random_choose_sink 的参数 needCatchUp 和 sinkNum（区分大小写）：
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

激活 BE 中的木桩并传递参数:
```
curl -X POST "http://127.0.0.1:8040/api/debug_point/add/TxnManager.prepare_txn.random_failed?percent=0.7
```
在 BE 代码中使用木桩 TxnManager.prepare_txn.random_failed 的参数 percent（区分大小写）：
```c++
DBUG_EXECUTE_IF("TxnManager.prepare_txn.random_failed",
		{if (rand() % 100 < (100 * dp->param("percent", 0.5))) {
		        LOG_WARNING("TxnManager.prepare_txn.random_failed random failed");
		        return Status::InternalError("debug prepare txn random failed");
		}}
);
```

## 关闭木桩

### API

```
POST /api/debug_point/remove/{debug_point_name}
```


### 参数

* `debug_point_name`
    木桩名字。必填。     


### Request body

无

### Response

```
{
    msg: "OK",
    code: 0
}
```
    
### Examples


关闭木桩`foo`。
	
	
```
curl -X POST "http://127.0.0.1:8030/api/debug_point/remove/foo"
```
    
## 清除所有木桩

### API

```
POST /api/debug_point/clear
```



### Request body

无

### Response

```
{
    msg: "OK",
    code: 0
}
```
    
### Examples


清除所有木桩。
	
```
curl -X POST "http://127.0.0.1:8030/api/debug_point/clear"
```
