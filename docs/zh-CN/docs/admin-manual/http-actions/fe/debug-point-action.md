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

代码打桩，是指在 FE 或 BE 源码中插入一段代码，当程序执行到这里时，可以改变程序的变量或行为，这样的一段代码称为一个`木桩`。

主要用于单元测试或回归测试，用来构造正常方法无法实现的异常。

每一个木桩都有一个名称，可以随便取名，可以通过一些机制控制木桩的开关，还可以向木桩传递参数。

FE 和 BE 都支持代码打桩，打桩完后要重新编译 BE 或 FE。

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
打开总开关后，还需要通过向 FE 或 BE 发送 http 请求的方式，打开或关闭指定名称的木桩，只有这样当代码执行到这个木桩时，相关代码才会被执行。

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
    木桩最大执行次数。默认值-1表示不限执行次数。可选。       


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


打开木桩 `foo`，最多执行5次。
	
	
```
curl -X POST "http://127.0.0.1:8030/api/debug_point/add/foo?execute=5"

```

    
## 向木桩传递参数

激活木桩时，除了前文所述的 timeout 和 execute，还可以传递其它自定义参数。<br/>
一个参数是一个形如 key=value 的 key-value 对，在 url 的路径部分，紧跟在木桩名称后，以字符 '?' 开头。

### API

```
POST /api/debug_point/add/{debug_point_name}[?k1=v1&k2=v2&k3=v3...]
```
* `k1=v1`
  k1为参数名称，v1为参数值，多个参数用&分隔。
  
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

假设 FE 在 fe.conf 中有配置 http_port=8030，则下面的请求激活 FE 中的木桩`foo`，并传递了两个参数 `percent` 和 `duration`：
		
```
curl -u root: -X POST "http://127.0.0.1:8030/api/debug_point/add/foo?percent=0.5&duration=3"
```

```
注意：
1、在 FE 或 BE 的代码中，参数名和参数值都是字符串。
2、在 FE 或 BE 的代码中和 http 请求中，参数名称和值都是大小写敏感的。
3、发给 FE 或 BE 的 http 请求，路径部分格式是相同的，只是 IP 地址和端口号不同。
```

### 在 FE 和 BE 代码中使用参数

激活 FE 中的木桩`OlapTableSink.write_random_choose_sink`并传递参数 `needCatchUp` 和 `sinkNum`:
>注意：可能需要用户名和密码
```
curl -u root: -X POST "http://127.0.0.1:8030/api/debug_point/add/OlapTableSink.write_random_choose_sink?needCatchUp=true&sinkNum=3"
```

在 FE 代码中使用木桩 OlapTableSink.write_random_choose_sink 的参数 `needCatchUp` 和 `sinkNum`：
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


激活 BE 中的木桩`TxnManager.prepare_txn.random_failed`并传递参数 `percent`:
```
curl -X POST "http://127.0.0.1:8040/api/debug_point/add/TxnManager.prepare_txn.random_failed?percent=0.7
```
在 BE 代码中使用木桩 `TxnManager.prepare_txn.random_failed` 的参数 `percent`：
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

## 在回归测试中使用木桩

> 提交PR时，社区 CI 系统默认开启 FE 和 BE 的`enable_debug_points`配置。

回归测试框架提供方法函数来开关指定的木桩，它们声明如下：

```groovy
// 打开木桩，name 是木桩名称，params 是一个key-value列表，是传给木桩的参数
def enableDebugPointForAllFEs(String name, Map<String, String> params = null);
def enableDebugPointForAllBEs(String name, Map<String, String> params = null);
// 关闭木桩，name 是木桩的名称
def disableDebugPointForAllFEs(String name);
def disableDebugPointForAllFEs(String name);
```
需要在调用测试 action 之前调用 `enableDebugPointForAllFEs()` 或 `enableDebugPointForAllBEs()` 来开启木桩， <br/>
这样执行到木桩代码时，相关代码才会被执行，<br/>
然后在调用测试 action 之后调用 `disableDebugPointForAllFEs()` 或 `disableDebugPointForAllBEs()` 来关闭木桩。

### 并发问题

FE 或 BE 中开启的木桩是全局生效的，同一个 Pull Request 中，并发跑的其它测试，可能会受影响而意外失败。
为了避免这种情况，我们规定，使用木桩的回归测试，必须放在 regression-test/suites/fault_injection_p0 目录下，
且组名必须设置为 `nonConcurrent`，社区 CI 系统对于这些用例，会串行运行。

### Examples

```groovy
// 测试用例的.groovy 文件必须放在 regression-test/suites/fault_injection_p0 目录下，
// 且组名设置为 'nonConcurrent'
suite('debugpoint_action', 'nonConcurrent') {
    try {
        // 打开所有FE中，名为 "PublishVersionDaemon.stop_publish" 的木桩
        // 传参数 timeout
        // 与上面curl调用时一样，execute 是执行次数，timeout 是超时秒数
        GetDebugPoint().enableDebugPointForAllFEs('PublishVersionDaemon.stop_publish', [timeout:1])
        // 打开所有BE中，名为 "Tablet.build_tablet_report_info.version_miss" 的木桩
        // 传参数 tablet_id, version_miss 和 timeout
        GetDebugPoint().enableDebugPointForAllBEs('Tablet.build_tablet_report_info.version_miss',
                                                  [tablet_id:'12345', version_miss:true, timeout:1])

        // 测试用例，会触发木桩代码的执行
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
        GetDebugPoint().disableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')
        GetDebugPoint().disableDebugPointForAllBEs('Tablet.build_tablet_report_info.version_miss')
    }
}
```
