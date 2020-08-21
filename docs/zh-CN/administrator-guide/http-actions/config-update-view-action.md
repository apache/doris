---
{
    "title": "VIEW UPDATE CONFIG",
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

# VIEW UPDATE CONFIG

## 查看配置接口
通过key，查看当前配置

```
curl -X GET http://host:port/api/config?key={key}
```

如果 key 不存在，直接返回错误
```
{
    "status" : "BAD",
    "msg" : "xxxxxxx"
}
```

如果访问成功，返回成功。
```
{
    "status" : "OK",
    "msg" : "success",
    "key" : "{key}",
    "value" : "{value}",
    "type" : "bool/int/double/string", 
    "modifiable" : true
}
```

## 动态更新配置接口
通过key、value, 设置变量

```
curl -X POST http://host:port/api/update_config?{key}={value}
```

如果 key 不存在或更新失败，直接返回错误
```
{
    "status" : "BAD",
    "msg" : "xxxxxxx"
}
```

如果更新成功，返回成功。
```
{
    "status" : "OK",
    "msg" : "success"
}
```

## 动态更新配置规则

* 可参考 be 配置项文档或参考在 config.h 源码中配置的变量以 m 开头表示一个变量可以动态修改。
