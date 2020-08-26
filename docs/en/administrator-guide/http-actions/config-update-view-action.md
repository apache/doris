---
{
    "title": "VIEW UPDATE CONFIG",
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

# VIEW UPDATE CONFIG

## View config api
View the current config by key.

```
curl -X GET http://host:port/api/config?key={key}
```

If the key does not exist, directly return an error.
```
{
    "status" : "BAD",
    "msg" : "xxxxxxx"
}
```

If api access successful, return success.
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

## Dynamic update config api
Set variables through key and value

```
curl -X POST http://host:port/api/update_config?{key}={value}
```

If the key does not exist or the update fails, return an error directly
```
{
    "status" : "BAD",
    "msg" : "xxxxxxx"
}
```

If the update is successful, return success.
```
{
    "status" : "OK",
    "msg" : "success"
}
```

## Dynamic update config rules

You can refer to the be configuration item document or refer to the variable configured in the config.h source code. The beginning of 'm' indicates that a variable can be dynamically modified.
