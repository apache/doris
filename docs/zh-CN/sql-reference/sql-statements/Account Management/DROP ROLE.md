---
{
    "title": "DROP ROLE",
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

# DROP ROLE
## description
    该语句用户删除一个角色

    语法：
        DROP ROLE [IF EXISTS] role1;

    删除一个角色，不会影响之前属于该角色的用户的权限。仅相当于将该角色与用户解耦。用户已经从该角色中获取到的权限，不会改变。

## example

    1. 删除一个角色

        DROP ROLE role1;

## keyword
   DROP, ROLE

