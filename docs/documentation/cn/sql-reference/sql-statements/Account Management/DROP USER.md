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

# DROP USER
## description

Syntax:

    DROP USER 'user_name'

    DROP USER 命令会删除一个 palo 用户。这里 Doris 不支持删除指定的 user_identity。当删除一个指定用户后，该用户所对应的所有 user_identity 都会被删除。比如之前通过 CREATE USER 语句创建了 jack@'192.%' 以及 jack@['domain'] 两个用户，则在执行 DROP USER 'jack' 后，jack@'192.%' 以及 jack@['domain'] 都将被删除。

## example

1. 删除用户 jack
   
    DROP USER 'jack'

## keyword

    DROP, USER

