---
{
    "title": "SQL MODE",
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

# SQL MODE

Doris新支持的sql mode参照了 Mysql 的sql mode管理机制，每个客户端都能设置自己的sql mode，拥有Admin权限的数据库管理员可以设置全局sql mode。

## sql mode 介绍

sql mode使用户能在不同风格的sql语法和数据校验严格度间做切换，使Doris对其他数据库有更好的兼容性。例如在一些数据库里，'||'符号是一个字符串连接符，但在Doris里却是与'or'等价的，这时用户只需要使用sql mode切换到自己想要的风格。每个客户端都能设置sql mode，并在当前对话中有效，只有拥有Admin权限的用户可以设置全局sql mode。

## 原理

sql mode用一个64位的Long型存储在SessionVariables中，这个地址的每一位都代表一个mode的开启/禁用(1表示开启，0表示禁用)状态，只要知道每一种mode具体是在哪一位，我们就可以通过位运算方便快速的对sql mode进行校验和操作。

每一次对sql mode的查询，都会对此Long型进行一次解析，变成用户可读的字符串形式，同理，用户发送给服务器的sql mode字符串，会被解析成能够存储在SessionVariables中的Long型。

已被设置好的全局sql mode会被持久化，因此对全局sql mode的操作总是只需一次，即使程序重启后仍可以恢复上一次的全局sql mode。

## 操作方式

1、设置sql mode

```
set global sql_mode = "DEFAULT"
set session sql_mode = "DEFAULT"
```
>目前Doris的默认sql mode是DEFAULT（但马上会在后续修改中会改变）。
>设置global sql mode需要Admin权限，并会影响所有在此后连接的客户端。
>设置session sql mode只会影响当前对话客户端，默认为session方式。

2、查询sql mode

```
select @@global.sql_mode
select @@session.sql_mode
```
>除了这种方式，你还可以通过下面方式返回所有session variables来查看当前sql mode

```
show global variables
show session variables
```

## 已支持mode

1. `PIPES_AS_CONCAT`

	在此模式下，'||'符号是一种字符串连接符号（同CONCAT()函数），而不是'OR'符号的同义词。(e.g., `'a'||'b' = 'ab'`, `1||0 = '10'`)

## 复合mode

（后续补充）