---
{
    "title": "SQL MODE",
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

# SQL MODE

The SQL MODE supported by Doris refers to the sql mode management mechanism of MySQL. Each client can set its own sql mode, and the database administrator with admin permission can set the global sql mode.

## Sql mode introduction

SQL MODE enables users to switch between different styles of SQL syntax and data verification strictness, making Doris more compatible with other databases. For example, in some databases, the '||' symbol is a string connector, but in Doris it is equivalent to 'or'. At this time, users only need to use SQL mode to switch to the style they want. Each client can set sql mode, which is valid in the current conversation. Only users with admin permission can set global SQL mode.

## Theory

SQL MODE is stored in session variables with a 64 bit long type. Each bit of this address represents the on / off (1 for on, 0 for off) state of a mode. As long as we know the specific bit of each mode, we can easily and quickly verify and operate SQL mode through bit operation.

Every time you query sql mode, the long type will be parsed into a user-readable string. Similarly, the sql mode string sent by the user to the server will be parsed into a long type that can be stored in session variables.

The set global sql mode will be persisted, so the operation on the global sql mode is always only once, even after the program is restarted, the last global sql mode can be recovered.

## Operation

1、set sql mode

```
set global sql_mode = "DEFAULT"
set session sql_mode = "DEFAULT"
```
>At present, Doris's default sql mode is DEFAULT (but it will be changed in the future modification).
>Setting global sql mode requires admin permission and affects all clients that connect later.
>Setting session sql mode will only affect the current conversation client. The default setting way is session.

2、select sql mode

```
select @@global.sql_mode
select @@session.sql_mode
```
>In addition to this method, you can also view the current sql mode by returning all session variables as follows

```
show global variables
show session variables
```

## supported mode

1. `PIPES_AS_CONCAT`
	
	Treat '||' as a string concatenation operator (same as CONCAT()) rather than as a synonym for OR. (e.g., `'a'||'b' = 'ab'`, `1||0 = '10'`)

## combine mode

(Work in progress)