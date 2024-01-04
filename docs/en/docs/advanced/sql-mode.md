---
{
"title": "SQL Mode",
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

# SQL Mode

The sql mode newly supported by Doris refers to the sql mode management mechanism of Mysql. Each client can set its own sql mode, and database administrators with Admin privileges can set the global sql mode.
## sql mode introduce

sql mode enables users to switch between different styles of sql syntax and data validation strictness, making Doris more compatible with other databases. For example, in some databases, the '||' symbol is a string concatenator, but in Doris it is equivalent to 'or', then users only need to use sql mode to switch to the style they want. Each client can set the sql mode, which is valid in the current session. Only users with Admin privileges can set the global sql mode.
## principle

The sql mode is stored in SessionVariables with a 64-bit Long type. Each bit of this address represents the enable/disable (1 means open, 0 means disable) state of a mode, as long as you know where each mode is. Bit, we can easily and quickly perform checksum operations on sql mode through bit operations.

Every time you query the sql mode, the Long type will be parsed and turned into a user-readable string. Similarly, the sql mode string sent by the user to the server will be parsed into a string that can be stored in SessionVariables. Long type.

The global sql mode that has been set will be persisted, so the operation on the global sql mode always only needs to be performed once, and the last global sql mode can be restored even after the program is restarted.

## Operation method

1. Set sql mode
```
set global sql_mode = "DEFAULT"
set session sql_mode = "DEFAULT"
```
>Currently Doris's default sql mode is DEFAULT (but this will be changed in subsequent revisions soon).
>Setting global sql mode requires Admin privileges and will affect all clients connecting thereafter.
>Setting session sql mode will only affect the current dialog client, the default is session mode.

2. Query sql mode

```
select @@global.sql_mode
select @@session.sql_mode
```
>In addition to this way, you can also check the current sql mode by returning all session variables in the following way
```
show global variables
show session variables
```

## mode is supported

1. `PIPES_AS_CONCAT`

In this mode, the '||' symbol is a string concatenation symbol (same as the CONCAT() function), not a synonym for the 'OR' symbol. (e.g., `'a'||'b' = 'ab'`, `1||0 = '10'`)
## composite mode

(subsequent additions)
