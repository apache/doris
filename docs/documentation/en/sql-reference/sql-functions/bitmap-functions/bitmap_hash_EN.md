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

# bitmap_hash
## description
### Syntax

`BITMAP BITMAP_HASH(expr)`

Compute the 32-bits hash value of a expr of any type, then return a bitmap containing that hash value. Mainly be used to load non-integer value into bitmap column, e.g.,

```
cat data | curl --location-trusted -u user:passwd -T - -H "columns: dt,page,device_id, device_id=bitmap_hash(device_id)"   http://host:8410/api/test/testDb/_stream_load
```

## example

```
mysql> select bitmap_count(bitmap_hash('hello'));
+------------------------------------+
| bitmap_count(bitmap_hash('hello')) |
+------------------------------------+
|                                  1 |
+------------------------------------+
```

## keyword

    BITMAP_HASH,BITMAP
