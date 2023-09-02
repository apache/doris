---
{
    "title": "BITMAP_HASH",
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

## bitmap_hash

### Name

BITMAP_HASH

### Description

Calculating hash value for what your input and return a BITMAP which contain the hash value. MurMur3 is used for this function because it is high-performance with low collision rate. More important, the MurMur3 distribution is "simili-random"; the Chi-Square distribution test is used to prove it. By the way, Different hardware platforms and different SEED may change the result of MurMur3. For more information about its performance, see [Smhasher](http://rurban.github.io/smhasher/).

#### Syntax

`BITMAP BITMAP_HASH(<any_value>)`

#### Arguments

`<any_value>`
any value or expression. 

#### Return Type

BITMAP

#### Remarks

Generally, MurMurHash 32 is friendly to random, short STRING with low collision rate about one-billionth. But for longer STRING, such as your path of system, can cause more frequent collision. If you indexed your system path, you will find a lot of collisions.

The following two values are the same.

```sql
SELECT bitmap_to_string(bitmap_hash('/System/Volumes/Data/Library/Developer/CommandLineTools/SDKs/MacOSX12.3.sdk/System/Library/Frameworks/KernelManagement.framework/KernelManagement.tbd')) AS a ,
       bitmap_to_string(bitmap_hash('/System/Library/PrivateFrameworks/Install.framework/Versions/Current/Resources/es_419.lproj/Architectures.strings')) AS b;
```

Here is the result.

```text
+-----------+-----------+
| a         | b         |
+-----------+-----------+
| 282251871 | 282251871 |
+-----------+-----------+
```

### Example

If you want to calculate MurMur3 of a certain value, you can

```
select bitmap_to_array(bitmap_hash('hello'))[1];
```

Here is the result.

```text
+-------------------------------------------------------------+
| %element_extract%(bitmap_to_array(bitmap_hash('hello')), 1) |
+-------------------------------------------------------------+
|                                                  1321743225 |
+-------------------------------------------------------------+
```

If you want to `count distinct` some columns, using bitmap has higher performance in some scenes. 

```sql
select bitmap_count(bitmap_union(bitmap_hash(`word`))) from `words`;
```

Here is the result.

```text
+-------------------------------------------------+
| bitmap_count(bitmap_union(bitmap_hash(`word`))) |
+-------------------------------------------------+
|                                        33263478 |
+-------------------------------------------------+
```

### Keywords

    BITMAP_HASH,BITMAP

### Best Practice

For more information, see also:
- [BITMAP_HASH64](./bitmap_hash64.md)
