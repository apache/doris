---
{
    "title": "BITMAP_HASH",
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

## bitmap_hash

### Name

BITMAP_HASH

### Description

对任意类型的输入，计算其 32 位的哈希值，并返回包含该哈希值的 bitmap。该函数使用的哈希算法为 MurMur3。MurMur3 算法是一种高性能的、低碰撞率的散列算法，其计算出来的值接近于随机分布，并且能通过卡方分布测试。需要注意的是，不同硬件平台、不同 Seed 值计算出来的散列值可能不同。关于此算法的性能可以参考 [Smhasher](http://rurban.github.io/smhasher/) 排行榜。

#### Syntax

`BITMAP BITMAP_HASH(<any_value>)`

#### Arguments

`<any_value>`
任何值或字段表达式。

#### Return Type

BITMAP

#### Remarks

一般来说，MurMur 32 位算法对于完全随机的、较短的字符串的散列效果较好，碰撞率能达到几十亿分之一，但对于较长的字符串，比如你的操作系统路径，碰撞率会比较高。如果你扫描你系统里的路径，就会发现碰撞率仅仅只能达到百万分之一甚至是十万分之一。

下面两个字符串的 MurMur3 散列值是一样的：

```sql
SELECT bitmap_to_string(bitmap_hash('/System/Volumes/Data/Library/Developer/CommandLineTools/SDKs/MacOSX12.3.sdk/System/Library/Frameworks/KernelManagement.framework/KernelManagement.tbd')) AS a ,
       bitmap_to_string(bitmap_hash('/System/Library/PrivateFrameworks/Install.framework/Versions/Current/Resources/es_419.lproj/Architectures.strings')) AS b;
```

结果如下：

```text
+-----------+-----------+
| a         | b         |
+-----------+-----------+
| 282251871 | 282251871 |
+-----------+-----------+
```

### Example

如果你想计算某个值的 MurMur3，你可以：

```
select bitmap_to_array(bitmap_hash('hello'))[1];
```

结果如下：

```text
+-------------------------------------------------------------+
| %element_extract%(bitmap_to_array(bitmap_hash('hello')), 1) |
+-------------------------------------------------------------+
|                                                  1321743225 |
+-------------------------------------------------------------+
```

如果你想统计某一列去重后的个数，可以使用位图的方式，某些场景下性能比 `count distinct` 好很多：

```sql
select bitmap_count(bitmap_union(bitmap_hash(`word`))) from `words`;
```

结果如下：

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

还可参见
- [BITMAP_HASH64](./bitmap_hash64.md)
