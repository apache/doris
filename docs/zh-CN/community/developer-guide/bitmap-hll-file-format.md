---
{
"title": "Bitmap/HLL 数据格式",
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

## Bitmap 格式
### 格式说明
Doris 中的bitmap 采用的是roaring bitmap 存储， be 端使用CRoaring，`Roaring` 的序列化格式在C++/java/go 等语言中兼容， 而C++ `Roaring64Map` 的格式序列化结果和Java中`Roaring64NavigableMap` 不兼容。Doris bimap 有5种类型， 分别用一个字节表示

Doris 中的bitmap 序列化格式说明如下:

```
 | flag     | data .....|
 <--1Byte--><--n bytes-->
```

Flag 值说明如下：

| 值   | 描述                                                         |
| ---- | ------------------------------------------------------------ |
| 0    | EMPTY，空 bitmap,  后面data 部分为空，整个序列化结果只有一个字节 |
| 1    | SINGLE32，bitmap 中只有一个32位无符号整型值， 后面4个字节表示32位无符号整型值 |
| 2    | BITMAP32，32 位bitmap 对应java 中类型为 `org.roaringbitmap.RoaringBitmap` C++ 中类型为`roaring::Roaring`， 后面data 为roaring::Roaring 序列后的结构， 可以使用java 中的 `org.roaringbitmap.RoaringBitmap`  或c++中`roaring::Roaring` 直接反序列化 |
| 3    | SINGLE64 ,bitmap 中只有一个64位无符号整型值，后面8个字节表示64位无符号整型值 |
| 4    | BITMAP64, 64 位bitmap 对应java 中类型为 `org.roaringbitmap.RoaringBitmap;` c++ 中类型为doris 中的`Roaring64Map`，数据结构和 roaring 库中的结果一致，但是序列化和反序列话方法有所不同，后面会有1-8个字节的变长编码的uint64 的表述bitmap 中size。后面的数据是各式是多个 4个字节的高位表示和32位 roaring bitmap 序列化数据重复而成 |

c++ 序列化和反序列化的示例 在 `be/src/util/bitmap_value.h` 的`BitmapValue::write()`   `BitmapValue::deserialize()`  方法中, Java示例在 `fe/fe-common/src/main/java/org/apache/doris/common/io/BitmapValue.java` 中的`serialize()` `deserialize()` 方法中。

## HLL 格式说明

HLL 格式序列化数据在Doris 中自己实现的。同Bitmap 类型类似，HLL 格式是1个字节的flag 后面跟随多个字节数据组成，flag 含义如下



| 值   | 描述                                                         |
| ---- | ------------------------------------------------------------ |
| 0    | HLL_DATA_EMPTY，空 HLL,  后面data 部分为空，整个序列化结果只有一个字节 |
| 1    | HLL_DATA_EXPLICIT， 后面1个字节 explicit 数据块个数，后面接多个数据块，每个数据块由8个字节长度和数据组成， |
| 2    | HLL_DATA_SPARSE，只存非0 值，后面4个字节 表示 register 个数， 后面连续多个 register 结构，每个register 由前面2个字节的index 和1个字节的值组成 |
| 3    | HLL_DATA_FULL ,表示所有16 * 1024个register都有值， 后面连续16 * 1024个字节的值数据 |

c++ 序列化和反序列化的示例 在 `be/src/olap/hll.h` 的`serialize()`   `deserialize()`  方法中, Java示例在 `fe/fe-common/src/main/java/org/apache/doris/common/io/hll.java` 中的`serialize()` `deserialize()` 方法中。