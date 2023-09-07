---
{
"title": "Bitmap/HLL data format",
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

## Bitmap format
### Format description
The bitmap in Doris uses roaring bitmap storage, and the be side uses CRoaring. The serialization format of `Roaring` is compatible in languages ​​such as C++/java/go, while the serialization result of the format of C++ `Roaring64Map` is the same as that of `Roaring64NavigableMap` in Java. Not compatible. There are 5 types of Doris bimap, each of which is represented by one byte

The bitmap serialization format in Doris is explained as follows:

```
 | flag     | data .....|
 <--1Byte--><--n bytes-->
```

The Flag value description is as follows:

| Value | Description |
| ---- | ------------------------------------------------------------ |
| 0 | EMPTY, empty bitmap, the following data part is empty, the whole serialization result is only one byte |
| 1 | SINGLE32, there is only one 32-bit unsigned integer value in the bitmap, and the next 4 bytes represent the 32-bit unsigned integer value |
| 2 | BITMAP32, 32-bit bitmap corresponds to the type `org.roaringbitmap.RoaringBitmap` in java. The type is `roaring::Roaring` in C++, and the following data is the structure after the sequence of roaring::Roaring. You can use `org in java. .roaringbitmap.RoaringBitmap` or `roaring::Roaring` in c++ directly deserialize |
| 3 | SINGLE64, there is only one 64-bit unsigned integer value in the bitmap, and the next 8 bytes represent the 64-bit unsigned integer value |
| 4 | BITMAP64, 64-bit bitmap corresponds to `org.roaringbitmap.RoaringBitmap` in java; `Roaring64Map` in doris in c++. The data structure is the same as the result in the roaring library, but the serialization and deserialization methods It is different, there will be 1-8 bytes of variable-length encoding uint64 in the bitmap representation of the size. The following data is a series of multiple high-order representations of 4 bytes and 32-bit roaring bitmap serialized data repeated |

C++ serialization and deserialization examples are in the `BitmapValue::write()` method in `be/src/util/bitmap_value.h` and the Java examples are in the `serialize()` `deserialize()` method in `fe/fe-common/src/main/java/org/apache/doris/common/io/BitmapValue.java`.

## HLL format description

Serialized data in HLL format is implemented in Doris itself. Similar to the Bitmap type, the HLL format is composed of a 1-byte flag followed by multiple bytes of data. The meaning of the flag is as follows



| Value | Description |
| ---- | ------------------------------------------------------------ |
| 0 | HLL_DATA_EMPTY, empty HLL, the following data part is empty, the entire serialization result is only one byte |
| 1 | HLL_DATA_EXPLICIT, the next byte is explicit The number of data blocks, followed by multiple data blocks, each data block is composed of 8 bytes in length and data, |
| 2 | HLL_DATA_SPARSE, only non-zero values are stored, the next 4 bytes indicate the number of registers, and there are multiple register structures in the back. Each register is composed of the index of the first 2 bytes and the value of 1 byte |
| 3 | HLL_DATA_FULL, which means that all 16 * 1024 registers have values, followed by 16 * 1024 bytes of value data |

C++ serialization and deserialization examples are in the `serialize()` `deserialize()` method of `be/src/olap/hll.h`, and the Java examples are in the `serialize()` `deserialize()` method in `fe/fe-common/src/main/java/org/apache/doris/common/io/hll.java`.
