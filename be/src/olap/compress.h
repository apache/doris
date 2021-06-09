// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef DORIS_BE_SRC_OLAP_COLUMN_FILE_COMPRESS_H
#define DORIS_BE_SRC_OLAP_COLUMN_FILE_COMPRESS_H

#include "olap/olap_define.h"

namespace doris {

class StorageByteBuffer;

// 定义压缩函数,将in中剩余的内存压缩,并保存到out中剩余的空间
// Inputs:
//     in - 输入缓冲区,压缩从position到limit位置的内存
//     out - 输出缓冲区,从position到limit的空间可以用于存放数据
//     smaller - 压缩后的数据大小是否小于压缩前的数据大小
// Returns:
//     OLAP_ERR_BUFFER_OVERFLOW - out中的剩余空间不足
//     OLAP_ERR_COMPRESS_ERROR - 压缩错误
typedef OLAPStatus (*Compressor)(StorageByteBuffer* in, StorageByteBuffer* out, bool* smaller);

// 定义解压缩函数,将in中剩余的内存解压缩,并保存到out中剩余的空间
// Inputs:
//     in - 输入缓冲区,解压缩从position到limit位置的内存
//     out - 输出缓冲区,从position到limit的空间可以用于存放数据
// Returns:
//     OLAP_ERR_BUFFER_OVERFLOW - out中的剩余空间不足
//     OLAP_ERR_DECOMPRESS_ERROR - 解压缩错误
typedef OLAPStatus (*Decompressor)(StorageByteBuffer* in, StorageByteBuffer* out);

#ifdef DORIS_WITH_LZO
OLAPStatus lzo_compress(StorageByteBuffer* in, StorageByteBuffer* out, bool* smaller);
OLAPStatus lzo_decompress(StorageByteBuffer* in, StorageByteBuffer* out);
#endif

OLAPStatus lz4_compress(StorageByteBuffer* in, StorageByteBuffer* out, bool* smaller);
OLAPStatus lz4_decompress(StorageByteBuffer* in, StorageByteBuffer* out);

} // namespace doris
#endif // DORIS_BE_SRC_OLAP_COLUMN_FILE_COMPRESS_H
