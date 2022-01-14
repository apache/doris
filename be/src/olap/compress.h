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

// Define a compression function to compress the remaining memory in the input buffer
// and save it to the remaining space in the output buffer
// Inputs:
//     in - input buffer,Compress memory from position to limit
//     out - output buffer,The space from position to limit can be used to store data
//     smaller - Whether the compressed data size is smaller than the data size before compression
// Returns:
//     OLAP_ERR_BUFFER_OVERFLOW - Insufficient space left in output buffer
//     OLAP_ERR_COMPRESS_ERROR - Compression error
typedef OLAPStatus (*Compressor)(StorageByteBuffer* in, StorageByteBuffer* out, bool* smaller);

// Define a decompression function to decompress the remaining memory in the input buffer
// and save it to the remaining space in the output buffer
// Inputs:
//     in - input buffer,Decompress memory from position to limit
//     out - output buffer,The space from position to limit can be used to store data
// Returns:
//     OLAP_ERR_BUFFER_OVERFLOW - Insufficient space left in output buffer
//     OLAP_ERR_DECOMPRESS_ERROR - decompression error
typedef OLAPStatus (*Decompressor)(StorageByteBuffer* in, StorageByteBuffer* out);

#ifdef DORIS_WITH_LZO
OLAPStatus lzo_compress(StorageByteBuffer* in, StorageByteBuffer* out, bool* smaller);
OLAPStatus lzo_decompress(StorageByteBuffer* in, StorageByteBuffer* out);
#endif

OLAPStatus lz4_compress(StorageByteBuffer* in, StorageByteBuffer* out, bool* smaller);
OLAPStatus lz4_decompress(StorageByteBuffer* in, StorageByteBuffer* out);

} // namespace doris
#endif // DORIS_BE_SRC_OLAP_COLUMN_FILE_COMPRESS_H
