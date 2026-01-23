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

#pragma once

#include <cstdint>

namespace doris::vectorized {

// Doris Native format file-level constants.
//
// File layout (byte stream):
//
//   +-------------------------------+---------------------------+---------------------------+ ...
//   | File header                   | Data block #0             | Data block #1             | ...
//   +-------------------------------+---------------------------+---------------------------+ ...
//
//   File header (12 bytes total):
//     - [0..7]   : magic bytes "DORISN1\0"  (DORIS_NATIVE_MAGIC)
//     - [8..11]  : uint32_t format_version (DORIS_NATIVE_FORMAT_VERSION, little-endian)
//
//   Each data block i:
//     - uint64_t block_size   : length in bytes of serialized PBlock (little-endian)
//     - uint8_t[block_size]   : PBlock protobuf payload produced by Block::serialize()
//
// NativeReader:
//   - Detects the optional file header by checking the first 8 bytes against DORIS_NATIVE_MAGIC.
//   - If the header is present, it skips 12 bytes and then starts reading blocks as
//     [uint64_t block_size][PBlock bytes]...
//   - If the header is absent (legacy files), it starts reading blocks from offset 0.
//
// VNativeTransformer:
//   - Writes the header once in open(), then appends each block in write() as
//     [uint64_t block_size][PBlock bytes]...
//
// These constants are shared between writer, reader and tests to keep the on-disk
// format definition in a single place.
// Header layout:
// [magic bytes "DORISN1\0"][uint32_t format_version]
static constexpr char DORIS_NATIVE_MAGIC[8] = {'D', 'O', 'R', 'I', 'S', 'N', '1', '\0'};
static constexpr uint32_t DORIS_NATIVE_FORMAT_VERSION = 1;

} // namespace doris::vectorized
