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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"

namespace doris::snii {

// Thin ZSTD wrapper. Used for compressing large payloads such as .prx windows. Decompression requires the caller to supply the original uncompressed length (from the block header).
Status zstd_compress(Slice input, int level, std::vector<uint8_t>* out);
Status zstd_decompress(Slice input, size_t expected_uncomp_len, std::vector<uint8_t>* out);

} // namespace doris::snii
