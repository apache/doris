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

#include <stddef.h>
#include <stdint.h>

// This namespace has wrappers for the Bitshuffle library which do runtime dispatch to
// either AVX2-accelerated or regular SSE2 implementations based on the available CPU.
namespace doris {
namespace bitshuffle {

// See <bitshuffle.h> for documentation on these functions.
size_t compress_lz4_bound(size_t size, size_t elem_size, size_t block_size);
int64_t compress_lz4(void* in, void* out, size_t size, size_t elem_size, size_t block_size);
int64_t decompress_lz4(void* in, void* out, size_t size, size_t elem_size, size_t block_size);

} // namespace bitshuffle
} // namespace doris
