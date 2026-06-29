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

#include "common/status.h"

namespace snii {

// LEB128 variable-length integer encoding + zigzag. out buffer must be >=10 bytes; returns number of bytes written.
size_t varint_len(uint64_t v);
size_t encode_varint32(uint32_t v, uint8_t* out);
size_t encode_varint64(uint64_t v, uint8_t* out);

// Decode a varint from the range [p, end); on success *next points to the next byte after the consumed input.
doris::Status decode_varint32(const uint8_t* p, const uint8_t* end, uint32_t* v,
                              const uint8_t** next);
doris::Status decode_varint64(const uint8_t* p, const uint8_t* end, uint64_t* v,
                              const uint8_t** next);

inline uint64_t zigzag_encode(int64_t v) {
    return (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
}
inline int64_t zigzag_decode(uint64_t v) {
    return static_cast<int64_t>(v >> 1) ^ -static_cast<int64_t>(v & 1);
}

} // namespace snii
