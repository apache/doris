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
#include "snii/common/slice.h"

namespace snii {

// Slice read cursor: all section deserialization goes through this; any overrun returns Corruption.
class ByteSource {
public:
    explicit ByteSource(Slice s) : s_(s) {}

    doris::Status get_u8(uint8_t* v);
    doris::Status get_fixed16(uint16_t* v);
    doris::Status get_fixed32(uint32_t* v);
    doris::Status get_fixed64(uint64_t* v);
    doris::Status get_varint32(uint32_t* v);
    doris::Status get_varint64(uint64_t* v);
    doris::Status get_zigzag(int64_t* v);
    doris::Status get_bytes(size_t n, Slice* out);

    size_t remaining() const { return s_.size() - pos_; }
    size_t position() const { return pos_; }
    bool eof() const { return pos_ == s_.size(); }

    // Returns a sub-view starting at absolute offset start with length len (used by framer etc. to rewind over the CRC coverage region).
    Slice slice_from(size_t start, size_t len) const { return s_.subslice(start, len); }

private:
    Slice s_;
    size_t pos_ = 0;
};

} // namespace snii
