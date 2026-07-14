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

#include "storage/index/snii/encoding/byte_sink.h"

#include "storage/index/snii/encoding/varint.h"

namespace doris::snii {

void ByteSink::put_fixed16(uint16_t v) {
    for (int i = 0; i < 2; ++i) buf_.push_back(static_cast<uint8_t>(v >> (8 * i)));
}

void ByteSink::put_fixed32(uint32_t v) {
    for (int i = 0; i < 4; ++i) buf_.push_back(static_cast<uint8_t>(v >> (8 * i)));
}

void ByteSink::put_fixed64(uint64_t v) {
    for (int i = 0; i < 8; ++i) buf_.push_back(static_cast<uint8_t>(v >> (8 * i)));
}

void ByteSink::put_varint32(uint32_t v) {
    uint8_t tmp[5];
    size_t n = encode_varint32(v, tmp);
    buf_.insert(buf_.end(), tmp, tmp + n);
}

void ByteSink::put_varint64(uint64_t v) {
    uint8_t tmp[10];
    size_t n = encode_varint64(v, tmp);
    buf_.insert(buf_.end(), tmp, tmp + n);
}

void ByteSink::put_zigzag(int64_t v) {
    put_varint64(zigzag_encode(v));
}

void ByteSink::put_bytes(Slice s) {
    buf_.insert(buf_.end(), s.data(), s.data() + s.size());
}

} // namespace doris::snii
