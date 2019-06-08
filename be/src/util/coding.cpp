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

#include "util/coding.h"

namespace doris {

uint8_t* encode_varint32(uint8_t* dst, uint32_t v) {
    // Operate on characters as unsigneds
    static const int B = 128;
    if (v < (1 << 7)) {
        *(dst++) = v;
    } else if (v < (1 << 14)) {
        *(dst++) = v | B;
        *(dst++) = v >> 7;
    } else if (v < (1 << 21)) {
        *(dst++) = v | B;
        *(dst++) = (v >> 7) | B;
        *(dst++) = v >> 14;
    } else if (v < (1 << 28)) {
        *(dst++) = v | B;
        *(dst++) = (v >> 7) | B;
        *(dst++) = (v >> 14) | B;
        *(dst++) = v >> 21;
    } else {
        *(dst++) = v | B;
        *(dst++) = (v >> 7) | B;
        *(dst++) = (v >> 14) | B;
        *(dst++) = (v >> 21) | B;
        *(dst++) = v >> 28;
    }
    return dst;
}

const uint8_t* decode_varint32_ptr_fallback(const uint8_t* p, const uint8_t* limit,
                                         uint32_t* value) {
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
        uint32_t byte = *p;
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return p;
        }
    }
    return nullptr;
}

const uint8_t* decode_varint64_ptr(const uint8_t* p, const uint8_t* limit, uint64_t* value) {
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint64_t byte = *p;
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return p;
        }
    }
    return nullptr;
}

}
