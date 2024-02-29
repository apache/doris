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

#include "codec.h"

#include <cstdint>

namespace doris::cloud {

void encode_bytes(std::string_view bytes, std::string* b) {
    //                     possible_num_0x00        prefix_suffix
    b->reserve(b->size() + (bytes.size() / 256 * 2) + 3);
    b->push_back(static_cast<char>(EncodingTag::BYTES_TAG));
    std::size_t escape_pos = 0;
    std::size_t start_pos = 0;
    while (true) {
        escape_pos = bytes.find(EncodingTag::BYTE_ESCAPE, escape_pos);
        if (escape_pos == std::string::npos) {
            break;
        }
        b->insert(b->end(), bytes.begin() + start_pos, bytes.begin() + escape_pos);
        b->push_back(EncodingTag::BYTE_ESCAPE);
        b->push_back(EncodingTag::ESCAPED_00);
        ++escape_pos;
        start_pos = escape_pos;
    }
    b->insert(b->end(), bytes.begin() + start_pos, bytes.end());
    b->push_back(EncodingTag::BYTE_ESCAPE);
    b->push_back(EncodingTag::BYTES_ENDING);
}

/**
 * Decodes byte sequence which is generated with `encode_bytes`
 *
 * @param in intput for decoding
 * @param out output
 * @return 0 for success
 */
int decode_bytes(std::string_view* in, std::string* out) {
    if (in->at(0) != EncodingTag::BYTES_TAG) return -1;
    using byte = unsigned char;
    in->remove_prefix(1); // Remove bytes marker
    while (true) {
        size_t pos = in->find(EncodingTag::BYTE_ESCAPE);
        if (pos == std::string::npos) { // At least one should be found
            // No EncodingTag::BYTE_ESCAPE found, array without ending
            return -2;
        }
        if ((pos + 1) >= in->size()) {
            // Malformed bytes encoding
            return -3;
        }
        byte c = static_cast<byte>((*in)[pos + 1]);
        if (c == EncodingTag::BYTES_ENDING) {
            if (out != nullptr) {
                out->append(in->data(), pos);
            }
            in->remove_prefix(pos + 2);
            break;
        } else if (c == EncodingTag::ESCAPED_00) {
            if (out != nullptr) {
                out->append(in->data(), pos + 1);
            }
            in->remove_prefix(pos + 2);
        } else {
            // undefined escaping marker
            return -4;
        }
    }
    return 0;
}

/**
 * Encodes int64 to 8-byte big endian
 * FIXME: use entire 8-bytes
 */
void encode_int64(int64_t val, std::string* b) {
    // static_assert(std::endian::little); // Since c++20
    std::string dat(9, '\x00');
    dat[0] = val < 0 ? EncodingTag::NEGATIVE_FIXED_INT_TAG : EncodingTag::POSITIVE_FIXED_INT_TAG;
    int64_t& v = *reinterpret_cast<int64_t*>(dat.data() + 1);
    v = val < 0 ? -val : val;
    // clang-format off
    // assert: highest bit (sign) is never 1
    v = ((v & 0xffffffff00000000) >> 32) | ((v & 0x00000000ffffffff) << 32);
    v = ((v & 0xffff0000ffff0000) >> 16) | ((v & 0x0000ffff0000ffff) << 16);
    v = ((v & 0xff00ff00ff00ff00) >> 8)  | ((v & 0x00ff00ff00ff00ff) << 8);
    // clang-format on
    b->reserve(b->size() + dat.size());
    b->insert(b->end(), dat.begin(), dat.end());
}

int decode_int64(std::string_view* in, int64_t* val) {
    // static_assert(std::endian::little); // Since c++20
    if (in->size() < 9) return -1; // Insufficient length to decode
    if (in->at(0) != EncodingTag::NEGATIVE_FIXED_INT_TAG &&
        in->at(0) != EncodingTag::POSITIVE_FIXED_INT_TAG) {
        // Invalid tag
        return -2;
    }
    bool is_negative = in->at(0) == EncodingTag::NEGATIVE_FIXED_INT_TAG;
    uint64_t v = *reinterpret_cast<const uint64_t*>(in->data() + 1);
    // clang-format off
    // assert: highest bit (sign) is never 1
    v = ((v & 0xffffffff00000000) >> 32) | ((v & 0x00000000ffffffff) << 32);
    v = ((v & 0xffff0000ffff0000) >> 16) | ((v & 0x0000ffff0000ffff) << 16);
    v = ((v & 0xff00ff00ff00ff00) >> 8)  | ((v & 0x00ff00ff00ff00ff) << 8);
    // clang-format on

    // We haven't used entire 64 bits of unsigned int64 yet, hence we treat
    // EncodingTag::NEGATIVE_FIXED_INT_TAG and EncodingTag::POSITIVE_FIXED_INT_TAG
    // the same here
    *val = static_cast<int64_t>(v);
    *val = is_negative ? -*val : *val;

    in->remove_prefix(9);

    return 0;
}

} // namespace doris::cloud
