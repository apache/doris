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

#include <string>

#include "meta-store/versionstamp.h"

namespace doris::cloud {

struct EncodingTag {
    // Tags for each type
    enum Tag : unsigned char {
        BYTES_TAG = 0x10,
        NEGATIVE_FIXED_INT_TAG = 0x11,
        POSITIVE_FIXED_INT_TAG = 0x12,
        VERSIONSTAMP_TAG = 0x13,
        VERSIONSTAMP_END_TAG = 0xFF,
    };

    // Magic value used for encoding
    enum E1 : unsigned char {
        BYTE_ESCAPE = 0x00,
        BYTES_ENDING = 0x01,
        ESCAPED_00 = 0xff,
    };
};

/**
 * Encodes a byte sequence. Order is preserved.
 *
 * e.g.
 *
 * 0xdead00beef => 0x10 dead 00ff beef 0001
 *
 * @param bytes byte sequence to encode
 * @param b output, result will append to this string
 */
void encode_bytes(std::string_view bytes, std::string* b);

/**
 * Decodes byte sequence which is generated with `encode_bytes`
 *
 * @param in intput for decoding
 * @param out output
 * @return 0 for success otherwise error
 */
int decode_bytes(std::string_view* in, std::string* out);

/**
 * Encodes a versionstamp.
 * The versionstamp is encoded as a 10-byte array with a tag.
 * The first byte is the tag (0x13 for versionstamp),
 * followed by the 10 bytes of the versionstamp.
 *
 * @param vs The versionstamp to encode
 * @param b Output string where the encoded versionstamp will be appended
 * @return The index of the versionstamp in the buffer
 */
uint32_t encode_versionstamp(const Versionstamp& vs, std::string* b);

/**
 * Decodes a versionstamp.
 * The input string must start with the versionstamp tag (0x13),
 * followed by the 10 bytes of the versionstamp.
 *
 * @param in Input string view containing the encoded versionstamp
 * @param vs Output versionstamp object where the decoded versionstamp will be stored
 * @return 0 for success, otherwise error
 */
int decode_versionstamp(std::string_view* in, Versionstamp* vs);

/**
 * Decodes a versionstamp from the tailing of the input string.
 * The input string must end with the versionstamp tag (0x13),
 * followed by the 10 bytes of the versionstamp.
 *
 * @param in Input string view containing the encoded versionstamp
 * @param vs Output versionstamp object where the decoded versionstamp will be stored
 * @return 0 for success, otherwise error
 */
int decode_tailing_versionstamp(std::string_view* in, Versionstamp* vs);

/**
 * Encodes the end of a versionstamp sequence.
 * This is used to mark the end of a versionstamp in a byte stream.
 *
 * @param b Output string where the encoded end marker will be appended
 */
void encode_versionstamp_end(std::string* b);

/**
 * Decodes the end of a versionstamp sequence.
 * This is used to mark the end of a versionstamp in a byte stream.
 *
 * @param in Input string view containing the encoded end marker
 * @return 0 for success, otherwise error
 */
int decode_versionstamp_end(std::string_view* in);

/**
 * Decodes the end of a versionstamp sequence from the tailing of the input string.
 *
 * @param in Input string view containing the encoded end marker
 * @return 0 for success, otherwise error
 */
int decode_tailing_versionstamp_end(std::string_view* in);

/**
 * Encodes int64 to 8-byte big endian
 * Negative 0x11 0000000000000000
 * Positive 0x12 0000000000000000
 * FIXME: use entire 8-bytes
 */
void encode_int64(int64_t val, std::string* b);

/**
 * Decodes byte sequence which is generated with `encode_int64`
 *
 * @param in intput for decoding
 * @param val output
 * @return 0 for success otherwise error
 */
int decode_int64(std::string_view* in, int64_t* val);

} // namespace doris::cloud
