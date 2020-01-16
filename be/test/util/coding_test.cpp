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

#include <gtest/gtest.h>

#include <iostream>

namespace doris {

class CodingTest : public testing::Test {
public:
    CodingTest() {}
    virtual ~CodingTest() {}
};

TEST_F(CodingTest, fixed_le) {
    uint8_t buf[64];

    encode_fixed8(buf, 124);
    uint8_t val8 = decode_fixed8(buf);
    ASSERT_EQ(124, val8);

    encode_fixed16_le(buf, 12345);
    uint16_t val16 = decode_fixed16_le(buf);
    ASSERT_EQ(12345, val16);

    encode_fixed32_le(buf, 1234554321);
    uint32_t val32 = decode_fixed32_le(buf);
    ASSERT_EQ(1234554321, val32);

    encode_fixed64_le(buf, 12345543211234554321UL);
    uint64_t val64 = decode_fixed64_le(buf);
    ASSERT_EQ(12345543211234554321UL, val64);

    std::string str;
    put_fixed32_le(&str, val32);
    put_fixed64_le(&str, val64);

    ASSERT_EQ(4 + 8, str.size());
    val32 = decode_fixed32_le((const uint8_t*)str.data());
    ASSERT_EQ(1234554321, val32);

    encode_fixed64_le(buf, 12345543211234554321UL);
    val64 = decode_fixed64_le((const uint8_t*)str.data() + 4);
    ASSERT_EQ(12345543211234554321UL, val64);
}

TEST_F(CodingTest, variant) {
    uint8_t buf[64];

    {
        uint8_t* ptr = buf;
        ptr = encode_varint32(ptr, 1);
        ptr = encode_varint64(ptr, 2);
        ASSERT_EQ(2, ptr - buf);
    }

    const uint8_t* ptr = buf;
    const uint8_t* limit = ptr + 64;
    uint32_t val32;
    ptr = decode_varint32_ptr(ptr, limit, &val32);
    ASSERT_NE(nullptr, ptr);
    ASSERT_EQ(1, val32);
    uint64_t val64;
    ptr = decode_varint64_ptr(ptr, limit, &val64);
    ASSERT_NE(nullptr, ptr);
    ASSERT_EQ(2, val64);
}

TEST_F(CodingTest, variant_bigvalue) {
    uint8_t buf[64];

    {
        uint8_t* ptr = buf;
        ptr = encode_varint32(ptr, 1234554321UL);
        ASSERT_EQ(5, ptr - buf);
        ptr = encode_varint64(ptr, 12345543211234554321UL);
        ASSERT_EQ(5 + 10, ptr - buf);
    }

    const uint8_t* ptr = buf;
    const uint8_t* limit = ptr + 64;
    uint32_t val32;
    ptr = decode_varint32_ptr(ptr, limit, &val32);
    ASSERT_NE(nullptr, ptr);
    ASSERT_EQ(1234554321UL, val32);
    uint64_t val64;
    ptr = decode_varint64_ptr(ptr, limit, &val64);
    ASSERT_NE(nullptr, ptr);
    ASSERT_EQ(12345543211234554321UL, val64);
}

TEST_F(CodingTest, variant_fail) {
    uint8_t buf[64];

    {
        uint8_t* ptr = buf;
        ptr = encode_varint32(ptr, 1234554321UL);
    }
    {
        const uint8_t* ptr = buf;
        const uint8_t* limit = ptr + 4;
        uint32_t val32;
        ptr = decode_varint32_ptr(ptr, limit, &val32);
        ASSERT_EQ(nullptr, ptr);
    }

    {
        uint8_t* ptr = buf;
        ptr = encode_varint64(ptr, 12345543211234554321UL);
    }
    {
        const uint8_t* ptr = buf;
        const uint8_t* limit = ptr + 4;
        uint64_t val64;
        ptr = decode_varint64_ptr(ptr, limit, &val64);
        ASSERT_EQ(nullptr, ptr);
    }
}

TEST_F(CodingTest, put_varint) {
    std::string val;

    put_varint32(&val, 1);
    put_varint64(&val, 2);
    put_varint64_varint32(&val, 3, 4);

    ASSERT_EQ(4, val.size());
    const uint8_t* ptr = (const uint8_t*)val.data();
    const uint8_t* limit = ptr + 4;
    uint32_t val32;
    uint64_t val64;
    ptr = decode_varint32_ptr(ptr, limit, &val32);
    ASSERT_EQ(1, val32);
    ptr = decode_varint64_ptr(ptr, limit, &val64);
    ASSERT_EQ(2, val64);
    ptr = decode_varint64_ptr(ptr, limit, &val64);
    ASSERT_EQ(3, val64);
    ptr = decode_varint32_ptr(ptr, limit, &val32);
    ASSERT_EQ(4, val32);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
