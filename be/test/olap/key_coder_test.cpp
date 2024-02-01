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

#include "olap/key_coder.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdlib.h>
#include <string.h>

#include <limits>

#include "gtest/gtest_pred_impl.h"
#include "olap/uint24.h"
#include "util/debug_util.h"

namespace doris {

class KeyCoderTest : public testing::Test {
public:
    KeyCoderTest() = default;
    virtual ~KeyCoderTest() = default;
};

template <FieldType type>
void test_integer_encode() {
    using CppType = typename CppTypeTraits<type>::CppType;

    auto key_coder = get_key_coder(type);

    {
        std::string buf;
        CppType val = std::numeric_limits<CppType>::min();
        key_coder->encode_ascending(&val, 1, &buf);

        std::string result;
        for (int i = 0; i < sizeof(CppType); ++i) {
            result.append("00");
        }

        EXPECT_STREQ(result.c_str(), hexdump(buf.data(), buf.size()).c_str());

        {
            Slice slice(buf);
            CppType check_val;
            static_cast<void>(
                    key_coder->decode_ascending(&slice, sizeof(CppType), (uint8_t*)&check_val));
            EXPECT_EQ(val, check_val);
        }
    }

    {
        std::string buf;
        CppType val = std::numeric_limits<CppType>::max();
        key_coder->encode_ascending(&val, sizeof(CppType), &buf);

        std::string result;
        for (int i = 0; i < sizeof(CppType); ++i) {
            result.append("FF");
        }

        EXPECT_STREQ(result.c_str(), hexdump(buf.data(), buf.size()).c_str());
        {
            Slice slice(buf);
            CppType check_val;
            static_cast<void>(
                    key_coder->decode_ascending(&slice, sizeof(CppType), (uint8_t*)&check_val));
            EXPECT_EQ(val, check_val);
        }
    }

    for (auto i = 0; i < 100; ++i) {
        CppType val1 = random();
        CppType val2 = random();

        std::string buf1;
        std::string buf2;

        key_coder->encode_ascending(&val1, sizeof(CppType), &buf1);
        key_coder->encode_ascending(&val2, sizeof(CppType), &buf2);

        if (val1 < val2) {
            EXPECT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) < 0);
        } else if (val1 > val2) {
            EXPECT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) > 0);
        } else {
            EXPECT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) == 0);
        }
    }
}

TEST_F(KeyCoderTest, test_int) {
    test_integer_encode<FieldType::OLAP_FIELD_TYPE_TINYINT>();
    test_integer_encode<FieldType::OLAP_FIELD_TYPE_SMALLINT>();
    test_integer_encode<FieldType::OLAP_FIELD_TYPE_INT>();
    test_integer_encode<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>();
    test_integer_encode<FieldType::OLAP_FIELD_TYPE_BIGINT>();
    test_integer_encode<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT>();
    test_integer_encode<FieldType::OLAP_FIELD_TYPE_LARGEINT>();

    test_integer_encode<FieldType::OLAP_FIELD_TYPE_DATETIME>();
}

TEST_F(KeyCoderTest, test_date) {
    using CppType = uint24_t;
    auto key_coder = get_key_coder(FieldType::OLAP_FIELD_TYPE_DATE);

    {
        std::string buf;
        CppType val = 0;
        key_coder->encode_ascending(&val, 1, &buf);

        std::string result;
        for (int i = 0; i < sizeof(uint24_t); ++i) {
            result.append("00");
        }

        EXPECT_STREQ(result.c_str(), hexdump(buf.data(), buf.size()).c_str());

        {
            Slice slice(buf);
            CppType check_val;
            static_cast<void>(
                    key_coder->decode_ascending(&slice, sizeof(CppType), (uint8_t*)&check_val));
            EXPECT_EQ(val, check_val);
        }
    }

    {
        std::string buf;
        CppType val = 10000;
        key_coder->encode_ascending(&val, sizeof(CppType), &buf);

        std::string result("002710");

        EXPECT_STREQ(result.c_str(), hexdump(buf.data(), buf.size()).c_str());
        {
            Slice slice(buf);
            CppType check_val;
            static_cast<void>(
                    key_coder->decode_ascending(&slice, sizeof(CppType), (uint8_t*)&check_val));
            EXPECT_EQ(val, check_val);
        }
    }

    for (auto i = 0; i < 100; ++i) {
        CppType val1 = random();
        CppType val2 = random();

        std::string buf1;
        std::string buf2;

        key_coder->encode_ascending(&val1, sizeof(CppType), &buf1);
        key_coder->encode_ascending(&val2, sizeof(CppType), &buf2);

        if (val1 < val2) {
            EXPECT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) < 0);
        } else if (val1 > val2) {
            EXPECT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) > 0);
        } else {
            EXPECT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) == 0);
        }
    }
}

TEST_F(KeyCoderTest, test_decimal) {
    auto key_coder = get_key_coder(FieldType::OLAP_FIELD_TYPE_DECIMAL);

    decimal12_t val1 = {1, 100000000};
    std::string buf1;

    key_coder->encode_ascending(&val1, sizeof(decimal12_t), &buf1);

    decimal12_t check_val;
    Slice slice1(buf1);
    static_cast<void>(
            key_coder->decode_ascending(&slice1, sizeof(decimal12_t), (uint8_t*)&check_val));
    EXPECT_EQ(check_val, val1);

    {
        decimal12_t val2 = {-1, -100000000};
        std::string buf2;
        key_coder->encode_ascending(&val2, sizeof(decimal12_t), &buf2);
        EXPECT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) > 0);
    }
    {
        decimal12_t val2 = {1, 100000001};
        std::string buf2;
        key_coder->encode_ascending(&val2, sizeof(decimal12_t), &buf2);
        EXPECT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) < 0);
    }
    {
        decimal12_t val2 = {0, 0};
        std::string buf2;
        key_coder->encode_ascending(&val2, sizeof(decimal12_t), &buf2);
        EXPECT_TRUE(memcmp(buf1.c_str(), buf2.c_str(), buf1.size()) > 0);

        std::string result("80");
        for (int i = 0; i < sizeof(int64_t) - 1; ++i) {
            result.append("00");
        }
        result.append("80");
        for (int i = 0; i < sizeof(int32_t) - 1; ++i) {
            result.append("00");
        }

        EXPECT_STREQ(result.c_str(), hexdump(buf2.data(), buf2.size()).c_str());
    }
}

TEST_F(KeyCoderTest, test_char) {
    auto key_coder = get_key_coder(FieldType::OLAP_FIELD_TYPE_CHAR);

    char buf[] = "1234567890";
    Slice slice(buf, 10);

    {
        std::string key;
        key_coder->encode_ascending(&slice, 10, &key);
        Slice encoded_key(key);
        /*
        Slice check_slice;
        auto st = key_coder->decode_ascending(&encoded_key, 10, (uint8_t*)&check_slice, &_pool);
        EXPECT_TRUE(st.ok());

        EXPECT_EQ(10, check_slice.size);
        EXPECT_EQ(strncmp("1234567890", check_slice.data, 10), 0);
        */
    }

    {
        std::string key;
        key_coder->encode_ascending(&slice, 5, &key);
        Slice encoded_key(key);
        /*
        Slice check_slice;
        auto st = key_coder->decode_ascending(&encoded_key, 5, (uint8_t*)&check_slice, &_pool);
        EXPECT_TRUE(st.ok());

        EXPECT_EQ(5, check_slice.size);
        EXPECT_EQ(strncmp("12345", check_slice.data, 5), 0);
        */
    }
}

TEST_F(KeyCoderTest, test_varchar) {
    auto key_coder = get_key_coder(FieldType::OLAP_FIELD_TYPE_VARCHAR);

    char buf[] = "1234567890";
    Slice slice(buf, 10);

    {
        std::string key;
        key_coder->encode_ascending(&slice, 15, &key);
        Slice encoded_key(key);
        /*
        Slice check_slice;
        auto st = key_coder->decode_ascending(&encoded_key, 15, (uint8_t*)&check_slice, &_pool);
        EXPECT_TRUE(st.ok());

        EXPECT_EQ(10, check_slice.size);
        EXPECT_EQ(strncmp("1234567890", check_slice.data, 10), 0);
        */
    }

    {
        std::string key;
        key_coder->encode_ascending(&slice, 5, &key);
        Slice encoded_key(key);
        /*
        Slice check_slice;
        auto st = key_coder->decode_ascending(&encoded_key, 5, (uint8_t*)&check_slice, &_pool);
        EXPECT_TRUE(st.ok());

        EXPECT_EQ(5, check_slice.size);
        EXPECT_EQ(strncmp("12345", check_slice.data, 5), 0);
        */
    }
}

} // namespace doris
