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

#include <fstream>
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

template <FieldType field_type>
typename CppTypeTraits<field_type>::CppType decode_float(const std::string& encoded) {
    Slice encoded_key(encoded);
    typename CppTypeTraits<field_type>::CppType result;
    uint8_t* ptr = reinterpret_cast<uint8_t*>(&result);
    Status status = KeyCoderTraits<field_type>::decode_ascending(&encoded_key, sizeof(result), ptr);
    EXPECT_TRUE(status.ok());
    return result;
}

template <FieldType field_type>
std::string encode_float(typename CppTypeTraits<field_type>::CppType value) {
    std::string buf;
    KeyCoderTraits<field_type>::full_encode_ascending(&value, &buf);
    return buf;
}

template <FieldType field_type>
void test_encode_decode(typename CppTypeTraits<field_type>::CppType value) {
    std::string encoded = encode_float<field_type>(value);
    typename CppTypeTraits<field_type>::CppType decoded = decode_float<field_type>(encoded);
    EXPECT_EQ(value, decoded);
}

template <FieldType field_type>
void test_ordering(typename CppTypeTraits<field_type>::CppType a,
                   typename CppTypeTraits<field_type>::CppType b) {
    std::string encoded_a = encode_float<field_type>(a);
    std::string encoded_b = encode_float<field_type>(b);

    bool a_is_nan = std::isnan(a);
    bool b_is_nan = std::isnan(b);

    if (a_is_nan && b_is_nan) {
        EXPECT_EQ(encoded_a, encoded_b);
    } else if (a_is_nan) {
        EXPECT_GT(encoded_a, encoded_b);
    } else if (b_is_nan) {
        EXPECT_LT(encoded_a, encoded_b);
    } else if (a < b) {
        EXPECT_LT(encoded_a, encoded_b);
    } else if (a > b) {
        EXPECT_GT(encoded_a, encoded_b);
    } else {
        if (std::signbit(a) && !std::signbit(b)) {
            EXPECT_LT(encoded_a, encoded_b);
        } else if (!std::signbit(a) && std::signbit(b)) {
            EXPECT_GT(encoded_a, encoded_b);
        } else {
            EXPECT_EQ(encoded_a, encoded_b);
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

TEST(KeyCoderTraitsTest, FloatEncodeDecode) {
    test_encode_decode<FieldType::OLAP_FIELD_TYPE_FLOAT>(3.14f);
    test_encode_decode<FieldType::OLAP_FIELD_TYPE_FLOAT>(-3.14f);
    test_encode_decode<FieldType::OLAP_FIELD_TYPE_FLOAT>(0.0f);
    test_encode_decode<FieldType::OLAP_FIELD_TYPE_FLOAT>(-0.0f);
    test_encode_decode<FieldType::OLAP_FIELD_TYPE_FLOAT>(std::numeric_limits<float>::max());
    test_encode_decode<FieldType::OLAP_FIELD_TYPE_FLOAT>(std::numeric_limits<float>::min());
    test_encode_decode<FieldType::OLAP_FIELD_TYPE_FLOAT>(std::numeric_limits<float>::lowest());
}

TEST(KeyCoderTraitsTest, FloatOrdering) {
    test_ordering<FieldType::OLAP_FIELD_TYPE_FLOAT>(-1.0f, 1.0f);
    test_ordering<FieldType::OLAP_FIELD_TYPE_FLOAT>(-2.0f, -1.0f);
    test_ordering<FieldType::OLAP_FIELD_TYPE_FLOAT>(-0.0f, 0.0f);
    test_ordering<FieldType::OLAP_FIELD_TYPE_FLOAT>(1.0f, 2.0f);
    test_ordering<FieldType::OLAP_FIELD_TYPE_FLOAT>(0.0f, 0.0f);
    test_ordering<FieldType::OLAP_FIELD_TYPE_FLOAT>(std::numeric_limits<float>::lowest(),
                                                    std::numeric_limits<float>::max());
}

TEST(KeyCoderTraitsTest, DoubleEncodeDecode) {
    test_encode_decode<FieldType::OLAP_FIELD_TYPE_DOUBLE>(3.1415926535);
    test_encode_decode<FieldType::OLAP_FIELD_TYPE_DOUBLE>(-3.1415926535);
    test_encode_decode<FieldType::OLAP_FIELD_TYPE_DOUBLE>(0.0);
    test_encode_decode<FieldType::OLAP_FIELD_TYPE_DOUBLE>(-0.0);
    test_encode_decode<FieldType::OLAP_FIELD_TYPE_DOUBLE>(std::numeric_limits<double>::max());
    test_encode_decode<FieldType::OLAP_FIELD_TYPE_DOUBLE>(std::numeric_limits<double>::min());
    test_encode_decode<FieldType::OLAP_FIELD_TYPE_DOUBLE>(std::numeric_limits<double>::lowest());
}

TEST(KeyCoderTraitsTest, DoubleOrdering) {
    test_ordering<FieldType::OLAP_FIELD_TYPE_DOUBLE>(-1.0, 1.0);
    test_ordering<FieldType::OLAP_FIELD_TYPE_DOUBLE>(-2.0, -1.0);
    test_ordering<FieldType::OLAP_FIELD_TYPE_DOUBLE>(-0.0, 0.0);
    test_ordering<FieldType::OLAP_FIELD_TYPE_DOUBLE>(1.0, 2.0);
    test_ordering<FieldType::OLAP_FIELD_TYPE_DOUBLE>(0.0, 0.0);
    test_ordering<FieldType::OLAP_FIELD_TYPE_DOUBLE>(std::numeric_limits<double>::lowest(),
                                                     std::numeric_limits<double>::max());
}

TEST(KeyCoderTraitsTest, FloatSpecialValues) {
    {
        std::string encoded = encode_float<FieldType::OLAP_FIELD_TYPE_FLOAT>(
                std::numeric_limits<float>::quiet_NaN());
        EXPECT_EQ("FFC00000", hexdump(encoded.data(), encoded.size()));
    }
    {
        std::string encoded = encode_float<FieldType::OLAP_FIELD_TYPE_FLOAT>(
                std::numeric_limits<float>::infinity());
        EXPECT_EQ("FF800000", hexdump(encoded.data(), encoded.size()));
    }
    {
        std::string encoded = encode_float<FieldType::OLAP_FIELD_TYPE_FLOAT>(1.0f);
        EXPECT_EQ("BF800000", hexdump(encoded.data(), encoded.size()));
    }
    {
        std::string encoded = encode_float<FieldType::OLAP_FIELD_TYPE_FLOAT>(0.0f);
        EXPECT_EQ("80000000", hexdump(encoded.data(), encoded.size()));
    }
    {
        std::string encoded = encode_float<FieldType::OLAP_FIELD_TYPE_FLOAT>(-0.0f);
        EXPECT_EQ("7FFFFFFF", hexdump(encoded.data(), encoded.size()));
    }
    {
        std::string encoded = encode_float<FieldType::OLAP_FIELD_TYPE_FLOAT>(-1.0f);
        EXPECT_EQ("407FFFFF", hexdump(encoded.data(), encoded.size()));
    }
    {
        std::string encoded = encode_float<FieldType::OLAP_FIELD_TYPE_FLOAT>(
                -std::numeric_limits<float>::infinity());
        EXPECT_EQ("007FFFFF", hexdump(encoded.data(), encoded.size()));
    }

    test_ordering<FieldType::OLAP_FIELD_TYPE_FLOAT>(-std::numeric_limits<float>::infinity(), -1.0f);
    test_ordering<FieldType::OLAP_FIELD_TYPE_FLOAT>(-1.0f, -0.0f);
    test_ordering<FieldType::OLAP_FIELD_TYPE_FLOAT>(-0.0f, 0.0f);
    test_ordering<FieldType::OLAP_FIELD_TYPE_FLOAT>(0.0f, 1.0f);
    test_ordering<FieldType::OLAP_FIELD_TYPE_FLOAT>(1.0f, std::numeric_limits<float>::infinity());
    test_ordering<FieldType::OLAP_FIELD_TYPE_FLOAT>(std::numeric_limits<float>::infinity(),
                                                    std::numeric_limits<float>::quiet_NaN());
    test_ordering<FieldType::OLAP_FIELD_TYPE_FLOAT>(std::numeric_limits<float>::quiet_NaN(),
                                                    std::numeric_limits<float>::quiet_NaN());
}

TEST(KeyCoderTraitsTest, DoubleSpecialValues) {
    {
        std::string encoded = encode_float<FieldType::OLAP_FIELD_TYPE_DOUBLE>(
                std::numeric_limits<double>::quiet_NaN());
        EXPECT_EQ("FFF8000000000000", hexdump(encoded.data(), encoded.size()));
    }
    {
        std::string encoded = encode_float<FieldType::OLAP_FIELD_TYPE_DOUBLE>(
                std::numeric_limits<double>::infinity());
        EXPECT_EQ("FFF0000000000000", hexdump(encoded.data(), encoded.size()));
    }
    {
        std::string encoded = encode_float<FieldType::OLAP_FIELD_TYPE_DOUBLE>(1.0);
        EXPECT_EQ("BFF0000000000000", hexdump(encoded.data(), encoded.size()));
    }
    {
        std::string encoded = encode_float<FieldType::OLAP_FIELD_TYPE_DOUBLE>(0.0);
        EXPECT_EQ("8000000000000000", hexdump(encoded.data(), encoded.size()));
    }
    {
        std::string encoded = encode_float<FieldType::OLAP_FIELD_TYPE_DOUBLE>(-0.0);
        EXPECT_EQ("7FFFFFFFFFFFFFFF", hexdump(encoded.data(), encoded.size()));
    }
    {
        std::string encoded = encode_float<FieldType::OLAP_FIELD_TYPE_DOUBLE>(-1.0);
        EXPECT_EQ("400FFFFFFFFFFFFF", hexdump(encoded.data(), encoded.size()));
    }
    {
        std::string encoded = encode_float<FieldType::OLAP_FIELD_TYPE_DOUBLE>(
                -std::numeric_limits<double>::infinity());
        EXPECT_EQ("000FFFFFFFFFFFFF", hexdump(encoded.data(), encoded.size()));
    }

    test_ordering<FieldType::OLAP_FIELD_TYPE_DOUBLE>(-std::numeric_limits<double>::infinity(),
                                                     -1.0);
    test_ordering<FieldType::OLAP_FIELD_TYPE_DOUBLE>(-1.0, -0.0);
    test_ordering<FieldType::OLAP_FIELD_TYPE_DOUBLE>(-0.0, 0.0);
    test_ordering<FieldType::OLAP_FIELD_TYPE_DOUBLE>(0.0, 1.0);
    test_ordering<FieldType::OLAP_FIELD_TYPE_DOUBLE>(1.0, std::numeric_limits<double>::infinity());
    test_ordering<FieldType::OLAP_FIELD_TYPE_DOUBLE>(std::numeric_limits<double>::infinity(),
                                                     std::numeric_limits<double>::quiet_NaN());
    test_ordering<FieldType::OLAP_FIELD_TYPE_DOUBLE>(std::numeric_limits<double>::quiet_NaN(),
                                                     std::numeric_limits<double>::quiet_NaN());
}

TEST(KeyCoderTraitsTest, FloatComprehensiveOrdering) {
    std::vector<float> values = {-std::numeric_limits<float>::infinity(),
                                 -100.0f,
                                 -1.0f,
                                 -0.0f,
                                 0.0f,
                                 1.0f,
                                 100.0f,
                                 std::numeric_limits<float>::infinity(),
                                 std::numeric_limits<float>::quiet_NaN()};

    for (size_t i = 0; i < values.size(); ++i) {
        for (size_t j = 0; j < values.size(); ++j) {
            test_ordering<FieldType::OLAP_FIELD_TYPE_FLOAT>(values[i], values[j]);
        }
    }
}

TEST(KeyCoderTraitsTest, DoubleComprehensiveOrdering) {
    std::vector<double> values = {-std::numeric_limits<double>::infinity(),
                                  -100.0,
                                  -1.0,
                                  -0.0,
                                  0.0,
                                  1.0,
                                  100.0,
                                  std::numeric_limits<double>::infinity(),
                                  std::numeric_limits<double>::quiet_NaN()};

    for (size_t i = 0; i < values.size(); ++i) {
        for (size_t j = 0; j < values.size(); ++j) {
            test_ordering<FieldType::OLAP_FIELD_TYPE_DOUBLE>(values[i], values[j]);
        }
    }
static const std::string _filename = "key_coder_test.dat";

// FieldType + original_value + encoded_value
template <typename T>
void WriteNumberToFile(std::ofstream& out, const T& value, FieldType field_type) {
    const KeyCoder* coder = get_key_coder(field_type);

    std::string encoded_buf;
    coder->full_encode_ascending(&value, &encoded_buf);

    const uint8_t field_type_value = static_cast<uint8_t>(field_type);

    // 1. FieldType
    out.write(reinterpret_cast<const char*>(&field_type_value), sizeof(field_type_value));

    // 2. original_value
    size_t value_size = sizeof(T);
    out.write(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
    out.write(reinterpret_cast<const char*>(&value), value_size);

    // 3. encoded_value
    size_t encoded_size = encoded_buf.size();
    out.write(reinterpret_cast<const char*>(&encoded_size), sizeof(encoded_size));
    out.write(encoded_buf.data(), encoded_size);
}

// FieldType + index_size + original_value + encoded_value
void WriteStringToFile(std::ofstream& out, const std::string& value, FieldType field_type,
                       size_t index_size = 0) {
    const KeyCoder* coder = get_key_coder(field_type);
    Slice slice(value.data(), value.size());
    std::string encoded_buf;
    coder->encode_ascending(&slice, index_size, &encoded_buf);

    const uint8_t field_type_value = static_cast<uint8_t>(field_type);

    // 1. FieldType
    out.write(reinterpret_cast<const char*>(&field_type_value), sizeof(field_type_value));

    // 2. index_size
    out.write(reinterpret_cast<const char*>(&index_size), sizeof(index_size));

    size_t value_size = value.size();

    // 3. original_value
    out.write(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
    out.write(value.data(), value_size);

    // 4. encoded_value
    size_t encoded_size = encoded_buf.size();
    out.write(reinterpret_cast<const char*>(&encoded_size), sizeof(encoded_size));
    out.write(encoded_buf.data(), encoded_size);
}

// Helper function to read from file and decode
template <FieldType field_type>
void ReadAndDecodeNumber(std::ifstream& in) {
    using T = typename CppTypeTraits<field_type>::CppType;

    // 2. read original_value
    size_t value_size = 0;
    in.read(reinterpret_cast<char*>(&value_size), sizeof(value_size));

    T original_value;
    in.read(reinterpret_cast<char*>(&original_value), value_size);

    // 3. read encoded_value
    size_t encoded_size;
    in.read(reinterpret_cast<char*>(&encoded_size), sizeof(encoded_size));
    std::vector<char> encoded_buf(encoded_size);
    in.read(encoded_buf.data(), encoded_size);

    const KeyCoder* coder = get_key_coder(field_type);
    ASSERT_NE(coder, nullptr);

    // test1: decode
    T decoded_value;
    Slice slice(encoded_buf.data(), encoded_size);
    auto st = coder->decode_ascending(&slice, 0, (uint8_t*)&decoded_value);
    EXPECT_TRUE(st.ok()) << st.msg();
    EXPECT_EQ(decoded_value, original_value);

    // test2: full_encode
    std::string encoded_buf_full;
    coder->full_encode_ascending(&original_value, &encoded_buf_full);
    EXPECT_EQ(std::string_view(encoded_buf_full),
              std::string_view(encoded_buf.data(), encoded_size));
}

template <FieldType field_type>
void ReadAndDecodeString(std::ifstream& in) {
    // 2. read index_size
    size_t index_size;
    in.read(reinterpret_cast<char*>(&index_size), sizeof(index_size));

    // 3. read original_value
    size_t value_size;
    in.read(reinterpret_cast<char*>(&value_size), sizeof(value_size));
    std::vector<char> value_buf(value_size);
    in.read(value_buf.data(), value_size);

    // 4. read encoded_value
    size_t encoded_size;
    in.read(reinterpret_cast<char*>(&encoded_size), sizeof(encoded_size));
    std::vector<char> encoded_buf(encoded_size);
    in.read(encoded_buf.data(), encoded_size);

    const KeyCoder* coder = get_key_coder(field_type);
    ASSERT_NE(coder, nullptr);

    // test
    EXPECT_EQ(std::string_view(value_buf.data(), index_size),
              std::string_view(encoded_buf.data(), encoded_size));
}

template <FieldType field_type>
void WriteAllBoundaryValues(std::ofstream& out) {
    using T = typename CppTypeTraits<field_type>::CppType;
    WriteNumberToFile<T>(out, std::numeric_limits<T>::min(), field_type);
    WriteNumberToFile<T>(out, T(0), field_type);
    WriteNumberToFile<T>(out, std::numeric_limits<T>::max(), field_type);
}

void WriteStringsValues(std::ofstream& out, const std::string& str) {
    WriteStringToFile(out, str, FieldType::OLAP_FIELD_TYPE_CHAR, 0);
    WriteStringToFile(out, str, FieldType::OLAP_FIELD_TYPE_CHAR, str.size() / 2);
    WriteStringToFile(out, str, FieldType::OLAP_FIELD_TYPE_CHAR, str.size());
    WriteStringToFile(out, str, FieldType::OLAP_FIELD_TYPE_VARCHAR, 0);
    WriteStringToFile(out, str, FieldType::OLAP_FIELD_TYPE_VARCHAR, str.size() / 2);
    WriteStringToFile(out, str, FieldType::OLAP_FIELD_TYPE_VARCHAR, str.size());
    WriteStringToFile(out, str, FieldType::OLAP_FIELD_TYPE_STRING, 0);
    WriteStringToFile(out, str, FieldType::OLAP_FIELD_TYPE_STRING, str.size() / 2);
    WriteStringToFile(out, str, FieldType::OLAP_FIELD_TYPE_STRING, str.size());
}

TEST_F(KeyCoderTest, write_and_read) {
    std::ofstream out(_filename, std::ios::binary | std::ios::trunc);
    EXPECT_TRUE(out.is_open()) << "Failed to open file for writing: " << _filename;

    // 1. write integers
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_TINYINT>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_SMALLINT>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_INT>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_BIGINT>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_LARGEINT>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DATETIME>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DATE>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DECIMAL>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_BOOL>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DATEV2>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DECIMAL32>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DECIMAL64>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DECIMAL256>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_IPV4>(out);
    WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_IPV6>(out);

    // 2. write strings
    const std::string empty_str = "";
    WriteStringsValues(out, empty_str);

    const std::string control_chars = "\n\r\t\b\f\v\a\\\"\'";
    WriteStringsValues(out, control_chars);

    const std::string null_byte(1, '\0');
    WriteStringsValues(out, null_byte);

    const std::string ascii_edges = std::string("\x00\x7F");
    WriteStringsValues(out, ascii_edges);

    const std::string chinese = "中文测试：龘，𠮷，丂！";
    WriteStringsValues(out, chinese);

    const std::string korean = "안녕하세요";
    WriteStringsValues(out, korean);

    const std::string japanese = "こんにちは世界";
    WriteStringsValues(out, japanese);

    const std::string symbols = "αβγδε ∑∏∞∫√";
    WriteStringsValues(out, symbols);

    const std::string html_like = "<div class=\"test\">&copy; 2025</div>";
    WriteStringsValues(out, html_like);

    const std::string escaped_literal = R"(This is not a real newline: \n)";
    WriteStringsValues(out, escaped_literal);

    const std::string long_str(1024, 'X');
    WriteStringsValues(out, long_str);

    std::string gbk_str = "\xC4\xE3\xBA\xC3";
    WriteStringsValues(out, gbk_str);

    std::string latin1_str = "\xE9\xE0\xF6";
    WriteStringsValues(out, latin1_str);

    out.close();
    std::ifstream in(_filename, std::ios::binary);
    EXPECT_TRUE(in.is_open()) << "Failed to open file for reading: " << _filename;

    while (in.peek() != EOF) {
        FieldType field_type;
        uint8_t field_type_value;

        in.read(reinterpret_cast<char*>(&field_type_value), sizeof(field_type_value));
        field_type = static_cast<FieldType>(field_type_value);

        switch (field_type) {
        case FieldType::OLAP_FIELD_TYPE_TINYINT:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_TINYINT>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_SMALLINT:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_SMALLINT>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_INT:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_INT>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_BIGINT:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_BIGINT>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_LARGEINT:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_LARGEINT>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_DATETIME:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_DATETIME>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_DATE:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_DATE>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_DECIMAL:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_DECIMAL>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_BOOL:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_BOOL>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_DATEV2:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_DATEV2>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_DECIMAL32>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_DECIMAL64>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_DECIMAL256>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_IPV4:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_IPV4>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_IPV6:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_IPV6>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_CHAR:
            ReadAndDecodeString<FieldType::OLAP_FIELD_TYPE_CHAR>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_VARCHAR:
            ReadAndDecodeString<FieldType::OLAP_FIELD_TYPE_VARCHAR>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_STRING:
            ReadAndDecodeString<FieldType::OLAP_FIELD_TYPE_STRING>(in);
            break;
        default:
            FAIL() << "Unsupported field type: " << static_cast<int>(field_type);
            break;
        }
    }
    in.close();
}

} // namespace doris
