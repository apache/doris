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
#include "testutil/test_util.h"
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
}

// single filedtype test
static const std::string filename = "./be/test/olap/test_data/key_coder_test.dat";
// multi filedtype test(two filedtype)
static const std::string complex_filename = "./be/test/olap/test_data/key_coder_complex.dat";

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
    EXPECT_NE(coder, nullptr);

    // test1: decode
    T decoded_value;
    Slice slice(encoded_buf.data(), encoded_size);
    auto st = coder->decode_ascending(&slice, 0, (uint8_t*)&decoded_value);
    EXPECT_TRUE(st.ok()) << st.msg();
    if constexpr (field_type == FieldType::OLAP_FIELD_TYPE_FLOAT ||
                  field_type == FieldType::OLAP_FIELD_TYPE_DOUBLE) {
        if (std::isnan(original_value)) {
            EXPECT_TRUE(std::isnan(decoded_value));
        }
    } else {
        EXPECT_EQ(decoded_value, original_value);
    }

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
    EXPECT_NE(coder, nullptr);

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

template <FieldType field_type>
void WriteFloatValues(std::ofstream& out) {
    using T = typename CppTypeTraits<field_type>::CppType;
    WriteNumberToFile<T>(out, +0.0, field_type);
    WriteNumberToFile<T>(out, -0.0, field_type);
    WriteNumberToFile<T>(out, 1.0, field_type);
    WriteNumberToFile<T>(out, -1.0, field_type);
    WriteNumberToFile<T>(out, std::numeric_limits<T>::min(), field_type);
    WriteNumberToFile<T>(out, std::numeric_limits<T>::max(), field_type);
    WriteNumberToFile<T>(out, std::numeric_limits<T>::quiet_NaN(), field_type);
    WriteNumberToFile<T>(out, std::numeric_limits<T>::infinity(), field_type);
    WriteNumberToFile<T>(out, -std::numeric_limits<T>::infinity(), field_type);
}

void WriteDecimalValues(std::ofstream& out) {
    WriteNumberToFile<decimal12_t>(out, decimal12_t(0, 0), FieldType::OLAP_FIELD_TYPE_DECIMAL);
    WriteNumberToFile<decimal12_t>(out, decimal12_t(1, 0), FieldType::OLAP_FIELD_TYPE_DECIMAL);
    WriteNumberToFile<decimal12_t>(out, decimal12_t(-1, 0), FieldType::OLAP_FIELD_TYPE_DECIMAL);
    WriteNumberToFile<decimal12_t>(out, decimal12_t(0, 500000000),
                                   FieldType::OLAP_FIELD_TYPE_DECIMAL);
    WriteNumberToFile<decimal12_t>(out, decimal12_t(0, -500000000),
                                   FieldType::OLAP_FIELD_TYPE_DECIMAL);
    WriteNumberToFile<decimal12_t>(out, decimal12_t(999999999999999999, 999999999),
                                   FieldType::OLAP_FIELD_TYPE_DECIMAL);
    WriteNumberToFile<decimal12_t>(out, decimal12_t(-999999999999999999, -999999999),
                                   FieldType::OLAP_FIELD_TYPE_DECIMAL);
    WriteNumberToFile<decimal12_t>(out, decimal12_t(0, 999999999),
                                   FieldType::OLAP_FIELD_TYPE_DECIMAL);
    WriteNumberToFile<decimal12_t>(out, decimal12_t(0, -999999999),
                                   FieldType::OLAP_FIELD_TYPE_DECIMAL);
    WriteNumberToFile<decimal12_t>(out, decimal12_t(123, 1), FieldType::OLAP_FIELD_TYPE_DECIMAL);
    WriteNumberToFile<decimal12_t>(out, decimal12_t(123, 999999999),
                                   FieldType::OLAP_FIELD_TYPE_DECIMAL);
    WriteNumberToFile<decimal12_t>(out, decimal12_t(1, 999999999),
                                   FieldType::OLAP_FIELD_TYPE_DECIMAL);
    WriteNumberToFile<decimal12_t>(out, decimal12_t(-1, -999999999),
                                   FieldType::OLAP_FIELD_TYPE_DECIMAL);
    WriteNumberToFile<decimal12_t>(out, decimal12_t(0, 123456789),
                                   FieldType::OLAP_FIELD_TYPE_DECIMAL);
    WriteNumberToFile<decimal12_t>(out, decimal12_t(0, -123456789),
                                   FieldType::OLAP_FIELD_TYPE_DECIMAL);
}

TEST_F(KeyCoderTest, write_and_read_single_filedtype) {
    // std::ofstream out(filename, std::ios::binary | std::ios::trunc);
    // EXPECT_TRUE(out.is_open()) << "Failed to open file for writing: " << filename;

    // // 1. write integers
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_TINYINT>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_SMALLINT>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_INT>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_BIGINT>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_LARGEINT>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DATETIME>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DATE>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_BOOL>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DATEV2>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DECIMAL32>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DECIMAL64>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_DECIMAL256>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_IPV4>(out);
    // WriteAllBoundaryValues<FieldType::OLAP_FIELD_TYPE_IPV6>(out);

    // // 2. write strings
    // const std::string empty_str = "";
    // WriteStringsValues(out, empty_str);

    // const std::string control_chars = "\n\r\t\b\f\v\a\\\"\'";
    // WriteStringsValues(out, control_chars);

    // const std::string null_byte(1, '\0');
    // WriteStringsValues(out, null_byte);

    // const std::string ascii_edges = std::string("\x00\x7F");
    // WriteStringsValues(out, ascii_edges);

    // const std::string chinese = "中文测试：龘𠮷丂𡃁𠮷𠱓𡘙！";
    // WriteStringsValues(out, chinese);

    // const std::string korean = "안녕하세요";
    // WriteStringsValues(out, korean);

    // const std::string japanese = "こんにちは世界";
    // WriteStringsValues(out, japanese);

    // const std::string symbols = "αβγδε ∑∏∞∫√";
    // WriteStringsValues(out, symbols);

    // const std::string html_like = "<div class=\"test\">&copy; 2025</div>";
    // WriteStringsValues(out, html_like);

    // const std::string escaped_literal = R"(This is not a real newline: \n)";
    // WriteStringsValues(out, escaped_literal);

    // const std::string long_str(1024, 'X');
    // WriteStringsValues(out, long_str);

    // std::string gbk_str = "\xC4\xE3\xBA\xC3";
    // WriteStringsValues(out, gbk_str);

    // std::string latin1_str = "\xE9\xE0\xF6";
    // WriteStringsValues(out, latin1_str);

    // // 3. write floats
    // WriteFloatValues<FieldType::OLAP_FIELD_TYPE_FLOAT>(out);
    // WriteFloatValues<FieldType::OLAP_FIELD_TYPE_DOUBLE>(out);

    // // 4. write decimal
    // WriteDecimalValues(out);

    // out.close();

    std::ifstream in(filename, std::ios::binary);
    EXPECT_TRUE(in.is_open()) << "Failed to open file for reading: " << filename;

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
        case FieldType::OLAP_FIELD_TYPE_FLOAT:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_FLOAT>(in);
            break;
        case FieldType::OLAP_FIELD_TYPE_DOUBLE:
            ReadAndDecodeNumber<FieldType::OLAP_FIELD_TYPE_DOUBLE>(in);
            break;
        default:
            FAIL() << "Unsupported field type: " << static_cast<int>(field_type);
            break;
        }
    }
    in.close();
}

template <FieldType field_type>
void WriteSingleValueToComplexFile(std::ofstream& out) {
    if constexpr (field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL) {
        // 2. original_value
        decimal12_t value(123, 1);
        size_t value_size = sizeof(value);
        out.write(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
        out.write(reinterpret_cast<const char*>(&value), value_size);
    } else if constexpr (field_is_numeric_type(field_type) ||
                         field_type == FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT) {
        using T = typename CppTypeTraits<field_type>::CppType;
        T value = std::numeric_limits<T>::min();
        size_t value_size = sizeof(value);
        out.write(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
        out.write(reinterpret_cast<const char*>(&value), value_size);
    } else if constexpr (field_is_slice_type(field_type)) {
        const std::string value =
                "abcdefghijklmnopqrstuvwxyz\n\r\t\b\f\v\a\\\"\'\0\x00\x7F龘𠮷丂𡃁𠮷𠱓𡘙";
        size_t index_size = 30;
        if (field_type == FieldType::OLAP_FIELD_TYPE_CHAR) {
            index_size = 10;
        } else if (field_type == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
            index_size = 30;
        } else if (field_type == FieldType::OLAP_FIELD_TYPE_STRING) {
            index_size = value.size();
        }
        // 2. index_size
        out.write(reinterpret_cast<const char*>(&index_size), sizeof(index_size));

        // 3. original_value
        size_t value_size = value.size();
        out.write(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
        out.write(value.data(), value_size);
    } else {
        FAIL() << "Unsupported field type: " << static_cast<int>(field_type);
    }
}

template <FieldType field_type>
void WriteSingleEncodedValueToComplexFile(std::ofstream& out) {
    auto coder = get_key_coder(field_type);
    EXPECT_NE(coder, nullptr);
    std::string encoded_buf;
    if constexpr (field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL) {
        // 2. original_value
        decimal12_t value(123, 1);
        coder->full_encode_ascending(&value, &encoded_buf);
    } else if constexpr (field_is_numeric_type(field_type) ||
                         field_type == FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT) {
        using T = typename CppTypeTraits<field_type>::CppType;
        T value = std::numeric_limits<T>::min();
        coder->full_encode_ascending(&value, &encoded_buf);
    } else if constexpr (field_is_slice_type(field_type)) {
        const std::string value =
                "abcdefghijklmnopqrstuvwxyz\n\r\t\b\f\v\a\\\"\'\0\x00\x7F龘𠮷丂𡃁𠮷𠱓𡘙";
        size_t index_size = 30;
        if (field_type == FieldType::OLAP_FIELD_TYPE_CHAR) {
            index_size = 10;
        } else if (field_type == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
            index_size = 30;
        } else if (field_type == FieldType::OLAP_FIELD_TYPE_STRING) {
            index_size = value.size();
        }
        coder->encode_ascending(&value, index_size, &encoded_buf);
    } else {
        FAIL() << "Unsupported field type: " << static_cast<int>(field_type);
    }

    size_t encoded_size = encoded_buf.size();
    out.write(reinterpret_cast<const char*>(&encoded_size), sizeof(encoded_size));
    out.write(encoded_buf.data(), encoded_size);
}

#define DISPATCH_FIELD_VALUE(F)                                             \
    case FieldType::OLAP_FIELD_TYPE_##F:                                    \
        WriteSingleValueToComplexFile<FieldType::OLAP_FIELD_TYPE_##F>(out); \
        break;
#define DISPATCH_FIELD_ENCODED_VALUE(F)                                            \
    case FieldType::OLAP_FIELD_TYPE_##F:                                           \
        WriteSingleEncodedValueToComplexFile<FieldType::OLAP_FIELD_TYPE_##F>(out); \
        break;

void WriteSingleValueToComplexFile(std::ofstream& out, FieldType field_type) {
    switch (field_type) {
        DISPATCH_FIELD_VALUE(TINYINT)
        DISPATCH_FIELD_VALUE(SMALLINT)
        DISPATCH_FIELD_VALUE(INT)
        DISPATCH_FIELD_VALUE(UNSIGNED_INT)
        DISPATCH_FIELD_VALUE(BIGINT)
        DISPATCH_FIELD_VALUE(UNSIGNED_BIGINT)
        DISPATCH_FIELD_VALUE(LARGEINT)
        DISPATCH_FIELD_VALUE(BOOL)
        DISPATCH_FIELD_VALUE(DATE)
        DISPATCH_FIELD_VALUE(DATEV2)
        DISPATCH_FIELD_VALUE(DATETIME)
        DISPATCH_FIELD_VALUE(DATETIMEV2)
        DISPATCH_FIELD_VALUE(DECIMAL)
        DISPATCH_FIELD_VALUE(DECIMAL32)
        DISPATCH_FIELD_VALUE(DECIMAL64)
        DISPATCH_FIELD_VALUE(DECIMAL128I)
        DISPATCH_FIELD_VALUE(DECIMAL256)
        DISPATCH_FIELD_VALUE(IPV4)
        DISPATCH_FIELD_VALUE(IPV6)
        DISPATCH_FIELD_VALUE(CHAR)
        DISPATCH_FIELD_VALUE(VARCHAR)
        DISPATCH_FIELD_VALUE(STRING)
        DISPATCH_FIELD_VALUE(FLOAT)
        DISPATCH_FIELD_VALUE(DOUBLE)
    default:
        FAIL() << "Unsupported field type: " << static_cast<int>(field_type);
        break;
    }
}

void WriteSingleEncodedValueToComplexFile(std::ofstream& out, FieldType field_type) {
    switch (field_type) {
        DISPATCH_FIELD_ENCODED_VALUE(TINYINT)
        DISPATCH_FIELD_ENCODED_VALUE(SMALLINT)
        DISPATCH_FIELD_ENCODED_VALUE(INT)
        DISPATCH_FIELD_ENCODED_VALUE(UNSIGNED_INT)
        DISPATCH_FIELD_ENCODED_VALUE(BIGINT)
        DISPATCH_FIELD_ENCODED_VALUE(UNSIGNED_BIGINT)
        DISPATCH_FIELD_ENCODED_VALUE(LARGEINT)
        DISPATCH_FIELD_ENCODED_VALUE(BOOL)
        DISPATCH_FIELD_ENCODED_VALUE(DATE)
        DISPATCH_FIELD_ENCODED_VALUE(DATEV2)
        DISPATCH_FIELD_ENCODED_VALUE(DATETIME)
        DISPATCH_FIELD_ENCODED_VALUE(DATETIMEV2)
        DISPATCH_FIELD_ENCODED_VALUE(DECIMAL)
        DISPATCH_FIELD_ENCODED_VALUE(DECIMAL32)
        DISPATCH_FIELD_ENCODED_VALUE(DECIMAL64)
        DISPATCH_FIELD_ENCODED_VALUE(DECIMAL128I)
        DISPATCH_FIELD_ENCODED_VALUE(DECIMAL256)
        DISPATCH_FIELD_ENCODED_VALUE(IPV4)
        DISPATCH_FIELD_ENCODED_VALUE(IPV6)
        DISPATCH_FIELD_ENCODED_VALUE(CHAR)
        DISPATCH_FIELD_ENCODED_VALUE(VARCHAR)
        DISPATCH_FIELD_ENCODED_VALUE(STRING)
        DISPATCH_FIELD_ENCODED_VALUE(FLOAT)
        DISPATCH_FIELD_ENCODED_VALUE(DOUBLE)
    default:
        FAIL() << "Unsupported field type: " << static_cast<int>(field_type);
        break;
    }
}

std::string ReadEncodedStringFromComplexFile(std::ifstream& in) {
    size_t encoded_size;
    in.read(reinterpret_cast<char*>(&encoded_size), sizeof(encoded_size));
    std::string encoded_buf(encoded_size, '\0');
    in.read(encoded_buf.data(), encoded_size);
    return encoded_buf;
}

template <FieldType field_type_i, FieldType field_type_j>
void ReadAndDecodeFromComplexFile(std::ifstream& in) {
    if constexpr (field_is_slice_type(field_type_i) && field_is_slice_type(field_type_j)) {
        size_t index_size_i;
        in.read(reinterpret_cast<char*>(&index_size_i), sizeof(index_size_i));
        size_t value_size_i;
        in.read(reinterpret_cast<char*>(&value_size_i), sizeof(value_size_i));
        std::string value_i(value_size_i, '\0');
        in.read(value_i.data(), value_size_i);

        size_t index_size_j;
        in.read(reinterpret_cast<char*>(&index_size_j), sizeof(index_size_j));
        size_t value_size_j;
        in.read(reinterpret_cast<char*>(&value_size_j), sizeof(value_size_j));
        std::string value_j(value_size_j, '\0');
        in.read(value_j.data(), value_size_j);

        auto coder_i = get_key_coder(field_type_i);
        auto coder_j = get_key_coder(field_type_j);
        EXPECT_NE(coder_i, nullptr);
        EXPECT_NE(coder_j, nullptr);

        std::string encoded_buf_i;
        coder_i->encode_ascending(&value_i, index_size_i, &encoded_buf_i);
        std::string encoded_buf_j;
        coder_j->encode_ascending(&value_j, index_size_j, &encoded_buf_j);

        std::string encoded_buf_i_str = ReadEncodedStringFromComplexFile(in);
        std::string encoded_buf_j_str = ReadEncodedStringFromComplexFile(in);
        EXPECT_EQ(encoded_buf_i_str, encoded_buf_i);
        EXPECT_EQ(encoded_buf_j_str, encoded_buf_j);
    } else if constexpr (field_is_slice_type(field_type_i) && !field_is_slice_type(field_type_j)) {
        size_t index_size_i;
        in.read(reinterpret_cast<char*>(&index_size_i), sizeof(index_size_i));
        size_t value_size_i;
        in.read(reinterpret_cast<char*>(&value_size_i), sizeof(value_size_i));
        std::string value_i(value_size_i, '\0');
        in.read(value_i.data(), value_size_i);

        using T = typename CppTypeTraits<field_type_j>::CppType;
        size_t value_size = 0;
        in.read(reinterpret_cast<char*>(&value_size), sizeof(value_size));

        T original_value;
        in.read(reinterpret_cast<char*>(&original_value), value_size);

        auto coder_i = get_key_coder(field_type_i);
        auto coder_j = get_key_coder(field_type_j);
        EXPECT_NE(coder_i, nullptr);
        EXPECT_NE(coder_j, nullptr);

        std::string encoded_buf_i;
        coder_i->encode_ascending(&value_i, index_size_i, &encoded_buf_i);
        std::string encoded_buf_j;
        coder_j->full_encode_ascending(&original_value, &encoded_buf_j);
        std::string encoded_buf_i_str = ReadEncodedStringFromComplexFile(in);
        std::string encoded_buf_j_str = ReadEncodedStringFromComplexFile(in);
        EXPECT_EQ(encoded_buf_i_str, encoded_buf_i);
        EXPECT_EQ(encoded_buf_j_str, encoded_buf_j);

        T decoded_value;
        Slice encoded_buf_j_str_slice(encoded_buf_j_str);
        auto status =
                coder_j->decode_ascending(&encoded_buf_j_str_slice, 0, (uint8_t*)&decoded_value);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(decoded_value, original_value);
    } else if constexpr (!field_is_slice_type(field_type_i) && field_is_slice_type(field_type_j)) {
        using T = typename CppTypeTraits<field_type_i>::CppType;
        size_t value_size = 0;
        in.read(reinterpret_cast<char*>(&value_size), sizeof(value_size));

        T original_value;
        in.read(reinterpret_cast<char*>(&original_value), value_size);

        size_t index_size_j;
        in.read(reinterpret_cast<char*>(&index_size_j), sizeof(index_size_j));
        size_t value_size_j;
        in.read(reinterpret_cast<char*>(&value_size_j), sizeof(value_size_j));
        std::string value_j(value_size_j, '\0');
        in.read(value_j.data(), value_size_j);

        auto coder_i = get_key_coder(field_type_i);
        auto coder_j = get_key_coder(field_type_j);
        EXPECT_NE(coder_i, nullptr);
        EXPECT_NE(coder_j, nullptr);

        std::string encoded_buf_i;
        coder_i->full_encode_ascending(&original_value, &encoded_buf_i);
        std::string encoded_buf_j;
        coder_j->encode_ascending(&value_j, index_size_j, &encoded_buf_j);

        std::string encoded_buf_i_str = ReadEncodedStringFromComplexFile(in);
        std::string encoded_buf_j_str = ReadEncodedStringFromComplexFile(in);
        EXPECT_EQ(encoded_buf_i_str, encoded_buf_i);
        EXPECT_EQ(encoded_buf_j_str, encoded_buf_j);

        T decoded_value;
        Slice encoded_buf_i_str_slice(encoded_buf_i_str);
        auto status =
                coder_i->decode_ascending(&encoded_buf_i_str_slice, 0, (uint8_t*)&decoded_value);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(decoded_value, original_value);
    } else {
        using T = typename CppTypeTraits<field_type_i>::CppType;
        using U = typename CppTypeTraits<field_type_j>::CppType;
        size_t value_size_i;
        in.read(reinterpret_cast<char*>(&value_size_i), sizeof(value_size_i));
        T original_value_i;
        in.read(reinterpret_cast<char*>(&original_value_i), value_size_i);

        size_t value_size_j;
        in.read(reinterpret_cast<char*>(&value_size_j), sizeof(value_size_j));
        U original_value_j;
        in.read(reinterpret_cast<char*>(&original_value_j), value_size_j);

        auto coder_i = get_key_coder(field_type_i);
        auto coder_j = get_key_coder(field_type_j);
        EXPECT_NE(coder_i, nullptr);
        EXPECT_NE(coder_j, nullptr);

        std::string encoded_buf_i;
        coder_i->full_encode_ascending(&original_value_i, &encoded_buf_i);
        std::string encoded_buf_j;
        coder_j->full_encode_ascending(&original_value_j, &encoded_buf_j);

        std::string encoded_buf_i_str = ReadEncodedStringFromComplexFile(in);
        std::string encoded_buf_j_str = ReadEncodedStringFromComplexFile(in);
        EXPECT_EQ(encoded_buf_i_str, encoded_buf_i);
        EXPECT_EQ(encoded_buf_j_str, encoded_buf_j);

        T decoded_value_i;
        Slice encoded_buf_i_str_slice(encoded_buf_i_str);
        auto status =
                coder_i->decode_ascending(&encoded_buf_i_str_slice, 0, (uint8_t*)&decoded_value_i);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(decoded_value_i, original_value_i);

        U decoded_value_j;
        Slice encoded_buf_j_str_slice(encoded_buf_j_str);
        status = coder_j->decode_ascending(&encoded_buf_j_str_slice, 0, (uint8_t*)&decoded_value_j);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(decoded_value_j, original_value_j);
    }
}

void ReadAndDecodeFromComplexFile(std::ifstream& in, FieldType field_type_i,
                                  FieldType field_type_j) {
#define HANDLE_COMBINATION(TYPE1, TYPE2)                                      \
    if (field_type_i == FieldType::OLAP_FIELD_TYPE_##TYPE1 &&                 \
        field_type_j == FieldType::OLAP_FIELD_TYPE_##TYPE2) {                 \
        ReadAndDecodeFromComplexFile<FieldType::OLAP_FIELD_TYPE_##TYPE1,      \
                                     FieldType::OLAP_FIELD_TYPE_##TYPE2>(in); \
        return;                                                               \
    }

    HANDLE_COMBINATION(TINYINT, TINYINT)
    HANDLE_COMBINATION(TINYINT, SMALLINT)
    HANDLE_COMBINATION(TINYINT, INT)
    HANDLE_COMBINATION(TINYINT, UNSIGNED_INT)
    HANDLE_COMBINATION(TINYINT, BIGINT)
    HANDLE_COMBINATION(TINYINT, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(TINYINT, LARGEINT)
    HANDLE_COMBINATION(TINYINT, BOOL)
    HANDLE_COMBINATION(TINYINT, DATE)
    HANDLE_COMBINATION(TINYINT, DATEV2)
    HANDLE_COMBINATION(TINYINT, DATETIME)
    HANDLE_COMBINATION(TINYINT, DATETIMEV2)
    HANDLE_COMBINATION(TINYINT, DECIMAL)
    HANDLE_COMBINATION(TINYINT, DECIMAL32)
    HANDLE_COMBINATION(TINYINT, DECIMAL64)
    HANDLE_COMBINATION(TINYINT, DECIMAL128I)
    HANDLE_COMBINATION(TINYINT, DECIMAL256)
    HANDLE_COMBINATION(TINYINT, IPV4)
    HANDLE_COMBINATION(TINYINT, IPV6)
    HANDLE_COMBINATION(TINYINT, CHAR)
    HANDLE_COMBINATION(TINYINT, VARCHAR)
    HANDLE_COMBINATION(TINYINT, STRING)
    HANDLE_COMBINATION(TINYINT, FLOAT)
    HANDLE_COMBINATION(TINYINT, DOUBLE)

    HANDLE_COMBINATION(SMALLINT, TINYINT)
    HANDLE_COMBINATION(SMALLINT, SMALLINT)
    HANDLE_COMBINATION(SMALLINT, INT)
    HANDLE_COMBINATION(SMALLINT, UNSIGNED_INT)
    HANDLE_COMBINATION(SMALLINT, BIGINT)
    HANDLE_COMBINATION(SMALLINT, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(SMALLINT, LARGEINT)
    HANDLE_COMBINATION(SMALLINT, BOOL)
    HANDLE_COMBINATION(SMALLINT, DATE)
    HANDLE_COMBINATION(SMALLINT, DATEV2)
    HANDLE_COMBINATION(SMALLINT, DATETIME)
    HANDLE_COMBINATION(SMALLINT, DATETIMEV2)
    HANDLE_COMBINATION(SMALLINT, DECIMAL)
    HANDLE_COMBINATION(SMALLINT, DECIMAL32)
    HANDLE_COMBINATION(SMALLINT, DECIMAL64)
    HANDLE_COMBINATION(SMALLINT, DECIMAL128I)
    HANDLE_COMBINATION(SMALLINT, DECIMAL256)
    HANDLE_COMBINATION(SMALLINT, IPV4)
    HANDLE_COMBINATION(SMALLINT, IPV6)
    HANDLE_COMBINATION(SMALLINT, CHAR)
    HANDLE_COMBINATION(SMALLINT, VARCHAR)
    HANDLE_COMBINATION(SMALLINT, STRING)
    HANDLE_COMBINATION(SMALLINT, FLOAT)
    HANDLE_COMBINATION(SMALLINT, DOUBLE)

    HANDLE_COMBINATION(INT, TINYINT)
    HANDLE_COMBINATION(INT, SMALLINT)
    HANDLE_COMBINATION(INT, INT)
    HANDLE_COMBINATION(INT, UNSIGNED_INT)
    HANDLE_COMBINATION(INT, BIGINT)
    HANDLE_COMBINATION(INT, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(INT, LARGEINT)
    HANDLE_COMBINATION(INT, BOOL)
    HANDLE_COMBINATION(INT, DATE)
    HANDLE_COMBINATION(INT, DATEV2)
    HANDLE_COMBINATION(INT, DATETIME)
    HANDLE_COMBINATION(INT, DATETIMEV2)
    HANDLE_COMBINATION(INT, DECIMAL)
    HANDLE_COMBINATION(INT, DECIMAL32)
    HANDLE_COMBINATION(INT, DECIMAL64)
    HANDLE_COMBINATION(INT, DECIMAL128I)
    HANDLE_COMBINATION(INT, DECIMAL256)
    HANDLE_COMBINATION(INT, IPV4)
    HANDLE_COMBINATION(INT, IPV6)
    HANDLE_COMBINATION(INT, CHAR)
    HANDLE_COMBINATION(INT, VARCHAR)
    HANDLE_COMBINATION(INT, STRING)
    HANDLE_COMBINATION(INT, FLOAT)
    HANDLE_COMBINATION(INT, DOUBLE)

    HANDLE_COMBINATION(UNSIGNED_INT, TINYINT)
    HANDLE_COMBINATION(UNSIGNED_INT, SMALLINT)
    HANDLE_COMBINATION(UNSIGNED_INT, INT)
    HANDLE_COMBINATION(UNSIGNED_INT, UNSIGNED_INT)
    HANDLE_COMBINATION(UNSIGNED_INT, BIGINT)
    HANDLE_COMBINATION(UNSIGNED_INT, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(UNSIGNED_INT, LARGEINT)
    HANDLE_COMBINATION(UNSIGNED_INT, BOOL)
    HANDLE_COMBINATION(UNSIGNED_INT, DATE)
    HANDLE_COMBINATION(UNSIGNED_INT, DATEV2)
    HANDLE_COMBINATION(UNSIGNED_INT, DATETIME)
    HANDLE_COMBINATION(UNSIGNED_INT, DATETIMEV2)
    HANDLE_COMBINATION(UNSIGNED_INT, DECIMAL)
    HANDLE_COMBINATION(UNSIGNED_INT, DECIMAL32)
    HANDLE_COMBINATION(UNSIGNED_INT, DECIMAL64)
    HANDLE_COMBINATION(UNSIGNED_INT, DECIMAL128I)
    HANDLE_COMBINATION(UNSIGNED_INT, DECIMAL256)
    HANDLE_COMBINATION(UNSIGNED_INT, IPV4)
    HANDLE_COMBINATION(UNSIGNED_INT, IPV6)
    HANDLE_COMBINATION(UNSIGNED_INT, CHAR)
    HANDLE_COMBINATION(UNSIGNED_INT, VARCHAR)
    HANDLE_COMBINATION(UNSIGNED_INT, STRING)
    HANDLE_COMBINATION(UNSIGNED_INT, FLOAT)
    HANDLE_COMBINATION(UNSIGNED_INT, DOUBLE)

    HANDLE_COMBINATION(BIGINT, TINYINT)
    HANDLE_COMBINATION(BIGINT, SMALLINT)
    HANDLE_COMBINATION(BIGINT, INT)
    HANDLE_COMBINATION(BIGINT, UNSIGNED_INT)
    HANDLE_COMBINATION(BIGINT, BIGINT)
    HANDLE_COMBINATION(BIGINT, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(BIGINT, LARGEINT)
    HANDLE_COMBINATION(BIGINT, BOOL)
    HANDLE_COMBINATION(BIGINT, DATE)
    HANDLE_COMBINATION(BIGINT, DATEV2)
    HANDLE_COMBINATION(BIGINT, DATETIME)
    HANDLE_COMBINATION(BIGINT, DATETIMEV2)
    HANDLE_COMBINATION(BIGINT, DECIMAL)
    HANDLE_COMBINATION(BIGINT, DECIMAL32)
    HANDLE_COMBINATION(BIGINT, DECIMAL64)
    HANDLE_COMBINATION(BIGINT, DECIMAL128I)
    HANDLE_COMBINATION(BIGINT, DECIMAL256)
    HANDLE_COMBINATION(BIGINT, IPV4)
    HANDLE_COMBINATION(BIGINT, IPV6)
    HANDLE_COMBINATION(BIGINT, CHAR)
    HANDLE_COMBINATION(BIGINT, VARCHAR)
    HANDLE_COMBINATION(BIGINT, STRING)
    HANDLE_COMBINATION(BIGINT, FLOAT)
    HANDLE_COMBINATION(BIGINT, DOUBLE)

    HANDLE_COMBINATION(UNSIGNED_BIGINT, TINYINT)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, SMALLINT)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, INT)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, UNSIGNED_INT)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, BIGINT)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, LARGEINT)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, BOOL)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, DATE)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, DATEV2)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, DATETIME)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, DATETIMEV2)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, DECIMAL)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, DECIMAL32)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, DECIMAL64)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, DECIMAL128I)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, DECIMAL256)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, IPV4)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, IPV6)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, CHAR)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, VARCHAR)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, STRING)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, FLOAT)
    HANDLE_COMBINATION(UNSIGNED_BIGINT, DOUBLE)

    HANDLE_COMBINATION(LARGEINT, TINYINT)
    HANDLE_COMBINATION(LARGEINT, SMALLINT)
    HANDLE_COMBINATION(LARGEINT, INT)
    HANDLE_COMBINATION(LARGEINT, UNSIGNED_INT)
    HANDLE_COMBINATION(LARGEINT, BIGINT)
    HANDLE_COMBINATION(LARGEINT, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(LARGEINT, LARGEINT)
    HANDLE_COMBINATION(LARGEINT, BOOL)
    HANDLE_COMBINATION(LARGEINT, DATE)
    HANDLE_COMBINATION(LARGEINT, DATEV2)
    HANDLE_COMBINATION(LARGEINT, DATETIME)
    HANDLE_COMBINATION(LARGEINT, DATETIMEV2)
    HANDLE_COMBINATION(LARGEINT, DECIMAL)
    HANDLE_COMBINATION(LARGEINT, DECIMAL32)
    HANDLE_COMBINATION(LARGEINT, DECIMAL64)
    HANDLE_COMBINATION(LARGEINT, DECIMAL128I)
    HANDLE_COMBINATION(LARGEINT, DECIMAL256)
    HANDLE_COMBINATION(LARGEINT, IPV4)
    HANDLE_COMBINATION(LARGEINT, IPV6)
    HANDLE_COMBINATION(LARGEINT, CHAR)
    HANDLE_COMBINATION(LARGEINT, VARCHAR)
    HANDLE_COMBINATION(LARGEINT, STRING)
    HANDLE_COMBINATION(LARGEINT, FLOAT)
    HANDLE_COMBINATION(LARGEINT, DOUBLE)

    HANDLE_COMBINATION(BOOL, TINYINT)
    HANDLE_COMBINATION(BOOL, SMALLINT)
    HANDLE_COMBINATION(BOOL, INT)
    HANDLE_COMBINATION(BOOL, UNSIGNED_INT)
    HANDLE_COMBINATION(BOOL, BIGINT)
    HANDLE_COMBINATION(BOOL, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(BOOL, LARGEINT)
    HANDLE_COMBINATION(BOOL, BOOL)
    HANDLE_COMBINATION(BOOL, DATE)
    HANDLE_COMBINATION(BOOL, DATEV2)
    HANDLE_COMBINATION(BOOL, DATETIME)
    HANDLE_COMBINATION(BOOL, DATETIMEV2)
    HANDLE_COMBINATION(BOOL, DECIMAL)
    HANDLE_COMBINATION(BOOL, DECIMAL32)
    HANDLE_COMBINATION(BOOL, DECIMAL64)
    HANDLE_COMBINATION(BOOL, DECIMAL128I)
    HANDLE_COMBINATION(BOOL, DECIMAL256)
    HANDLE_COMBINATION(BOOL, IPV4)
    HANDLE_COMBINATION(BOOL, IPV6)
    HANDLE_COMBINATION(BOOL, CHAR)
    HANDLE_COMBINATION(BOOL, VARCHAR)
    HANDLE_COMBINATION(BOOL, STRING)
    HANDLE_COMBINATION(BOOL, FLOAT)
    HANDLE_COMBINATION(BOOL, DOUBLE)

    HANDLE_COMBINATION(DATE, TINYINT)
    HANDLE_COMBINATION(DATE, SMALLINT)
    HANDLE_COMBINATION(DATE, INT)
    HANDLE_COMBINATION(DATE, UNSIGNED_INT)
    HANDLE_COMBINATION(DATE, BIGINT)
    HANDLE_COMBINATION(DATE, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(DATE, LARGEINT)
    HANDLE_COMBINATION(DATE, BOOL)
    HANDLE_COMBINATION(DATE, DATE)
    HANDLE_COMBINATION(DATE, DATEV2)
    HANDLE_COMBINATION(DATE, DATETIME)
    HANDLE_COMBINATION(DATE, DATETIMEV2)
    HANDLE_COMBINATION(DATE, DECIMAL)
    HANDLE_COMBINATION(DATE, DECIMAL32)
    HANDLE_COMBINATION(DATE, DECIMAL64)
    HANDLE_COMBINATION(DATE, DECIMAL128I)
    HANDLE_COMBINATION(DATE, DECIMAL256)
    HANDLE_COMBINATION(DATE, IPV4)
    HANDLE_COMBINATION(DATE, IPV6)
    HANDLE_COMBINATION(DATE, CHAR)
    HANDLE_COMBINATION(DATE, VARCHAR)
    HANDLE_COMBINATION(DATE, STRING)
    HANDLE_COMBINATION(DATE, FLOAT)
    HANDLE_COMBINATION(DATE, DOUBLE)

    HANDLE_COMBINATION(DATEV2, TINYINT)
    HANDLE_COMBINATION(DATEV2, SMALLINT)
    HANDLE_COMBINATION(DATEV2, INT)
    HANDLE_COMBINATION(DATEV2, UNSIGNED_INT)
    HANDLE_COMBINATION(DATEV2, BIGINT)
    HANDLE_COMBINATION(DATEV2, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(DATEV2, LARGEINT)
    HANDLE_COMBINATION(DATEV2, BOOL)
    HANDLE_COMBINATION(DATEV2, DATE)
    HANDLE_COMBINATION(DATEV2, DATEV2)
    HANDLE_COMBINATION(DATEV2, DATETIME)
    HANDLE_COMBINATION(DATEV2, DATETIMEV2)
    HANDLE_COMBINATION(DATEV2, DECIMAL)
    HANDLE_COMBINATION(DATEV2, DECIMAL32)
    HANDLE_COMBINATION(DATEV2, DECIMAL64)
    HANDLE_COMBINATION(DATEV2, DECIMAL128I)
    HANDLE_COMBINATION(DATEV2, DECIMAL256)
    HANDLE_COMBINATION(DATEV2, IPV4)
    HANDLE_COMBINATION(DATEV2, IPV6)
    HANDLE_COMBINATION(DATEV2, CHAR)
    HANDLE_COMBINATION(DATEV2, VARCHAR)
    HANDLE_COMBINATION(DATEV2, STRING)
    HANDLE_COMBINATION(DATEV2, FLOAT)
    HANDLE_COMBINATION(DATEV2, DOUBLE)

    HANDLE_COMBINATION(DATETIME, TINYINT)
    HANDLE_COMBINATION(DATETIME, SMALLINT)
    HANDLE_COMBINATION(DATETIME, INT)
    HANDLE_COMBINATION(DATETIME, UNSIGNED_INT)
    HANDLE_COMBINATION(DATETIME, BIGINT)
    HANDLE_COMBINATION(DATETIME, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(DATETIME, LARGEINT)
    HANDLE_COMBINATION(DATETIME, BOOL)
    HANDLE_COMBINATION(DATETIME, DATE)
    HANDLE_COMBINATION(DATETIME, DATEV2)
    HANDLE_COMBINATION(DATETIME, DATETIME)
    HANDLE_COMBINATION(DATETIME, DATETIMEV2)
    HANDLE_COMBINATION(DATETIME, DECIMAL)
    HANDLE_COMBINATION(DATETIME, DECIMAL32)
    HANDLE_COMBINATION(DATETIME, DECIMAL64)
    HANDLE_COMBINATION(DATETIME, DECIMAL128I)
    HANDLE_COMBINATION(DATETIME, DECIMAL256)
    HANDLE_COMBINATION(DATETIME, IPV4)
    HANDLE_COMBINATION(DATETIME, IPV6)
    HANDLE_COMBINATION(DATETIME, CHAR)
    HANDLE_COMBINATION(DATETIME, VARCHAR)
    HANDLE_COMBINATION(DATETIME, STRING)
    HANDLE_COMBINATION(DATETIME, FLOAT)
    HANDLE_COMBINATION(DATETIME, DOUBLE)

    HANDLE_COMBINATION(DATETIMEV2, TINYINT)
    HANDLE_COMBINATION(DATETIMEV2, SMALLINT)
    HANDLE_COMBINATION(DATETIMEV2, INT)
    HANDLE_COMBINATION(DATETIMEV2, UNSIGNED_INT)
    HANDLE_COMBINATION(DATETIMEV2, BIGINT)
    HANDLE_COMBINATION(DATETIMEV2, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(DATETIMEV2, LARGEINT)
    HANDLE_COMBINATION(DATETIMEV2, BOOL)
    HANDLE_COMBINATION(DATETIMEV2, DATE)
    HANDLE_COMBINATION(DATETIMEV2, DATEV2)
    HANDLE_COMBINATION(DATETIMEV2, DATETIME)
    HANDLE_COMBINATION(DATETIMEV2, DATETIMEV2)
    HANDLE_COMBINATION(DATETIMEV2, DECIMAL)
    HANDLE_COMBINATION(DATETIMEV2, DECIMAL32)
    HANDLE_COMBINATION(DATETIMEV2, DECIMAL64)
    HANDLE_COMBINATION(DATETIMEV2, DECIMAL128I)
    HANDLE_COMBINATION(DATETIMEV2, DECIMAL256)
    HANDLE_COMBINATION(DATETIMEV2, IPV4)
    HANDLE_COMBINATION(DATETIMEV2, IPV6)
    HANDLE_COMBINATION(DATETIMEV2, CHAR)
    HANDLE_COMBINATION(DATETIMEV2, VARCHAR)
    HANDLE_COMBINATION(DATETIMEV2, STRING)
    HANDLE_COMBINATION(DATETIMEV2, FLOAT)
    HANDLE_COMBINATION(DATETIMEV2, DOUBLE)

    HANDLE_COMBINATION(DECIMAL, TINYINT)
    HANDLE_COMBINATION(DECIMAL, SMALLINT)
    HANDLE_COMBINATION(DECIMAL, INT)
    HANDLE_COMBINATION(DECIMAL, UNSIGNED_INT)
    HANDLE_COMBINATION(DECIMAL, BIGINT)
    HANDLE_COMBINATION(DECIMAL, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(DECIMAL, LARGEINT)
    HANDLE_COMBINATION(DECIMAL, BOOL)
    HANDLE_COMBINATION(DECIMAL, DATE)
    HANDLE_COMBINATION(DECIMAL, DATEV2)
    HANDLE_COMBINATION(DECIMAL, DATETIME)
    HANDLE_COMBINATION(DECIMAL, DATETIMEV2)
    HANDLE_COMBINATION(DECIMAL, DECIMAL)
    HANDLE_COMBINATION(DECIMAL, DECIMAL32)
    HANDLE_COMBINATION(DECIMAL, DECIMAL64)
    HANDLE_COMBINATION(DECIMAL, DECIMAL128I)
    HANDLE_COMBINATION(DECIMAL, DECIMAL256)
    HANDLE_COMBINATION(DECIMAL, IPV4)
    HANDLE_COMBINATION(DECIMAL, IPV6)
    HANDLE_COMBINATION(DECIMAL, CHAR)
    HANDLE_COMBINATION(DECIMAL, VARCHAR)
    HANDLE_COMBINATION(DECIMAL, STRING)
    HANDLE_COMBINATION(DECIMAL, FLOAT)
    HANDLE_COMBINATION(DECIMAL, DOUBLE)

    HANDLE_COMBINATION(DECIMAL32, TINYINT)
    HANDLE_COMBINATION(DECIMAL32, SMALLINT)
    HANDLE_COMBINATION(DECIMAL32, INT)
    HANDLE_COMBINATION(DECIMAL32, UNSIGNED_INT)
    HANDLE_COMBINATION(DECIMAL32, BIGINT)
    HANDLE_COMBINATION(DECIMAL32, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(DECIMAL32, LARGEINT)
    HANDLE_COMBINATION(DECIMAL32, BOOL)
    HANDLE_COMBINATION(DECIMAL32, DATE)
    HANDLE_COMBINATION(DECIMAL32, DATEV2)
    HANDLE_COMBINATION(DECIMAL32, DATETIME)
    HANDLE_COMBINATION(DECIMAL32, DATETIMEV2)
    HANDLE_COMBINATION(DECIMAL32, DECIMAL)
    HANDLE_COMBINATION(DECIMAL32, DECIMAL32)
    HANDLE_COMBINATION(DECIMAL32, DECIMAL64)
    HANDLE_COMBINATION(DECIMAL32, DECIMAL128I)
    HANDLE_COMBINATION(DECIMAL32, DECIMAL256)
    HANDLE_COMBINATION(DECIMAL32, IPV4)
    HANDLE_COMBINATION(DECIMAL32, IPV6)
    HANDLE_COMBINATION(DECIMAL32, CHAR)
    HANDLE_COMBINATION(DECIMAL32, VARCHAR)
    HANDLE_COMBINATION(DECIMAL32, STRING)
    HANDLE_COMBINATION(DECIMAL32, FLOAT)
    HANDLE_COMBINATION(DECIMAL32, DOUBLE)

    HANDLE_COMBINATION(DECIMAL64, TINYINT)
    HANDLE_COMBINATION(DECIMAL64, SMALLINT)
    HANDLE_COMBINATION(DECIMAL64, INT)
    HANDLE_COMBINATION(DECIMAL64, UNSIGNED_INT)
    HANDLE_COMBINATION(DECIMAL64, BIGINT)
    HANDLE_COMBINATION(DECIMAL64, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(DECIMAL64, LARGEINT)
    HANDLE_COMBINATION(DECIMAL64, BOOL)
    HANDLE_COMBINATION(DECIMAL64, DATE)
    HANDLE_COMBINATION(DECIMAL64, DATEV2)
    HANDLE_COMBINATION(DECIMAL64, DATETIME)
    HANDLE_COMBINATION(DECIMAL64, DATETIMEV2)
    HANDLE_COMBINATION(DECIMAL64, DECIMAL)
    HANDLE_COMBINATION(DECIMAL64, DECIMAL32)
    HANDLE_COMBINATION(DECIMAL64, DECIMAL64)
    HANDLE_COMBINATION(DECIMAL64, DECIMAL128I)
    HANDLE_COMBINATION(DECIMAL64, DECIMAL256)
    HANDLE_COMBINATION(DECIMAL64, IPV4)
    HANDLE_COMBINATION(DECIMAL64, IPV6)
    HANDLE_COMBINATION(DECIMAL64, CHAR)
    HANDLE_COMBINATION(DECIMAL64, VARCHAR)
    HANDLE_COMBINATION(DECIMAL64, STRING)
    HANDLE_COMBINATION(DECIMAL64, FLOAT)
    HANDLE_COMBINATION(DECIMAL64, DOUBLE)

    HANDLE_COMBINATION(DECIMAL128I, TINYINT)
    HANDLE_COMBINATION(DECIMAL128I, SMALLINT)
    HANDLE_COMBINATION(DECIMAL128I, INT)
    HANDLE_COMBINATION(DECIMAL128I, UNSIGNED_INT)
    HANDLE_COMBINATION(DECIMAL128I, BIGINT)
    HANDLE_COMBINATION(DECIMAL128I, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(DECIMAL128I, LARGEINT)
    HANDLE_COMBINATION(DECIMAL128I, BOOL)
    HANDLE_COMBINATION(DECIMAL128I, DATE)
    HANDLE_COMBINATION(DECIMAL128I, DATEV2)
    HANDLE_COMBINATION(DECIMAL128I, DATETIME)
    HANDLE_COMBINATION(DECIMAL128I, DATETIMEV2)
    HANDLE_COMBINATION(DECIMAL128I, DECIMAL)
    HANDLE_COMBINATION(DECIMAL128I, DECIMAL32)
    HANDLE_COMBINATION(DECIMAL128I, DECIMAL64)
    HANDLE_COMBINATION(DECIMAL128I, DECIMAL128I)
    HANDLE_COMBINATION(DECIMAL128I, DECIMAL256)
    HANDLE_COMBINATION(DECIMAL128I, IPV4)
    HANDLE_COMBINATION(DECIMAL128I, IPV6)
    HANDLE_COMBINATION(DECIMAL128I, CHAR)
    HANDLE_COMBINATION(DECIMAL128I, VARCHAR)
    HANDLE_COMBINATION(DECIMAL128I, STRING)
    HANDLE_COMBINATION(DECIMAL128I, FLOAT)
    HANDLE_COMBINATION(DECIMAL128I, DOUBLE)

    HANDLE_COMBINATION(DECIMAL256, TINYINT)
    HANDLE_COMBINATION(DECIMAL256, SMALLINT)
    HANDLE_COMBINATION(DECIMAL256, INT)
    HANDLE_COMBINATION(DECIMAL256, UNSIGNED_INT)
    HANDLE_COMBINATION(DECIMAL256, BIGINT)
    HANDLE_COMBINATION(DECIMAL256, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(DECIMAL256, LARGEINT)
    HANDLE_COMBINATION(DECIMAL256, BOOL)
    HANDLE_COMBINATION(DECIMAL256, DATE)
    HANDLE_COMBINATION(DECIMAL256, DATEV2)
    HANDLE_COMBINATION(DECIMAL256, DATETIME)
    HANDLE_COMBINATION(DECIMAL256, DATETIMEV2)
    HANDLE_COMBINATION(DECIMAL256, DECIMAL)
    HANDLE_COMBINATION(DECIMAL256, DECIMAL32)
    HANDLE_COMBINATION(DECIMAL256, DECIMAL64)
    HANDLE_COMBINATION(DECIMAL256, DECIMAL128I)
    HANDLE_COMBINATION(DECIMAL256, DECIMAL256)
    HANDLE_COMBINATION(DECIMAL256, IPV4)
    HANDLE_COMBINATION(DECIMAL256, IPV6)
    HANDLE_COMBINATION(DECIMAL256, CHAR)
    HANDLE_COMBINATION(DECIMAL256, VARCHAR)
    HANDLE_COMBINATION(DECIMAL256, STRING)
    HANDLE_COMBINATION(DECIMAL256, FLOAT)
    HANDLE_COMBINATION(DECIMAL256, DOUBLE)

    HANDLE_COMBINATION(IPV4, TINYINT)
    HANDLE_COMBINATION(IPV4, SMALLINT)
    HANDLE_COMBINATION(IPV4, INT)
    HANDLE_COMBINATION(IPV4, UNSIGNED_INT)
    HANDLE_COMBINATION(IPV4, BIGINT)
    HANDLE_COMBINATION(IPV4, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(IPV4, LARGEINT)
    HANDLE_COMBINATION(IPV4, BOOL)
    HANDLE_COMBINATION(IPV4, DATE)
    HANDLE_COMBINATION(IPV4, DATEV2)
    HANDLE_COMBINATION(IPV4, DATETIME)
    HANDLE_COMBINATION(IPV4, DATETIMEV2)
    HANDLE_COMBINATION(IPV4, DECIMAL)
    HANDLE_COMBINATION(IPV4, DECIMAL32)
    HANDLE_COMBINATION(IPV4, DECIMAL64)
    HANDLE_COMBINATION(IPV4, DECIMAL128I)
    HANDLE_COMBINATION(IPV4, DECIMAL256)
    HANDLE_COMBINATION(IPV4, IPV4)
    HANDLE_COMBINATION(IPV4, IPV6)
    HANDLE_COMBINATION(IPV4, CHAR)
    HANDLE_COMBINATION(IPV4, VARCHAR)
    HANDLE_COMBINATION(IPV4, STRING)
    HANDLE_COMBINATION(IPV4, FLOAT)
    HANDLE_COMBINATION(IPV4, DOUBLE)

    HANDLE_COMBINATION(IPV6, TINYINT)
    HANDLE_COMBINATION(IPV6, SMALLINT)
    HANDLE_COMBINATION(IPV6, INT)
    HANDLE_COMBINATION(IPV6, UNSIGNED_INT)
    HANDLE_COMBINATION(IPV6, BIGINT)
    HANDLE_COMBINATION(IPV6, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(IPV6, LARGEINT)
    HANDLE_COMBINATION(IPV6, BOOL)
    HANDLE_COMBINATION(IPV6, DATE)
    HANDLE_COMBINATION(IPV6, DATEV2)
    HANDLE_COMBINATION(IPV6, DATETIME)
    HANDLE_COMBINATION(IPV6, DATETIMEV2)
    HANDLE_COMBINATION(IPV6, DECIMAL)
    HANDLE_COMBINATION(IPV6, DECIMAL32)
    HANDLE_COMBINATION(IPV6, DECIMAL64)
    HANDLE_COMBINATION(IPV6, DECIMAL128I)
    HANDLE_COMBINATION(IPV6, DECIMAL256)
    HANDLE_COMBINATION(IPV6, IPV4)
    HANDLE_COMBINATION(IPV6, IPV6)
    HANDLE_COMBINATION(IPV6, CHAR)
    HANDLE_COMBINATION(IPV6, VARCHAR)
    HANDLE_COMBINATION(IPV6, STRING)
    HANDLE_COMBINATION(IPV6, FLOAT)
    HANDLE_COMBINATION(IPV6, DOUBLE)

    HANDLE_COMBINATION(CHAR, TINYINT)
    HANDLE_COMBINATION(CHAR, SMALLINT)
    HANDLE_COMBINATION(CHAR, INT)
    HANDLE_COMBINATION(CHAR, UNSIGNED_INT)
    HANDLE_COMBINATION(CHAR, BIGINT)
    HANDLE_COMBINATION(CHAR, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(CHAR, LARGEINT)
    HANDLE_COMBINATION(CHAR, BOOL)
    HANDLE_COMBINATION(CHAR, DATE)
    HANDLE_COMBINATION(CHAR, DATEV2)
    HANDLE_COMBINATION(CHAR, DATETIME)
    HANDLE_COMBINATION(CHAR, DATETIMEV2)
    HANDLE_COMBINATION(CHAR, DECIMAL)
    HANDLE_COMBINATION(CHAR, DECIMAL32)
    HANDLE_COMBINATION(CHAR, DECIMAL64)
    HANDLE_COMBINATION(CHAR, DECIMAL128I)
    HANDLE_COMBINATION(CHAR, DECIMAL256)
    HANDLE_COMBINATION(CHAR, IPV4)
    HANDLE_COMBINATION(CHAR, IPV6)
    HANDLE_COMBINATION(CHAR, CHAR)
    HANDLE_COMBINATION(CHAR, VARCHAR)
    HANDLE_COMBINATION(CHAR, STRING)
    HANDLE_COMBINATION(CHAR, FLOAT)
    HANDLE_COMBINATION(CHAR, DOUBLE)

    HANDLE_COMBINATION(VARCHAR, TINYINT)
    HANDLE_COMBINATION(VARCHAR, SMALLINT)
    HANDLE_COMBINATION(VARCHAR, INT)
    HANDLE_COMBINATION(VARCHAR, UNSIGNED_INT)
    HANDLE_COMBINATION(VARCHAR, BIGINT)
    HANDLE_COMBINATION(VARCHAR, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(VARCHAR, LARGEINT)
    HANDLE_COMBINATION(VARCHAR, BOOL)
    HANDLE_COMBINATION(VARCHAR, DATE)
    HANDLE_COMBINATION(VARCHAR, DATEV2)
    HANDLE_COMBINATION(VARCHAR, DATETIME)
    HANDLE_COMBINATION(VARCHAR, DATETIMEV2)
    HANDLE_COMBINATION(VARCHAR, DECIMAL)
    HANDLE_COMBINATION(VARCHAR, DECIMAL32)
    HANDLE_COMBINATION(VARCHAR, DECIMAL64)
    HANDLE_COMBINATION(VARCHAR, DECIMAL128I)
    HANDLE_COMBINATION(VARCHAR, DECIMAL256)
    HANDLE_COMBINATION(VARCHAR, IPV4)
    HANDLE_COMBINATION(VARCHAR, IPV6)
    HANDLE_COMBINATION(VARCHAR, CHAR)
    HANDLE_COMBINATION(VARCHAR, VARCHAR)
    HANDLE_COMBINATION(VARCHAR, STRING)
    HANDLE_COMBINATION(VARCHAR, FLOAT)
    HANDLE_COMBINATION(VARCHAR, DOUBLE)

    HANDLE_COMBINATION(STRING, TINYINT)
    HANDLE_COMBINATION(STRING, SMALLINT)
    HANDLE_COMBINATION(STRING, INT)
    HANDLE_COMBINATION(STRING, UNSIGNED_INT)
    HANDLE_COMBINATION(STRING, BIGINT)
    HANDLE_COMBINATION(STRING, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(STRING, LARGEINT)
    HANDLE_COMBINATION(STRING, BOOL)
    HANDLE_COMBINATION(STRING, DATE)
    HANDLE_COMBINATION(STRING, DATEV2)
    HANDLE_COMBINATION(STRING, DATETIME)
    HANDLE_COMBINATION(STRING, DATETIMEV2)
    HANDLE_COMBINATION(STRING, DECIMAL)
    HANDLE_COMBINATION(STRING, DECIMAL32)
    HANDLE_COMBINATION(STRING, DECIMAL64)
    HANDLE_COMBINATION(STRING, DECIMAL128I)
    HANDLE_COMBINATION(STRING, DECIMAL256)
    HANDLE_COMBINATION(STRING, IPV4)
    HANDLE_COMBINATION(STRING, IPV6)
    HANDLE_COMBINATION(STRING, CHAR)
    HANDLE_COMBINATION(STRING, VARCHAR)
    HANDLE_COMBINATION(STRING, STRING)
    HANDLE_COMBINATION(STRING, FLOAT)
    HANDLE_COMBINATION(STRING, DOUBLE)

    HANDLE_COMBINATION(FLOAT, TINYINT)
    HANDLE_COMBINATION(FLOAT, SMALLINT)
    HANDLE_COMBINATION(FLOAT, INT)
    HANDLE_COMBINATION(FLOAT, UNSIGNED_INT)
    HANDLE_COMBINATION(FLOAT, BIGINT)
    HANDLE_COMBINATION(FLOAT, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(FLOAT, LARGEINT)
    HANDLE_COMBINATION(FLOAT, BOOL)
    HANDLE_COMBINATION(FLOAT, DATE)
    HANDLE_COMBINATION(FLOAT, DATEV2)
    HANDLE_COMBINATION(FLOAT, DATETIME)
    HANDLE_COMBINATION(FLOAT, DATETIMEV2)
    HANDLE_COMBINATION(FLOAT, DECIMAL)
    HANDLE_COMBINATION(FLOAT, DECIMAL32)
    HANDLE_COMBINATION(FLOAT, DECIMAL64)
    HANDLE_COMBINATION(FLOAT, DECIMAL128I)
    HANDLE_COMBINATION(FLOAT, DECIMAL256)
    HANDLE_COMBINATION(FLOAT, IPV4)
    HANDLE_COMBINATION(FLOAT, IPV6)
    HANDLE_COMBINATION(FLOAT, CHAR)
    HANDLE_COMBINATION(FLOAT, VARCHAR)
    HANDLE_COMBINATION(FLOAT, STRING)
    HANDLE_COMBINATION(FLOAT, FLOAT)
    HANDLE_COMBINATION(FLOAT, DOUBLE)

    HANDLE_COMBINATION(DOUBLE, TINYINT)
    HANDLE_COMBINATION(DOUBLE, SMALLINT)
    HANDLE_COMBINATION(DOUBLE, INT)
    HANDLE_COMBINATION(DOUBLE, UNSIGNED_INT)
    HANDLE_COMBINATION(DOUBLE, BIGINT)
    HANDLE_COMBINATION(DOUBLE, UNSIGNED_BIGINT)
    HANDLE_COMBINATION(DOUBLE, LARGEINT)
    HANDLE_COMBINATION(DOUBLE, BOOL)
    HANDLE_COMBINATION(DOUBLE, DATE)
    HANDLE_COMBINATION(DOUBLE, DATEV2)
    HANDLE_COMBINATION(DOUBLE, DATETIME)
    HANDLE_COMBINATION(DOUBLE, DATETIMEV2)
    HANDLE_COMBINATION(DOUBLE, DECIMAL)
    HANDLE_COMBINATION(DOUBLE, DECIMAL32)
    HANDLE_COMBINATION(DOUBLE, DECIMAL64)
    HANDLE_COMBINATION(DOUBLE, DECIMAL128I)
    HANDLE_COMBINATION(DOUBLE, DECIMAL256)
    HANDLE_COMBINATION(DOUBLE, IPV4)
    HANDLE_COMBINATION(DOUBLE, IPV6)
    HANDLE_COMBINATION(DOUBLE, CHAR)
    HANDLE_COMBINATION(DOUBLE, VARCHAR)
    HANDLE_COMBINATION(DOUBLE, STRING)
    HANDLE_COMBINATION(DOUBLE, FLOAT)
    HANDLE_COMBINATION(DOUBLE, DOUBLE)

#undef HANDLE_COMBINATION

    FAIL() << "Unsupported field type combination: " << static_cast<int>(field_type_i) << " and "
           << static_cast<int>(field_type_j);
}

TEST_F(KeyCoderTest, write_and_read_complex_filedtype) {
    // std::ofstream out(complex_filename, std::ios::binary | std::ios::trunc);
    // EXPECT_TRUE(out.is_open()) << "Failed to open file for writing: " << complex_filename;

    // std::vector<FieldType> field_types = {
    //     FieldType::OLAP_FIELD_TYPE_TINYINT,
    //     FieldType::OLAP_FIELD_TYPE_SMALLINT,
    //     FieldType::OLAP_FIELD_TYPE_INT,
    //     FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT,
    //     FieldType::OLAP_FIELD_TYPE_BIGINT,
    //     FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT,
    //     FieldType::OLAP_FIELD_TYPE_LARGEINT,
    //     FieldType::OLAP_FIELD_TYPE_BOOL,
    //     FieldType::OLAP_FIELD_TYPE_DATE,
    //     FieldType::OLAP_FIELD_TYPE_DATEV2,
    //     FieldType::OLAP_FIELD_TYPE_DATETIME,
    //     FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
    //     FieldType::OLAP_FIELD_TYPE_DECIMAL,
    //     FieldType::OLAP_FIELD_TYPE_DECIMAL32,
    //     FieldType::OLAP_FIELD_TYPE_DECIMAL64,
    //     FieldType::OLAP_FIELD_TYPE_DECIMAL128I,
    //     FieldType::OLAP_FIELD_TYPE_DECIMAL256,
    //     FieldType::OLAP_FIELD_TYPE_IPV4,
    //     FieldType::OLAP_FIELD_TYPE_IPV6,
    //     FieldType::OLAP_FIELD_TYPE_CHAR,
    //     FieldType::OLAP_FIELD_TYPE_VARCHAR,
    //     FieldType::OLAP_FIELD_TYPE_STRING,
    //     FieldType::OLAP_FIELD_TYPE_FLOAT,
    //     FieldType::OLAP_FIELD_TYPE_DOUBLE
    // };
    // for (int i = 0; i < field_types.size(); i++) {
    //     for (int j = 0; j < field_types.size(); j++) {
    //         const uint8_t field_type_value_i = static_cast<uint8_t>(field_types[i]);
    //         const uint8_t field_type_value_j = static_cast<uint8_t>(field_types[j]);

    //         // 1. FieldType
    //         out.write(reinterpret_cast<const char*>(&field_type_value_i), sizeof(field_type_value_i));
    //         out.write(reinterpret_cast<const char*>(&field_type_value_j), sizeof(field_type_value_j));

    //         // 2. original values
    //         WriteSingleValueToComplexFile(out, field_types[i]);
    //         WriteSingleValueToComplexFile(out, field_types[j]);

    //         // 3. encoded values
    //         WriteSingleEncodedValueToComplexFile(out, field_types[i]);
    //         WriteSingleEncodedValueToComplexFile(out, field_types[j]);
    //     }
    // }
    // out.close();

    std::ifstream in(complex_filename, std::ios::binary);
    EXPECT_TRUE(in.is_open()) << "Failed to open file for reading: " << complex_filename;
    while (in.peek() != EOF) {
        // read first field
        FieldType field_type_i;
        uint8_t field_type_value_i;
        in.read(reinterpret_cast<char*>(&field_type_value_i), sizeof(field_type_value_i));
        field_type_i = static_cast<FieldType>(field_type_value_i);

        // read second field
        FieldType field_type_j;
        uint8_t field_type_value_j;
        in.read(reinterpret_cast<char*>(&field_type_value_j), sizeof(field_type_value_j));
        field_type_j = static_cast<FieldType>(field_type_value_j);

        ReadAndDecodeFromComplexFile(in, field_type_i, field_type_j);
    }
    in.close();
}
} // namespace doris
