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

#include "util/jsonb_document.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"
#include "vec/core/types.h"

namespace doris {
class JsonbDocumentTest : public testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(JsonbDocumentTest, writer) {
    JsonbWriter writer;
    writer.writeStartObject();

    writer.writeKey("key_null");
    writer.writeNull();

    writer.writeKey("key_true");
    writer.writeBool(true);

    writer.writeKey("key_false");
    writer.writeBool(false);

    writer.writeKey("key_int");
    writer.writeInt(12345);

    writer.writeKey("key_float");
    writer.writeFloat(123.456);

    writer.writeKey("key_string");
    writer.writeStartString();
    writer.writeString("hello world");
    writer.writeEndString();

    writer.writeKey("key_array");

    writer.writeStartArray();
    writer.writeInt(1);
    writer.writeStartString();
    writer.writeString("array string");
    writer.writeEndString();
    writer.writeEndArray();

    writer.writeKey("key_int128");

    __int128_t int128_value = __int128_t(std::numeric_limits<uint64_t>::max()) + 1;
    writer.writeInt128(int128_value);

    writer.writeKey("key_decimal32");
    vectorized::Decimal32 decimal_value32(int32_t(99999999));
    writer.writeDecimal(decimal_value32, 9, 4);

    writer.writeKey("key_decimal64");
    vectorized::Decimal64 decimal_value64(int64_t(999999999999999999ULL));
    writer.writeDecimal(decimal_value64, 18, 4);

    writer.writeKey("key_decimal128");
    vectorized::Decimal128V3 decimal_value((__int128_t(std::numeric_limits<uint64_t>::max())));
    writer.writeDecimal(decimal_value, 30, 8);

    writer.writeKey("key_decimal256");
    wide::Int256 int256_value(wide::Int256(std::numeric_limits<__int128_t>::max()) * 2);
    vectorized::Decimal256 decimal256_value(int256_value);
    writer.writeDecimal(decimal256_value, 40, 8);

    writer.writeEndObject();

    JsonbDocument* doc = writer.getDocument();
    auto* root = doc->getValue();
    ASSERT_TRUE(root->type == JsonbType::T_Object)
            << "invalid root type:" << static_cast<JsonbTypeUnder>(root->type);

    const auto* root_obj = root->unpack<ObjectVal>();
    ASSERT_EQ(root_obj->numElem(), 12);
    auto it = root_obj->begin();
    ASSERT_EQ(std::string_view(it->getKeyStr(), it->klen()), "key_null");
    ASSERT_TRUE(it->value()->isNull());
    ASSERT_TRUE(it->value()->get_primitive_type() == TYPE_NULL);

    ++it;
    ASSERT_EQ(std::string_view(it->getKeyStr(), it->klen()), "key_true");
    ASSERT_TRUE(it->value()->isTrue());
    ASSERT_TRUE(it->value()->get_primitive_type() == TYPE_BOOLEAN);
    ++it;
    ASSERT_EQ(std::string_view(it->getKeyStr(), it->klen()), "key_false");
    ASSERT_TRUE(it->value()->isFalse());
    ASSERT_TRUE(it->value()->get_primitive_type() == TYPE_BOOLEAN);

    ++it;
    ASSERT_EQ(std::string_view(it->getKeyStr(), it->klen()), "key_int");
    ASSERT_TRUE(it->value()->isInt());

    ASSERT_TRUE(it->value()->isInt())
            << "value type is not int, actual type: " << static_cast<int>(it->value()->type);
    ASSERT_EQ(it->value()->int_val(), 12345);

    ++it;
    ASSERT_EQ(std::string_view(it->getKeyStr(), it->klen()), "key_float");
    ASSERT_TRUE(it->value()->isFloat());
    ASSERT_TRUE(it->value()->get_primitive_type() == TYPE_FLOAT);
    const auto* jsonb_float_value = it->value()->unpack<JsonbFloatVal>();
    ASSERT_EQ(jsonb_float_value->val(), 123.456F);

    ++it;
    ASSERT_EQ(std::string_view(it->getKeyStr(), it->klen()), "key_string");
    ASSERT_TRUE(it->value()->isString());
    ASSERT_TRUE(it->value()->get_primitive_type() == TYPE_STRING);
    const auto* jsonb_string_value = it->value()->unpack<JsonbStringVal>();
    ASSERT_EQ(jsonb_string_value->length(), 11);
    ASSERT_EQ(std::string(jsonb_string_value->getBlob(), jsonb_string_value->length()),
              "hello world");

    ++it;
    ASSERT_EQ(std::string_view(it->getKeyStr(), it->klen()), "key_array");
    ASSERT_TRUE(it->value()->isArray());
    ASSERT_TRUE(it->value()->get_primitive_type() == TYPE_ARRAY);
    const auto* jsonb_array_value = it->value()->unpack<ArrayVal>();
    ASSERT_EQ(jsonb_array_value->numElem(), 2);
    auto array_it = jsonb_array_value->begin();
    ASSERT_TRUE(array_it->isInt());
    ASSERT_EQ((static_cast<const JsonbValue*>(array_it))->int_val(), 1);

    ++array_it;
    ASSERT_TRUE(array_it->isString());
    const auto* array_string_value = array_it->unpack<JsonbStringVal>();
    ASSERT_EQ(array_string_value->length(), 12);
    ASSERT_EQ(std::string(array_string_value->getBlob(), array_string_value->length()),
              "array string");

    ++it;
    ASSERT_EQ(std::string_view(it->getKeyStr(), it->klen()), "key_int128");
    ASSERT_TRUE(it->value()->isInt128());
    ASSERT_TRUE(it->value()->get_primitive_type() == TYPE_LARGEINT);
    const auto* jsonb_int128_value = it->value()->unpack<JsonbInt128Val>();
    ASSERT_EQ(jsonb_int128_value->val(), int128_value);

    ++it;
    ASSERT_EQ(std::string_view(it->getKeyStr(), it->klen()), "key_decimal32");
    ASSERT_TRUE(it->value()->isDecimal32());
    ASSERT_TRUE(it->value()->get_primitive_type() == TYPE_DECIMAL32);
    ASSERT_TRUE(it->value()->isDecimal());
    const auto* jsonb_decimal_value = it->value()->unpack<JsonbDecimal32>();
    ASSERT_EQ(int32_t(jsonb_decimal_value->val()), int32_t(99999999));
    ASSERT_EQ(jsonb_decimal_value->scale, 4);
    ASSERT_EQ(jsonb_decimal_value->precision, 9);

    ++it;
    ASSERT_EQ(std::string_view(it->getKeyStr(), it->klen()), "key_decimal64");
    ASSERT_TRUE(it->value()->isDecimal64());
    ASSERT_TRUE(it->value()->get_primitive_type() == TYPE_DECIMAL64);
    ASSERT_TRUE(it->value()->isDecimal());
    const auto* jsonb_decimal64_value = it->value()->unpack<JsonbDecimal64>();
    ASSERT_EQ(int64_t(jsonb_decimal64_value->val()), int64_t(999999999999999999ULL));
    ASSERT_EQ(jsonb_decimal64_value->scale, 4);
    ASSERT_EQ(jsonb_decimal64_value->precision, 18);

    ++it;
    ASSERT_EQ(std::string_view(it->getKeyStr(), it->klen()), "key_decimal128");
    ASSERT_TRUE(it->value()->isDecimal128());
    ASSERT_TRUE(it->value()->get_primitive_type() == TYPE_DECIMAL128I);
    ASSERT_TRUE(it->value()->isDecimal());
    const auto* jsonb_decimal128_value = it->value()->unpack<JsonbDecimal128>();
    ASSERT_EQ(__int128_t(jsonb_decimal128_value->val()),
              __int128_t(__int128_t(std::numeric_limits<uint64_t>::max())));
    ASSERT_EQ(jsonb_decimal128_value->scale, 8);
    ASSERT_EQ(jsonb_decimal128_value->precision, 30);

    ++it;
    ASSERT_EQ(std::string_view(it->getKeyStr(), it->klen()), "key_decimal256");
    ASSERT_TRUE(it->value()->isDecimal256());
    ASSERT_TRUE(it->value()->get_primitive_type() == TYPE_DECIMAL256);
    ASSERT_TRUE(it->value()->isDecimal());
    const auto* jsonb_decimal256_value = it->value()->unpack<JsonbDecimal256>();
    ASSERT_EQ(jsonb_decimal256_value->val(), int256_value);
    ASSERT_EQ(jsonb_decimal256_value->scale, 8);
    ASSERT_EQ(jsonb_decimal256_value->precision, 40);

    auto json_string = JsonbToJson::jsonb_to_json_string(writer.getOutput()->getBuffer(),
                                                         writer.getOutput()->getSize());

    std::string expected_string(
            R"({"key_null":null,"key_true":true,"key_false":false,"key_int":12345,"key_float":123.456001281738,)"
            R"("key_string":"hello world","key_array":[1,"array string"],"key_int128":18446744073709551616,)"
            R"("key_decimal32":9999.9999,"key_decimal64":99999999999999.9999,"key_decimal128":184467440737.09551615,)"
            R"("key_decimal256":3402823669209384634633746074317.68211454})");
    ASSERT_EQ(json_string, expected_string);
}
} // namespace doris