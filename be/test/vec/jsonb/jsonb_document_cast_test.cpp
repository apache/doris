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

#include "util/jsonb_document_cast.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "vec/core/types.h"
#include "vec/functions/cast/cast_base.h"

namespace doris::vectorized {
class JsonbDocumentCastTest : public testing::Test {
protected:
    void SetUp() override {
        params.is_strict = false;
        jsonbs.clear();
        auto to_jsonbs = [&](JsonbWriter& writer) {
            const auto* data = writer.getOutput()->getBuffer();
            auto size = writer.getOutput()->getSize();
            std::string jsonb_string(data, size);
            jsonbs.push_back(jsonb_string);
        };

        {
            JsonbWriter writer;
            writer.writeBool(true);
            to_jsonbs(writer);
        }

        {
            JsonbWriter writer;
            writer.writeBool(false);
            to_jsonbs(writer);
        }

        {
            JsonbWriter writer;
            writer.writeInt8(123);
            to_jsonbs(writer);
        }

        {
            JsonbWriter writer;
            writer.writeInt16(12345);
            to_jsonbs(writer);
        }

        {
            JsonbWriter writer;
            writer.writeInt32(1234567890);
            to_jsonbs(writer);
        }

        {
            JsonbWriter writer;
            writer.writeInt64(1234567890123456789LL);
            to_jsonbs(writer);
        }

        {
            JsonbWriter writer;
            writer.writeInt128(1234567890123456789LL);
            to_jsonbs(writer);
        }

        {
            JsonbWriter writer;
            writer.writeFloat(123.456F);
            to_jsonbs(writer);
        }

        {
            JsonbWriter writer;
            writer.writeDouble(123.4567890123456789);
            to_jsonbs(writer);
        }

        {
            JsonbWriter writer;
            Decimal32 dec = 12345678;
            writer.writeDecimal(dec, 4, 1);
            to_jsonbs(writer);
        }

        {
            JsonbWriter writer;
            Decimal64 dec = 1234567890123456789;
            writer.writeDecimal(dec, 18, 2);
            to_jsonbs(writer);
        }

        {
            JsonbWriter writer;
            Decimal128V3 dec = 1234567890123456789;
            writer.writeDecimal(dec, 38, 3);
            to_jsonbs(writer);
        }

        {
            JsonbWriter writer;
            Decimal256 dec {1234567890123456789};
            writer.writeDecimal(dec, 76, 3);
            to_jsonbs(writer);
        }
    }

    void TearDown() override {}

    CastParameters params;

    std::vector<std::string> jsonbs;
};

TEST_F(JsonbDocumentCastTest, test_to_bool) {
    auto get_json_value = [&](int idx) -> JsonbValue* {
        auto& val = jsonbs[idx];
        JsonbDocument* doc = nullptr;
        auto st = JsonbDocument::checkAndCreateDocument(val.data(), val.size(), &doc);
        JsonbValue* value = doc->getValue();
        return value;
    };

    {
        JsonbValue* jsonb_value = get_json_value(0); // true
        UInt8 to = false;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_boolean(jsonb_value, to, params));
        EXPECT_TRUE(to);
    }

    {
        JsonbValue* jsonb_value = get_json_value(1); // false
        UInt8 to = true;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_boolean(jsonb_value, to, params));
        EXPECT_FALSE(to);
    }

    {
        JsonbValue* jsonb_value = get_json_value(2); // Int8 123
        UInt8 to = false;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_boolean(jsonb_value, to, params));
        EXPECT_TRUE(to);
    }

    {
        JsonbValue* jsonb_value = get_json_value(3); // Int16 12345
        UInt8 to = false;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_boolean(jsonb_value, to, params));
        EXPECT_TRUE(to);
    }

    {
        JsonbValue* jsonb_value = get_json_value(4); // Int32 1234567890
        UInt8 to = false;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_boolean(jsonb_value, to, params));
        EXPECT_TRUE(to);
    }

    {
        JsonbValue* jsonb_value = get_json_value(5); // Int64 1234567890123456789
        UInt8 to = false;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_boolean(jsonb_value, to, params));
        EXPECT_TRUE(to);
    }

    {
        JsonbValue* jsonb_value = get_json_value(6); // Int128 1234567890123456789
        UInt8 to = false;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_boolean(jsonb_value, to, params));
        EXPECT_TRUE(to);
    }

    {
        JsonbValue* jsonb_value = get_json_value(7); // Float 123.456F
        UInt8 to = false;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_boolean(jsonb_value, to, params));
        EXPECT_TRUE(to);
    }

    {
        JsonbValue* jsonb_value = get_json_value(8); // Double 123.4567890123456789
        UInt8 to = false;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_boolean(jsonb_value, to, params));
        EXPECT_TRUE(to);
    }

    {
        JsonbValue* jsonb_value = get_json_value(9); // Decimal32 12345678
        UInt8 to = false;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_boolean(jsonb_value, to, params));
        EXPECT_TRUE(to);
    }

    {
        JsonbValue* jsonb_value = get_json_value(10); // Decimal64 1234567890123456789
        UInt8 to = false;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_boolean(jsonb_value, to, params));
        EXPECT_TRUE(to);
    }

    {
        JsonbValue* jsonb_value = get_json_value(11); // Decimal128V3 1234567890123456789
        UInt8 to = false;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_boolean(jsonb_value, to, params));
        EXPECT_TRUE(to);
    }

    {
        JsonbValue* jsonb_value = get_json_value(12); // Decimal256 1234567890123456789
        UInt8 to = false;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_boolean(jsonb_value, to, params));
        EXPECT_TRUE(to);
    }
}

TEST_F(JsonbDocumentCastTest, test_to_int) {
    auto get_json_value = [&](int idx) -> JsonbValue* {
        auto& val = jsonbs[idx];
        JsonbDocument* doc = nullptr;
        auto st = JsonbDocument::checkAndCreateDocument(val.data(), val.size(), &doc);
        JsonbValue* value = doc->getValue();
        return value;
    };

    {
        JsonbValue* jsonb_value = get_json_value(0); // true
        Int8 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_int(jsonb_value, to, params));
        EXPECT_EQ(to, 1);
    }

    {
        JsonbValue* jsonb_value = get_json_value(1); // false
        Int8 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_int(jsonb_value, to, params));
        EXPECT_EQ(to, 0);
    }

    {
        JsonbValue* jsonb_value = get_json_value(2); // Int8 123
        Int8 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_int(jsonb_value, to, params));
        EXPECT_EQ(to, 123);
    }

    {
        JsonbValue* jsonb_value = get_json_value(3); // Int16 12345
        Int16 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_int(jsonb_value, to, params));
        EXPECT_EQ(to, 12345);
    }

    {
        JsonbValue* jsonb_value = get_json_value(4); // Int32 1234567890
        Int32 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_int(jsonb_value, to, params));
        EXPECT_EQ(to, 1234567890);
    }

    {
        JsonbValue* jsonb_value = get_json_value(5); // Int64 1234567890123456789
        Int64 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_int(jsonb_value, to, params));
        EXPECT_EQ(to, 1234567890123456789LL);
    }

    {
        JsonbValue* jsonb_value = get_json_value(6); // Int128 1234567890123456789
        Int128 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_int(jsonb_value, to, params));
        EXPECT_EQ(to, 1234567890123456789LL);
    }

    {
        JsonbValue* jsonb_value = get_json_value(7); // Float 123.456F
        Int32 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_int(jsonb_value, to, params));
        EXPECT_EQ(to, 123);
    }

    {
        JsonbValue* jsonb_value = get_json_value(8); // Double 123.4567890123456789
        Int64 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_int(jsonb_value, to, params));
        EXPECT_EQ(to, 123);
    }

    {
        JsonbValue* jsonb_value = get_json_value(9); // Decimal32 12345678
        Int32 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_int(jsonb_value, to, params));
        EXPECT_EQ(to, 1234567);
    }

    {
        JsonbValue* jsonb_value = get_json_value(10); // Decimal64 1234567890123456789
        Int64 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_int(jsonb_value, to, params));
        EXPECT_EQ(to, 12345678901234567);
    }

    {
        JsonbValue* jsonb_value = get_json_value(11); // Decimal128V3 1234567890123456789
        Int128 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_int(jsonb_value, to, params));
        EXPECT_EQ(to, 1234567890123456);
    }

    {
        JsonbValue* jsonb_value = get_json_value(12); // Decimal256 1234567890123456789
        Int128 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_int(jsonb_value, to, params));
        EXPECT_EQ(to, 1234567890123456);
    }
}

TEST_F(JsonbDocumentCastTest, test_to_float) {
    auto get_json_value = [&](int idx) -> JsonbValue* {
        auto& val = jsonbs[idx];
        JsonbDocument* doc = nullptr;
        auto st = JsonbDocument::checkAndCreateDocument(val.data(), val.size(), &doc);
        JsonbValue* value = doc->getValue();
        return value;
    };

    {
        JsonbValue* jsonb_value = get_json_value(0); // true
        Float32 to = 0.0F;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_float(jsonb_value, to, params));
        EXPECT_EQ(to, 1.0F);
    }

    {
        JsonbValue* jsonb_value = get_json_value(1); // false
        Float32 to = 0.0F;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_float(jsonb_value, to, params));
        EXPECT_EQ(to, 0.0F);
    }

    {
        JsonbValue* jsonb_value = get_json_value(2); // Int8 123
        Float32 to = 0.0F;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_float(jsonb_value, to, params));
        EXPECT_EQ(to, 123.0F);
    }

    {
        JsonbValue* jsonb_value = get_json_value(3); // Int16 12345
        Float32 to = 0.0F;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_float(jsonb_value, to, params));
        EXPECT_EQ(to, 12345.0F);
    }

    {
        JsonbValue* jsonb_value = get_json_value(4); // Int32 1234567890
        Float32 to = 0.0F;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_float(jsonb_value, to, params));
        EXPECT_EQ(to, 1234567890.0F);
    }

    {
        JsonbValue* jsonb_value = get_json_value(5); // Int64 1234567890123456789
        Float32 to = 0.0F;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_float(jsonb_value, to, params));
        EXPECT_EQ(to, 1234567890123456789LL);
    }

    {
        JsonbValue* jsonb_value = get_json_value(6); // Int128 1234567890123456789
        Float32 to = 0.0F;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_float(jsonb_value, to, params));
        EXPECT_EQ(to, 1234567890123456789LL);
    }

    {
        JsonbValue* jsonb_value = get_json_value(7); // Float 123.456F
        Float32 to = 0.0F;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_float(jsonb_value, to, params));
        EXPECT_EQ(to, 123.456F);
    }

    {
        JsonbValue* jsonb_value = get_json_value(8); // Double 123.4567890123456789
        Float32 to = 0.0F;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_float(jsonb_value, to, params));
        EXPECT_EQ(to, 123.456789F);
    }

    {
        JsonbValue* jsonb_value = get_json_value(9); // Decimal32 12345678
        Float32 to = 0.0F;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_float(jsonb_value, to, params));
        EXPECT_EQ(to, 1234567.8F);
    }

    {
        JsonbValue* jsonb_value = get_json_value(10); // Decimal64 1234567890123456789
        Float32 to = 0.0F;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_float(jsonb_value, to, params));
        EXPECT_EQ(to, 12345678901234567.89F);
    }

    {
        JsonbValue* jsonb_value = get_json_value(11); // Decimal128V3 1234567890123456789
        Float32 to = 0.0F;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_float(jsonb_value, to, params));
        EXPECT_EQ(to, 1234567890123456.789F);
    }

    {
        JsonbValue* jsonb_value = get_json_value(12); // Decimal256 1234567890123456789
        Float32 to = 0.0F;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_float(jsonb_value, to, params));
        EXPECT_EQ(to, 1234567890123456.789F);
    }
}

TEST_F(JsonbDocumentCastTest, test_to_decimal) {
    auto get_json_value = [&](int idx) -> JsonbValue* {
        auto& val = jsonbs[idx];
        JsonbDocument* doc = nullptr;
        auto st = JsonbDocument::checkAndCreateDocument(val.data(), val.size(), &doc);
        JsonbValue* value = doc->getValue();
        return value;
    };

    {
        JsonbValue* jsonb_value = get_json_value(0); // true
        Decimal32 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_decimal(jsonb_value, to, 4, 1, params));
        EXPECT_EQ(to.value, 10);
    }

    {
        JsonbValue* jsonb_value = get_json_value(1); // false
        Decimal32 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_decimal(jsonb_value, to, 4, 1, params));
        EXPECT_EQ(to.value, 0);
    }

    {
        JsonbValue* jsonb_value = get_json_value(2); // Int8 123
        Decimal32 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_decimal(jsonb_value, to, 4, 1, params));
        EXPECT_EQ(to.value, 1230);
    }

    {
        JsonbValue* jsonb_value = get_json_value(3); // Int16 12345
        Decimal32 to = 0;
        EXPECT_FALSE(JsonbCast::cast_from_json_to_decimal(jsonb_value, to, 4, 1, params));
    }

    {
        JsonbValue* jsonb_value = get_json_value(4); // Int32 1234567890
        Decimal32 to = 0;
        EXPECT_FALSE(JsonbCast::cast_from_json_to_decimal(jsonb_value, to, 4, 1, params));
    }

    {
        JsonbValue* jsonb_value = get_json_value(5); // Int64 1234567890123456789
        Decimal32 to = 0;
        EXPECT_FALSE(JsonbCast::cast_from_json_to_decimal(jsonb_value, to, 4, 1, params));
    }

    {
        JsonbValue* jsonb_value = get_json_value(6); // Int128 1234567890123456789
        Decimal32 to = 0;
        EXPECT_FALSE(JsonbCast::cast_from_json_to_decimal(jsonb_value, to, 4, 1, params));
    }

    {
        JsonbValue* jsonb_value = get_json_value(7); // Float 123.456F
        Decimal32 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_decimal(jsonb_value, to, 4, 1, params));
        EXPECT_EQ(to.value, 1235);
    }

    {
        JsonbValue* jsonb_value = get_json_value(8); // Double 123.4567890123456789
        Decimal32 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_decimal(jsonb_value, to, 4, 1, params));
        EXPECT_EQ(to.value, 1235);
    }

    {
        JsonbValue* jsonb_value = get_json_value(9); // Decimal32 12345678
        Decimal32 to = 0;
        EXPECT_TRUE(JsonbCast::cast_from_json_to_decimal(jsonb_value, to, 4, 1, params));
        EXPECT_EQ(to.value, 12345678);
    }

    {
        JsonbValue* jsonb_value = get_json_value(10); // Decimal64 1234567890123456789
        Decimal32 to = 0;
        EXPECT_FALSE(JsonbCast::cast_from_json_to_decimal(jsonb_value, to, 4, 1, params));
    }

    {
        JsonbValue* jsonb_value = get_json_value(11); // Decimal128V3 1234567890123456789
        Decimal32 to = 0;
        EXPECT_FALSE(JsonbCast::cast_from_json_to_decimal(jsonb_value, to, 4, 1, params));
    }

    {
        JsonbValue* jsonb_value = get_json_value(12); // Decimal256 1234567890123456789
        Decimal32 to = 0;
        EXPECT_FALSE(JsonbCast::cast_from_json_to_decimal(jsonb_value, to, 4, 1, params));
    }
}

} // namespace doris::vectorized