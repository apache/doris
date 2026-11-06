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

#include <gtest/gtest.h>

#include <cmath>
#include <limits>

#ifdef ADDRESS_SANITIZER
#include <sanitizer/allocator_interface.h>
#endif

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type_serde/bson_value_view.h"
#include "core/data_type_serde/data_type_number_serde.h"
#include "core/value/vdatetime_value.h"
#include "util/jsonb_utils.h"

namespace doris {
namespace {
#ifdef ADDRESS_SANITIZER
thread_local bool measure_bson_allocations = false;
thread_local size_t largest_bson_allocation = 0;
void bson_allocation_hook(const volatile void*, size_t bytes) {
    if (measure_bson_allocations) {
        largest_bson_allocation = std::max(largest_bson_allocation, bytes);
    }
}
void bson_free_hook(const volatile void*) {}
#endif
BsonBinding binding(std::string name = "v", std::vector<int> tags = {}) {
    BsonBinding result;
    result.name = name;
    result.path = {name};
    result.types = std::move(tags);
    return result;
}
class BsonSerdeTest : public testing::Test {
protected:
    Status read(const DataTypePtr& type, IColumn& column, const std::string& json,
                const BsonBinding& field, bool null_on_error = false) {
        bson_error_t error {};
        bson_t* document = bson_new_from_json(reinterpret_cast<const uint8_t*>(json.data()),
                                              json.size(), &error);
        if (!document) {
            return Status::InvalidArgument("Invalid test BSON");
        }
        auto value = BsonValueView::document(*document).at_path(field.path);
        auto status = type->get_serde()->read_one_cell_from_bson(
                column, value, {.binding = &field, .mismatch_as_null = null_on_error});
        bson_destroy(document);
        return status;
    }
    Status read(const DataTypePtr& type, IColumn& column, const std::string& json,
                bool null_on_error = false) {
        return read(type, column, json, binding(), null_on_error);
    }
};
} // namespace

TEST_F(BsonSerdeTest, IntegerWidthsAreExactAndOverflowDoesNotAppend) {
    auto type = std::make_shared<DataTypeInt64>();
    auto column = type->create_column();
    ASSERT_TRUE(read(type, *column, R"({"v":{"$numberLong":"9223372036854775807"}})").ok());
    ASSERT_TRUE(read(type, *column, R"({"v":{"$numberLong":"-9223372036854775808"}})").ok());
    const auto& data = assert_cast<const ColumnInt64&>(*column).get_data();
    EXPECT_EQ(data[0], std::numeric_limits<int64_t>::max());
    EXPECT_EQ(data[1], std::numeric_limits<int64_t>::min());
    EXPECT_FALSE(read(type, *column, R"({"v":1.5})").ok());
    EXPECT_FALSE(read(type, *column, R"({"v":"123"})").ok());
    EXPECT_EQ(column->size(), 2);
    auto narrow = std::make_shared<DataTypeInt8>();
    auto small = narrow->create_column();
    EXPECT_TRUE(read(narrow, *small, R"({"v":127})").ok());
    EXPECT_FALSE(read(narrow, *small, R"({"v":128})").ok());
    EXPECT_EQ(small->size(), 1);
}
TEST_F(BsonSerdeTest, BooleanAndDoubleKeepTheirPhysicalTags) {
    auto boolean = std::make_shared<DataTypeUInt8>();
    auto c = boolean->create_column();
    EXPECT_TRUE(read(boolean, *c, R"({"v":true})").ok());
    EXPECT_FALSE(read(boolean, *c, R"({"v":1})").ok());
    auto type = std::make_shared<DataTypeFloat64>();
    auto d = type->create_column();
    EXPECT_TRUE(read(type, *d, R"({"v":{"$numberDouble":"NaN"}})").ok());
    EXPECT_TRUE(read(type, *d, R"({"v":{"$numberDouble":"Infinity"}})").ok());
    EXPECT_FALSE(read(type, *d, R"({"v":2})").ok());
    const auto& values = assert_cast<ColumnFloat64&>(*d).get_data();
    EXPECT_TRUE(std::isnan(values[0]));
    EXPECT_TRUE(std::isinf(values[1]));
}
TEST_F(BsonSerdeTest, MissingAndNullRemainDistinctFromUndefined) {
    auto type = make_nullable(std::make_shared<DataTypeInt32>());
    auto column = type->create_column();
    EXPECT_TRUE(read(type, *column, "{}").ok());
    EXPECT_TRUE(read(type, *column, R"({"v":null})").ok());
    EXPECT_FALSE(read(type, *column, R"({"v":{"$undefined":true}})").ok());
    EXPECT_EQ(column->size(), 2);
    EXPECT_TRUE(assert_cast<ColumnNullable&>(*column).is_null_at(0));
}
TEST_F(BsonSerdeTest, LiteralDotAndNestedPathsDoNotAlias) {
    auto type = std::make_shared<DataTypeInt32>();
    auto column = type->create_column();
    BsonBinding field = binding("a.b");
    ASSERT_TRUE(read(type, *column, R"({"a.b":1,"a":{"b":2}})", field).ok());
    field.path = {"a", "b"};
    ASSERT_TRUE(read(type, *column, R"({"a.b":1,"a":{"b":2}})", field).ok());
    EXPECT_EQ(assert_cast<ColumnInt32&>(*column).get_data()[0], 1);
    EXPECT_EQ(assert_cast<ColumnInt32&>(*column).get_data()[1], 2);
}
TEST_F(BsonSerdeTest, StringCopiesBorrowedStorageAndPreservesEmbeddedNull) {
    auto type = std::make_shared<DataTypeString>();
    auto column = type->create_column();
    ASSERT_TRUE(read(type, *column, R"({"v":"a\u0000b"})").ok());
    EXPECT_EQ(column->get_data_at(0).to_string(), std::string("a\0b", 3));
    EXPECT_FALSE(read(type, *column, R"({"v":{"a":1}})").ok());
}
TEST_F(BsonSerdeTest, ObjectIdIsTwelveBytesAndNotInterchangeableWithBinData) {
    auto type = std::make_shared<DataTypeVarbinary>(12);
    auto column = type->create_column();
    auto field = binding("v", {7});
    ASSERT_TRUE(read(type, *column, R"({"v":{"$oid":"000102030405060708090a0b"}})", field).ok());
    std::string expected;
    for (int i = 0; i < 12; ++i) {
        expected += static_cast<char>(i);
    }
    EXPECT_EQ(column->get_data_at(0).to_string(), expected);
    ASSERT_TRUE(read(type, *column, R"({"v":{"$oid":"00ff807f0100020304050607"}})", field).ok());
    const std::string high_bytes("\x00\xff\x80\x7f\x01\x00\x02\x03\x04\x05\x06\x07", 12);
    EXPECT_EQ(column->get_data_at(1).to_string(), high_bytes);
    EXPECT_FALSE(read(type, *column,
                      R"({"v":{"$binary":{"base64":"AAECAwQFBgcICQoL","subType":"00"}}})", field)
                         .ok());
}
TEST_F(BsonSerdeTest, BinarySubtypeIsCheckedAndEmptyBinaryIsNotNull) {
    auto type = make_nullable(std::make_shared<DataTypeVarbinary>());
    auto column = type->create_column();
    auto field = binding("v", {5});
    field.subtype = 128;
    ASSERT_TRUE(
            read(type, *column, R"({"v":{"$binary":{"base64":"","subType":"80"}}})", field).ok());
    EXPECT_FALSE(assert_cast<ColumnNullable&>(*column).is_null_at(0));
    EXPECT_FALSE(
            read(type, *column, R"({"v":{"$binary":{"base64":"","subType":"00"}}})", field).ok());
    EXPECT_EQ(column->size(), 1);
}
TEST_F(BsonSerdeTest, DecimalScaleNeverSilentlyRounds) {
    auto type = std::make_shared<DataTypeDecimal<TYPE_DECIMAL64>>(10, 2);
    auto column = type->create_column();
    for (const auto& value : {"123.4500", "-1.2E+2", "0E-6176", "1E-2"}) {
        EXPECT_TRUE(
                read(type, *column, "{\"v\":{\"$numberDecimal\":\"" + std::string(value) + "\"}}")
                        .ok());
    }
    for (const auto& value : {"1.234", "100000000.00", "NaN", "Infinity", "1E+6144"}) {
        EXPECT_FALSE(
                read(type, *column, "{\"v\":{\"$numberDecimal\":\"" + std::string(value) + "\"}}")
                        .ok());
    }
    EXPECT_EQ(column->size(), 4);
}
TEST_F(BsonSerdeTest, NegativeEpochMillisecondsUseFloorDivision) {
    auto type = std::make_shared<DataTypeDateTimeV2>(3);
    auto column = type->create_column();
    ASSERT_TRUE(read(type, *column, R"({"v":{"$date":{"$numberLong":"-1"}}})").ok());
    auto packed = assert_cast<ColumnDateTimeV2&>(*column).get_data()[0];
    DateV2Value<DateTimeV2ValueType> value;
    memcpy(&value, &packed, sizeof(value));
    EXPECT_EQ(value.year(), 1969);
    EXPECT_EQ(value.month(), 12);
    EXPECT_EQ(value.day(), 31);
    EXPECT_EQ(value.second(), 59);
    EXPECT_EQ(value.microsecond(), 999000);
    EXPECT_FALSE(
            read(type, *column, R"({"v":{"$date":{"$numberLong":"9223372036854775807"}}})").ok());
    auto seconds = std::make_shared<DataTypeDateTimeV2>(0);
    auto other = seconds->create_column();
    EXPECT_FALSE(read(seconds, *other, R"({"v":{"$date":{"$numberLong":"-1"}}})").ok());
}
TEST_F(BsonSerdeTest, ArrayFailureRollsBackNestedDataBeforeNullPolicy) {
    auto type = make_nullable(
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>())));
    auto column = type->create_column();
    auto field = binding("v", {4});
    field.children = {binding("element", {16})};
    ASSERT_TRUE(read(type, *column, R"({"v":[1,2]})", field).ok());
    ASSERT_FALSE(read(type, *column, R"({"v":[3,"bad"]})", field).ok());
    EXPECT_EQ(column->size(), 1);
    const auto& array =
            assert_cast<ColumnArray&>(assert_cast<ColumnNullable&>(*column).get_nested_column());
    EXPECT_EQ(array.get_data().size(), 2);
    // A failed array is one SQL NULL; partial elements must not escape into the next row.
    ASSERT_TRUE(read(type, *column, R"({"v":[3,"bad",4]})", field, true).ok());
    EXPECT_EQ(array.get_data().size(), 2);
    EXPECT_TRUE(assert_cast<ColumnNullable&>(*column).is_null_at(1));
    ASSERT_TRUE(read(type, *column, R"({"v":{}})", field, true).ok());
    EXPECT_TRUE(assert_cast<ColumnNullable&>(*column).is_null_at(2));
}
TEST_F(BsonSerdeTest, StructUsesFieldNamesAndRollsBackEveryChild) {
    DataTypes children {make_nullable(std::make_shared<DataTypeInt32>()),
                        make_nullable(std::make_shared<DataTypeString>())};
    auto type = std::make_shared<DataTypeStruct>(children, Strings {"a", "b"});
    auto column = type->create_column();
    auto field = binding("v", {3});
    field.children = {binding("a", {16}), binding("b", {2})};
    ASSERT_TRUE(read(type, *column, R"({"v":{"b":"ok","a":1,"extra":true}})", field).ok());
    ASSERT_FALSE(read(type, *column, R"({"v":{"a":2,"b":3}})", field).ok());
    auto& structure = assert_cast<ColumnStruct&>(*column);
    EXPECT_EQ(structure.get_column(0).size(), 1);
    EXPECT_EQ(structure.get_column(1).size(), 1);
    ASSERT_TRUE(read(type, *column, R"({"v":{"a":3}})", field).ok());
    EXPECT_TRUE(assert_cast<const ColumnNullable&>(structure.get_column(1)).is_null_at(1));
}
TEST_F(BsonSerdeTest, TimestampRetainsUnsignedSecondsAndIncrement) {
    auto integer = make_nullable(std::make_shared<DataTypeInt64>());
    auto type = std::make_shared<DataTypeStruct>(DataTypes {integer, integer},
                                                 Strings {"seconds", "increment"});
    auto column = type->create_column();
    auto field = binding("v", {17});
    field.children = {binding("seconds", {18}), binding("increment", {18})};
    ASSERT_TRUE(
            read(type, *column, R"({"v":{"$timestamp":{"t":4294967295,"i":4294967295}}})", field)
                    .ok());
    const auto& child =
            assert_cast<const ColumnNullable&>(assert_cast<ColumnStruct&>(*column).get_column(0));
    EXPECT_EQ(assert_cast<const ColumnInt64&>(child.get_nested_column()).get_data()[0],
              4294967295LL);
}
TEST_F(BsonSerdeTest, CanonicalExtendedJsonRetainsAllSpecialBsonTags) {
    auto type = std::make_shared<DataTypeJsonb>();
    auto column = type->create_column();
    std::vector<std::string> values {
            R"({"$numberLong":"9007199254740993"})",
            R"({"$numberDecimal":"123.4500"})",
            R"({"$oid":"000102030405060708090a0b"})",
            R"({"$date":{"$numberLong":"-1"}})",
            R"({"$binary":{"base64":"AA==","subType":"80"}})",
            R"({"$regularExpression":{"pattern":"a.*","options":"i"}})",
            R"({"$timestamp":{"t":42,"i":7}})",
            R"({"$minKey":1})",
            R"({"$maxKey":1})",
            R"({"$undefined":true})",
            R"({"$dbPointer":{"$ref":"sample.records","$id":{"$oid":"000102030405060708090a0b"}}})",
            R"({"$code":"return 1"})",
            R"({"$symbol":"s"})",
            R"({"$code":"return n","$scope":{"n":1}})"};
    for (const auto& value : values) {
        ASSERT_TRUE(read(type, *column, "{\"v\":" + value + "}").ok()) << value;
    }
    auto first = column->get_data_at(0);
    EXPECT_NE(JsonbToJson::jsonb_to_json_string(first.data, first.size).find("9007199254740993"),
              std::string::npos);
    EXPECT_EQ(column->size(), values.size());
}
TEST_F(BsonSerdeTest, DecodeDepthAndDescriptorLimitsAreEnforced) {
    auto type = std::make_shared<DataTypeInt32>();
    auto column = type->create_column();
    bson_value_t v {};
    v.value_type = BSON_TYPE_INT32;
    v.value.v_int32 = 1;
    auto field = binding();
    EXPECT_FALSE(
            type->get_serde()
                    ->read_one_cell_from_bson(*column, BsonValueView::of(v), {&field, false, 65})
                    .ok());
    std::vector<BsonBinding> fields;
    EXPECT_FALSE(parse_bson_bindings("{}", &fields).ok());
    EXPECT_FALSE(parse_bson_bindings(std::string(1024 * 1024 + 1, 'x'), &fields).ok());
    EXPECT_FALSE(
            parse_bson_bindings(R"({"columns":[{"name":"v","path":[],"bson_types":[],"children":[]},
        {"name":"v","path":[],"bson_types":[],"children":[]}]})",
                                &fields)
                    .ok());
}

TEST_F(BsonSerdeTest, WideDocumentValidationUsesBoundedScratch) {
    bson_t document = BSON_INITIALIZER;
    bson_array_builder_t* array = nullptr;
    ASSERT_TRUE(BSON_APPEND_ARRAY_BUILDER_BEGIN(&document, "wide", &array));
    bson_t empty = BSON_INITIALIZER;
    for (uint32_t i = 0; i < 900000; ++i) {
        ASSERT_TRUE(bson_array_builder_append_document(array, &empty));
    }
    ASSERT_TRUE(bson_append_array_builder_end(&document, array));
    ASSERT_LT(document.len, BsonDecodeContext::MAX_DOCUMENT_BYTES);
#ifdef ADDRESS_SANITIZER
    // Measure only validation scratch: the source BSON is already allocated. Ordinary vectors
    // bypass MemTracker, so source/output row budgets cannot detect this allocation regression.
    static const int installed =
            __sanitizer_install_malloc_and_free_hooks(bson_allocation_hook, bson_free_hook);
    ASSERT_EQ(installed, 1);
    largest_bson_allocation = 0;
    measure_bson_allocations = true;
#endif
    Status status = validate_bson_tree(BsonValueView::document(document));
#ifdef ADDRESS_SANITIZER
    measure_bson_allocations = false;
    EXPECT_LT(largest_bson_allocation, 64 * 1024);
#endif
    EXPECT_TRUE(status.ok()) << status;
    bson_destroy(&empty);
    bson_destroy(&document);
}

TEST_F(BsonSerdeTest, TreeDepthLimitIncludesDocumentsArraysAndCodeScopes) {
    for (auto kind : {BSON_TYPE_DOCUMENT, BSON_TYPE_ARRAY, BSON_TYPE_CODEWSCOPE}) {
        bson_t nested = BSON_INITIALIZER;
        for (int depth = 0; depth <= BsonDecodeContext::MAX_DEPTH; ++depth) {
            if (depth == BsonDecodeContext::MAX_DEPTH) {
                EXPECT_TRUE(validate_bson_tree(BsonValueView::document(nested)).ok());
            }
            bson_t parent = BSON_INITIALIZER;
            bool appended;
            if (kind == BSON_TYPE_DOCUMENT) {
                appended = BSON_APPEND_DOCUMENT(&parent, "0", &nested);
            } else if (kind == BSON_TYPE_ARRAY) {
                appended = BSON_APPEND_ARRAY(&parent, "0", &nested);
            } else {
                appended = BSON_APPEND_CODE_WITH_SCOPE(&parent, "0", "", &nested);
            }
            ASSERT_TRUE(appended);
            bson_destroy(&nested);
            bson_copy_to(&parent, &nested);
            bson_destroy(&parent);
        }
        EXPECT_FALSE(validate_bson_tree(BsonValueView::document(nested)).ok());
        bson_destroy(&nested);
    }
}

TEST_F(BsonSerdeTest, TreeElementLimitIsGlobalAndInclusive) {
    bson_t document = BSON_INITIALIZER;
    for (uint32_t i = 0; i < BsonDecodeContext::MAX_ELEMENTS; ++i) {
        char key[16];
        const char* key_ptr = nullptr;
        bson_uint32_to_string(i, &key_ptr, key, sizeof(key));
        ASSERT_TRUE(bson_append_int32(&document, key_ptr, -1, 1));
    }
    ASSERT_LT(document.len, BsonDecodeContext::MAX_DOCUMENT_BYTES);
    EXPECT_TRUE(validate_bson_tree(BsonValueView::document(document)).ok());
    ASSERT_TRUE(BSON_APPEND_INT32(&document, "extra", 1));
    EXPECT_FALSE(validate_bson_tree(BsonValueView::document(document)).ok());
    bson_destroy(&document);
}

TEST_F(BsonSerdeTest, RawDocumentBytesRoundTripWithoutJsonOrBorrowedStorage) {
    auto type = std::make_shared<DataTypeVarbinary>(1);
    auto column = type->create_column();
    bson_t document = BSON_INITIALIZER;
    BSON_APPEND_INT64(&document, "exact", 9007199254740993LL);
    BsonBinding field;
    field.special = "bson";
    std::string bytes(reinterpret_cast<const char*>(bson_get_data(&document)), document.len);
    ASSERT_TRUE(
            type->get_serde()
                    ->read_one_cell_from_bson(*column, BsonValueView::document(document), {&field})
                    .ok());
    bson_destroy(&document);
    EXPECT_EQ(column->get_data_at(0).to_string(), bytes);
}
TEST_F(BsonSerdeTest, VarbinaryLengthIsMetadataForBsonPayloads) {
    auto type = make_nullable(std::make_shared<DataTypeVarbinary>(4));
    auto column = type->create_column();
    auto field = binding("v", {7});
    ASSERT_TRUE(read(type, *column, R"({"v":{"$oid":"000102030405060708090a0b"}})", field).ok());
    ASSERT_TRUE(
            read(type, *column, R"({"v":{"$oid":"000102030405060708090a0b"}})", field, true).ok());
    const auto& nullable = assert_cast<ColumnNullable&>(*column);
    EXPECT_FALSE(nullable.is_null_at(0));
    EXPECT_FALSE(nullable.is_null_at(1));
    EXPECT_EQ(nullable.get_nested_column().get_data_at(0).to_string(),
              std::string("\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b", 12));
    field = binding("v", {5});
    field.subtype = 128;
    ASSERT_TRUE(
            read(type, *column, R"({"v":{"$binary":{"base64":"AAECAwQF","subType":"80"}}})", field)
                    .ok());
    EXPECT_FALSE(nullable.is_null_at(2));
    EXPECT_EQ(nullable.get_nested_column().get_data_at(2).to_string(),
              std::string("\x00\x01\x02\x03\x04\x05", 6));
}
TEST_F(BsonSerdeTest, NullableBinarySubtypeMismatchStillUsesConfiguredPolicy) {
    auto type = make_nullable(std::make_shared<DataTypeVarbinary>(4));
    auto column = type->create_column();
    auto field = binding("v", {5});
    field.subtype = 0;
    EXPECT_FALSE(read(type, *column, R"({"v":{"$binary":{"base64":"AA==","subType":"80"}}})", field)
                         .ok());
    ASSERT_TRUE(read(type, *column, R"({"v":{"$binary":{"base64":"AA==","subType":"80"}}})", field,
                     true)
                        .ok());
    EXPECT_EQ(column->size(), 1);
    EXPECT_TRUE(assert_cast<ColumnNullable&>(*column).is_null_at(0));
}
TEST_F(BsonSerdeTest, RegexStructPreservesPatternAndOptionsWithoutExecutingThem) {
    auto text = make_nullable(std::make_shared<DataTypeString>());
    auto type = std::make_shared<DataTypeStruct>(DataTypes {text, text},
                                                 Strings {"pattern", "options"});
    auto column = type->create_column();
    auto field = binding("v", {11});
    field.children = {binding("pattern", {2}), binding("options", {2})};
    ASSERT_TRUE(read(type, *column,
                     R"({"v":{"$regularExpression":{"pattern":"a.*b","options":"im"}}})", field)
                        .ok());
    const auto& target = assert_cast<ColumnStruct&>(*column);
    EXPECT_EQ(assert_cast<const ColumnNullable&>(target.get_column(0))
                      .get_nested_column()
                      .get_data_at(0)
                      .to_string(),
              "a.*b");
    EXPECT_EQ(assert_cast<const ColumnNullable&>(target.get_column(1))
                      .get_nested_column()
                      .get_data_at(0)
                      .to_string(),
              "im");
}
TEST_F(BsonSerdeTest, NestedArrayFailureDoesNotAlterTheFollowingRow) {
    auto integer = make_nullable(std::make_shared<DataTypeInt32>());
    auto inner = make_nullable(std::make_shared<DataTypeArray>(integer));
    auto type = make_nullable(std::make_shared<DataTypeArray>(inner));
    auto column = type->create_column();
    auto field = binding("v", {4});
    auto child = binding("element", {4});
    child.children = {binding("element", {16})};
    field.children = {child};
    ASSERT_TRUE(read(type, *column, R"({"v":[[1,2],[3,"bad"]]})", field, true).ok());
    ASSERT_TRUE(read(type, *column, R"({"v":[[],[4,null]]})", field, true).ok());
    const auto& top = assert_cast<ColumnNullable&>(*column);
    EXPECT_TRUE(top.is_null_at(0));
    EXPECT_FALSE(top.is_null_at(1));
    const auto& outer = assert_cast<const ColumnArray&>(top.get_nested_column());
    const auto& nullable_inner = assert_cast<const ColumnNullable&>(outer.get_data());
    const auto& arrays = assert_cast<const ColumnArray&>(nullable_inner.get_nested_column());
    EXPECT_EQ(arrays.get_data().size(), 2);
}
TEST_F(BsonSerdeTest, DeepExtendedJsonIsRejectedBeforeRecursiveEncoding) {
    std::string json = "1";
    for (int i = 0; i < 70; ++i) {
        json = "[" + json + "]";
    }
    auto type = std::make_shared<DataTypeJsonb>();
    auto column = type->create_column();
    EXPECT_FALSE(read(type, *column, "{\"v\":" + json + "}").ok());
    EXPECT_EQ(column->size(), 0);
}
TEST_F(BsonSerdeTest, ExplicitEpochMillisecondsAvoidsDatetimeRangeLoss) {
    auto type = std::make_shared<DataTypeInt64>();
    auto column = type->create_column();
    auto field = binding("v", {9});
    ASSERT_TRUE(
            read(type, *column, R"({"v":{"$date":{"$numberLong":"9223372036854775807"}}})", field)
                    .ok());
    EXPECT_EQ(assert_cast<ColumnInt64&>(*column).get_data()[0],
              std::numeric_limits<int64_t>::max());
}

TEST_F(BsonSerdeTest, StructProjectionUsesNamesAfterPruningAndReordering) {
    auto type = std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeString>())}, Strings {"b"});
    auto column = type->create_column();
    auto field = binding("v", {3});
    field.children = {binding("a", {16}), binding("b", {2})};
    field.child_positions = {{"a", 0}, {"b", 1}};
    ASSERT_TRUE(read(type, *column, R"({"v":{"b":"projected","a":123}})", field).ok());
    const auto& target = assert_cast<ColumnStruct&>(*column);
    EXPECT_EQ(assert_cast<const ColumnNullable&>(target.get_column(0))
                      .get_nested_column()
                      .get_data_at(0)
                      .to_string(),
              "projected");
}

TEST_F(BsonSerdeTest, ScalarAncestorIsATypeMismatchRatherThanMissing) {
    auto type = make_nullable(std::make_shared<DataTypeInt32>());
    auto column = type->create_column();
    auto field = binding("v");
    field.path = {"parent", "child"};
    EXPECT_FALSE(read(type, *column, R"({"parent":1})", field).ok());
    EXPECT_EQ(column->size(), 0);
    EXPECT_TRUE(read(type, *column, R"({"parent":1})", field, true).ok());
    EXPECT_TRUE(read(type, *column, R"({"parent":null})", field).ok());
    EXPECT_TRUE(read(type, *column, "{}", field).ok());
    EXPECT_EQ(column->size(), 3);
}

TEST_F(BsonSerdeTest, HeterogeneousArrayCanPreserveBsonTagsAsJson) {
    auto type = std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeJsonb>()));
    auto column = type->create_column();
    auto field = binding("v", {4});
    field.children = {binding("element")};
    ASSERT_TRUE(
            read(type, *column,
                 R"({"v":[{"$oid":"000102030405060708090a0b"},null,{"n":{"$numberLong":"9007199254740993"}}]})",
                 field)
                    .ok());
    const auto& array = assert_cast<ColumnArray&>(*column);
    const auto& nested = assert_cast<const ColumnNullable&>(array.get_data());
    EXPECT_EQ(nested.size(), 3);
    EXPECT_TRUE(nested.is_null_at(1));
    auto raw = nested.get_nested_column().get_data_at(2);
    EXPECT_NE(JsonbToJson::jsonb_to_json_string(raw.data, raw.size).find("9007199254740993"),
              std::string::npos);
}

TEST_F(BsonSerdeTest, AllocationFailureIsNotConvertedToNullAndRollsBackAppends) {
    class ThrowingSerDe final : public DataTypeNumberSerDe<TYPE_INT> {
    public:
        Status decode_bson_value(IColumn& column, const BsonValueView&,
                                 const BsonDecodeContext&) const override {
            column.insert_default();
            throw Exception(ErrorCode::MEM_ALLOC_FAILED, "fixture allocation failure");
        }
    };
    auto type = make_nullable(std::make_shared<DataTypeInt32>());
    auto column = type->create_column();
    DataTypeNullableSerDe serde(std::make_shared<ThrowingSerDe>());
    bson_value_t v {};
    v.value_type = BSON_TYPE_INT32;
    v.value.v_int32 = 1;
    auto field = binding("v", {16});
    auto status = serde.read_one_cell_from_bson(*column, BsonValueView::of(v),
                                                {.binding = &field, .mismatch_as_null = true});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(column->size(), 0);
    EXPECT_EQ(assert_cast<ColumnNullable&>(*column).get_nested_column().size(), 0);
}

TEST_F(BsonSerdeTest, DecimalStorageWidthsPreserveExactScaledValues) {
    auto t32 = std::make_shared<DataTypeDecimal<TYPE_DECIMAL32>>(9, 2);
    auto c32 = t32->create_column();
    ASSERT_TRUE(read(t32, *c32, R"({"v":{"$numberDecimal":"123.4500"}})").ok());
    EXPECT_EQ(assert_cast<ColumnDecimal<TYPE_DECIMAL32>&>(*c32).get_data()[0].value, 12345);
    auto t128 = std::make_shared<DataTypeDecimal<TYPE_DECIMAL128I>>(38, 10);
    auto c128 = t128->create_column();
    ASSERT_TRUE(read(t128, *c128, R"({"v":{"$numberDecimal":"123.4500"}})").ok());
    EXPECT_EQ(assert_cast<ColumnDecimal<TYPE_DECIMAL128I>&>(*c128).get_data()[0].value,
              static_cast<Int128>(1234500000000LL));
}

} // namespace doris
