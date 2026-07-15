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

#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "common/config.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant_v2.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type_serde/data_type_nullable_serde.h"
#include "core/data_type_serde/data_type_string_serde.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/string_buffer.hpp"
#include "gen_cpp/types.pb.h"

namespace doris {
namespace {

class ScopedInvalidJsonMode {
public:
    explicit ScopedInvalidJsonMode(bool value)
            : _old(config::variant_throw_exeception_on_invalid_json) {
        config::variant_throw_exeception_on_invalid_json = value;
    }
    ~ScopedInvalidJsonMode() { config::variant_throw_exeception_on_invalid_json = _old; }

private:
    bool _old;
};

Status deserialize_json(const DataTypeVariantV2SerDe& serde, ColumnVariantV2& column,
                        std::string_view text) {
    Slice slice(text.data(), text.size());
    DataTypeSerDe::FormatOptions options;
    return serde.deserialize_one_cell_from_json(column, slice, options);
}

Status deserialize_csv(const DataTypeVariantV2SerDe& serde, ColumnVariantV2& column,
                       std::string_view text) {
    Slice slice(text.data(), text.size());
    DataTypeSerDe::FormatOptions options;
    return serde.deserialize_one_cell_from_csv(column, slice, options);
}

std::string json_at(const DataTypeVariantV2SerDe& serde, const IColumn& column, size_t row) {
    auto output = ColumnString::create();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    const Status status = serde.serialize_one_cell_to_json(column, row, writer, options);
    EXPECT_TRUE(status.ok()) << status;
    if (!status.ok()) {
        return {};
    }
    writer.commit();
    return output->get_data_at(0).to_string();
}

ColumnVariantV2::MutablePtr typed_int(int32_t value) {
    auto nested = ColumnInt32::create();
    nested->insert_value(value);
    auto nulls = ColumnUInt8::create();
    nulls->insert_value(0);
    return ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(nested), std::move(nulls)),
            std::make_shared<DataTypeInt32>());
}

} // namespace

TEST(DataTypeVariantV2SerdeInputTest, JsonUsesT15ValidEmptyAndInvalidPolicies) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    {
        ScopedInvalidJsonMode mode(false);
        EXPECT_TRUE(deserialize_json(serde, *column, R"({"b":[1,null]})").ok());
        EXPECT_TRUE(deserialize_json(serde, *column, {}).ok());
        EXPECT_TRUE(deserialize_json(serde, *column, "not-json").ok());
    }
    ASSERT_EQ(column->size(), 3);
    EXPECT_EQ(json_at(serde, *column, 0), R"({"b":[1,null]})");
    EXPECT_EQ(json_at(serde, *column, 1), "{}");
    EXPECT_EQ(json_at(serde, *column, 2), R"("not-json")");

    const size_t before = column->size();
    {
        ScopedInvalidJsonMode mode(true);
        EXPECT_EQ(deserialize_json(serde, *column, "not-json").code(), ErrorCode::INVALID_ARGUMENT);
    }
    EXPECT_EQ(column->size(), before);

    const std::string invalid_utf8("\xC3\x28", 2);
    {
        ScopedInvalidJsonMode mode(false);
        EXPECT_EQ(deserialize_json(serde, *column, invalid_utf8).code(),
                  ErrorCode::INVALID_ARGUMENT);
    }
    EXPECT_EQ(column->size(), before);
}

TEST(DataTypeVariantV2SerdeInputTest, VectorFailureIsAtomicAndCounterIsNotAdvanced) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"sentinel":1})").ok());
    const size_t before = column->size();

    std::string valid = R"({"a":1})";
    std::string invalid = "{";
    std::vector<Slice> slices {{valid.data(), valid.size()}, {invalid.data(), invalid.size()}};
    uint64_t num_deserialized = 0;
    DataTypeSerDe::FormatOptions options;
    ScopedInvalidJsonMode mode(true);
    const Status status =
            serde.deserialize_column_from_json_vector(*column, slices, &num_deserialized, options);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(column->size(), before);
    EXPECT_EQ(num_deserialized, 0);
    EXPECT_EQ(json_at(serde, *column, 0), R"({"sentinel":1})");
}

TEST(DataTypeVariantV2SerdeInputTest, EmptyVectorAndCounterOverflowDoNotMutateState) {
    DataTypeVariantV2SerDe serde;
    auto column = typed_int(1);
    DataTypeSerDe::FormatOptions options;
    std::vector<Slice> empty;
    uint64_t counter = 7;
    EXPECT_TRUE(serde.deserialize_column_from_json_vector(*column, empty, &counter, options).ok());
    EXPECT_EQ(counter, 7);
    EXPECT_EQ(column->size(), 1);
    EXPECT_TRUE(column->is_typed());

    std::string value = "2";
    std::vector<Slice> one {{value.data(), value.size()}};
    counter = std::numeric_limits<uint64_t>::max();
    EXPECT_EQ(serde.deserialize_column_from_json_vector(*column, one, &counter, options).code(),
              ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(counter, std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(column->size(), 1);
    EXPECT_TRUE(column->is_typed());
    EXPECT_EQ(json_at(serde, *column, 0), "1");
}

TEST(DataTypeVariantV2SerdeInputTest, SuccessfulVectorAdvancesExistingCounter) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    std::string first = "1";
    std::string second = R"({"a":2})";
    std::vector<Slice> slices {{first.data(), first.size()}, {second.data(), second.size()}};
    uint64_t counter = 5;
    DataTypeSerDe::FormatOptions options;
    ASSERT_TRUE(serde.deserialize_column_from_json_vector(*column, slices, &counter, options).ok());
    EXPECT_EQ(counter, 7);
    ASSERT_EQ(column->size(), 2);
    EXPECT_EQ(json_at(serde, *column, 0), "1");
    EXPECT_EQ(json_at(serde, *column, 1), R"({"a":2})");
}

TEST(DataTypeVariantV2SerdeInputTest, CsvIsAlwaysRawStringScalar) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_csv(serde, *column, R"({"a":1})").ok());
    ASSERT_TRUE(deserialize_csv(serde, *column, "null").ok());
    EXPECT_EQ(json_at(serde, *column, 0), R"("{\"a\":1}")");
    EXPECT_EQ(json_at(serde, *column, 1), R"("null")");
}

TEST(DataTypeVariantV2SerdeInputTest, CsvUnescapesStringScalarBeforeEncoding) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    std::string text = "left,right\"\"quoted\\\\path\nnext";
    Slice slice(text.data(), text.size());
    DataTypeSerDe::FormatOptions options;
    options.escape_char = '\\';
    options.quote_char = '"';
    ASSERT_TRUE(serde.deserialize_one_cell_from_csv(*column, slice, options).ok());
    ASSERT_EQ(column->size(), 1);
    EXPECT_EQ(json_at(serde, *column, 0), R"("left,right\"quoted\\path\nnext")");
}

TEST(DataTypeVariantV2SerdeInputTest, NullableCsvPreservesSqlNullAndRawStringScalar) {
    DataTypeVariantV2SerDe serde;
    MutableColumnPtr nested = ColumnVariantV2::create();
    auto null_map = ColumnUInt8::create();
    auto outer = ColumnNullable::create(std::move(nested), std::move(null_map));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    DataTypeSerDe::FormatOptions options;
    options.null_format = "NULL";
    options.null_len = 4;

    std::string sql_null = "NULL";
    Slice sql_null_slice(sql_null.data(), sql_null.size());
    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_csv(*outer, sql_null_slice, options).ok());

    std::string raw_null = "null";
    Slice raw_null_slice(raw_null.data(), raw_null.size());
    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_csv(*outer, raw_null_slice, options).ok());

    options.converted_from_string = true;
    std::string quoted_null_format = R"("NULL")";
    Slice quoted_null_format_slice(quoted_null_format.data(), quoted_null_format.size());
    ASSERT_TRUE(
            nullable_serde.deserialize_one_cell_from_csv(*outer, quoted_null_format_slice, options)
                    .ok());

    ASSERT_EQ(outer->size(), 3);
    EXPECT_TRUE(outer->is_null_at(0));
    EXPECT_FALSE(outer->is_null_at(1));
    EXPECT_FALSE(outer->is_null_at(2));
    EXPECT_EQ(json_at(serde, outer->get_nested_column(), 1), R"("null")");
    EXPECT_EQ(json_at(serde, outer->get_nested_column(), 2), R"("NULL")");
}

TEST(DataTypeVariantV2SerdeInputTest, NullableCsvNestedFailureAppendsOneSqlNull) {
    MutableColumnPtr nested = ColumnVariantV2::create();
    auto null_map = ColumnUInt8::create();
    auto outer = ColumnNullable::create(std::move(nested), std::move(null_map));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    Slice invalid(static_cast<char*>(nullptr), 1);
    DataTypeSerDe::FormatOptions options;
    options.null_len = 0;
    options.escape_char = '\\';

    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_csv(*outer, invalid, options).ok());
    ASSERT_EQ(outer->size(), 1);
    EXPECT_TRUE(outer->is_null_at(0));
    EXPECT_EQ(outer->get_nested_column().size(), 1);
}

TEST(DataTypeVariantV2SerdeInputTest, NullableCsvDispatchesForNonVariantNestedSerde) {
    MutableColumnPtr nested = ColumnString::create();
    auto null_map = ColumnUInt8::create();
    auto outer = ColumnNullable::create(std::move(nested), std::move(null_map));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeStringSerDe>(TYPE_STRING));
    std::string text = R"(left""right\\tail)";
    Slice slice(text.data(), text.size());
    DataTypeSerDe::FormatOptions options;
    options.escape_char = '\\';
    options.quote_char = '"';

    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_csv(*outer, slice, options).ok());
    ASSERT_EQ(outer->size(), 1);
    EXPECT_FALSE(outer->is_null_at(0));
    EXPECT_EQ(assert_cast<const ColumnString&>(outer->get_nested_column()).get_data_at(0),
              StringRef("left\"right\\tail"));
}

TEST(DataTypeVariantV2SerdeInputTest, InvalidCsvInputDoesNotPublishARow) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_csv(serde, *column, "sentinel").ok());
    const size_t before = column->size();
    Slice invalid(static_cast<char*>(nullptr), 1);
    DataTypeSerDe::FormatOptions options;
    options.escape_char = '\\';
    EXPECT_EQ(serde.deserialize_one_cell_from_csv(*column, invalid, options).code(),
              ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(column->size(), before);
    EXPECT_EQ(json_at(serde, *column, 0), R"("sentinel")");
}

TEST(DataTypeVariantV2SerdeInputTest, OuterSqlNullAndVariantNullRemainDistinct) {
    DataTypeVariantV2SerDe serde;
    auto variant_null = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_json(serde, *variant_null, "null").ok());
    EXPECT_EQ(json_at(serde, *variant_null, 0), "null");

    MutableColumnPtr nested = ColumnVariantV2::create();
    auto null_map = ColumnUInt8::create();
    auto outer = ColumnNullable::create(std::move(nested), std::move(null_map));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    std::string sql_null = "\\N";
    Slice slice(sql_null.data(), sql_null.size());
    DataTypeSerDe::FormatOptions options;
    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_json(*outer, slice, options).ok());
    ASSERT_EQ(outer->size(), 1);
    EXPECT_TRUE(outer->is_null_at(0));
}

TEST(DataTypeVariantV2SerdeInputTest, PbAndArrowReadKeepExplicitGuards) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    PValues values;
    EXPECT_EQ(serde.write_column_to_pb(*column, values, 0, 0).code(),
              ErrorCode::NOT_IMPLEMENTED_ERROR);
    EXPECT_EQ(serde.read_column_from_pb(*column, values).code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
    EXPECT_EQ(serde.read_column_from_arrow(*column, nullptr, 0, 0, cctz::utc_time_zone()).code(),
              ErrorCode::NOT_IMPLEMENTED_ERROR);
}

} // namespace doris
