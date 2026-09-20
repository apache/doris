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

#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type_serde/data_type_decimal_serde.h"
#include "core/data_type_serde/data_type_number_serde.h"
#include "core/value/jsonb_value.h"
#include "exprs/function/cast/cast_parameters.h"

namespace doris {

namespace {

std::string jsonb_of(const std::string& json) {
    JsonBinaryValue value;
    Status st = value.from_json_string(json);
    EXPECT_TRUE(st.ok()) << json << ": " << st;
    return std::string(value.value(), value.size());
}

// deserialize_column_from_jsonb_vector sizes the nested payload up front and then skips NULL
// rows. Make sure the buffer it grows into already holds stale non-zero bytes, the way a
// recycled allocation does in production: fill, then clear() which keeps the capacity.
template <typename Container>
void poison_capacity(Container& data, size_t rows, typename Container::value_type value) {
    data.resize_fill(rows, value);
    data.clear();
}

} // namespace

// CAST(jsonb AS BOOLEAN) on a value that has no boolean interpretation (missing key, JSON null,
// object, ...) yields NULL. The nested byte of such a row must still be a well-defined 0:
// VCompoundPred OR reads it raw, and VCaseExpr turns the raw byte into a branch index.
TEST(DataTypeSerDeJsonbNullPayloadTest, BooleanNullRowsHaveZeroPayload) {
    const std::vector<std::string> rows = {jsonb_of("true"),
                                           /* jsonb_extract miss */ "",
                                           jsonb_of("false"),
                                           jsonb_of("null"),
                                           jsonb_of("{\"late_delivery\": 1}"),
                                           jsonb_of("true")};
    const std::vector<uint8_t> expected_null {0, 1, 0, 1, 1, 0};
    const std::vector<uint8_t> expected_data {1, 0, 0, 0, 0, 1};

    auto col_from = ColumnString::create();
    for (const auto& row : rows) {
        col_from->insert_data(row.data(), row.size());
    }

    auto column_to = ColumnNullable::create(ColumnUInt8::create(), ColumnUInt8::create());
    auto& nested = assert_cast<ColumnUInt8&>(column_to->get_nested_column()).get_data();
    poison_capacity(nested, rows.size(), 0x41);
    poison_capacity(column_to->get_null_map_data(), rows.size(), 1);

    DataTypeNumberSerDe<TYPE_BOOLEAN> serde;
    CastParameters params;
    params.is_strict = false;
    Status st = serde.deserialize_column_from_jsonb_vector(*column_to, *col_from, params);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(column_to->size(), rows.size());

    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(column_to->get_null_map_data()[i], expected_null[i]) << "row " << i;
        EXPECT_EQ(nested[i], expected_data[i]) << "row " << i;
    }
}

TEST(DataTypeSerDeJsonbNullPayloadTest, IntegerNullRowsHaveZeroPayload) {
    const std::vector<std::string> rows = {jsonb_of("7"), "", jsonb_of("\"abc\""), jsonb_of("-3")};
    const std::vector<uint8_t> expected_null {0, 1, 1, 0};
    const std::vector<int64_t> expected_data {7, 0, 0, -3};

    auto col_from = ColumnString::create();
    for (const auto& row : rows) {
        col_from->insert_data(row.data(), row.size());
    }

    auto column_to = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto& nested = assert_cast<ColumnInt64&>(column_to->get_nested_column()).get_data();
    poison_capacity(nested, rows.size(), int64_t {0x4141414141414141});
    poison_capacity(column_to->get_null_map_data(), rows.size(), 1);

    DataTypeNumberSerDe<TYPE_BIGINT> serde;
    CastParameters params;
    params.is_strict = false;
    Status st = serde.deserialize_column_from_jsonb_vector(*column_to, *col_from, params);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(column_to->size(), rows.size());

    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(column_to->get_null_map_data()[i], expected_null[i]) << "row " << i;
        EXPECT_EQ(nested[i], expected_data[i]) << "row " << i;
    }
}

TEST(DataTypeSerDeJsonbNullPayloadTest, DecimalNullRowsHaveZeroPayload) {
    const std::vector<std::string> rows = {jsonb_of("1.5"), "", jsonb_of("\"abc\""),
                                           jsonb_of("-2.25")};
    const std::vector<uint8_t> expected_null {0, 1, 1, 0};
    // DECIMAL64(10, 2): 1.50 -> 150, -2.25 -> -225
    const std::vector<int64_t> expected_data {150, 0, 0, -225};

    auto col_from = ColumnString::create();
    for (const auto& row : rows) {
        col_from->insert_data(row.data(), row.size());
    }

    auto column_to = ColumnNullable::create(ColumnDecimal64::create(0, 2), ColumnUInt8::create());
    auto& nested = assert_cast<ColumnDecimal64&>(column_to->get_nested_column()).get_data();
    poison_capacity(nested, rows.size(), Decimal64 {int64_t {0x4141414141414141}});
    poison_capacity(column_to->get_null_map_data(), rows.size(), 1);

    DataTypeDecimalSerDe<TYPE_DECIMAL64> serde(10, 2);
    CastParameters params;
    params.is_strict = false;
    Status st = serde.deserialize_column_from_jsonb_vector(*column_to, *col_from, params);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(column_to->size(), rows.size());

    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(column_to->get_null_map_data()[i], expected_null[i]) << "row " << i;
        EXPECT_EQ(nested[i].value, expected_data[i]) << "row " << i;
    }
}

} // namespace doris
