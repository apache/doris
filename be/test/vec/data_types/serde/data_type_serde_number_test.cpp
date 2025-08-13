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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <streamvbyte.h>

#include <cmath>
#include <cstddef>
#include <iostream>
#include <limits>
#include <type_traits>

#include "runtime/primitive_type.h"
#include "testutil/test_util.h"
#include "util/slice.h"
#include "util/string_util.h"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
static std::string test_data_dir;

static auto serde_float32 = std::make_shared<DataTypeNumberSerDe<TYPE_FLOAT>>();
static auto serde_float64 = std::make_shared<DataTypeNumberSerDe<TYPE_DOUBLE>>();
static auto serde_int8 = std::make_shared<DataTypeNumberSerDe<TYPE_TINYINT>>();
static auto serde_int16 = std::make_shared<DataTypeNumberSerDe<TYPE_SMALLINT>>();
static auto serde_int32 = std::make_shared<DataTypeNumberSerDe<TYPE_INT>>();
static auto serde_int64 = std::make_shared<DataTypeNumberSerDe<TYPE_BIGINT>>();
static auto serde_int128 = std::make_shared<DataTypeNumberSerDe<TYPE_LARGEINT>>();
static auto serde_uint8 = std::make_shared<DataTypeNumberSerDe<TYPE_BOOLEAN>>();

static ColumnFloat32::MutablePtr column_float32;
static ColumnFloat64::MutablePtr column_float64;
static ColumnInt8::MutablePtr column_int8;
static ColumnInt16::MutablePtr column_int16;
static ColumnInt32::MutablePtr column_int32;
static ColumnInt64::MutablePtr column_int64;
static ColumnInt128::MutablePtr column_int128;
static ColumnUInt8::MutablePtr column_uint8;

class DataTypeNumberSerDeTest : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";

        column_float32 = ColumnFloat32::create();
        column_float64 = ColumnFloat64::create();

        column_int8 = ColumnInt8::create();
        column_int16 = ColumnInt16::create();
        column_int32 = ColumnInt32::create();
        column_int64 = ColumnInt64::create();
        column_int128 = ColumnInt128::create();

        column_uint8 = ColumnUInt8::create();

        load_columns_data();
    }
    static void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        auto test_func = [&](const MutableColumnPtr& column, const auto& serde,
                             const std::string& data_file_name) {
            MutableColumns columns;
            columns.push_back(column->get_ptr());
            DataTypeSerDeSPtrs serdes = {serde};
            load_columns_data_from_file(columns, serdes, ';', {0},
                                        test_data_dir + "/" + data_file_name);
            EXPECT_TRUE(!column->empty());
        };
        test_func(column_float32->get_ptr(), serde_float32, "FLOAT.csv");
        test_func(column_float64->get_ptr(), serde_float64, "DOUBLE.csv");

        test_func(column_int8->get_ptr(), serde_int8, "TINYINT.csv");
        test_func(column_int16->get_ptr(), serde_int16, "SMALLINT.csv");
        test_func(column_int32->get_ptr(), serde_int32, "INT.csv");
        test_func(column_int64->get_ptr(), serde_int64, "BIGINT.csv");
        test_func(column_int128->get_ptr(), serde_int128, "LARGEINT.csv");

        test_func(column_uint8->get_ptr(), serde_uint8, "TINYINT_UNSIGNED.csv");
    }
};
TEST_F(DataTypeNumberSerDeTest, serdes) {
    auto test_func = [](const auto& serde, const auto& source_column) {
        using SerdeType = decltype(serde);
        using ColumnType = typename std::remove_reference<SerdeType>::type::ColumnType;

        auto row_count = source_column->size();
        auto option = DataTypeSerDe::FormatOptions();
        char field_delim = ';';
        option.field_delim = std::string(1, field_delim);

        {
            auto ser_col = ColumnString::create();
            ser_col->reserve(row_count);
            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());

            VectorBufferWriter buffer_writer(*ser_col.get());
            for (size_t j = 0; j != row_count; ++j) {
                auto st =
                        serde.serialize_one_cell_to_json(*source_column, j, buffer_writer, option);
                EXPECT_TRUE(st.ok()) << "Failed to serialize column at row " << j << ": " << st;

                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                Slice slice {actual_str_value.data(), actual_str_value.size()};
                st = serde.deserialize_one_cell_from_json(*deser_column, slice, option);
                EXPECT_TRUE(st.ok()) << "Failed to deserialize column at row " << j << ": " << st;
                if constexpr (std::is_same_v<ColumnType, ColumnFloat32> ||
                              std::is_same_v<ColumnType, ColumnFloat64>) {
                    // for float and double, we need to check the value with a tolerance
                    auto expected_value = source_column->get_element(j);
                    auto actual_value = deser_col_with_type->get_element(j);
                    if (std::isnan(expected_value)) {
                        EXPECT_TRUE(std::isnan(actual_value))
                                << "Row " << j << " value mismatch: expected NaN, got "
                                << actual_value;
                    } else if (std::isinf(expected_value)) {
                        EXPECT_EQ(actual_value, expected_value);
                    } else {
                        EXPECT_NEAR(actual_value, expected_value, 0.00001)
                                << "Row " << j << " value mismatch: expected " << expected_value
                                << ", got " << actual_value;
                    }
                } else {
                    EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
                }
            }
        }

        // test serialize_column_to_json
        {
            auto ser_col = ColumnString::create();
            ser_col->reserve(row_count);

            VectorBufferWriter buffer_writer(*ser_col.get());
            auto st = serde.serialize_column_to_json(*source_column, 0, source_column->size(),
                                                     buffer_writer, option);
            EXPECT_TRUE(st.ok()) << "Failed to serialize column to json: " << st;
            buffer_writer.commit();

            std::string json_data((char*)ser_col->get_chars().data(), ser_col->get_chars().size());
            std::vector<std::string> strs = doris::split(json_data, std::string(1, field_delim));
            std::vector<Slice> slices;
            for (const auto& s : strs) {
                Slice tmp_slice(s.data(), s.size());
                tmp_slice.trim_prefix();
                slices.emplace_back(tmp_slice);
            }

            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());
            uint64_t num_deserialized = 0;
            st = serde.deserialize_column_from_json_vector(*deser_column, slices, &num_deserialized,
                                                           option);
            EXPECT_TRUE(st.ok()) << "Failed to deserialize column from json: " << st;
            EXPECT_EQ(num_deserialized, row_count);
            for (size_t j = 0; j != row_count; ++j) {
                if constexpr (std::is_same_v<ColumnType, ColumnFloat32> ||
                              std::is_same_v<ColumnType, ColumnFloat64>) {
                    // for float and double, we need to check the value with a tolerance
                    auto expected_value = source_column->get_element(j);
                    auto actual_value = deser_col_with_type->get_element(j);
                    if (std::isnan(expected_value)) {
                        EXPECT_TRUE(std::isnan(actual_value))
                                << "Row " << j << " value mismatch: expected NaN, got "
                                << actual_value;
                    } else if (std::isinf(expected_value)) {
                        EXPECT_TRUE(std::isinf(actual_value))
                                << "Row " << j << " value mismatch: expected inf, got "
                                << actual_value;
                    } else {
                        // EXPECT_EQ(actual_value, expected_value);
                        EXPECT_NEAR(actual_value, expected_value, 0.00001)
                                << "Row " << j << " value mismatch: expected " << expected_value
                                << ", got " << actual_value;
                    }
                } else {
                    EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
                }
            }
        }

        {
            // test write_column_to_pb/read_column_from_pb
            PValues pv = PValues();
            Status st = serde.write_column_to_pb(*source_column, pv, 0, row_count);
            EXPECT_TRUE(st.ok()) << "Failed to write column to pb: " << st;

            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());
            st = serde.read_column_from_pb(*deser_column, pv);
            EXPECT_TRUE(st.ok()) << "Failed to read column from pb: " << st;
            for (size_t j = 0; j != row_count; ++j) {
                if constexpr (std::is_same_v<ColumnType, ColumnFloat32> ||
                              std::is_same_v<ColumnType, ColumnFloat64>) {
                    // for float and double, we need to check the value with a tolerance
                    auto expected_value = source_column->get_element(j);
                    auto actual_value = deser_col_with_type->get_element(j);
                    if (std::isnan(expected_value)) {
                        EXPECT_TRUE(std::isnan(actual_value))
                                << "Row " << j << " value mismatch: expected NaN, got "
                                << actual_value;
                    } else {
                        EXPECT_EQ(actual_value, expected_value);
                    }
                } else {
                    EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
                }
            }
        }
        {
            // test write_one_cell_to_jsonb/read_one_cell_from_jsonb
            JsonbWriterT<JsonbOutStream> jsonb_writer;
            jsonb_writer.writeStartObject();
            Arena pool;

            for (size_t j = 0; j != row_count; ++j) {
                serde.write_one_cell_to_jsonb(*source_column, jsonb_writer, pool, 0, j);
            }
            jsonb_writer.writeEndObject();

            auto ser_col = ColumnString::create();
            ser_col->reserve(row_count);
            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());
            JsonbDocument* pdoc = nullptr;
            auto st = JsonbDocument::checkAndCreateDocument(jsonb_writer.getOutput()->getBuffer(),
                                                            jsonb_writer.getOutput()->getSize(),
                                                            &pdoc);
            ASSERT_TRUE(st.ok()) << "checkAndCreateDocument failed: " << st.to_string();
            JsonbDocument& doc = *pdoc;
            for (auto it = doc->begin(); it != doc->end(); ++it) {
                serde.read_one_cell_from_jsonb(*deser_column, it->value());
            }
            for (size_t j = 0; j != row_count; ++j) {
                if constexpr (std::is_same_v<ColumnType, ColumnFloat32> ||
                              std::is_same_v<ColumnType, ColumnFloat64>) {
                    // for float and double, we need to check the value with a tolerance
                    auto expected_value = source_column->get_element(j);
                    auto actual_value = deser_col_with_type->get_element(j);
                    if (std::isnan(expected_value)) {
                        EXPECT_TRUE(std::isnan(actual_value))
                                << "Row " << j << " value mismatch: expected NaN, got "
                                << actual_value;
                    } else {
                        EXPECT_EQ(actual_value, expected_value);
                    }
                } else {
                    EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
                }
            }
        }
        {
            // test write_column_to_mysql
            MysqlRowBuffer<false> mysql_rb;
            for (int row_idx = 0; row_idx < row_count; ++row_idx) {
                auto st = serde.write_column_to_mysql(*source_column, mysql_rb, row_idx, false,
                                                      option);
                EXPECT_TRUE(st.ok()) << "Failed to write column to mysql: " << st;
            }
        }
    };
    test_func(*serde_float32, column_float32);
    test_func(*serde_float64, column_float64);
    test_func(*serde_int8, column_int8);
    test_func(*serde_int16, column_int16);
    test_func(*serde_int32, column_int32);
    test_func(*serde_int64, column_int64);
    test_func(*serde_int128, column_int128);
    test_func(*serde_uint8, column_uint8);
}
} // namespace doris::vectorized