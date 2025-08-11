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

#include <cstddef>
#include <iostream>
#include <type_traits>

#include "testutil/test_util.h"
#include "util/slice.h"
#include "util/string_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_decimal.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_decimal_serde.h"

namespace doris::vectorized {
static std::string test_data_dir;

static auto serde_decimal32_1 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMAL32>>(1, 0);
static auto serde_decimal32_2 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMAL32>>(1, 1);
static auto serde_decimal32_3 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMAL32>>(8, 3);
static auto serde_decimal32_4 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMAL32>>(9, 0);
static auto serde_decimal32_5 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMAL32>>(9, 9);

static auto serde_decimal64_1 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMAL64>>(18, 0);
static auto serde_decimal64_2 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMAL64>>(18, 9);
static auto serde_decimal64_3 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMAL64>>(18, 18);

static auto serde_decimal128v2 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMALV2>>(27, 9);

static auto serde_decimal128v3_1 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMAL128I>>(38, 0);
static auto serde_decimal128v3_2 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMAL128I>>(38, 30);
static auto serde_decimal128v3_3 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMAL128I>>(38, 38);

static auto serde_decimal256_1 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMAL256>>(76, 0);
static auto serde_decimal256_2 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMAL256>>(76, 38);
static auto serde_decimal256_3 = std::make_shared<DataTypeDecimalSerDe<TYPE_DECIMAL256>>(76, 76);

static ColumnDecimal32::MutablePtr column_decimal32_1; // decimal32(1,0)
static ColumnDecimal32::MutablePtr column_decimal32_2; // decimal32(1,1)
static ColumnDecimal32::MutablePtr column_decimal32_3; // decimal32(8,3)
static ColumnDecimal32::MutablePtr column_decimal32_4; // decimal32(9,0)
static ColumnDecimal32::MutablePtr column_decimal32_5; // decimal32(9,9)

static ColumnDecimal64::MutablePtr column_decimal64_1; // decimal64(18,0)
static ColumnDecimal64::MutablePtr column_decimal64_2; // decimal64(18,9)
static ColumnDecimal64::MutablePtr column_decimal64_3; // decimal64(18,18)

static ColumnDecimal128V2::MutablePtr column_decimal128_v2;

static ColumnDecimal128V3::MutablePtr column_decimal128v3_1; // decimal128(38,0)
static ColumnDecimal128V3::MutablePtr column_decimal128v3_2; // decimal128(38,30)
static ColumnDecimal128V3::MutablePtr column_decimal128v3_3; // decimal128(38,38)

static ColumnDecimal256::MutablePtr column_decimal256_1; // decimal256(76,0)
static ColumnDecimal256::MutablePtr column_decimal256_2; // decimal256(76,38)
static ColumnDecimal256::MutablePtr column_decimal256_3; // decimal256(76,76)
class DataTypeDecimalSerDeTest : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";

        column_decimal32_1 = ColumnDecimal32::create(0, 0);
        column_decimal32_2 = ColumnDecimal32::create(0, 1);
        column_decimal32_3 = ColumnDecimal32::create(0, 3);
        column_decimal32_4 = ColumnDecimal32::create(0, 0);
        column_decimal32_5 = ColumnDecimal32::create(0, 9);

        column_decimal64_1 = ColumnDecimal64::create(0, 0);
        column_decimal64_2 = ColumnDecimal64::create(0, 9);
        column_decimal64_3 = ColumnDecimal64::create(0, 18);

        column_decimal128_v2 = ColumnDecimal128V2::create(0, 9);

        column_decimal128v3_1 = ColumnDecimal128V3::create(0, 0);
        column_decimal128v3_2 = ColumnDecimal128V3::create(0, 30);
        column_decimal128v3_3 = ColumnDecimal128V3::create(0, 38);

        column_decimal256_1 = ColumnDecimal256::create(0, 0);
        column_decimal256_2 = ColumnDecimal256::create(0, 38);
        column_decimal256_3 = ColumnDecimal256::create(0, 76);

        load_columns_data();
    }
    static void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        auto load_data_func = [](auto& serde, auto& column, const std::string& data_file) {
            MutableColumns columns;
            columns.push_back(column->get_ptr());
            DataTypeSerDeSPtrs serdes = {serde};
            load_columns_data_from_file(columns, serdes, ';', {0}, data_file);
            EXPECT_TRUE(!column->empty());
        };
        load_data_func(serde_decimal128v2, column_decimal128_v2,
                       test_data_dir + "/DECIMALV2(27,9).csv");

        load_data_func(serde_decimal32_1, column_decimal32_1,
                       test_data_dir + "/DECIMALV3(1,0).csv");
        load_data_func(serde_decimal32_2, column_decimal32_2,
                       test_data_dir + "/DECIMALV3(1,1).csv");
        load_data_func(serde_decimal32_3, column_decimal32_3,
                       test_data_dir + "/DECIMALV3(8,3).csv");
        load_data_func(serde_decimal32_4, column_decimal32_4,
                       test_data_dir + "/DECIMALV3(9,0).csv");
        load_data_func(serde_decimal32_5, column_decimal32_5,
                       test_data_dir + "/DECIMALV3(9,9).csv");

        load_data_func(serde_decimal64_1, column_decimal64_1,
                       test_data_dir + "/DECIMALV3(18,0).csv");
        load_data_func(serde_decimal64_2, column_decimal64_2,
                       test_data_dir + "/DECIMALV3(18,9).csv");
        load_data_func(serde_decimal64_3, column_decimal64_3,
                       test_data_dir + "/DECIMALV3(18,18).csv");

        load_data_func(serde_decimal128v3_1, column_decimal128v3_1,
                       test_data_dir + "/DECIMALV3(38,0).csv");
        load_data_func(serde_decimal128v3_2, column_decimal128v3_2,
                       test_data_dir + "/DECIMALV3(38,30).csv");
        load_data_func(serde_decimal128v3_3, column_decimal128v3_3,
                       test_data_dir + "/DECIMALV3(38,38).csv");

        load_data_func(serde_decimal256_1, column_decimal256_1,
                       test_data_dir + "/DECIMALV3(76,0).csv");
        load_data_func(serde_decimal256_2, column_decimal256_2,
                       test_data_dir + "/DECIMALV3(76,38).csv");
        load_data_func(serde_decimal256_3, column_decimal256_3,
                       test_data_dir + "/DECIMALV3(76,76).csv");

        std::cout << "loading test dataset done" << std::endl;
    }
    void SetUp() override { helper = std::make_unique<CommonDataTypeTest>(); }
    std::unique_ptr<CommonDataTypeTest> helper;
};

TEST_F(DataTypeDecimalSerDeTest, serdes) {
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
                EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
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
                EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
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
                EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
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
            EXPECT_TRUE(st.ok()) << "Failed to create JsonbDocument: " << st;
            JsonbDocument& doc = *pdoc;
            for (auto it = doc->begin(); it != doc->end(); ++it) {
                serde.read_one_cell_from_jsonb(*deser_column, it->value());
            }
            for (size_t j = 0; j != row_count; ++j) {
                EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
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
    test_func(*serde_decimal32_1, column_decimal32_1);
    test_func(*serde_decimal32_2, column_decimal32_2);
    test_func(*serde_decimal32_3, column_decimal32_3);
    test_func(*serde_decimal32_4, column_decimal32_4);
    test_func(*serde_decimal32_5, column_decimal32_5);

    test_func(*serde_decimal64_1, column_decimal64_1);
    test_func(*serde_decimal64_2, column_decimal64_2);
    test_func(*serde_decimal64_3, column_decimal64_3);

    test_func(*serde_decimal128v3_1, column_decimal128v3_1);
    test_func(*serde_decimal128v3_2, column_decimal128v3_2);
    test_func(*serde_decimal128v3_3, column_decimal128v3_3);

    test_func(*serde_decimal256_1, column_decimal256_1);
    test_func(*serde_decimal256_2, column_decimal256_2);
    test_func(*serde_decimal256_3, column_decimal256_3);

    test_func(*serde_decimal128v2, column_decimal128_v2);
}
} // namespace doris::vectorized