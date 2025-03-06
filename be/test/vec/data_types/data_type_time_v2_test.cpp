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

#include "vec/data_types/data_type_time_v2.h"

#include <gen_cpp/Types_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <streamvbyte.h>

#include <cstddef>
#include <iostream>
#include <limits>
#include <type_traits>

#include "agent/be_exec_version_manager.h"
#include "gutil/integral_types.h"
#include "testutil/test_util.h"
#include "vec/columns/column.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"

namespace doris::vectorized {
static std::string test_data_dir;

static DataTypeDateV2 dt_date_v2;
static DataTypeDateTimeV2 dt_datetime_v2_0(0);
static DataTypeDateTimeV2 dt_datetime_v2_5(5);
static DataTypeDateTimeV2 dt_datetime_v2_6(6);

// static ColumnDateTime::MutablePtr column_datetime;
// static ColumnDate::MutablePtr column_date;
static ColumnDateTimeV2::MutablePtr column_datetime_v2_0;
static ColumnDateTimeV2::MutablePtr column_datetime_v2_5;
static ColumnDateTimeV2::MutablePtr column_datetime_v2_6;
static ColumnDateV2::MutablePtr column_date_v2;
class DataTypeDateTimeV2Test : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";

        column_datetime_v2_0 = ColumnDateTimeV2::create();
        column_datetime_v2_5 = ColumnDateTimeV2::create();
        column_datetime_v2_6 = ColumnDateTimeV2::create();
        column_date_v2 = ColumnDateV2::create();

        load_columns_data();
    }
    static void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        auto test_func = [&](const MutableColumnPtr& column, const auto& dt,
                             const std::string& data_file_name) {
            MutableColumns columns;
            columns.push_back(column->get_ptr());
            DataTypeSerDeSPtrs serde = {dt.get_serde()};
            load_columns_data_from_file(columns, serde, ';', {0},
                                        test_data_dir + "/" + data_file_name);
            EXPECT_TRUE(!column->empty());
        };
        test_func(column_datetime_v2_0->get_ptr(), dt_datetime_v2_0, "DATETIMEV2(0).csv");
        test_func(column_datetime_v2_5->get_ptr(), dt_datetime_v2_5, "DATETIMEV2(5).csv");
        test_func(column_datetime_v2_6->get_ptr(), dt_datetime_v2_6, "DATETIMEV2(6).csv");
        test_func(column_date_v2->get_ptr(), dt_date_v2, "DATEV2.csv");

        std::cout << "loading test dataset done" << std::endl;
    }
    void SetUp() override { helper = std::make_unique<CommonDataTypeTest>(); }
    std::unique_ptr<CommonDataTypeTest> helper;
};
TEST_F(DataTypeDateTimeV2Test, simple_func_test) {
    auto test_func = [](auto& dt) {
        using DataType = decltype(dt);
        using FieldType = typename std::remove_reference<DataType>::type::FieldType;
        EXPECT_FALSE(dt.have_subtypes());
        EXPECT_TRUE(dt.should_align_right_in_pretty_formats());
        EXPECT_TRUE(dt.text_can_contain_only_valid_utf8());
        EXPECT_TRUE(dt.is_comparable());
        EXPECT_TRUE(dt.is_value_represented_by_number());
        EXPECT_TRUE(dt.is_value_unambiguously_represented_in_contiguous_memory_region());
        EXPECT_TRUE(dt.have_maximum_size_of_value());
        EXPECT_EQ(dt.get_size_of_value_in_memory(), sizeof(FieldType));
        EXPECT_TRUE(dt.can_be_inside_low_cardinality());

        EXPECT_FALSE(dt.is_null_literal());
        dt.set_null_literal(true);
        EXPECT_TRUE(dt.is_null_literal());
        dt.set_null_literal(false);

        EXPECT_TRUE(dt.equals(dt));
    };
    test_func(dt_datetime_v2_0);
    test_func(dt_datetime_v2_5);
    test_func(dt_datetime_v2_6);
    test_func(dt_date_v2);

    EXPECT_THROW(DataTypeDateTimeV2(7), Exception);
    EXPECT_THROW(create_datetimev2(7), Exception);
}
TEST_F(DataTypeDateTimeV2Test, get_default) {
    EXPECT_EQ(dt_datetime_v2_0.get_default(), 0UL);
    EXPECT_EQ(dt_datetime_v2_5.get_default(), 0UL);
    EXPECT_EQ(dt_datetime_v2_6.get_default(), 0UL);
    EXPECT_EQ(dt_date_v2.get_default(), 0UL);
}
TEST_F(DataTypeDateTimeV2Test, get_field) {
    {
        TExprNode expr_node;
        expr_node.date_literal.value = "abc";
        EXPECT_THROW(dt_date_v2.get_field(expr_node), Exception);
    }
    {
        TExprNode expr_node;
        expr_node.date_literal.value = "abc";
        EXPECT_THROW(dt_datetime_v2_0.get_field(expr_node), Exception);
    }
    {
        TExprNode expr_node;
        expr_node.date_literal.value = "2021-01-01";
        auto field = dt_date_v2.get_field(expr_node);
        auto int_value = field.get<UInt32>();
        std::cout << "field: " << int_value << std::endl;
        DateV2Value<DateV2ValueType> date_value =
                binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(int_value);

        EXPECT_EQ(date_value.year(), 2021);
        EXPECT_EQ(date_value.month(), 1);
        EXPECT_EQ(date_value.day(), 1);
    }
    {
        TExprNode expr_node;
        expr_node.date_literal.value = "2021-12-31 12:23:34";
        auto field = dt_datetime_v2_0.get_field(expr_node);
        auto int_value = field.get<UInt64>();
        std::cout << "field: " << int_value << std::endl;
        DateV2Value<DateTimeV2ValueType> date_value =
                binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(int_value);

        EXPECT_EQ(date_value.year(), 2021);
        EXPECT_EQ(date_value.month(), 12);
        EXPECT_EQ(date_value.day(), 31);
        EXPECT_EQ(date_value.hour(), 12);
        EXPECT_EQ(date_value.minute(), 23);
        EXPECT_EQ(date_value.second(), 34);
    }
    {
        TExprNode expr_node;
        TTypeNode type_node;
        type_node.scalar_type.scale = 5;
        expr_node.type.types.push_back(type_node);
        expr_node.date_literal.value = "2021-12-31 12:23:34.12345";
        auto field = dt_datetime_v2_5.get_field(expr_node);
        auto int_value = field.get<UInt64>();
        std::cout << "field: " << int_value << std::endl;
        DateV2Value<DateTimeV2ValueType> date_value =
                binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(int_value);

        EXPECT_EQ(date_value.year(), 2021);
        EXPECT_EQ(date_value.month(), 12);
        EXPECT_EQ(date_value.day(), 31);
        EXPECT_EQ(date_value.hour(), 12);
        EXPECT_EQ(date_value.minute(), 23);
        EXPECT_EQ(date_value.second(), 34);
        EXPECT_EQ(date_value.microsecond(), 123450);
    }
}
TEST_F(DataTypeDateTimeV2Test, ser_deser) {
    auto test_func = [](auto& dt, const auto& column, int be_exec_version) {
        std::cout << "test serialize/deserialize datatype " << dt.get_family_name()
                  << ", be ver: " << be_exec_version << std::endl;
        using DataType = decltype(dt);
        using ColumnType = typename std::remove_reference<DataType>::type::ColumnType;
        auto tmp_col = dt.create_column();
        auto* col_with_type = assert_cast<ColumnType*>(tmp_col.get());

        size_t count = 0;
        col_with_type->clear();
        col_with_type->insert_many_vals(1, count);
        auto expected_data_size = sizeof(typename ColumnType::value_type) * count;
        // binary: const flag| row num | real saved num| data
        auto content_uncompressed_size =
                dt.get_uncompressed_serialized_bytes(*tmp_col, be_exec_version);
        if (be_exec_version >= USE_CONST_SERDE) {
            EXPECT_EQ(content_uncompressed_size, 17 + expected_data_size);
        } else {
            EXPECT_EQ(content_uncompressed_size, 4 + expected_data_size);
        }
        {
            std::string column_values;
            column_values.resize(content_uncompressed_size);
            char* buf = column_values.data();
            buf = dt.serialize(*tmp_col, buf, be_exec_version);
            const size_t serialize_bytes = buf - column_values.data() + STREAMVBYTE_PADDING;
            column_values.resize(serialize_bytes);

            MutableColumnPtr deser_column = dt.create_column();
            (void)dt.deserialize(column_values.data(), &deser_column, be_exec_version);
            EXPECT_EQ(deser_column->size(), 0);
        }

        count = 1;
        col_with_type->clear();
        col_with_type->insert_many_vals(1, count);
        expected_data_size = sizeof(typename ColumnType::value_type) * count;
        content_uncompressed_size = dt.get_uncompressed_serialized_bytes(*tmp_col, be_exec_version);
        if (be_exec_version >= USE_CONST_SERDE) {
            EXPECT_EQ(content_uncompressed_size, 17 + expected_data_size);
        } else {
            EXPECT_EQ(content_uncompressed_size, 4 + expected_data_size);
        }
        {
            std::string column_values;
            column_values.resize(content_uncompressed_size);
            char* buf = column_values.data();
            buf = dt.serialize(*tmp_col, buf, be_exec_version);
            const size_t serialize_bytes = buf - column_values.data() + STREAMVBYTE_PADDING;
            column_values.resize(serialize_bytes);

            MutableColumnPtr deser_column = dt.create_column();
            (void)dt.deserialize(column_values.data(), &deser_column, be_exec_version);
            EXPECT_EQ(deser_column->size(), count);
            for (size_t i = 0; i != count; ++i) {
                EXPECT_EQ(deser_column->get_data_at(i), tmp_col->get_data_at(i));
            }
        }

        count = SERIALIZED_MEM_SIZE_LIMIT + 1;
        col_with_type->clear();
        col_with_type->insert_many_vals(1, count);
        content_uncompressed_size = dt.get_uncompressed_serialized_bytes(*tmp_col, be_exec_version);
        expected_data_size = sizeof(typename ColumnType::value_type) * count;
        if (be_exec_version >= USE_CONST_SERDE) {
            EXPECT_EQ(content_uncompressed_size,
                      17 + 8 +
                              std::max(expected_data_size,
                                       streamvbyte_max_compressedbytes(
                                               cast_set<UInt32>(upper_int32(expected_data_size)))));
        } else {
            EXPECT_EQ(content_uncompressed_size,
                      12 + std::max(expected_data_size,
                                    streamvbyte_max_compressedbytes(
                                            cast_set<UInt32>(upper_int32(expected_data_size)))));
        }
        {
            std::string column_values;
            column_values.resize(content_uncompressed_size);
            char* buf = column_values.data();
            buf = dt.serialize(*tmp_col, buf, be_exec_version);
            const size_t serialize_bytes = buf - column_values.data() + STREAMVBYTE_PADDING;
            column_values.resize(serialize_bytes);

            MutableColumnPtr deser_column = dt.create_column();
            (void)dt.deserialize(column_values.data(), &deser_column, be_exec_version);
            EXPECT_EQ(deser_column->size(), count);
            for (size_t i = 0; i != count; ++i) {
                EXPECT_EQ(deser_column->get_data_at(i), tmp_col->get_data_at(i));
            }
        }

        {
            content_uncompressed_size =
                    dt.get_uncompressed_serialized_bytes(column, be_exec_version);
            std::string column_values;
            column_values.resize(content_uncompressed_size);
            char* buf = column_values.data();
            buf = dt.serialize(column, buf, be_exec_version);
            const size_t serialize_bytes = buf - column_values.data() + STREAMVBYTE_PADDING;
            column_values.resize(serialize_bytes);

            MutableColumnPtr deser_column = dt.create_column();
            (void)dt.deserialize(column_values.data(), &deser_column, be_exec_version);
            count = column.size();
            EXPECT_EQ(deser_column->size(), count);
            for (size_t i = 0; i != count; ++i) {
                EXPECT_EQ(deser_column->get_data_at(i), column.get_data_at(i));
            }
        }
    };
    test_func(dt_date_v2, *column_date_v2, USE_CONST_SERDE);
    test_func(dt_date_v2, *column_date_v2, AGGREGATION_2_1_VERSION);

    test_func(dt_datetime_v2_0, *column_datetime_v2_0, USE_CONST_SERDE);
    test_func(dt_datetime_v2_0, *column_datetime_v2_0, AGGREGATION_2_1_VERSION);
    test_func(dt_datetime_v2_5, *column_datetime_v2_5, USE_CONST_SERDE);
    test_func(dt_datetime_v2_5, *column_datetime_v2_5, AGGREGATION_2_1_VERSION);
    test_func(dt_datetime_v2_6, *column_datetime_v2_6, USE_CONST_SERDE);
    test_func(dt_datetime_v2_6, *column_datetime_v2_6, AGGREGATION_2_1_VERSION);
}
TEST_F(DataTypeDateTimeV2Test, to_string) {
    auto test_func = [](auto& dt, const auto& source_column) {
        std::cout << "test datatype to string: " << dt.get_family_name() << std::endl;
        using DataType = decltype(dt);
        using ColumnType = typename std::remove_reference<DataType>::type::ColumnType;
        const auto* col_with_type = assert_cast<const ColumnType*>(&source_column);

        size_t row_count = source_column.size();
        {
            ColumnString col_str_to_str;
            BufferWritable buffer(col_str_to_str);

            for (size_t i = 0; i != row_count; ++i) {
                dt.to_string(source_column, i, buffer);
                buffer.commit();
            }
            ColumnType col_from_str;
            for (size_t i = 0; i != row_count; ++i) {
                auto item = col_str_to_str.get_data_at(i);
                ReadBuffer rb((char*)item.data, item.size);
                auto status = dt.from_string(rb, &col_from_str);
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(col_from_str.get_element(i), source_column.get_element(i));
            }
        }
        {
            ColumnType col_from_str;
            for (size_t i = 0; i != row_count; ++i) {
                auto str = dt.to_string(source_column, i);
                ReadBuffer rb(str.data(), str.size());
                auto status = dt.from_string(rb, &col_from_str);
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(col_from_str.get_element(i), source_column.get_element(i));
            }
        }
        {
            ColumnType col_from_str;
            for (size_t i = 0; i != row_count; ++i) {
                auto str = dt.to_string(col_with_type->get_element(i));
                ReadBuffer rb(str.data(), str.size());
                auto status = dt.from_string(rb, &col_from_str);
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(col_from_str.get_element(i), source_column.get_element(i));
            }
        }
        // to string batch
        {
            ColumnString col_str_to_str;
            dt.to_string_batch(source_column, col_str_to_str);
            EXPECT_EQ(col_str_to_str.size(), row_count);

            ColumnType col_from_str;
            for (size_t i = 0; i != row_count; ++i) {
                auto item = col_str_to_str.get_data_at(i);
                ReadBuffer rb((char*)item.data, item.size);
                auto status = dt.from_string(rb, &col_from_str);
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(col_from_str.get_element(i), source_column.get_element(i));
            }
        }
    };
    test_func(dt_date_v2, *column_date_v2);
    test_func(dt_datetime_v2_0, *column_datetime_v2_0);
    test_func(dt_datetime_v2_5, *column_datetime_v2_5);
    test_func(dt_datetime_v2_6, *column_datetime_v2_6);
}
} // namespace doris::vectorized