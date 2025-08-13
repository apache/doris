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

#include "vec/data_types/data_type_decimal.h"

#include <fast_float/float_common.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <streamvbyte.h>

#include <cstddef>
#include <iostream>
#include <limits>
#include <type_traits>

#include "common/exception.h"
#include "testutil/test_util.h"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {
static std::string test_data_dir;
static std::string test_result_dir;

static DataTypeDecimal32 dt_decimal32_1(1, 0);
static DataTypeDecimal32 dt_decimal32_2(1, 1);
static DataTypeDecimal32 dt_decimal32_3(8, 3);
static DataTypeDecimal32 dt_decimal32_4(9, 0);
static DataTypeDecimal32 dt_decimal32_5(9, 9);

static DataTypeDecimal64 dt_decimal64_1(18, 0);
static DataTypeDecimal64 dt_decimal64_2(18, 9);
static DataTypeDecimal64 dt_decimal64_3(18, 18);

static DataTypeDecimalV2 dt_decimal128v2(27, 9);

static DataTypeDecimal128 dt_decimal128v3_1(38, 0);
static DataTypeDecimal128 dt_decimal128v3_2(38, 30);
static DataTypeDecimal128 dt_decimal128v3_3(38, 38);

static DataTypeDecimal256 dt_decimal256_1(76, 0);
static DataTypeDecimal256 dt_decimal256_2(76, 38);
static DataTypeDecimal256 dt_decimal256_3(76, 76);

static std::shared_ptr<DataTypeDecimal32> dt_decimal32_1_ptr =
        std::make_shared<DataTypeDecimal32>(1, 0);

static std::shared_ptr<DataTypeDecimal32> dt_decimal32_2_ptr =
        std::make_shared<DataTypeDecimal32>(1, 1);
static std::shared_ptr<DataTypeDecimal32> dt_decimal32_3_ptr =
        std::make_shared<DataTypeDecimal32>(8, 3);
static std::shared_ptr<DataTypeDecimal32> dt_decimal32_4_ptr =
        std::make_shared<DataTypeDecimal32>(9, 0);
static std::shared_ptr<DataTypeDecimal32> dt_decimal32_5_ptr =
        std::make_shared<DataTypeDecimal32>(9, 9);

static std::shared_ptr<DataTypeDecimal64> dt_decimal64_1_ptr =
        std::make_shared<DataTypeDecimal64>(18, 0);
static std::shared_ptr<DataTypeDecimal64> dt_decimal64_2_ptr =
        std::make_shared<DataTypeDecimal64>(18, 9);
static std::shared_ptr<DataTypeDecimal64> dt_decimal64_3_ptr =
        std::make_shared<DataTypeDecimal64>(18, 18);

static std::shared_ptr<DataTypeDecimalV2> dt_decimal128v2_ptr =
        std::make_shared<DataTypeDecimalV2>(27, 9);

static std::shared_ptr<DataTypeDecimal128> dt_decimal128v3_1_ptr =
        std::make_shared<DataTypeDecimal128>(38, 0);
static std::shared_ptr<DataTypeDecimal128> dt_decimal128v3_2_ptr =
        std::make_shared<DataTypeDecimal128>(38, 30);
static std::shared_ptr<DataTypeDecimal128> dt_decimal128v3_3_ptr =
        std::make_shared<DataTypeDecimal128>(38, 38);

static std::shared_ptr<DataTypeDecimal256> dt_decimal256_1_ptr =
        std::make_shared<DataTypeDecimal256>(76, 0);
static std::shared_ptr<DataTypeDecimal256> dt_decimal256_2_ptr =
        std::make_shared<DataTypeDecimal256>(76, 38);
static std::shared_ptr<DataTypeDecimal256> dt_decimal256_3_ptr =
        std::make_shared<DataTypeDecimal256>(76, 76);

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
class DataTypeDecimalTest : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";
        test_result_dir = root_dir + "/be/test/expected_result/vec/data_types";

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
        auto load_data_func = [](auto& dt, auto& column, const std::string& data_file) {
            MutableColumns columns;
            columns.push_back(column->get_ptr());
            DataTypeSerDeSPtrs serde = {dt.get_serde()};
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
            EXPECT_TRUE(!column->empty());
        };
        load_data_func(dt_decimal128v2, column_decimal128_v2,
                       test_data_dir + "/DECIMALV2(27,9).csv");
        load_data_func(dt_decimal32_1, column_decimal32_1, test_data_dir + "/DECIMALV3(1,0).csv");
        load_data_func(dt_decimal32_2, column_decimal32_2, test_data_dir + "/DECIMALV3(1,1).csv");
        load_data_func(dt_decimal32_3, column_decimal32_3, test_data_dir + "/DECIMALV3(8,3).csv");
        load_data_func(dt_decimal32_4, column_decimal32_4, test_data_dir + "/DECIMALV3(9,0).csv");
        load_data_func(dt_decimal32_5, column_decimal32_5, test_data_dir + "/DECIMALV3(9,9).csv");

        load_data_func(dt_decimal64_1, column_decimal64_1, test_data_dir + "/DECIMALV3(18,0).csv");
        load_data_func(dt_decimal64_2, column_decimal64_2, test_data_dir + "/DECIMALV3(18,9).csv");
        load_data_func(dt_decimal64_3, column_decimal64_3, test_data_dir + "/DECIMALV3(18,18).csv");

        load_data_func(dt_decimal128v3_1, column_decimal128v3_1,
                       test_data_dir + "/DECIMALV3(38,0).csv");
        load_data_func(dt_decimal128v3_2, column_decimal128v3_2,
                       test_data_dir + "/DECIMALV3(38,30).csv");
        load_data_func(dt_decimal128v3_3, column_decimal128v3_3,
                       test_data_dir + "/DECIMALV3(38,38).csv");

        load_data_func(dt_decimal256_1, column_decimal256_1,
                       test_data_dir + "/DECIMALV3(76,0).csv");
        load_data_func(dt_decimal256_2, column_decimal256_2,
                       test_data_dir + "/DECIMALV3(76,38).csv");
        load_data_func(dt_decimal256_3, column_decimal256_3,
                       test_data_dir + "/DECIMALV3(76,76).csv");

        std::cout << "loading test dataset done" << std::endl;
    }
    void SetUp() override { helper = std::make_unique<CommonDataTypeTest>(); }
    std::unique_ptr<CommonDataTypeTest> helper;
};
TEST_F(DataTypeDecimalTest, MetaInfoTest) {
    auto tmp_dt = DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_DECIMAL32,
                                                               1, 0);
    auto type_descriptor = DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_DECIMAL32, false, 1, 0);
    auto col_meta = std::make_shared<PColumnMeta>();
    col_meta->set_type(PGenericType_TypeId_DECIMAL32);
    CommonDataTypeTest::DataTypeMetaInfo meta_info_to_assert = {
            .type_id = PrimitiveType::TYPE_DECIMAL32,
            .type_as_type_descriptor = type_descriptor,
            .family_name = tmp_dt->get_family_name(),
            .has_subtypes = false,
            .storage_field_type = doris::FieldType::OLAP_FIELD_TYPE_DECIMAL32,
            .should_align_right_in_pretty_formats = true,
            .text_can_contain_only_valid_utf8 = true,
            .have_maximum_size_of_value = true,
            .size_of_value_in_memory = sizeof(Decimal32),
            .precision = tmp_dt->get_precision(),
            .scale = tmp_dt->get_scale(),
            .is_null_literal = false,
            .is_value_represented_by_number = true,
            .pColumnMeta = col_meta.get(),
            .is_value_unambiguously_represented_in_contiguous_memory_region = true,
            .default_field = Field::create_field<TYPE_DECIMAL32>(
                    DecimalField<Decimal32>(0, tmp_dt->get_scale())),
    };
    helper->meta_info_assert(tmp_dt, meta_info_to_assert);
}
TEST_F(DataTypeDecimalTest, ctor) {
    // check invalid precision: 0
    EXPECT_THROW(DataTypeDecimal32(0, 10), Exception);
    EXPECT_THROW(DataTypeDecimal64(0, 10), Exception);
    EXPECT_THROW(DataTypeDecimalV2(0, 10), Exception);
    EXPECT_THROW(DataTypeDecimal128(0, 10), Exception);
    EXPECT_THROW(DataTypeDecimal256(0, 10), Exception);

    // check invalid precision: bigger than max precision
    EXPECT_THROW(DataTypeDecimal32(10, 10), Exception);
    EXPECT_THROW(DataTypeDecimal64(19, 10), Exception);
    EXPECT_THROW(DataTypeDecimalV2(28, 10), Exception);
    EXPECT_THROW(DataTypeDecimal128(39, 10), Exception);
    EXPECT_THROW(DataTypeDecimal256(77, 10), Exception);

    // check invalid scale: bigger than precision
    EXPECT_THROW(DataTypeDecimal32(9, 10), Exception);
    EXPECT_THROW(DataTypeDecimal64(18, 19), Exception);
    EXPECT_THROW(DataTypeDecimalV2(27, 28), Exception);
    EXPECT_THROW(DataTypeDecimal128(38, 39), Exception);
    EXPECT_THROW(DataTypeDecimal256(76, 77), Exception);

    // check scale > precision
    EXPECT_THROW(DataTypeDecimal32(8, 9), Exception);
    EXPECT_THROW(DataTypeDecimal64(17, 18), Exception);
    EXPECT_THROW(DataTypeDecimalV2(26, 27), Exception);
    EXPECT_THROW(DataTypeDecimal128(37, 38), Exception);
    EXPECT_THROW(DataTypeDecimal256(75, 76), Exception);

    // decimal v2
    EXPECT_THROW(DataTypeDecimalV2(27, 9, 28, 9), Exception);
    EXPECT_THROW(DataTypeDecimalV2(27, 9, 27, 28), Exception);
    EXPECT_THROW(DataTypeDecimalV2(27, 9, 26, 27), Exception);
}
TEST_F(DataTypeDecimalTest, simple_func_test) {
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
        EXPECT_FALSE(dt.can_be_inside_low_cardinality());

        EXPECT_FALSE(dt.is_null_literal());

        EXPECT_TRUE(dt.equals(dt));

        EXPECT_EQ(dt.do_get_name(), "Decimal(" + std::to_string(dt.get_precision()) + ", " +
                                            std::to_string(dt.get_scale()) + ")");
        EXPECT_EQ(dt.get_format_scale(), dt.get_scale());
        EXPECT_EQ(dt.get_scale_multiplier(), dt.get_scale_multiplier(dt.get_scale()));
    };
    test_func(dt_decimal32_1);
    EXPECT_EQ(dt_decimal32_1.max_precision(), 9);
    EXPECT_TRUE(dt_decimal32_1.equals(dt_decimal32_1));
    EXPECT_FALSE(dt_decimal32_1.equals(dt_decimal32_2));
    EXPECT_EQ(dt_decimal32_1.get_primitive_type(), TYPE_DECIMAL32);
    test_func(dt_decimal32_2);
    test_func(dt_decimal32_3);
    test_func(dt_decimal32_4);
    test_func(dt_decimal32_5);

    EXPECT_EQ(dt_decimal64_1.max_precision(), 18);
    EXPECT_EQ(dt_decimal64_1.get_primitive_type(), TYPE_DECIMAL64);
    test_func(dt_decimal64_1);
    test_func(dt_decimal64_2);
    test_func(dt_decimal64_3);

    EXPECT_EQ(dt_decimal128v3_1.max_precision(), 38);
    EXPECT_EQ(dt_decimal128v3_1.get_primitive_type(), TYPE_DECIMAL128I);
    test_func(dt_decimal128v3_1);
    test_func(dt_decimal128v3_2);
    test_func(dt_decimal128v3_3);

    EXPECT_EQ(dt_decimal256_1.max_precision(), 76);
    EXPECT_EQ(dt_decimal256_1.get_primitive_type(), TYPE_DECIMAL256);
    test_func(dt_decimal256_1);
    test_func(dt_decimal256_2);
    test_func(dt_decimal256_3);
    EXPECT_FALSE(dt_decimal32_1.equals(dt_decimal256_1));
}
TEST_F(DataTypeDecimalTest, get_type_as_type_descriptor) {
    {
        EXPECT_TRUE(dt_decimal32_1.equals(*DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_DECIMAL32, false, dt_decimal32_1.get_precision(),
                dt_decimal32_1.get_scale())));
    }
    {
        EXPECT_TRUE(dt_decimal64_1.equals(*DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_DECIMAL64, false, dt_decimal64_1.get_precision(),
                dt_decimal64_1.get_scale())));
    }
    {
        EXPECT_TRUE(dt_decimal128v2.equals(*DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_DECIMALV2, false, dt_decimal128v2.get_precision(),
                dt_decimal128v2.get_scale())));
    }
    {
        EXPECT_TRUE(dt_decimal128v3_1.equals(*DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_DECIMAL128I, false, dt_decimal128v3_1.get_precision(),
                dt_decimal128v3_1.get_scale())));
    }
    {
        EXPECT_TRUE(dt_decimal256_1.equals(*DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_DECIMAL256, false, dt_decimal256_1.get_precision(),
                dt_decimal256_1.get_scale())));
    }
}

TEST_F(DataTypeDecimalTest, get_storage_field_type) {
    EXPECT_EQ(dt_decimal32_1.get_storage_field_type(), doris::FieldType::OLAP_FIELD_TYPE_DECIMAL32);
    EXPECT_EQ(dt_decimal64_1.get_storage_field_type(), doris::FieldType::OLAP_FIELD_TYPE_DECIMAL64);
    EXPECT_EQ(dt_decimal128v2.get_storage_field_type(), doris::FieldType::OLAP_FIELD_TYPE_DECIMAL);
    EXPECT_EQ(dt_decimal128v3_1.get_storage_field_type(),
              doris::FieldType::OLAP_FIELD_TYPE_DECIMAL128I);
    EXPECT_EQ(dt_decimal256_1.get_storage_field_type(),
              doris::FieldType::OLAP_FIELD_TYPE_DECIMAL256);
}
TEST_F(DataTypeDecimalTest, ser_deser) {
    auto test_func = [](auto dt, const auto& column, int be_exec_version) {
        std::cout << "test serialize/deserialize datatype " << dt.get_family_name()
                  << ", be ver: " << be_exec_version << std::endl;
        using DataType = decltype(dt);
        using ColumnType = typename DataType::ColumnType;
        auto tmp_col = dt.create_column();
        auto* col_with_type = assert_cast<ColumnType*>(tmp_col.get());

        size_t count = 0;
        col_with_type->clear();
        col_with_type->insert_many_defaults(count);
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
        col_with_type->insert_many_defaults(count);
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
        col_with_type->insert_many_defaults(count);
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
    test_func(dt_decimal32_1, *column_decimal32_1, USE_CONST_SERDE);
    test_func(dt_decimal32_1, *column_decimal32_1, AGGREGATION_2_1_VERSION);
    test_func(dt_decimal32_2, *column_decimal32_2, USE_CONST_SERDE);
    test_func(dt_decimal32_2, *column_decimal32_2, AGGREGATION_2_1_VERSION);
    test_func(dt_decimal32_3, *column_decimal32_3, USE_CONST_SERDE);
    test_func(dt_decimal32_3, *column_decimal32_3, AGGREGATION_2_1_VERSION);
    test_func(dt_decimal32_4, *column_decimal32_4, USE_CONST_SERDE);
    test_func(dt_decimal32_4, *column_decimal32_4, AGGREGATION_2_1_VERSION);
    test_func(dt_decimal32_5, *column_decimal32_5, USE_CONST_SERDE);
    test_func(dt_decimal32_5, *column_decimal32_5, AGGREGATION_2_1_VERSION);

    test_func(dt_decimal64_1, *column_decimal64_1, USE_CONST_SERDE);
    test_func(dt_decimal64_1, *column_decimal64_1, AGGREGATION_2_1_VERSION);
    test_func(dt_decimal64_2, *column_decimal64_2, USE_CONST_SERDE);
    test_func(dt_decimal64_2, *column_decimal64_2, AGGREGATION_2_1_VERSION);
    test_func(dt_decimal64_3, *column_decimal64_3, USE_CONST_SERDE);
    test_func(dt_decimal64_3, *column_decimal64_3, AGGREGATION_2_1_VERSION);

    test_func(dt_decimal128v2, *column_decimal128_v2, USE_CONST_SERDE);
    test_func(dt_decimal128v2, *column_decimal128_v2, AGGREGATION_2_1_VERSION);

    test_func(dt_decimal128v3_1, *column_decimal128v3_1, USE_CONST_SERDE);
    test_func(dt_decimal128v3_1, *column_decimal128v3_1, AGGREGATION_2_1_VERSION);
    test_func(dt_decimal128v3_2, *column_decimal128v3_2, USE_CONST_SERDE);
    test_func(dt_decimal128v3_2, *column_decimal128v3_2, AGGREGATION_2_1_VERSION);
    test_func(dt_decimal128v3_3, *column_decimal128v3_3, USE_CONST_SERDE);
    test_func(dt_decimal128v3_3, *column_decimal128v3_3, AGGREGATION_2_1_VERSION);

    test_func(dt_decimal256_1, *column_decimal256_1, USE_CONST_SERDE);
    test_func(dt_decimal256_1, *column_decimal256_1, AGGREGATION_2_1_VERSION);
    test_func(dt_decimal256_2, *column_decimal256_2, USE_CONST_SERDE);
    test_func(dt_decimal256_2, *column_decimal256_2, AGGREGATION_2_1_VERSION);
    test_func(dt_decimal256_3, *column_decimal256_3, USE_CONST_SERDE);
    test_func(dt_decimal256_3, *column_decimal256_3, AGGREGATION_2_1_VERSION);
}
TEST_F(DataTypeDecimalTest, to_pb_column_meta) {
    auto test_func = [](auto dt, PGenericType_TypeId expected_type) {
        auto col_meta = std::make_shared<PColumnMeta>();
        dt.to_pb_column_meta(col_meta.get());
        EXPECT_EQ(col_meta->type(), expected_type);
        EXPECT_EQ(col_meta->decimal_param().precision(), dt.get_precision());
        EXPECT_EQ(col_meta->decimal_param().scale(), dt.get_scale());
    };
    test_func(dt_decimal32_1, PGenericType::DECIMAL32);
    test_func(dt_decimal64_1, PGenericType::DECIMAL64);
    test_func(dt_decimal128v2, PGenericType::DECIMAL128);
    test_func(dt_decimal128v3_1, PGenericType::DECIMAL128I);
    test_func(dt_decimal256_1, PGenericType::DECIMAL256);
}
TEST_F(DataTypeDecimalTest, get_default) {
    auto test_func = [](auto dt) {
        using DataType = decltype(dt);
        using ColumnType = typename DataType::ColumnType;
        auto default_field = dt.get_default();
        auto decimal_field =
                default_field.template get<DecimalField<typename ColumnType::value_type>>();
        EXPECT_EQ(decimal_field.get_scale(), dt.get_scale());
        EXPECT_EQ(decimal_field.get_value(), typename ColumnType::value_type());
    };
    test_func(dt_decimal32_1);
    test_func(dt_decimal64_1);
    test_func(dt_decimal128v2);
    test_func(dt_decimal128v3_1);
    test_func(dt_decimal256_1);
}
TEST_F(DataTypeDecimalTest, get_field) {
    TExprNode expr_node;
    expr_node.node_type = TExprNodeType::DECIMAL_LITERAL;
    expr_node.__isset.decimal_literal = true;

    std::string line;
    auto test_func = [&](auto dt, const std::string& input_data_file_name) {
        using DataType = decltype(dt);
        using ColumnType = typename DataType::ColumnType;
        {
            expr_node.decimal_literal.value = "abc";
            EXPECT_THROW(dt.get_field(expr_node), Exception);
        }
        auto input_data_file = test_data_dir + "/" + input_data_file_name;
        std::ifstream file(input_data_file);
        if (!file) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "can not open the file: {} ",
                                   input_data_file);
        }

        std::string res_file_name =
                fmt::format("decimalv3_{}_{}_get_field.out", dt.get_precision(), dt.get_scale());
        auto output_file = test_result_dir + "/" + res_file_name;
        std::vector<std::string> res;
        while (std::getline(file, line)) {
            if (fast_float::fastfloat_strncasecmp(line.data(), "null",
                                                  std::min(line.size(), 4UL))) {
                continue;
            }
            expr_node.decimal_literal.value = line;
            auto field = dt.get_field(expr_node);
            auto decimal_field =
                    field.template get<DecimalField<typename ColumnType::value_type>>();
            EXPECT_EQ(decimal_field.get_scale(), dt.get_scale());
            res.push_back(decimal_field.get_value().to_string(dt.get_scale()));
        }
        check_or_generate_res_file(output_file, {res});
    };
    test_func(dt_decimal32_1, "DECIMALV3(1,0).csv");
    test_func(dt_decimal32_2, "DECIMALV3(1,1).csv");
    test_func(dt_decimal32_3, "DECIMALV3(8,3).csv");
    test_func(dt_decimal32_4, "DECIMALV3(9,0).csv");
    test_func(dt_decimal32_5, "DECIMALV3(9,9).csv");

    test_func(dt_decimal64_1, "DECIMALV3(18,0).csv");
    test_func(dt_decimal64_2, "DECIMALV3(18,9).csv");
    test_func(dt_decimal64_3, "DECIMALV3(18,18).csv");

    test_func(dt_decimal128v2, "DECIMALV2(27,9).csv");

    test_func(dt_decimal128v3_1, "DECIMALV3(38,0).csv");
    test_func(dt_decimal128v3_2, "DECIMALV3(38,30).csv");
    test_func(dt_decimal128v3_3, "DECIMALV3(38,38).csv");

    test_func(dt_decimal256_1, "DECIMALV3(76,0).csv");
    test_func(dt_decimal256_2, "DECIMALV3(76,38).csv");
    test_func(dt_decimal256_3, "DECIMALV3(76,76).csv");
}
TEST_F(DataTypeDecimalTest, to_string) {
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
            ColumnType col_from_str(0, dt.get_scale());
            for (size_t i = 0; i != row_count; ++i) {
                auto item = col_str_to_str.get_data_at(i);
                StringRef rb((char*)item.data, item.size);
                auto status = dt.from_string(rb, &col_from_str);
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(col_from_str.get_element(i), source_column.get_element(i));
            }
        }
        {
            ColumnType col_from_str(0, dt.get_scale());
            for (size_t i = 0; i != row_count; ++i) {
                auto str = dt.to_string(source_column, i);
                StringRef rb(str.data(), str.size());
                auto status = dt.from_string(rb, &col_from_str);
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(col_from_str.get_element(i), source_column.get_element(i));
            }
        }
        {
            ColumnType col_from_str(0, dt.get_scale());
            for (size_t i = 0; i != row_count; ++i) {
                auto str = dt.to_string(col_with_type->get_element(i));
                StringRef rb(str.data(), str.size());
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

            ColumnType col_from_str(0, dt.get_scale());
            for (size_t i = 0; i != row_count; ++i) {
                auto item = col_str_to_str.get_data_at(i);
                StringRef rb((char*)item.data, item.size);
                auto status = dt.from_string(rb, &col_from_str);
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(col_from_str.get_element(i), source_column.get_element(i));
            }
        }
    };
    test_func(dt_decimal32_1, *column_decimal32_1);
    test_func(dt_decimal32_2, *column_decimal32_2);
    test_func(dt_decimal32_3, *column_decimal32_3);
    test_func(dt_decimal32_4, *column_decimal32_4);
    test_func(dt_decimal32_5, *column_decimal32_5);

    test_func(dt_decimal64_1, *column_decimal64_1);
    test_func(dt_decimal64_2, *column_decimal64_2);
    test_func(dt_decimal64_3, *column_decimal64_3);

    test_func(dt_decimal128v2, *column_decimal128_v2);
    test_func(dt_decimal128v3_1, *column_decimal128v3_1);
    test_func(dt_decimal128v3_2, *column_decimal128v3_2);
    test_func(dt_decimal128v3_3, *column_decimal128v3_3);

    test_func(dt_decimal256_1, *column_decimal256_1);
    test_func(dt_decimal256_2, *column_decimal256_2);
    test_func(dt_decimal256_3, *column_decimal256_3);
}
TEST_F(DataTypeDecimalTest, scale_factor_for) {
    EXPECT_THROW(dt_decimal32_1.scale_factor_for(dt_decimal32_2), Exception);
    EXPECT_THROW(dt_decimal32_1.scale_factor_for(dt_decimal64_2), Exception);
    EXPECT_THROW(dt_decimal32_1.scale_factor_for(dt_decimal128v3_2), Exception);
    EXPECT_THROW(dt_decimal32_1.scale_factor_for(dt_decimal256_2), Exception);

    EXPECT_THROW(dt_decimal256_1.scale_factor_for(dt_decimal32_2), Exception);
    EXPECT_THROW(dt_decimal256_1.scale_factor_for(dt_decimal64_2), Exception);
    EXPECT_THROW(dt_decimal256_1.scale_factor_for(dt_decimal128v3_2), Exception);
    EXPECT_THROW(dt_decimal256_1.scale_factor_for(dt_decimal256_2), Exception);

    EXPECT_THROW(dt_decimal32_3.scale_factor_for(dt_decimal32_5), Exception);
    EXPECT_THROW(dt_decimal32_3.scale_factor_for(dt_decimal64_2), Exception);

    EXPECT_THROW(dt_decimal128v3_2.scale_factor_for(dt_decimal256_2), Exception);

    EXPECT_EQ(dt_decimal32_3.scale_factor_for(dt_decimal64_1), 1000);
    EXPECT_EQ(dt_decimal32_3.scale_factor_for(dt_decimal256_1), 1000);

    EXPECT_EQ(dt_decimal64_3.scale_factor_for(dt_decimal64_2),
              std::pow(10, dt_decimal64_3.get_scale() - dt_decimal64_2.get_scale()));

    EXPECT_EQ(dt_decimal256_3.scale_factor_for(dt_decimal256_2),
              DataTypeDecimal256::get_scale_multiplier(dt_decimal256_3.get_scale() -
                                                       dt_decimal256_2.get_scale()));
}

TEST_F(DataTypeDecimalTest, decimal_result_type) {
    {
        DataTypeDecimalV2 tmp_dt_decimal128v2(27, 9);
        auto res_type =
                decimal_result_type(dt_decimal128v2, tmp_dt_decimal128v2, true, false, false);
        const auto* result_acutal_type = check_decimal<TYPE_DECIMALV2>(*res_type);
        EXPECT_EQ(result_acutal_type->get_precision(), 27);
        EXPECT_EQ(result_acutal_type->get_scale(), 9);
    }
    {
        {
            // multiply
            auto res_type = decimal_result_type(dt_decimal32_1, dt_decimal32_1, true, false, false);
            const auto* result_acutal_type = check_decimal<TYPE_DECIMAL32>(*res_type);
            EXPECT_EQ(result_acutal_type->get_precision(), dt_decimal32_1.get_precision() * 2);
            EXPECT_EQ(result_acutal_type->get_scale(), dt_decimal32_1.get_scale() * 2);
        }
        {
            // divide
            auto res_type = decimal_result_type(dt_decimal32_1, dt_decimal32_1, false, true, false);
            const auto* result_acutal_type = check_decimal<TYPE_DECIMAL32>(*res_type);
            EXPECT_EQ(result_acutal_type->get_precision(), dt_decimal32_1.get_precision());
            EXPECT_EQ(result_acutal_type->get_scale(), dt_decimal32_1.get_scale());
        }
        {
            // plus and sub
            auto res_type = decimal_result_type(dt_decimal32_1, dt_decimal32_1, false, false, true);
            const auto* result_acutal_type = check_decimal<TYPE_DECIMAL32>(*res_type);
            EXPECT_EQ(result_acutal_type->get_precision(), 2);
            EXPECT_EQ(result_acutal_type->get_scale(), dt_decimal32_1.get_scale());
        }
    }
}
TEST_F(DataTypeDecimalTest, get_decimal_scale) {
    auto test_func = [](auto dt) { EXPECT_EQ(get_decimal_scale(dt), dt.get_scale()); };
    test_func(dt_decimal32_1);
    test_func(dt_decimal32_2);
    test_func(dt_decimal32_3);
    test_func(dt_decimal32_4);
    test_func(dt_decimal32_5);
    test_func(dt_decimal64_1);
    test_func(dt_decimal64_2);
    test_func(dt_decimal64_3);
    test_func(dt_decimal128v2);
    test_func(dt_decimal128v3_1);
    test_func(dt_decimal128v3_2);
    test_func(dt_decimal128v3_3);
    test_func(dt_decimal256_1);
    test_func(dt_decimal256_2);
    test_func(dt_decimal256_3);

    EXPECT_EQ(get_decimal_scale(DataTypeInt32()), 0);
    EXPECT_EQ(get_decimal_scale(DataTypeInt64()), 0);
    EXPECT_EQ(get_decimal_scale(DataTypeInt128()), 0);
    EXPECT_EQ(get_decimal_scale(DataTypeUInt8()), 0);
    EXPECT_EQ(get_decimal_scale(DataTypeFloat32()), 0);
    EXPECT_EQ(get_decimal_scale(DataTypeFloat64()), 0);
}

TEST_F(DataTypeDecimalTest, SerdeArrowTest) {
    auto test_func = [](auto dt, const auto& source_column) {
        MutableColumns columns;
        DataTypes types;
        columns.push_back(source_column->get_ptr());
        types.push_back(dt);
        CommonDataTypeSerdeTest::assert_arrow_format(columns, types);
    };
    test_func(dt_decimal32_1_ptr, column_decimal32_1);
    test_func(dt_decimal32_2_ptr, column_decimal32_2);
    test_func(dt_decimal32_3_ptr, column_decimal32_3);
    test_func(dt_decimal32_4_ptr, column_decimal32_4);
    test_func(dt_decimal32_5_ptr, column_decimal32_5);

    test_func(dt_decimal64_1_ptr, column_decimal64_1);
    test_func(dt_decimal64_2_ptr, column_decimal64_2);
    test_func(dt_decimal64_3_ptr, column_decimal64_3);

    test_func(dt_decimal128v2_ptr, column_decimal128_v2);
    test_func(dt_decimal128v3_1_ptr, column_decimal128v3_1);
    test_func(dt_decimal128v3_2_ptr, column_decimal128v3_2);
    test_func(dt_decimal128v3_3_ptr, column_decimal128v3_3);

    test_func(dt_decimal256_1_ptr, column_decimal256_1);
    test_func(dt_decimal256_2_ptr, column_decimal256_2);
    test_func(dt_decimal256_3_ptr, column_decimal256_3);
}

TEST_F(DataTypeDecimalTest, GetFieldWithDataTypeTest) {
    auto column_decimal128v3_1 = dt_decimal128v3_1.create_column();
    Field field_decimal128v3_1 =
            Field::create_field<TYPE_DECIMAL128I>(DecimalField<Decimal128V3>(1234567890, 0));
    column_decimal128v3_1->insert(field_decimal128v3_1);
    EXPECT_EQ(dt_decimal128v3_1.get_field_with_data_type(*column_decimal128v3_1, 0).field,
              field_decimal128v3_1);
}

} // namespace doris::vectorized