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

#include "vec/data_types/data_type_number.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <streamvbyte.h>

#include <cstddef>
#include <iostream>
#include <limits>
#include <type_traits>

#include "agent/be_exec_version_manager.h"
#include "olap/olap_common.h"
#include "runtime/define_primitive_type.h"
#include "runtime/large_int_value.h"
#include "testutil/test_util.h"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
static std::string test_data_dir;
static std::string test_result_dir;
static DataTypeFloat32 dt_float32;
static DataTypeFloat64 dt_float64;
static DataTypeInt8 dt_int8;
static DataTypeInt16 dt_int16;
static DataTypeInt32 dt_int32;
static DataTypeInt64 dt_int64;
static DataTypeInt128 dt_int128;
static DataTypeUInt8 dt_uint8;

static ColumnFloat32::MutablePtr column_float32;
static ColumnFloat64::MutablePtr column_float64;
static ColumnInt8::MutablePtr column_int8;
static ColumnInt16::MutablePtr column_int16;
static ColumnInt32::MutablePtr column_int32;
static ColumnInt64::MutablePtr column_int64;
static ColumnInt128::MutablePtr column_int128;
static ColumnUInt8::MutablePtr column_uint8;

class DataTypeNumberTest : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";
        test_result_dir = root_dir + "/be/test/expected_result/vec/data_types";

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
        auto test_func = [&](const MutableColumnPtr& column, const auto& dt,
                             const std::string& data_file_name) {
            MutableColumns columns;
            columns.push_back(column->get_ptr());
            DataTypeSerDeSPtrs serde = {dt.get_serde()};
            load_columns_data_from_file(columns, serde, ';', {0},
                                        test_data_dir + "/" + data_file_name);
            EXPECT_TRUE(!column->empty());
        };
        test_func(column_float32->get_ptr(), dt_float32, "FLOAT.csv");
        test_func(column_float64->get_ptr(), dt_float64, "DOUBLE.csv");

        test_func(column_int8->get_ptr(), dt_int8, "TINYINT.csv");
        test_func(column_int16->get_ptr(), dt_int16, "SMALLINT.csv");
        test_func(column_int32->get_ptr(), dt_int32, "INT.csv");
        test_func(column_int64->get_ptr(), dt_int64, "BIGINT.csv");
        test_func(column_int128->get_ptr(), dt_int128, "LARGEINT.csv");

        test_func(column_uint8->get_ptr(), dt_uint8, "TINYINT_UNSIGNED.csv");
    }
    void SetUp() override { helper = std::make_unique<CommonDataTypeTest>(); }
    std::unique_ptr<CommonDataTypeTest> helper;
};

TEST_F(DataTypeNumberTest, MetaInfoTest) {
    auto type_descriptor =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_TINYINT, false);
    auto col_meta = std::make_shared<PColumnMeta>();
    col_meta->set_type(PGenericType_TypeId_INT8);
    CommonDataTypeTest::DataTypeMetaInfo meta_info_to_assert = {
            .type_id = PrimitiveType::TYPE_TINYINT,
            .type_as_type_descriptor = type_descriptor,
            .family_name = dt_int8.get_family_name(),
            .has_subtypes = false,
            .storage_field_type = doris::FieldType::OLAP_FIELD_TYPE_TINYINT,
            .should_align_right_in_pretty_formats = true,
            .text_can_contain_only_valid_utf8 = true,
            .have_maximum_size_of_value = true,
            .size_of_value_in_memory = sizeof(Int8),
            .precision = size_t(-1),
            .scale = size_t(-1),
            .is_null_literal = false,
            .is_value_represented_by_number = true,
            .pColumnMeta = col_meta.get(),
            .is_value_unambiguously_represented_in_contiguous_memory_region = true,
            .default_field = Field::create_field<TYPE_TINYINT>((Int8)0),
    };
    auto tmp_dt = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_TINYINT, false);
    helper->meta_info_assert(tmp_dt, meta_info_to_assert);
}
TEST_F(DataTypeNumberTest, get_type_as_type_descriptor) {
    EXPECT_EQ(dt_int8.get_primitive_type(), PrimitiveType::TYPE_TINYINT);
    EXPECT_EQ(dt_int16.get_primitive_type(), PrimitiveType::TYPE_SMALLINT);
    EXPECT_EQ(dt_int32.get_primitive_type(), PrimitiveType::TYPE_INT);
    EXPECT_EQ(dt_int64.get_primitive_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(dt_int128.get_primitive_type(), PrimitiveType::TYPE_LARGEINT);

    EXPECT_EQ(dt_uint8.get_primitive_type(), PrimitiveType::TYPE_BOOLEAN);
}

TEST_F(DataTypeNumberTest, get_storage_field_type) {
    EXPECT_EQ(dt_int8.get_storage_field_type(), doris::FieldType::OLAP_FIELD_TYPE_TINYINT);
    EXPECT_EQ(dt_int16.get_storage_field_type(), doris::FieldType::OLAP_FIELD_TYPE_SMALLINT);
    EXPECT_EQ(dt_int32.get_storage_field_type(), doris::FieldType::OLAP_FIELD_TYPE_INT);
    EXPECT_EQ(dt_int64.get_storage_field_type(), doris::FieldType::OLAP_FIELD_TYPE_BIGINT);
    EXPECT_EQ(dt_int128.get_storage_field_type(), doris::FieldType::OLAP_FIELD_TYPE_LARGEINT);

    EXPECT_EQ(dt_uint8.get_storage_field_type(), doris::FieldType::OLAP_FIELD_TYPE_BOOL);
}
TEST_F(DataTypeNumberTest, get_default) {
    EXPECT_EQ(dt_int8.get_default(), Field::create_field<TYPE_TINYINT>(0));
    EXPECT_EQ(dt_int16.get_default(), Field::create_field<TYPE_SMALLINT>(0));
    EXPECT_EQ(dt_int32.get_default(), Field::create_field<TYPE_INT>(0));
    EXPECT_EQ(dt_int64.get_default(), Field::create_field<TYPE_BIGINT>(0));
    EXPECT_EQ(dt_int128.get_default(), Field::create_field<TYPE_LARGEINT>(Int128(0)));
}
template <PrimitiveType T>
void test_int_field(const typename PrimitiveTypeTraits<T>::DataType& dt) {
    TExprNode expr_node;
    typename PrimitiveTypeTraits<T>::ColumnItemType value {
            std::numeric_limits<typename PrimitiveTypeTraits<T>::ColumnItemType>::min()};
    expr_node.int_literal.value = value;
    EXPECT_EQ(dt.get_field(expr_node), Field::create_field<T>(value));

    value = std::numeric_limits<typename PrimitiveTypeTraits<T>::ColumnItemType>::max();
    expr_node.int_literal.value = value;
    EXPECT_EQ(dt.get_field(expr_node), Field::create_field<T>(value));

    value = 0;
    expr_node.int_literal.value = value;
    EXPECT_EQ(dt.get_field(expr_node), Field::create_field<T>(value));

    value = -1;
    expr_node.int_literal.value = value;
    EXPECT_EQ(dt.get_field(expr_node), Field::create_field<T>(value));

    value = 1;
    expr_node.int_literal.value = value;
    EXPECT_EQ(dt.get_field(expr_node), Field::create_field<T>(value));
}
TEST_F(DataTypeNumberTest, get_field) {
    std::cout << "get field bool\n";
    {
        TExprNode expr_node;
        expr_node.bool_literal.value = true;
        EXPECT_EQ(dt_uint8.get_field(expr_node), Field::create_field<TYPE_BOOLEAN>(UInt8(1)));
        expr_node.bool_literal.value = false;
        EXPECT_EQ(dt_uint8.get_field(expr_node), Field::create_field<TYPE_BOOLEAN>(UInt8(0)));
    }
    std::cout << "get field int8\n";
    test_int_field<TYPE_TINYINT>(dt_int8);
    std::cout << "get field int16\n";
    test_int_field<TYPE_SMALLINT>(dt_int16);
    std::cout << "get field int32\n";
    test_int_field<TYPE_INT>(dt_int32);
    std::cout << "get field int64\n";
    test_int_field<TYPE_BIGINT>(dt_int64);
    std::cout << "get field int128\n";
    {
        TExprNode expr_node;
        expr_node.large_int_literal.value = "0";
        EXPECT_EQ(dt_int128.get_field(expr_node), Field::create_field<TYPE_LARGEINT>(Int128(0)));
        expr_node.large_int_literal.value = "-1";
        EXPECT_EQ(dt_int128.get_field(expr_node), Field::create_field<TYPE_LARGEINT>(Int128(-1)));
        expr_node.large_int_literal.value = "1";
        EXPECT_EQ(dt_int128.get_field(expr_node), Field::create_field<TYPE_LARGEINT>(Int128(1)));

        // max
        expr_node.large_int_literal.value = "170141183460469231731687303715884105727";
        EXPECT_EQ(dt_int128.get_field(expr_node), Field::create_field<TYPE_LARGEINT>(MAX_INT128));
        // min
        expr_node.large_int_literal.value = "-170141183460469231731687303715884105728";
        EXPECT_EQ(dt_int128.get_field(expr_node), Field::create_field<TYPE_LARGEINT>(MIN_INT128));
    }
    std::cout << "get field float\n";
    {
        TExprNode expr_node;
        Float32 value = 0;
        expr_node.float_literal.value = value;
        EXPECT_EQ(dt_float32.get_field(expr_node), Field::create_field<TYPE_FLOAT>(value));

        value = -1;
        expr_node.float_literal.value = value;
        EXPECT_EQ(dt_float32.get_field(expr_node), Field::create_field<TYPE_FLOAT>(value));

        value = 1;
        expr_node.float_literal.value = value;
        EXPECT_EQ(dt_float32.get_field(expr_node), Field::create_field<TYPE_FLOAT>(value));
    }
}

TEST_F(DataTypeNumberTest, ser_deser) {
    auto test_func = [](auto dt, const auto& column, int be_exec_version) {
        std::cout << "test serialize/deserialize datatype " << dt.get_family_name()
                  << ", be ver: " << be_exec_version << std::endl;
        using DataType = decltype(dt);
        using ColumnType = typename DataType::ColumnType;
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
    test_func(DataTypeInt8(), *column_int8, USE_CONST_SERDE);
    test_func(DataTypeInt8(), *column_int8, AGGREGATION_2_1_VERSION);

    test_func(DataTypeInt16(), *column_int16, USE_CONST_SERDE);
    test_func(DataTypeInt16(), *column_int16, AGGREGATION_2_1_VERSION);

    test_func(DataTypeInt32(), *column_int32, USE_CONST_SERDE);
    test_func(DataTypeInt32(), *column_int32, AGGREGATION_2_1_VERSION);

    test_func(DataTypeInt64(), *column_int64, USE_CONST_SERDE);
    test_func(DataTypeInt64(), *column_int64, AGGREGATION_2_1_VERSION);

    test_func(DataTypeInt128(), *column_int128, USE_CONST_SERDE);
    test_func(DataTypeInt128(), *column_int128, AGGREGATION_2_1_VERSION);

    test_func(DataTypeUInt8(), *column_uint8, USE_CONST_SERDE);
    test_func(DataTypeUInt8(), *column_uint8, AGGREGATION_2_1_VERSION);
}

TEST_F(DataTypeNumberTest, to_string) {
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
                StringRef rb((char*)item.data, item.size);
                auto status = dt.from_string(rb, &col_from_str);
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(col_from_str.get_element(i), source_column.get_element(i));
            }
        }
        {
            ColumnType col_from_str;
            for (size_t i = 0; i != row_count; ++i) {
                auto str = dt.to_string(source_column, i);
                StringRef rb(str.data(), str.size());
                auto status = dt.from_string(rb, &col_from_str);
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(col_from_str.get_element(i), source_column.get_element(i));
            }
        }
        {
            ColumnType col_from_str;
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

            ColumnType col_from_str;
            for (size_t i = 0; i != row_count; ++i) {
                auto item = col_str_to_str.get_data_at(i);
                StringRef rb((char*)item.data, item.size);
                auto status = dt.from_string(rb, &col_from_str);
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(col_from_str.get_element(i), source_column.get_element(i));
            }
        }
    };
    test_func(dt_int8, *column_int8);
    test_func(dt_int16, *column_int16);
    test_func(dt_int32, *column_int32);
    test_func(dt_int64, *column_int64);
    test_func(dt_int128, *column_int128);

    test_func(dt_uint8, *column_uint8);
}
TEST_F(DataTypeNumberTest, simple_func_test) {
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
    test_func(dt_float32);
    test_func(dt_float64);
    test_func(dt_int8);
    EXPECT_FALSE(dt_int8.equals(dt_uint8));
    EXPECT_FALSE(dt_int8.equals(dt_int16));
    test_func(dt_int16);
    test_func(dt_int32);
    test_func(dt_int64);
    test_func(dt_int128);
    test_func(dt_uint8);
}

TEST_F(DataTypeNumberTest, GetFieldWithDataTypeTest) {
    auto column_int8 = dt_int8.create_column();
    column_int8->insert(Field::create_field<TYPE_TINYINT>(1));
    EXPECT_EQ(dt_int8.get_field_with_data_type(*column_int8, 0).field,
              Field::create_field<TYPE_TINYINT>(1));
}

} // namespace doris::vectorized