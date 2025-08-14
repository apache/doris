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

#include "vec/data_types/data_type_string.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <lz4/lz4.h>
#include <streamvbyte.h>

#include <cstddef>
#include <iostream>
#include <memory>
#include <type_traits>

#include "agent/be_exec_version_manager.h"
#include "olap/olap_common.h"
#include "runtime/define_primitive_type.h"
#include "testutil/test_util.h"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/serde/data_type_string_serde.h"

namespace doris::vectorized {
static std::string test_data_dir;
static std::string test_result_dir;

static ColumnString::MutablePtr column_str32;
static ColumnString64::MutablePtr column_str64;
static DataTypeString dt_str;
class DataTypeStringTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";
        test_result_dir = root_dir + "/be/test/expected_result/vec/data_types";

        column_str32 = ColumnString::create();
        column_str64 = ColumnString64::create();

        load_columns_data();
    }

    static void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        {
            MutableColumns columns;
            columns.push_back(column_str32->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_str.get_serde()};
            std::string data_file = test_data_dir + "/STRING.csv";
            load_columns_data_from_file(columns, serde, ';', {0}, data_file);
            EXPECT_TRUE(!column_str32->empty());
            column_str32->insert_default();

            column_str64->insert_range_from(*column_str32, 0, column_str32->size());
        }
        std::cout << "column str size: " << column_str32->size() << std::endl;
    }
    void SetUp() override { helper = std::make_unique<CommonDataTypeTest>(); }
    std::unique_ptr<CommonDataTypeTest> helper;
};
TEST_F(DataTypeStringTest, MetaInfoTest) {
    auto type_descriptor =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, false);
    auto col_meta = std::make_shared<PColumnMeta>();
    col_meta->set_type(PGenericType_TypeId_STRING);
    CommonDataTypeTest::DataTypeMetaInfo meta_info_to_assert = {
            .type_id = PrimitiveType::TYPE_VARCHAR,
            .type_as_type_descriptor = type_descriptor,
            .family_name = dt_str.get_family_name(),
            .has_subtypes = false,
            .storage_field_type = doris::FieldType::OLAP_FIELD_TYPE_STRING,
            .should_align_right_in_pretty_formats = false,
            .text_can_contain_only_valid_utf8 = false,
            .have_maximum_size_of_value = false,
            .size_of_value_in_memory = 0,
            .precision = size_t(-1),
            .scale = size_t(-1),
            .is_null_literal = false,
            .is_value_represented_by_number = false,
            .pColumnMeta = col_meta.get(),
            .is_value_unambiguously_represented_in_contiguous_memory_region = true,
            .default_field = Field::create_field<TYPE_STRING>(""),
    };
    auto tmp_dt = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, false);
    helper->meta_info_assert(tmp_dt, meta_info_to_assert);
}

TEST_F(DataTypeStringTest, ser_deser) {
    auto test_func = [](auto& dt, const auto& column, int be_exec_version) {
        std::cout << "test serialize/deserialize datatype " << dt.get_family_name()
                  << ", be ver: " << be_exec_version << std::endl;
        using DataType = std::remove_reference_t<decltype(dt)>;
        using ColumnType = typename DataType::ColumnType;

        // const flag | row num | read saved num
        int64_t prefix_size = sizeof(bool) + sizeof(size_t) + sizeof(size_t);
        int64_t prefix_size2 = sizeof(uint32_t) + sizeof(uint64_t);

        {
            auto tmp_col = dt.create_column();
            auto* col_with_type = assert_cast<ColumnType*>(tmp_col.get());
            auto offsets_size = tmp_col->size() * sizeof(IColumn::Offset);
            auto data_size = col_with_type->get_chars().size();

            auto content_uncompressed_size =
                    dt.get_uncompressed_serialized_bytes(*tmp_col, be_exec_version);
            if (be_exec_version >= USE_CONST_SERDE) {
                auto expected_data_size = prefix_size;
                expected_data_size += offsets_size;
                // chars size
                expected_data_size += sizeof(size_t);
                expected_data_size += data_size;
                EXPECT_EQ(content_uncompressed_size, expected_data_size);
            } else {
                auto expected_data_size = prefix_size2;
                expected_data_size += offsets_size;
                expected_data_size += data_size;
                EXPECT_EQ(content_uncompressed_size, expected_data_size);
            }
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

        {
            size_t count = 1;
            auto tmp_col = dt.create_column();
            auto* col_with_type = assert_cast<ColumnType*>(tmp_col.get());
            col_with_type->insert_many_defaults(count);
            auto offsets_size = tmp_col->size() * sizeof(IColumn::Offset);
            auto data_size = col_with_type->get_chars().size();

            auto content_uncompressed_size =
                    dt.get_uncompressed_serialized_bytes(*tmp_col, be_exec_version);
            if (be_exec_version >= USE_CONST_SERDE) {
                auto expected_data_size = prefix_size;
                expected_data_size += offsets_size;
                // chars size
                expected_data_size += sizeof(size_t);
                expected_data_size += data_size;
                EXPECT_EQ(content_uncompressed_size, expected_data_size);
            } else {
                auto expected_data_size = prefix_size2;
                expected_data_size += offsets_size;
                expected_data_size += data_size;
                EXPECT_EQ(content_uncompressed_size, expected_data_size);
            }
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
            size_t count = SERIALIZED_MEM_SIZE_LIMIT + 1;
            auto tmp_col = dt.create_column();
            auto* col_with_type = assert_cast<ColumnType*>(tmp_col.get());
            for (size_t i = 0; i != count; ++i) {
                col_with_type->insert_data("a", 1);
            }
            auto offsets_size = tmp_col->size() * sizeof(IColumn::Offset);
            auto data_size = col_with_type->get_chars().size();

            auto content_uncompressed_size =
                    dt.get_uncompressed_serialized_bytes(*tmp_col, be_exec_version);
            if (be_exec_version >= USE_CONST_SERDE) {
                auto expected_data_size = prefix_size;
                expected_data_size +=
                        sizeof(size_t) +
                        std::max(offsets_size, streamvbyte_max_compressedbytes(cast_set<UInt32>(
                                                       upper_int32(offsets_size))));
                expected_data_size += sizeof(size_t);
                expected_data_size +=
                        sizeof(size_t) +
                        std::max(data_size, (size_t)LZ4_compressBound(cast_set<UInt32>(data_size)));
                EXPECT_EQ(content_uncompressed_size, expected_data_size);
            } else {
                auto expected_data_size = prefix_size2;
                expected_data_size +=
                        sizeof(size_t) +
                        std::max(offsets_size, streamvbyte_max_compressedbytes(cast_set<UInt32>(
                                                       upper_int32(offsets_size))));
                expected_data_size +=
                        sizeof(size_t) +
                        std::max(data_size, (size_t)LZ4_compressBound(cast_set<UInt32>(data_size)));
                EXPECT_EQ(content_uncompressed_size, expected_data_size);
            }
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
            auto content_uncompressed_size =
                    dt.get_uncompressed_serialized_bytes(column, be_exec_version);
            std::string column_values;
            column_values.resize(content_uncompressed_size);
            char* buf = column_values.data();
            buf = dt.serialize(column, buf, be_exec_version);
            const size_t serialize_bytes = buf - column_values.data() + STREAMVBYTE_PADDING;
            column_values.resize(serialize_bytes);

            MutableColumnPtr deser_column = dt.create_column();
            (void)dt.deserialize(column_values.data(), &deser_column, be_exec_version);
            auto count = column.size();
            EXPECT_EQ(deser_column->size(), count);
            for (size_t i = 0; i != count; ++i) {
                EXPECT_EQ(deser_column->get_data_at(i), column.get_data_at(i));
            }
        }
    };
    test_func(dt_str, *column_str32, USE_CONST_SERDE);
    test_func(dt_str, *column_str32, AGGREGATION_2_1_VERSION);
}
TEST_F(DataTypeStringTest, simple_func_test) {
    auto test_func = [](auto& dt) {
        EXPECT_FALSE(dt.have_subtypes());
        EXPECT_FALSE(dt.should_align_right_in_pretty_formats());
        EXPECT_TRUE(dt.is_comparable());
        EXPECT_TRUE(dt.is_value_unambiguously_represented_in_contiguous_memory_region());
        EXPECT_FALSE(dt.have_maximum_size_of_value());
        EXPECT_TRUE(dt.can_be_inside_low_cardinality());

        EXPECT_FALSE(dt.is_null_literal());

        EXPECT_TRUE(dt.equals(dt));

        EXPECT_EQ(std::string(dt.get_family_name()), std::string("String"));

        EXPECT_EQ(dt.get_default(), Field::create_field<TYPE_STRING>(String()));
    };
    test_func(dt_str);
    EXPECT_EQ(dt_str.get_primitive_type(), TYPE_STRING);
}
TEST_F(DataTypeStringTest, to_string) {
    auto test_func = [](auto& dt, const auto& source_column) {
        using DataType = std::remove_reference_t<decltype(dt)>;
        using ColumnType = typename DataType::ColumnType;
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
                EXPECT_EQ(col_from_str.get_data_at(i), source_column.get_data_at(i));
            }
        }
        {
            ColumnType col_from_str;
            for (size_t i = 0; i != row_count; ++i) {
                auto str = dt.to_string(source_column, i);
                StringRef rb(str.data(), str.size());
                auto status = dt.from_string(rb, &col_from_str);
                EXPECT_TRUE(status.ok());
                EXPECT_EQ(col_from_str.get_data_at(i), source_column.get_data_at(i));
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
                EXPECT_EQ(col_from_str.get_data_at(i), source_column.get_data_at(i));
            }
        }
    };
    test_func(dt_str, *column_str32);
}
TEST_F(DataTypeStringTest, get_field) {
    TExprNode expr_node;
    expr_node.node_type = TExprNodeType::STRING_LITERAL;
    expr_node.__isset.string_literal = true;
    expr_node.string_literal.value = "a";
    EXPECT_EQ(dt_str.get_field(expr_node), Field::create_field<TYPE_STRING>("a"));
}
TEST_F(DataTypeStringTest, escape_string) {
    {
        char test_str[] = "hello\\world";
        size_t len = strlen(test_str);
        escape_string(test_str, &len, '\\');
        EXPECT_EQ(std::string(test_str, len), "helloworld");
    }
    {
        char test_str[] = "helloworld";
        size_t len = strlen(test_str);
        escape_string(test_str, &len, '\\');
        EXPECT_EQ(std::string(test_str, len), "helloworld");
    }
    {
        char test_str[] = R"(hello\\world)";
        size_t len = strlen(test_str);
        escape_string(test_str, &len, '\\');
        EXPECT_EQ(std::string(test_str, len), R"(hello\world)");
    }
    {
        char test_str[] = R"(\\hello\\)";
        size_t len = strlen(test_str);
        escape_string(test_str, &len, '\\');
        EXPECT_EQ(std::string(test_str, len), R"(\hello\)");
    }
}

TEST_F(DataTypeStringTest, escape_string_for_csv) {
    {
        char test_str[] = R"(hello""world)";
        size_t len = strlen(test_str);
        escape_string_for_csv(test_str, &len, '\\', '"');
        EXPECT_EQ(std::string(test_str, len), R"(hello"world)");
    }
    {
        char test_str[] = "helloworld";
        size_t len = strlen(test_str);
        escape_string_for_csv(test_str, &len, '\\', '"');
        EXPECT_EQ(std::string(test_str, len), "helloworld");
    }
    {
        char test_str[] = R"("hello""world")";
        size_t len = strlen(test_str);
        escape_string_for_csv(test_str, &len, '\\', '"');
        EXPECT_EQ(std::string(test_str, len), R"("hello"world")");
    }
    {
        char test_str[] = R"(\\"hello\\""world\\)";
        size_t len = strlen(test_str);
        escape_string_for_csv(test_str, &len, '\\', '"');
        EXPECT_EQ(std::string(test_str, len), R"(\"hello\"world\)");
    }
    {
        char test_str[] = "";
        size_t len = strlen(test_str);
        escape_string_for_csv(test_str, &len, '\\', '"');
        EXPECT_EQ(std::string(test_str, len), "");
    }
}

TEST_F(DataTypeStringTest, GetFieldWithDataTypeTest) {
    auto column_str = dt_str.create_column();
    column_str->insert_data("a", 1);
    EXPECT_EQ(dt_str.get_field_with_data_type(*column_str, 0).field,
              Field::create_field<TYPE_STRING>("a"));

    // wrap with nullable
    auto nullable_dt = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto nullable_column = nullable_dt->create_column();
    nullable_column->insert_data("a", 1);
    nullable_column->insert_default();
    EXPECT_EQ(nullable_dt->get_field_with_data_type(*nullable_column, 0).field,
              Field::create_field<TYPE_STRING>("a"));
    EXPECT_EQ(nullable_dt->get_field_with_data_type(*nullable_column, 1).field, Field());
}

} // namespace doris::vectorized