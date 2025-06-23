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

#include "vec/data_types/data_type_variant.h"

#include <gtest/gtest.h>
#include <streamvbyte.h>

#include <memory>

#include "common/status.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/define_primitive_type.h"
#include "testutil/test_util.h"
#include "testutil/variant_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_variant.h"
#include "vec/columns/common_column_test.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/json/path_in_data.h"

namespace doris::vectorized {
static std::string test_data_dir;
static std::string test_result_dir;
static ColumnVariant::MutablePtr column_variant;
static DataTypeVariant dt_obj_1(5);

class DataTypeVariantTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";
        test_result_dir = root_dir + "/be/test/expected_result/vec/data_types";

        column_variant = ColumnVariant::create(true);
    }
    void SetUp() override { helper = std::make_unique<CommonDataTypeTest>(); }
    std::unique_ptr<CommonDataTypeTest> helper;

    static void common_gen_out_file(const std::string& function_name,
                                    const ColumnString& col_str_for_obj, size_t row_count) {
        std::vector<std::vector<std::string>> res;
        EXPECT_TRUE(col_str_for_obj.size() >= row_count);
        for (size_t i = 0; i != row_count; ++i) {
            std::vector<std::string> data;
            data.push_back("variant in row: " + std::to_string(i) + "\n" +
                           col_str_for_obj.get_data_at(i).to_string());
            res.push_back(data);
        }
        std::string file_name = test_result_dir + "/column_object_" + function_name + ".out";
        check_or_generate_res_file(file_name, res);
    }
};

TEST_F(DataTypeVariantTest, MetaInfoTest) {
    auto col_meta = std::make_shared<PColumnMeta>();
    col_meta->set_type(PGenericType_TypeId_VARIANT);
    auto tmp_dt = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_VARIANT, 0, 0);
    CommonDataTypeTest::DataTypeMetaInfo meta_info_to_assert = {
            .type_id = PrimitiveType::TYPE_VARIANT,
            .type_as_type_descriptor = tmp_dt,
            .family_name = tmp_dt->get_family_name(),
            .has_subtypes = true,
            .storage_field_type = doris::FieldType::OLAP_FIELD_TYPE_VARIANT,
            .should_align_right_in_pretty_formats = false,
            .text_can_contain_only_valid_utf8 = false,
            .have_maximum_size_of_value = false,
            .size_of_value_in_memory = 0,
            .precision = size_t(-1),
            .scale = size_t(-1),
            .is_null_literal = false,
            .is_value_represented_by_number = false,
            .pColumnMeta = col_meta.get(),
            .is_value_unambiguously_represented_in_contiguous_memory_region = false,
            .default_field = doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap()),
    };
    helper->meta_info_assert(tmp_dt, meta_info_to_assert);
}

TEST_F(DataTypeVariantTest, BasicFunctionsTest) {
    DataTypeVariant dt_obj(10);
    // Test name and family name
    EXPECT_EQ(dt_obj.do_get_name(), "Variant(max subcolumns count = 10)");
    EXPECT_EQ(std::string(dt_obj.get_family_name()), "Variant");

    // Test type ID and descriptor
    EXPECT_EQ(dt_obj.get_primitive_type(), PrimitiveType::TYPE_VARIANT);

    // Test storage field type
    EXPECT_EQ(dt_obj.get_storage_field_type(), doris::FieldType::OLAP_FIELD_TYPE_VARIANT);

    // Test have_subtypes
    EXPECT_TRUE(dt_obj.have_subtypes());

    // Test equals
    DataTypeVariant dt_obj2(10);
    DataTypeVariant dt_obj3(20);
    EXPECT_TRUE(dt_obj.equals(dt_obj2));
    EXPECT_FALSE(dt_obj.equals(dt_obj3));

    // Test variant_max_subcolumns_count
    EXPECT_EQ(dt_obj.variant_max_subcolumns_count(), 10);
}

TEST_F(DataTypeVariantTest, CreateColumnTest) {
    DataTypeVariant dt_obj;
    auto col_with_type = dt_obj.create_column();
    EXPECT_EQ(col_with_type->size(), 0);
    EXPECT_EQ(col_with_type->get_name(), "variant");

    // Test inserting different types
    col_with_type->insert(doris::vectorized::Field::create_field<TYPE_STRING>("str"));
    // insert Array() will meet core because nested in array in DataTypeNothing which is INVALID_TYPE
    //  and then in Subcolumn::insert() : data.back()->insert(field); will make ColumnString to insert Invalid field
    //    col_with_type->insert(doris::vectorized::Field::create_field<TYPE_ARRAY>(Array()));
    Field arr = doris::vectorized::Field::create_field<TYPE_ARRAY>(Array());
    arr.get<Array>().push_back(doris::vectorized::Field::create_field<TYPE_STRING>("str"));
    col_with_type->insert(arr);
    EXPECT_EQ(col_with_type->size(), 2);
}

TEST_F(DataTypeVariantTest, GetFieldTest) {
    DataTypeVariant dt_obj;
    TExprNode string_node;
    string_node.node_type = TExprNodeType::STRING_LITERAL;
    string_node.string_literal.value = "test";
    EXPECT_ANY_THROW(dt_obj.get_field(string_node));
    string_node.__isset.string_literal = true;
    EXPECT_EQ(dt_obj.get_field(string_node),
              doris::vectorized::Field::create_field<TYPE_STRING>(String("test")));

    TExprNode null_node;
    null_node.node_type = TExprNodeType::NULL_LITERAL;
    EXPECT_EQ(dt_obj.get_field(null_node),
              doris::vectorized::Field::create_field<TYPE_NULL>(Null()));

    // Test unknown literal type
    TExprNode unknown_node;
    unknown_node.node_type = TExprNodeType::INT_LITERAL;
    EXPECT_THROW(dt_obj.get_field(unknown_node), doris::Exception);
}

TEST_F(DataTypeVariantTest, SerializationTest) {
    auto test_func = [](auto& dt, const auto& column, int be_exec_version) {
        std::cout << "test serialize/deserialize datatype " << dt.get_family_name()
                  << ", be ver: " << be_exec_version << std::endl;

        // TEST EMPTY column
        {
            auto tmp_col = dt.create_column();

            auto content_uncompressed_size =
                    dt.get_uncompressed_serialized_bytes(*tmp_col, be_exec_version);
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

        // TEST DEFAULT VALUE in Column
        {
            if (be_exec_version < VARIANT_SERDE) {
                std::cout << "variant old serde do not support" << std::endl;
                return;
            }
            size_t count = 1;
            auto tmp_col = dt.create_column();
            auto* col_with_type = assert_cast<ColumnVariant*>(tmp_col.get());
            col_with_type->insert_many_defaults(count);

            int64_t content_uncompressed_size =
                    dt.get_uncompressed_serialized_bytes(*tmp_col, be_exec_version);
            std::string column_values;
            column_values.resize(content_uncompressed_size);
            char* buf = column_values.data();
            buf = dt.serialize(*tmp_col, buf, be_exec_version);
            const size_t serialize_bytes = buf - column_values.data() + STREAMVBYTE_PADDING;
            column_values.resize(serialize_bytes);

            MutableColumnPtr deser_column = dt.create_column();
            (void)dt.deserialize(column_values.data(), &deser_column, be_exec_version);
            EXPECT_EQ(deser_column->size(), 1);
            CommonColumnTest::checkColumn(*deser_column, *tmp_col, count);
        }

        // Test serde limit
        {
            if (be_exec_version < VARIANT_SERDE) {
                std::cout << "variant old serde do not support" << std::endl;
                return;
            }
            size_t count = SERIALIZED_MEM_SIZE_LIMIT + 1;
            auto tmp_col = dt.create_column();
            auto* col_with_type = assert_cast<ColumnVariant*>(tmp_col.get());
            for (size_t i = 0; i != count; ++i) {
                col_with_type->insert(
                        doris::vectorized::Field::create_field<TYPE_STRING>(String("str")));
                Field arr = doris::vectorized::Field::create_field<TYPE_ARRAY>(Array());
                arr.get<Array>().push_back(
                        doris::vectorized::Field::create_field<TYPE_STRING>("str"));
                col_with_type->insert(arr);
            }

            auto content_uncompressed_size =
                    dt.get_uncompressed_serialized_bytes(*tmp_col, be_exec_version);
            std::string column_values;
            column_values.resize(content_uncompressed_size);
            char* buf = column_values.data();
            buf = dt.serialize(*tmp_col, buf, be_exec_version);
            const size_t serialize_bytes = buf - column_values.data() + STREAMVBYTE_PADDING;
            column_values.resize(serialize_bytes);

            MutableColumnPtr deser_column = dt.create_column();
            (void)dt.deserialize(column_values.data(), &deser_column, be_exec_version);
            EXPECT_EQ(deser_column->size(), count * 2);
            CommonColumnTest::checkColumn(*deser_column, *tmp_col, count);
        }

        {
            if (be_exec_version < VARIANT_SERDE) {
                std::cout << "variant old serde do not support" << std::endl;
                return;
            }
            auto tmp = column->clone();
            auto content_uncompressed_size =
                    dt.get_uncompressed_serialized_bytes(*column, be_exec_version);
            std::string column_values;
            column_values.resize(content_uncompressed_size);
            char* buf = column_values.data();
            buf = dt.serialize(*column, buf, be_exec_version);
            const size_t serialize_bytes = buf - column_values.data() + STREAMVBYTE_PADDING;
            column_values.resize(serialize_bytes);

            MutableColumnPtr deser_column = dt.create_column();
            (void)dt.deserialize(column_values.data(), &deser_column, be_exec_version);
            auto count = column->size();
            EXPECT_EQ(deser_column->size(), count);
            CommonColumnTest::checkColumn(*deser_column, *column, count);

            column_values.clear();
            column_values.resize(content_uncompressed_size);
            buf = column_values.data();
            buf = dt.serialize(*tmp, buf, be_exec_version);
            const size_t serialize_bytes1 = buf - column_values.data() + STREAMVBYTE_PADDING;
            column_values.resize(serialize_bytes1);

            MutableColumnPtr deser_column2 = dt.create_column();
            (void)dt.deserialize(column_values.data(), &deser_column2, be_exec_version);
            EXPECT_EQ(deser_column2->size(), count);
            CommonColumnTest::checkColumn(*deser_column2, *tmp, count);
        }
    };

    auto src_column = VariantUtil::construct_basic_varint_column();
    test_func(dt_obj_1, src_column, USE_NEW_SERDE);
    test_func(dt_obj_1, src_column, VARIANT_SERDE);
    test_func(dt_obj_1, src_column, VARIANT_SPARSE_SERDE);
    auto sparse_column = VariantUtil::construct_advanced_varint_column();
    test_func(dt_obj_1, sparse_column, USE_NEW_SERDE);        // support variant sparse column
    test_func(dt_obj_1, sparse_column, VARIANT_SERDE);        // support variant sparse column
    test_func(dt_obj_1, sparse_column, VARIANT_SPARSE_SERDE); // support variant sparse column
}

TEST_F(DataTypeVariantTest, ToStringTest) {
    auto test_func = [](auto& dt, auto& source_column) {
        size_t row_count = source_column.size();
        {
            ColumnString col_obj_to_str;
            BufferWritable buffer(col_obj_to_str);

            for (size_t i = 0; i != row_count; ++i) {
                dt.to_string(source_column, i, buffer);
                buffer.commit();
            }
            common_gen_out_file("to_string_with_buffer", col_obj_to_str, row_count);
        }
        {
            ColumnString col_obj_to_str;
            for (size_t i = 0; i != row_count; ++i) {
                auto str = dt.to_string(source_column, i);
                ReadBuffer rb(str.data(), str.size());
                col_obj_to_str.insert_data(str.data(), str.size());
            }
            common_gen_out_file("to_string", col_obj_to_str, row_count);
        }
        // to string batch
        {
            ColumnString col_obj_to_str;
            dt.to_string_batch(source_column, col_obj_to_str);
            EXPECT_EQ(col_obj_to_str.size(), row_count);
            common_gen_out_file("to_string_batch", col_obj_to_str, row_count);
        }
        // from_string not implement
        {
            std::string str = "test";
            ReadBuffer rb(str.data(), str.size() - 1);
            ColumnVariant obj(1);
            EXPECT_ANY_THROW(Status st = dt.from_string(rb, &obj));
        }
    };
    auto column_basic_v = VariantUtil::construct_basic_varint_column();
    test_func(dt_obj_1, *column_basic_v);
    auto column_advanced_v = VariantUtil::construct_advanced_varint_column();
    test_func(dt_obj_1, *column_advanced_v);
}

TEST_F(DataTypeVariantTest, GetTypeFieldTest) {
    // Test basic variant column with simple types
    {
        auto basic_variant = VariantUtil::construct_basic_varint_column();
        // First 5 rows have these fields as Variant type
        std::vector<std::pair<std::string, Field>> first_batch = {
                {"v.a", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.b", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.c", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.f", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.e", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())}};

        // Next 5 rows have additional fields with specific types
        std::vector<std::pair<std::string, Field>> second_batch = {
                {"v.a", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.b", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.b.d", doris::vectorized::Field::create_field<TYPE_BIGINT>(Int64(30))},
                {"v.c", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.c.d", doris::vectorized::Field::create_field<TYPE_BIGINT>(Int64(30))},
                {"v.d.d", doris::vectorized::Field::create_field<TYPE_STRING>(String("50"))},
                {"v.e", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.f", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())}};

        // Test first 5 rows
        for (size_t i = 0; i < 5; i++) {
            Field type_field = dt_obj_1.get_type_field(*basic_variant, i);
            VariantMap expected_map;
            for (const auto& [key, value] : first_batch) {
                expected_map[PathInData(key)] = value;
            }
            EXPECT_EQ(type_field,
                      doris::vectorized::Field::create_field<TYPE_VARIANT>(expected_map));
        }

        // Test next 5 rows
        for (size_t i = 5; i < 10; i++) {
            Field type_field = dt_obj_1.get_type_field(*basic_variant, i);
            VariantMap expected_map;
            for (const auto& [key, value] : second_batch) {
                expected_map[PathInData(key)] = value;
            }
            EXPECT_EQ(type_field,
                      doris::vectorized::Field::create_field<TYPE_VARIANT>(expected_map));
        }
    }

    // Test advanced variant column with complex types
    {
        auto advanced_variant = VariantUtil::construct_advanced_varint_column();
        // First 5 rows have these fields
        std::vector<std::pair<std::string, Field>> first_batch = {
                {"v.a", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.b", doris::vectorized::Field::create_field<TYPE_JSONB>(JsonbField())},
                {"v.c", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.e", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.f", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())}};

        // Next rows have additional fields
        std::vector<std::pair<std::string, Field>> second_batch = {
                {"v.a", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.b", doris::vectorized::Field::create_field<TYPE_JSONB>(JsonbField())},
                {"v.b.d", doris::vectorized::Field::create_field<TYPE_JSONB>(JsonbField())},
                {"v.c", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.c.d", doris::vectorized::Field::create_field<TYPE_JSONB>(JsonbField())},
                {"v.d.d", doris::vectorized::Field::create_field<TYPE_JSONB>(JsonbField())},
                {"v.e", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.f", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())}};

        // Test first 5 rows
        for (size_t i = 0; i < 5; i++) {
            Field type_field = dt_obj_1.get_type_field(*advanced_variant, i);
            VariantMap expected_map;
            for (const auto& [key, value] : first_batch) {
                expected_map[PathInData(key)] = value;
            }
            EXPECT_EQ(type_field,
                      doris::vectorized::Field::create_field<TYPE_VARIANT>(expected_map));
        }

        // Test remaining rows
        for (size_t i = 5; i < advanced_variant->size(); i++) {
            Field type_field = dt_obj_1.get_type_field(*advanced_variant, i);
            VariantMap expected_map;
            for (const auto& [key, value] : second_batch) {
                expected_map[PathInData(key)] = value;
            }
            EXPECT_EQ(type_field,
                      doris::vectorized::Field::create_field<TYPE_VARIANT>(expected_map));
        }
    }

    // Test variant column with only subcolumns
    {
        auto subcolumns_variant = VariantUtil::construct_varint_column_only_subcolumns();
        // First 5 rows have these fields as Variant type
        std::vector<std::pair<std::string, Field>> fields = {
                {"v.a", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.b", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.c", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.e", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.f", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())}};

        // Test first 5 rows
        for (size_t i = 0; i < 5; i++) {
            Field type_field = dt_obj_1.get_type_field(*subcolumns_variant, i);
            VariantMap expected_map;
            for (const auto& [key, value] : fields) {
                expected_map[PathInData(key)] = value;
            }
            EXPECT_EQ(type_field,
                      doris::vectorized::Field::create_field<TYPE_VARIANT>(expected_map));
        }
    }

    // Test variant column with more subcolumns
    {
        auto more_subcolumns_variant = VariantUtil::construct_varint_column_more_subcolumns();
        std::vector<std::pair<std::string, Field>> fields = {
                {"v.a", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.b", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.c", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.e", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.f", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.s", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.x", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.y", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())},
                {"v.z", doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap())}};

        // Test all rows (they have the same structure)
        for (size_t i = 0; i < 5; i++) {
            Field type_field = dt_obj_1.get_type_field(*more_subcolumns_variant, i);
            VariantMap expected_map;
            for (const auto& [key, value] : fields) {
                expected_map[PathInData(key)] = value;
            }
            EXPECT_EQ(type_field,
                      doris::vectorized::Field::create_field<TYPE_VARIANT>(expected_map));
        }
    }
}

TEST_F(DataTypeVariantTest, ToPbColumnMetaTest) {
    DataTypeVariant dt_obj(10);
    PColumnMeta col_meta;
    dt_obj.to_pb_column_meta(&col_meta);

    EXPECT_EQ(col_meta.type(), PGenericType_TypeId_VARIANT);
    EXPECT_EQ(col_meta.variant_max_subcolumns_count(), 10);
}

} // namespace doris::vectorized
