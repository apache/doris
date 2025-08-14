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

#include "vec/columns/column_variant.h"

#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <rapidjson/prettywriter.h>
#include <stdio.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include "common/cast_set.h"
#include "runtime/jsonb_value.h"
#include "testutil/test_util.h"
#include "testutil/variant_util.h"
#include "vec/columns/column_variant.cpp"
#include "vec/columns/common_column_test.h"
#include "vec/columns/subcolumn_tree.h"
#include "vec/common/schema_util.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"

using namespace doris;
namespace doris::vectorized {
static std::string root_dir;
static std::string test_data_dir;
static std::string test_result_dir;
static std::string test_data_dir_json;
static DataTypePtr dt_variant =
        DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_VARIANT, 0, 0);

DataTypeSerDeSPtrs serde;

static ColumnVariant::MutablePtr column_variant;

class ColumnVariantTest : public CommonColumnTest {
protected:
    static void SetUpTestSuite() {
        root_dir = std::string(getenv("ROOT"));
        std::cout << "root_dir: " << root_dir << std::endl;
        test_data_dir = root_dir + "/be/test/data/vec/columns";
        test_result_dir = root_dir + "/be/test/expected_result/vec/columns";

        column_variant = ColumnVariant::create(true);
        std::cout << dt_variant->get_name() << std::endl;

        load_json_columns_data();
    }

    static void load_json_columns_data() {
        std::cout << "loading json dataset : " << FLAGS_gen_out << std::endl;
        {
            MutableColumns columns;
            columns.push_back(column_variant->get_ptr());
            serde = {dt_variant->get_serde()};
            test_data_dir_json = root_dir + "/regression-test/data/nereids_function_p0/";
            std::vector<std::string> json_files = {
                    test_data_dir_json + "json_variant/boolean_boundary.jsonl",
                    test_data_dir_json + "json_variant/null_boundary.jsonl",
                    test_data_dir_json + "json_variant/number_boundary.jsonl",
                    test_data_dir_json + "json_variant/string_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_boolean_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_nullable_null_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_number_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_string_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_object_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_nullable_boolean_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_nullable_number_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_nullable_string_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_nullable_object_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_array_boolean_boundary.jsonl",
                    test_data_dir_json + "json_variant/array_array_number_boundary.jsonl",
                    test_data_dir_json +
                            "json_variant/array_nullable_array_nullable_boolean_boundary.jsonl",
                    test_data_dir_json +
                            "json_variant/array_nullable_array_nullable_null_boundary.jsonl",
                    test_data_dir_json +
                            "json_variant/array_nullable_array_nullable_number_boundary.jsonl",
                    test_data_dir_json + "json_variant/object_boundary.jsonl",
                    test_data_dir_json + "json_variant/object_nested_1025.jsonl"};

            for (const auto& json_file : json_files) {
                load_columns_data_from_file(columns, serde, '\n', {0}, json_file);
                EXPECT_TRUE(!column_variant->empty());
                column_variant->insert_default();
                std::cout << "column variant size: " << column_variant->size() << std::endl;
            }
            column_variant->finalize();
            std::cout << "column variant finalize size: " << column_variant->size() << std::endl;
        }
    }

    template <typename T>
    void column_common_test(T callback) {
        callback(ColumnVariant(true), column_variant->get_ptr());
    }

    void hash_common_test(
            const std::string& function_name,
            std::function<void(const MutableColumns& load_cols, DataTypeSerDeSPtrs serders,
                               const std::string& res_file_name)>
                    assert_callback) {
        {
            MutableColumns columns;
            columns.push_back(column_variant->get_ptr());
            DataTypeSerDeSPtrs serdes = {dt_variant->get_serde()};
            assert_callback(columns, serdes,
                            test_result_dir + "/column_variant_" + function_name + ".out");
        }
    }
};

TEST_F(ColumnVariantTest, insert_try_insert) {
    auto v = VariantUtil::construct_dst_varint_column();
    FieldInfo info;
    info.scalar_type_id = PrimitiveType::TYPE_BIGINT;
    info.num_dimensions = 0;
    PathInData path("v.f");
    auto sub = v->get_subcolumn(path);
    Int64 value = 43;
    sub->insert(Field::create_field<TYPE_BIGINT>(value), info);

    info.num_dimensions = 1;
    sub->insert(Field::create_field<TYPE_BIGINT>(value), info);

    info.num_dimensions = 2;
    sub->insert(Field::create_field<TYPE_BIGINT>(value), info);
}

TEST_F(ColumnVariantTest, basic_finalize) {
    auto variant = VariantUtil::construct_basic_varint_column();
    // 4. finalize
    variant->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
    EXPECT_EQ(variant->subcolumns.size(), 9);
    EXPECT_TRUE(variant->pick_subcolumns_to_sparse_column({}, false).ok());
    EXPECT_EQ(variant->size(), 10);

    // check finalized subcolumn
    // 5 subcolumn + 1 root
    EXPECT_EQ(variant->subcolumns.size(), 6);
    for (const auto& column : variant->subcolumns) {
        if (column->data.is_root) {
            continue;
        }
        EXPECT_EQ(column->data.data.size(), 1);
    }

    // check sparse column
    const auto& offsets = variant->serialized_sparse_column_offsets();
    for (int row = 0; row < 5; ++row) {
        EXPECT_EQ(offsets[row], 0);
    }
    for (int row = 5; row < 10; ++row) {
        EXPECT_EQ(offsets[row] - offsets[row - 1], 3);
    }
}

// TEST
TEST_F(ColumnVariantTest, basic_deserialize) {
    auto variant = VariantUtil::construct_basic_varint_column();

    // 4. finalize
    variant->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
    EXPECT_TRUE(variant->pick_subcolumns_to_sparse_column({}, false).ok());
    EXPECT_EQ(variant->size(), 10);

    const auto& [path, value] = variant->get_sparse_data_paths_and_values();
    const auto& offsets = variant->serialized_sparse_column_offsets();
    for (size_t row = 5; row < 10; ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];

        auto data = path->get_data_at(start);
        EXPECT_EQ(data, StringRef("v.b.d", 5));
        auto pair = variant->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(pair.first.get<Int64>(), 30);

        auto data2 = path->get_data_at(start);
        auto pair2 = variant->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data2, StringRef("v.c.d", 5));
        EXPECT_EQ(pair2.first.get<Int64>(), 30);

        auto data3 = path->get_data_at(start);
        auto pair3 = variant->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data3, StringRef("v.d.d", 5));
        EXPECT_EQ(pair3.first.get<String>(), "50");
        EXPECT_EQ(start, end);
    }
}

TEST_F(ColumnVariantTest, test_pop_back_multiple_types) {
    ColumnVariant::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);

    Field field_int8 = Field::create_field<TYPE_TINYINT>(42);
    subcolumn.insert(field_int8);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(TINYINT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(TINYINT)");

    Field field_int16 = Field::create_field<TYPE_SMALLINT>(12345);
    subcolumn.insert(field_int16);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 2);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(TINYINT)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(SMALLINT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(SMALLINT)");

    Field field_int32 = Field::create_field<TYPE_INT>(1234567);
    subcolumn.insert(field_int32);
    EXPECT_EQ(subcolumn.size(), 3);
    EXPECT_EQ(subcolumn.data_types.size(), 3);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(TINYINT)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(SMALLINT)");
    EXPECT_EQ(subcolumn.data_types[2]->get_name(), "Nullable(INT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(INT)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 2);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(TINYINT)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(SMALLINT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(SMALLINT)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(TINYINT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(TINYINT)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 0);
    EXPECT_EQ(subcolumn.data_types.size(), 0);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nothing");

    subcolumn.insert(field_int32);
    EXPECT_EQ(subcolumn.size(), 1);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(INT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(INT)");

    subcolumn.insert(field_int16);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(INT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(INT)");

    subcolumn.insert(field_int8);
    EXPECT_EQ(subcolumn.size(), 3);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(INT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(INT)");

    subcolumn.pop_back(1);
    EXPECT_EQ(subcolumn.size(), 2);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(INT)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(INT)");

    Field field_string = Field::create_field<TYPE_STRING>("hello");
    subcolumn.insert(field_string);
    EXPECT_EQ(subcolumn.size(), 3);
    EXPECT_EQ(subcolumn.data_types.size(), 2);
    EXPECT_EQ(subcolumn.data_types[0]->get_name(), "Nullable(INT)");
    EXPECT_EQ(subcolumn.data_types[1]->get_name(), "Nullable(JSONB)");
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nullable(JSONB)");

    subcolumn.pop_back(3);
    EXPECT_EQ(subcolumn.size(), 0);
    EXPECT_EQ(subcolumn.data_types.size(), 0);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_name(), "Nothing");
}

TEST_F(ColumnVariantTest, basic_inset_range_from) {
    auto src = VariantUtil::construct_basic_varint_column();
    src->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
    EXPECT_TRUE(src->pick_subcolumns_to_sparse_column({}, false).ok());
    EXPECT_EQ(src->size(), 10);

    // dst is an empty column, has 5 subcolumn + 1 root
    auto dst = VariantUtil::construct_dst_varint_column();

    // subcolumn->subcolumn          v.b v.f v.e
    // subcolumn->sparse_column      v.a v.c
    // sparse_column->subcolumn      v.b.d v.c.d
    // sparse_column->sparse_column  v.d.d
    dst->insert_range_from(*src, 0, 10);
    dst->finalize();
    EXPECT_EQ(dst->size(), 10);

    // 5 subcolumn
    EXPECT_EQ(dst->subcolumns.size(), 6);
    ColumnVariant::Subcolumns dst_subcolumns = dst->subcolumns;
    std::sort(
            dst_subcolumns.begin(), dst_subcolumns.end(),
            [](const auto& lhsItem, const auto& rhsItem) { return lhsItem->path < rhsItem->path; });

    for (const auto& column : dst_subcolumns) {
        if (column->data.is_root) {
            continue;
        }
        EXPECT_EQ(column->data.data.size(), 1);
        EXPECT_EQ(column->data.data[0]->size(), 10);
        if (column->path.get_path().size() == 3) {
            EXPECT_EQ(column->data.get_non_null_value_size(), 10);
        } else {
            EXPECT_EQ(column->path.get_path().size(), 5);
            EXPECT_EQ(column->data.get_non_null_value_size(), 5);
            for (size_t row = 0; row != 5; ++row) {
                EXPECT_TRUE(column->data.data[0]->is_null_at(row));
            }
            for (size_t row = 5; row != 10; ++row) {
                EXPECT_EQ((*column->data.data[0])[row].get<Int64>(), 30);
            }
        }
    }

    // check sparse column
    const auto& [path, value] = dst->get_sparse_data_paths_and_values();
    const auto& offsets = dst->serialized_sparse_column_offsets();

    // v.a v.c
    for (int row = 0; row < 5; ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];

        auto data = path->get_data_at(start);
        EXPECT_EQ(data, StringRef("v.a", 3));
        auto pair = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(pair.first.get<Int64>(), 20);

        auto data2 = path->get_data_at(start);
        EXPECT_EQ(data2, StringRef("v.c", 3));
        auto pair2 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(pair2.first.get<Int64>(), 20);

        EXPECT_EQ(start, end);
    }

    // v.a v.c v.d.d
    for (int row = 5; row < 10; ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];

        auto data = path->get_data_at(start);
        EXPECT_EQ(data, StringRef("v.a", 3));
        auto pair = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(pair.first.get<Int64>(), 20);

        auto data2 = path->get_data_at(start);
        EXPECT_EQ(data2, StringRef("v.c", 3));
        auto pair2 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(pair2.first.get<Int64>(), 20);

        auto data3 = path->get_data_at(start);
        EXPECT_EQ(data3, StringRef("v.d.d", 5));
        auto pair3 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(pair3.first.get<String>(), "50");

        EXPECT_EQ(start, end);
    }
}

auto convert_to_jsonb_field(auto serde, auto& column) {
    vectorized::DataTypeSerDe::FormatOptions options;
    options.escape_char = '\\';
    auto tmp_col = ColumnString::create();
    VectorBufferWriter write_buffer(*tmp_col.get());
    EXPECT_TRUE(serde->serialize_column_to_json(column, 0, 1, write_buffer, options).ok());

    write_buffer.commit();
    auto str_ref = tmp_col->get_data_at(0);
    Slice data((char*)(str_ref.data), str_ref.size);

    auto jsonb_type = doris::vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_JSONB, false);
    auto jsonb_serde = jsonb_type->get_serde();
    auto jsonb_column = jsonb_type->create_column();

    DataTypeSerDe::FormatOptions format_options;
    format_options.converted_from_string = true;
    EXPECT_TRUE(
            jsonb_serde->deserialize_one_cell_from_json(*jsonb_column, data, format_options).ok());
    auto res = jsonb_column->get_data_at(0);
    return JsonbField(res.data, res.size);
}

auto convert_string_to_jsonb_field(auto& column) {
    auto str_ref = column.get_data_at(0);
    Slice data((char*)(str_ref.data), str_ref.size);

    auto jsonb_type = doris::vectorized::DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_JSONB, false);
    auto jsonb_serde = jsonb_type->get_serde();
    auto jsonb_column = jsonb_type->create_column();
    DataTypeSerDe::FormatOptions format_options;
    format_options.converted_from_string = true;
    format_options.escape_char = '\\';

    EXPECT_TRUE(
            jsonb_serde->deserialize_one_cell_from_json(*jsonb_column, data, format_options).ok());
    auto res = jsonb_column->get_data_at(0);
    return JsonbField(res.data, res.size);
}

doris::vectorized::Field get_jsonb_field(std::string_view type) {
    static std::unordered_map<std::string_view, doris::vectorized::Field> field_map;
    if (field_map.empty()) {
        DataTypePtr data_type_int = doris::vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, false);
        DataTypePtr data_type_array_int =
                std::make_shared<doris::vectorized::DataTypeArray>(data_type_int);
        auto array_column_int = data_type_array_int->create_column();
        array_column_int->insert(VariantUtil::get_field("array_int"));
        auto array_serde_int = data_type_array_int->get_serde();
        field_map["array_int"] = doris::vectorized::Field::create_field<TYPE_JSONB>(
                convert_to_jsonb_field(array_serde_int, *array_column_int));

        DataTypePtr data_type_str = doris::vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_STRING, false);
        DataTypePtr data_type_array_str =
                std::make_shared<doris::vectorized::DataTypeArray>(data_type_str);
        auto array_column_str = data_type_array_str->create_column();
        array_column_str->insert(VariantUtil::get_field("array_str"));
        auto array_serde_str = data_type_array_str->get_serde();
        field_map["array_str"] = doris::vectorized::Field::create_field<TYPE_JSONB>(
                convert_to_jsonb_field(array_serde_str, *array_column_str));

        auto column_int = data_type_int->create_column();
        column_int->insert(VariantUtil::get_field("int"));
        auto serde_int = data_type_int->get_serde();
        field_map["int"] = doris::vectorized::Field::create_field<TYPE_JSONB>(
                convert_to_jsonb_field(serde_int, *column_int));

        // auto column_str = data_type_str->create_column();
        // column_str->insert(VariantUtil::get_field("string"));
        // field_map["string"] = convert_string_to_jsonb_field(*column_str);
    }
    return field_map[type];
}

// std::string convert_jsonb_field_to_string(doris::vectorized::Field jsonb) {
//     const auto& val = jsonb.get<JsonbField>();
//     const JsonbValue* json_val = JsonbDocument::createValue(val.get_value(), val.get_size());

//     rapidjson::Document doc;
//     doc.SetObject();
//     rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();
//     rapidjson::Value json_value;
//     convert_jsonb_to_rapidjson(*json_val, json_value, allocator);
//     doc.AddMember("value", json_value, allocator);
//     rapidjson::StringBuffer buffer;
//     rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
//     doc.Accept(writer);
//     return std::string(buffer.GetString());
// }

std::string convert_field_to_string(doris::vectorized::Field array) {
    rapidjson::Document doc;
    doc.SetObject();
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();
    rapidjson::Value json_value;
    // DataTypeSerDe::convert_field_to_rapidjson(array, json_value, allocator);
    doc.AddMember("value", json_value, allocator);
    rapidjson::StringBuffer buffer;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    return std::string(buffer.GetString());
}

TEST_F(ColumnVariantTest, is_null_at) {
    auto v = VariantUtil::construct_dst_varint_column();
    PathInData path("v.f");
    auto sub = v->get_subcolumn(path);
    std::cout << sub->get_least_common_typeBase()->get_name() << std::endl;
    EXPECT_TRUE(sub->is_null_at(0));

    auto v1 = VariantUtil::construct_advanced_varint_column();
    PathInData path1("v.b.d");
    auto sub1 = v1->get_subcolumn(path1);
    EXPECT_TRUE(sub1->is_null_at(2));
    EXPECT_ANY_THROW(sub1->is_null_at(16));
    vectorized::FieldWithDataType f;
    EXPECT_ANY_THROW(sub1->get(16, f));
    std::cout << sub1->num_rows << std::endl;
    EXPECT_NO_THROW(sub1->resize(sub1->num_rows));

    auto [sparse_column_keys, sparse_column_values] = v1->get_sparse_data_paths_and_values();
    std::string_view pa("v.a");
    EXPECT_NO_THROW(
            sub1->serialize_to_sparse_column(sparse_column_keys, pa, sparse_column_values, 2));
    EXPECT_ANY_THROW(
            sub1->serialize_to_sparse_column(sparse_column_keys, pa, sparse_column_values, 16));
}

TEST_F(ColumnVariantTest, advanced_finalize) {
    auto variant = VariantUtil::construct_advanced_varint_column();

    // 4. finalize
    variant->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
    EXPECT_TRUE(variant->pick_subcolumns_to_sparse_column({}, false).ok());
    EXPECT_EQ(variant->size(), 15);

    // check finalized subcolumn
    // 5 subcolumn + 1 root
    EXPECT_EQ(variant->subcolumns.size(), 6);
    for (const auto& column : variant->subcolumns) {
        if (column->data.is_root) {
            continue;
        }
        EXPECT_EQ(column->data.data.size(), 1);
    }

    // check sparse column
    const auto& offsets = variant->serialized_sparse_column_offsets();
    for (int row = 0; row < 5; ++row) {
        EXPECT_EQ(offsets[row] - offsets[row - 1], 0);
    }
    for (int row = 5; row < 15; ++row) {
        EXPECT_EQ(offsets[row] - offsets[row - 1], 3);
    }

    {
        // Test fill_path_column_from_sparse_data
        auto map = std::make_unique<NullMap>(15, 0);
        vectorized::ColumnVariant::fill_path_column_from_sparse_data(
                *variant->get_subcolumn({}) /*root*/, map.get(), StringRef {"array"},
                variant->get_sparse_column(), 0, 5);
        vectorized::ColumnVariant::fill_path_column_from_sparse_data(
                *variant->get_subcolumn({}) /*root*/, map.get(), StringRef {"array"},
                variant->get_sparse_column(), 5, 15);
    }
}

TEST_F(ColumnVariantTest, advanced_deserialize) {
    auto variant = VariantUtil::construct_advanced_varint_column();

    // 4. finalize
    variant->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
    EXPECT_TRUE(variant->pick_subcolumns_to_sparse_column({}, false).ok());
    EXPECT_EQ(variant->size(), 15);

    const auto& [path, value] = variant->get_sparse_data_paths_and_values();
    const auto& offsets = variant->serialized_sparse_column_offsets();
    for (size_t row = 5; row < 10; ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];

        auto data = path->get_data_at(start);
        auto pair = variant->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data, StringRef("v.b.d", 5));
        EXPECT_EQ(convert_field_to_string(pair.first),
                  convert_field_to_string(get_jsonb_field("array_int")));

        auto data2 = path->get_data_at(start);
        auto pair2 = variant->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data2, StringRef("v.c.d", 5));
        EXPECT_EQ(convert_field_to_string(pair2.first),
                  convert_field_to_string(VariantUtil::get_field("string")));

        auto data3 = path->get_data_at(start);
        auto pair3 = variant->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data3, StringRef("v.d.d", 5));
        EXPECT_EQ(convert_field_to_string(pair3.first),
                  convert_field_to_string(get_jsonb_field("array_int")));
        EXPECT_EQ(start, end);
    }

    for (size_t row = 10; row < 15; ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];

        auto data = path->get_data_at(start);
        auto pair = variant->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data, StringRef("v.b.d", 5));
        EXPECT_EQ(convert_field_to_string(pair.first),
                  convert_field_to_string(get_jsonb_field("array_str")));

        auto data2 = path->get_data_at(start);
        auto pair2 = variant->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data2, StringRef("v.c.d", 5));
        EXPECT_EQ(convert_field_to_string(pair2.first),
                  convert_field_to_string(get_jsonb_field("int")));

        auto data3 = path->get_data_at(start);
        auto pair3 = variant->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data3, StringRef("v.d.d", 5));
        EXPECT_EQ(convert_field_to_string(pair3.first),
                  convert_field_to_string(get_jsonb_field("array_str")));
        EXPECT_EQ(start, end);
    }
}

TEST_F(ColumnVariantTest, advanced_insert_range_from) {
    auto src = VariantUtil::construct_advanced_varint_column();
    src->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
    EXPECT_TRUE(src->pick_subcolumns_to_sparse_column({}, false).ok());
    EXPECT_EQ(src->size(), 15);

    auto dst = VariantUtil::construct_dst_varint_column();

    // subcolumn->subcolumn          v.b v.f v.e
    // subcolumn->sparse_column      v.a v.c
    // sparse_column->subcolumn      v.b.d v.c.d
    // sparse_column->sparse_column  v.d.d
    dst->insert_range_from(*src, 0, src->size());
    dst->finalize();
    EXPECT_EQ(dst->size(), 15);

    EXPECT_EQ(dst->subcolumns.size(), 6);
    ColumnVariant::Subcolumns dst_subcolumns = dst->subcolumns;

    std::sort(
            dst_subcolumns.begin(), dst_subcolumns.end(),
            [](const auto& lhsItem, const auto& rhsItem) { return lhsItem->path < rhsItem->path; });

    // subcolumns
    for (const auto& column : dst_subcolumns) {
        if (column->data.is_root) {
            continue;
        }
        EXPECT_EQ(column->data.data.size(), 1);
        EXPECT_EQ(column->data.data[0]->size(), 15);

        if (column->path.get_path().size() == 3) {
            EXPECT_EQ(column->data.get_non_null_value_size(), 15);
            if (column->path.get_path() == "v.b") {
                EXPECT_EQ(assert_cast<const DataTypeNullable*>(column->data.data_types[0].get())
                                  ->get_nested_type()
                                  ->get_primitive_type(),
                          PrimitiveType::TYPE_JSONB);
            }
        } else if (column->path.get_path().size() == 5) {
            EXPECT_EQ(column->data.get_non_null_value_size(), 10);
            auto nested_type =
                    assert_cast<const DataTypeNullable*>(column->data.data_types[0].get())
                            ->get_nested_type();
            if (nested_type->get_primitive_type() == PrimitiveType::TYPE_ARRAY) {
                // ARRAY(JSONB)
                auto array_type = assert_cast<const DataTypeArray*>(nested_type.get());
                EXPECT_EQ(array_type->get_nested_type()->get_primitive_type(),
                          PrimitiveType::TYPE_JSONB);
            } else {
                // JSONB
                EXPECT_EQ(nested_type->get_primitive_type(), PrimitiveType::TYPE_JSONB);
            }

            for (size_t row = 0; row < 5; ++row) {
                EXPECT_TRUE(column->data.data[0]->is_null_at(row));
            }
        }
    }

    // sparse columns
    const auto& [path, value] = dst->get_sparse_data_paths_and_values();
    const auto& offsets = dst->serialized_sparse_column_offsets();

    // v.a v.c
    for (int row = 0; row < 5; ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];

        auto data = path->get_data_at(start);
        EXPECT_EQ(data, StringRef("v.a", 3));
        auto pair = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(pair.first.get<Int64>(), 20);

        auto data2 = path->get_data_at(start);
        EXPECT_EQ(data2, StringRef("v.c", 3));
        auto pair2 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(convert_field_to_string(pair2.first),
                  convert_field_to_string(VariantUtil::get_field("array_int")));

        EXPECT_EQ(start, end);
    }

    for (int row = 5; row < 10; ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];

        auto data = path->get_data_at(start);
        auto pair = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data, StringRef("v.a", 3));
        EXPECT_EQ(pair.first.get<Int64>(), 20);

        auto data2 = path->get_data_at(start);
        auto pair2 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data2, StringRef("v.c", 3));
        EXPECT_EQ(convert_field_to_string(pair2.first),
                  convert_field_to_string(VariantUtil::get_field("array_int")));

        auto data3 = path->get_data_at(start);
        auto pair3 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data3, StringRef("v.d.d", 5));
        EXPECT_EQ(convert_field_to_string(pair3.first),
                  convert_field_to_string(get_jsonb_field("array_int")));

        EXPECT_EQ(start, end);
    }

    for (int row = 10; row < 15; ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];

        auto data = path->get_data_at(start);
        auto pair = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data, StringRef("v.a", 3));
        EXPECT_EQ(pair.first.get<Int64>(), 20);

        auto data2 = path->get_data_at(start);
        auto pair2 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data2, StringRef("v.c", 3));
        EXPECT_EQ(convert_field_to_string(pair2.first),
                  convert_field_to_string(VariantUtil::get_field("array_int")));

        auto data3 = path->get_data_at(start);
        auto pair3 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data3, StringRef("v.d.d", 5));
        EXPECT_EQ(convert_field_to_string(pair3.first),
                  convert_field_to_string(get_jsonb_field("array_str")));

        EXPECT_EQ(start, end);
    }
}

TEST_F(ColumnVariantTest, empty_inset_range_from) {
    auto src = VariantUtil::construct_varint_column_only_subcolumns();
    src->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
    EXPECT_TRUE(src->pick_subcolumns_to_sparse_column({}, false).ok());
    EXPECT_EQ(src->size(), 6);

    // dst is an empty column
    auto dst = ColumnVariant::create(5);

    // subcolumn->subcolumn          v.a v.b v.c v.f v.e
    dst->insert_range_from(*src, 0, 6);
    EXPECT_EQ(dst->size(), 6);

    // 5 subcolumn
    EXPECT_EQ(dst->subcolumns.size(), 6);

    for (const auto& column : dst->subcolumns) {
        if (column->data.is_root) {
            EXPECT_EQ(column->data.data.size(), 1);
            EXPECT_EQ(column->data.data[0]->size(), 6);
            EXPECT_EQ(column->data.get_non_null_value_size(), 1);
            continue;
        }
        EXPECT_EQ(column->data.data.size(), 1);
        EXPECT_EQ(column->data.data[0]->size(), 6);
        EXPECT_EQ(column->data.get_non_null_value_size(), 5);
    }

    // empty sparse column
    const auto& [path, value] = dst->get_sparse_data_paths_and_values();
    const auto& offsets = dst->serialized_sparse_column_offsets();
    EXPECT_EQ(offsets[4], offsets[-1]);
    EXPECT_EQ(path->size(), value->size());

    auto src_contains_seven_subcolumns = VariantUtil::construct_varint_column_more_subcolumns();

    src_contains_seven_subcolumns->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
    EXPECT_TRUE(src_contains_seven_subcolumns->pick_subcolumns_to_sparse_column({}, false).ok());
    EXPECT_EQ(src_contains_seven_subcolumns->size(), 5);

    // subcolumn->subcolumn          v.a v.b v.c v.f v.e
    // add sprase columns            v.s v.x v.y v.z
    dst->insert_range_from(*src_contains_seven_subcolumns, 0, 5);
    EXPECT_EQ(dst->size(), 11);

    // 5 subcolumn
    EXPECT_EQ(dst->subcolumns.size(), 6);

    for (int row = 0; row < 6; ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];

        EXPECT_EQ(start, end);
    }

    // v.s v.x v.y v.z
    for (int row = 6; row < 11; ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];

        auto data0 = path->get_data_at(start);
        EXPECT_EQ(data0, StringRef("v.s", 3));
        auto pair0 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(convert_field_to_string(pair0.first),
                  convert_field_to_string(VariantUtil::get_field("string")));

        auto data = path->get_data_at(start);
        EXPECT_EQ(data, StringRef("v.x", 3));
        auto pair = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(pair.first.get<Int16>(), std::numeric_limits<Int16>::max());

        auto data2 = path->get_data_at(start);
        EXPECT_EQ(data2, StringRef("v.y", 3));
        auto pair2 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(pair2.first.get<Int32>(), std::numeric_limits<Int32>::max());

        auto data3 = path->get_data_at(start);
        EXPECT_EQ(data3, StringRef("v.z", 3));
        auto pair3 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(pair3.first.get<Int64>(),
                  Int64(static_cast<Int64>(std::numeric_limits<Int32>::max()) + 1));

        EXPECT_EQ(start, end);
    }

    auto src_contains_subcoumns_and_sparse_columns = VariantUtil::construct_basic_varint_column();
    src_contains_subcoumns_and_sparse_columns->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
    EXPECT_TRUE(
            src_contains_subcoumns_and_sparse_columns->pick_subcolumns_to_sparse_column({}, false)
                    .ok());
    EXPECT_EQ(src_contains_subcoumns_and_sparse_columns->size(), 10);

    // subcolumn->subcolumn          v.a v.b v.c v.f v.e
    // add sprase columns            v.s v.x v.y v.b.d v.c.d v.d.d
    dst->insert_range_from(*src_contains_subcoumns_and_sparse_columns, 0, 10);
    EXPECT_EQ(dst->size(), 21);

    // 5 subcolumn
    EXPECT_EQ(dst->subcolumns.size(), 6);

    for (int row = 0; row < 6; ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];

        EXPECT_EQ(start, end);
    }

    // v.x v.y
    for (int row = 6; row < 11; ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];

        auto data0 = path->get_data_at(start);
        EXPECT_EQ(data0, StringRef("v.s", 3));
        auto pair0 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(convert_field_to_string(pair0.first),
                  convert_field_to_string(VariantUtil::get_field("string")));

        auto data = path->get_data_at(start);
        EXPECT_EQ(data, StringRef("v.x", 3));
        auto pair = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(pair.first.get<Int16>(), std::numeric_limits<Int16>::max());

        auto data2 = path->get_data_at(start);
        EXPECT_EQ(data2, StringRef("v.y", 3));
        auto pair2 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(pair2.first.get<Int32>(), std::numeric_limits<Int32>::max());

        auto data3 = path->get_data_at(start);
        EXPECT_EQ(data3, StringRef("v.z", 3));
        auto pair3 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(pair3.first.get<Int64>(),
                  Int64(static_cast<Int64>(std::numeric_limits<Int32>::max()) + 1));

        EXPECT_EQ(start, end);
    }

    for (int row = 11; row < 16; ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];

        EXPECT_EQ(start, end);
    }

    //v.b.d v.c.d v.d.d
    for (int row = 16; row < 21; ++row) {
        size_t start = offsets[row - 1];
        size_t end = offsets[row];

        auto data = path->get_data_at(start);
        EXPECT_EQ(data, StringRef("v.b.d", 5));
        auto pair = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(pair.first.get<Int64>(), 30);

        auto data2 = path->get_data_at(start);
        auto pair2 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data2, StringRef("v.c.d", 5));
        EXPECT_EQ(pair2.first.get<Int64>(), 30);

        auto data3 = path->get_data_at(start);
        auto pair3 = dst->deserialize_from_sparse_column(value, start++);
        EXPECT_EQ(data3, StringRef("v.d.d", 5));
        EXPECT_EQ(pair3.first.get<String>(), "50");
        EXPECT_EQ(start, end);
    }
}

//TEST_F(ColumnVariantTest, insert_null_to_decimal_column) {
//    ColumnVariant::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);
//    Field null_field;
//    subcolumn.insert(null_field);
//    subcolumn.finalize();
//    EXPECT_EQ(subcolumn.data.size(), 1);
//    EXPECT_EQ(subcolumn.data[0]->size(), 1);
//    EXPECT_EQ(subcolumn.data_types.size(), 1);
//    // NOTICE: here is invalid not NULL
//    EXPECT_EQ(subcolumn.least_common_type.get_base_type_id(), PrimitiveType::INVALID_TYPE);
//    Field decimal_field = Field::create_field<TYPE_DECIMALV2>(DecimalField<Decimal128V2>(Decimal128V2(20), 9));
//    subcolumn.insert(decimal_field);
//    subcolumn.finalize();
//    EXPECT_EQ(subcolumn.get_non_null_value_size(), 1);
//    EXPECT_EQ(subcolumn.data.size(), 1);
//    EXPECT_EQ(subcolumn.data[0]->size(), 2);
//    EXPECT_EQ(subcolumn.data[0]->is_null_at(0), true);
//    EXPECT_EQ(subcolumn.data[0]->is_null_at(1), false);
//    EXPECT_EQ(subcolumn.data_types.size(), 1);
//    EXPECT_EQ(subcolumn.least_common_type.get_base_type_id(), PrimitiveType::TYPE_DECIMALV2);
//}

TEST_F(ColumnVariantTest, test_insert_indices_from) {
    // Test case 1: Insert from scalar variant source to empty destination
    {
        // Create source column with scalar values
        auto src_column = ColumnVariant::create(true);
        VariantUtil::insert_root_scalar_field(*src_column, Field::create_field<TYPE_INT>(123));
        VariantUtil::insert_root_scalar_field(*src_column, Field::create_field<TYPE_INT>(456));
        src_column->finalize();
        EXPECT_TRUE(src_column->is_scalar_variant());
        EXPECT_TRUE(src_column->is_finalized());
        EXPECT_EQ(src_column->size(), 2);

        // Create empty destination column
        auto dst_column = ColumnVariant::create(true);
        EXPECT_EQ(dst_column->size(), 0);

        // Create indices
        std::vector<uint32_t> indices = {0, 1};

        // Insert using indices
        dst_column->insert_indices_from(*src_column, indices.data(),
                                        indices.data() + indices.size());

        // Verify results
        EXPECT_EQ(dst_column->size(), 2);
        EXPECT_TRUE(dst_column->is_scalar_variant());
        EXPECT_TRUE(dst_column->is_finalized());
        EXPECT_EQ(dst_column->get_root_type()->get_name(), src_column->get_root_type()->get_name());

        Field result1;
        dst_column->get(0, result1);

        const auto& fv = result1.get<const VariantMap&>();
        auto res = fv.at(PathInData());
        EXPECT_EQ(res.field.get<Int64>(), 123);

        Field result2;
        dst_column->get(1, result2);
        EXPECT_EQ(result2.get<VariantMap>().at(PathInData()).field.get<Int64>(), 456);
    }

    // Test case 2: Insert from scalar variant source to non-empty destination of same type
    {
        // Create source column with scalar values
        auto src_column = ColumnVariant::create(true);
        VariantUtil::insert_root_scalar_field(*src_column, Field::create_field<TYPE_INT>(123));
        VariantUtil::insert_root_scalar_field(*src_column, Field::create_field<TYPE_INT>(456));
        src_column->finalize();
        EXPECT_TRUE(src_column->is_scalar_variant());

        // Create destination column with same type
        auto dst_column = ColumnVariant::create(true);
        VariantUtil::insert_root_scalar_field(*dst_column, Field::create_field<TYPE_INT>(789));
        dst_column->finalize();
        EXPECT_TRUE(dst_column->is_scalar_variant());
        EXPECT_EQ(dst_column->size(), 1);

        // Create indices for selecting specific elements
        std::vector<uint32_t> indices = {1, 0};

        // Insert using indices (reversed order)
        dst_column->insert_indices_from(*src_column, indices.data(),
                                        indices.data() + indices.size());

        // Verify results
        EXPECT_EQ(dst_column->size(), 3);

        Field result1, result2, result3;
        dst_column->get(0, result1);
        dst_column->get(1, result2);
        dst_column->get(2, result3);

        EXPECT_EQ(result1.get<VariantMap>().at(PathInData()).field.get<Int64>(), 789);
        EXPECT_EQ(result2.get<VariantMap>().at(PathInData()).field.get<Int64>(), 456);
        EXPECT_EQ(result3.get<VariantMap>().at(PathInData()).field.get<Int64>(), 123);
    }

    // Test case 3: Insert from non-scalar or different type source (fallback to try_insert)
    {
        // Create source column with object values (non-scalar)
        auto src_column = ColumnVariant::create(true);

        // Create a map with {"a": 123}
        Field field_map = Field::create_field<TYPE_VARIANT>(VariantMap());
        auto& map1 = field_map.get<VariantMap&>();
        map1.insert_or_assign(PathInData("a"),
                              FieldWithDataType {.field = Field::create_field<TYPE_INT>(123),
                                                 .base_scalar_type_id = PrimitiveType::TYPE_INT});
        src_column->try_insert(field_map);

        // Create another map with {"b": "hello"}
        field_map = Field::create_field<TYPE_VARIANT>(VariantMap());
        auto& map2 = field_map.get<VariantMap&>();
        map2.insert_or_assign(
                PathInData("b"),
                FieldWithDataType {.field = Field::create_field<TYPE_STRING>(String("hello")),
                                   .base_scalar_type_id = PrimitiveType::TYPE_STRING});
        src_column->try_insert(field_map);

        src_column->finalize();
        EXPECT_FALSE(src_column->is_scalar_variant());

        // Create destination column (empty)
        auto dst_column = ColumnVariant::create(true);

        // Create indices
        std::vector<uint32_t> indices = {1, 0};

        // Insert using indices
        dst_column->insert_indices_from(*src_column, indices.data(),
                                        indices.data() + indices.size());

        // Verify results
        EXPECT_EQ(dst_column->size(), 2);

        Field result1, result2;
        dst_column->get(0, result1);
        dst_column->get(1, result2);

        EXPECT_TRUE(result1.get_type() == PrimitiveType::TYPE_VARIANT);
        EXPECT_TRUE(result2.get_type() == PrimitiveType::TYPE_VARIANT);

        const auto& result1_map = result1.get<const VariantMap&>();
        const auto& result2_map = result2.get<const VariantMap&>();

        EXPECT_EQ(result1_map.at(PathInData("b")).field.get<const String&>(), "hello");
        EXPECT_EQ(result2_map.at(PathInData("a")).field.get<Int64>(), 123);
    }
}

TEST_F(ColumnVariantTest, is_variable_length) {
    EXPECT_TRUE(column_variant->is_variable_length());
}

TEST_F(ColumnVariantTest, byte_size) {
    hash_common_test("byte_size", assert_byte_size_with_file_callback);
}

//TEST_F(ColumnVariantTest, has_enough_capacity) {
//    auto test_func = [](const auto& src_col) {
//        auto src_size = src_col->size();
//        // variant always return fasle
//        auto assert_col = src_col->clone_empty();
//        ASSERT_FALSE(assert_col->has_enough_capacity(*src_col));
//        assert_col->reserve(src_size);
//        ASSERT_FALSE(assert_col->has_enough_capacity(*src_col));
//    };
//    test_func(column_variant);
//}

TEST_F(ColumnVariantTest, allocated_bytes) {
    hash_common_test("allocated_bytes", assert_allocated_bytes_with_file_callback);
}

TEST_F(ColumnVariantTest, clone_resized) {
    auto src_size = column_variant->size();
    auto test_func = [&](size_t clone_count) {
        auto target_column = column_variant->clone_resized(clone_count);
        EXPECT_NE(target_column.get(), column_variant.get());
        EXPECT_EQ(target_column->size(), clone_count);
        size_t same_count = std::min(clone_count, src_size);
        size_t i = 0;
        for (; i < same_count; ++i) {
            checkField(*target_column, *column_variant, i, i);
        }
        for (; i < clone_count; ++i) {
            // more than source size
            Field target_field;
            Field source_field = column_variant->get_root_type()->get_default();
            target_column->get(i, target_field);
            EXPECT_EQ(target_field, source_field)
                    << "target_field: " << target_field.get_type_name()
                    << ", source_field: " << source_field.get_type_name();
        }
    };
    test_func(0);
    test_func(3);
    test_func(src_size);
    test_func(src_size + 10);
    // test clone_empty

    auto target_column = column_variant->clone_empty();
    EXPECT_NE(target_column.get(), column_variant.get());
    // assert subcolumns
    auto target_subcolumns = assert_cast<ColumnVariant*>(target_column.get())->get_subcolumns();
    // always has root for ColumnVariant(0)
    EXPECT_EQ(target_subcolumns.size(), 1);
}
TEST_F(ColumnVariantTest, field_test) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        {
            auto assert_col = source_column->clone();
            for (size_t i = 0; i != src_size; ++i) {
                Field f;
                source_column->get(i, f);
                assert_col->insert(f);
            }
            for (size_t i = 0; i != src_size; ++i) {
                Field assert_field;
                assert_col->get(i, assert_field);
                Field source_field;
                source_column->get(i, source_field);
                ASSERT_EQ(assert_field, source_field);
            }
        }
        {
            auto assert_col = source_column->clone();
            std::cout << source_column->size() << std::endl;
            for (size_t i = 0; i != src_size; ++i) {
                VariantMap jsonbf;
                Field f = Field::create_field<TYPE_VARIANT>(std::move(jsonbf));
                source_column->get(i, f);
                assert_col->insert(f);
            }
            for (size_t i = 0; i != src_size; ++i) {
                VariantMap jsonbf;
                Field f = Field::create_field<TYPE_VARIANT>(std::move(jsonbf));
                assert_col->get(i, f);
                Field source_field;
                source_column->get(i, source_field);
                EXPECT_EQ(f, source_field);
            }
        }
    };
    ColumnVariant::MutablePtr obj;
    obj = ColumnVariant::create(1);
    MutableColumns cols;
    cols.push_back(obj->get_ptr());
    const auto& json_file_obj = test_data_dir_json + "json_variant/object_boundary.jsonl";
    load_columns_data_from_file(cols, serde, '\n', {0}, json_file_obj);
    EXPECT_TRUE(!obj->empty());
    test_func(obj);
}

// is seri
TEST_F(ColumnVariantTest, is_column_string64) {
    EXPECT_FALSE(column_variant->is_column_string64());
}

TEST_F(ColumnVariantTest, is_column_string) {
    EXPECT_FALSE(column_variant->is_column_string());
}

TEST_F(ColumnVariantTest, serialize_one_row_to_string) {
    {
        const auto* variant = assert_cast<const ColumnVariant*>(column_variant.get());
        // Serialize hierarchy types to json format
        std::string buffer;
        for (size_t row_idx = 2000; row_idx < variant->size(); ++row_idx) {
            variant->serialize_one_row_to_string(row_idx, &buffer);
        }
        {
            // TEST buffer
            auto tmp_col = ColumnString::create();
            VectorBufferWriter write_buffer(*tmp_col.get());
            for (size_t row_idx = 2000; row_idx < variant->size(); ++row_idx) {
                variant->serialize_one_row_to_string(row_idx, write_buffer);
            }
        }
    }
    {
        // TEST SCALA_VARAINT
        // 1. create an empty variant column
        auto v = ColumnVariant::create(true);
        auto dt = DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_STRING, 0,
                                                               0);
        auto cs = dt->create_column();
        cs->insert(vectorized::Field::create_field<TYPE_STRING>("amory"));
        cs->insert(vectorized::Field::create_field<TYPE_STRING>("doris"));
        v->create_root(dt, std::move(cs));
        EXPECT_TRUE(v->is_scalar_variant());

        // 3. serialize
        std::string buf2;
        for (size_t row_idx = 0; row_idx < v->size(); ++row_idx) {
            v->serialize_one_row_to_string(row_idx, &buf2);
        }
        auto tmp_col = ColumnString::create();
        VectorBufferWriter write_buffer(*tmp_col.get());
        for (size_t row_idx = 0; row_idx < v->size(); ++row_idx) {
            v->serialize_one_row_to_string(row_idx, write_buffer);
        }
    }
}
// insert interface
// not implemented: insert_many_fix_len_data, insert_many_dict_data, insert_many_continuous_binary_data, insert_from_multi_column
// insert_many_strings, insert_many_strings_overflow, insert_range_from_ignore_overflow, insert_many_raw_data, insert_data, get_data_at, replace_column_data,
// serialize_value_into_arena, deserialize_and_insert_from_arena
TEST_F(ColumnVariantTest, insert_many_fix_len_data) {
    EXPECT_ANY_THROW(column_variant->insert_many_fix_len_data(nullptr, 0));
}

TEST_F(ColumnVariantTest, insert_many_dict_data) {
    EXPECT_ANY_THROW(column_variant->insert_many_dict_data(nullptr, 0, nullptr, 0, 0));
}

TEST_F(ColumnVariantTest, insert_many_continuous_binary_data) {
    EXPECT_ANY_THROW(column_variant->insert_many_continuous_binary_data(nullptr, 0, 0));
}

//TEST_F(ColumnVariantTest, insert_from_multi_column) {
//    EXPECT_ANY_THROW(column_variant->insert_from_multi_column({column_variant.get()}, {0}));
//}

TEST_F(ColumnVariantTest, insert_many_strings) {
    EXPECT_ANY_THROW(column_variant->insert_many_strings(nullptr, 0));
}

TEST_F(ColumnVariantTest, insert_many_strings_overflow) {
    EXPECT_ANY_THROW(column_variant->insert_many_strings_overflow(nullptr, 0, 0));
}

TEST_F(ColumnVariantTest, insert_many_raw_data) {
    EXPECT_ANY_THROW(column_variant->insert_many_raw_data(nullptr, 0));
}

TEST_F(ColumnVariantTest, insert_data) {
    EXPECT_ANY_THROW(column_variant->insert_data(nullptr, 0));
}

TEST_F(ColumnVariantTest, get_data_at) {
    EXPECT_ANY_THROW(column_variant->get_data_at(0));
}

TEST_F(ColumnVariantTest, replace_column_data) {
    EXPECT_ANY_THROW(
            column_variant->replace_column_data(column_variant->assume_mutable_ref(), 0, 0));
}

TEST_F(ColumnVariantTest, serialize_value_into_arena) {
    Arena a;
    const char* begin = nullptr;
    EXPECT_ANY_THROW(column_variant->serialize_value_into_arena(0, a, begin));
}

TEST_F(ColumnVariantTest, deserialize_and_insert_from_arena) {
    EXPECT_ANY_THROW(column_variant->deserialize_and_insert_from_arena(nullptr));
}

// insert series:
// insert_from, insert_many_from, insert_range_from, insert_range_from_ignore_overflow, insert_indices_from
// insert_default, insert_many_defaults
TEST_F(ColumnVariantTest, insert_many_from) {
    assert_insert_many_from_with_field_callback(column_variant->get_ptr());
}

TEST_F(ColumnVariantTest, insert_from) {
    assert_insert_from_with_field_callback(column_variant->get_ptr());
}

TEST_F(ColumnVariantTest, insert_range_from) {
    // insert_range_from_ignore_overflow call insert_range_from
    assert_insert_range_from_with_field_callback(column_variant->get_ptr());
}

TEST_F(ColumnVariantTest, insert_indices_from) {
    assert_insert_indices_from_with_field_callback(column_variant->get_ptr());
}

TEST_F(ColumnVariantTest, insert_default_insert_many_defaults) {
    assert_insert_default_with_field_callback(column_variant->get_ptr());
}

TEST_F(ColumnVariantTest, get_name) {
    EXPECT_TRUE(column_variant->get_name().find("variant") != std::string::npos);
}

// pop_back interface
TEST_F(ColumnVariantTest, pop_back_test) {
    assert_pop_back_with_field_callback(column_variant->get_ptr());
}

// serialize and deserialize is not implemented
// serialize_vec, deserialize_vec, serialize_vec_with_null_map, deserialize_vec_with_null_map, get_max_row_byte_size
// hash interface
TEST_F(ColumnVariantTest, update_xxHash_with_value) {
    hash_common_test("update_xxHash_with_value", assert_update_xxHash_with_value_callback);
}

// hang
//TEST_F(ColumnVariantTest, update_sip_hash_with_value_test) {
//    hash_common_test("update_sip_hash_with_value",
//                     assert_column_vector_update_siphashes_with_value_callback);
//}
TEST_F(ColumnVariantTest, update_hashes_with_value_test) {
    hash_common_test("update_hashes_with_value",
                     assert_column_vector_update_hashes_with_value_callback);
}
TEST_F(ColumnVariantTest, update_crc_with_value_test) {
    hash_common_test("update_crc_with_value", assert_update_crc_with_value_callback);
}
TEST_F(ColumnVariantTest, update_crcs_with_value_test) {
    std::string function_name = "update_crcs_with_value";
    MutableColumns columns;
    columns.push_back(column_variant->get_ptr());
    DataTypeSerDeSPtrs serdes = {dt_variant->get_serde()};
    std::vector<PrimitiveType> pts(columns.size(), PrimitiveType::TYPE_VARIANT);
    assert_column_vector_update_crc_hashes_callback(
            columns, serdes, pts, test_result_dir + "/column_variant_" + function_name + ".out");
}

// filter interface
TEST_F(ColumnVariantTest, filter) {
    assert_filter_with_field_callback(column_variant->get_ptr());
}

TEST_F(ColumnVariantTest, filter_by_selector) {
    auto test_func = [&](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size <= UINT16_MAX);

        auto target_column = source_column->clone_empty();

        std::vector<uint16_t> indices(src_size);
        std::iota(indices.begin(), indices.end(), 0);
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(indices.begin(), indices.end(), g);
        size_t sel_size = src_size / 2;
        indices.resize(sel_size);
        std::sort(indices.begin(), indices.end());

        EXPECT_ANY_THROW(Status st = source_column->filter_by_selector(indices.data(), 0,
                                                                       target_column.get()));
    };
    test_func(column_variant);
}
TEST_F(ColumnVariantTest, permute) {
    {
        // test empty column and limit == 0
        IColumn::Permutation permutation(0);
        auto col = column_variant->clone_empty();
        col->permute(permutation, 0);
        EXPECT_EQ(col->size(), 0);
    }
    {
        IColumn::Permutation permutation(0);
        EXPECT_THROW(column_variant->permute(permutation, 10), Exception);
    }

    MutableColumns columns;
    columns.push_back(column_variant->get_ptr());
    assert_column_vector_permute(columns, 0, false);
    assert_column_vector_permute(columns, 1, false);
    assert_column_vector_permute(columns, column_variant->size(), false);
    assert_column_vector_permute(columns, UINT64_MAX, false);
}

// not support
TEST_F(ColumnVariantTest, get_permutation) {
    EXPECT_ANY_THROW(assert_column_permutations2(*column_variant, dt_variant));
}
TEST_F(ColumnVariantTest, structure_equals) {
    auto cl = column_variant->clone_empty();
    EXPECT_ANY_THROW(column_variant->structure_equals(*cl));
}

// Compare Interface not implement: compare_at, compare_internal
TEST_F(ColumnVariantTest, compare_at) {
    EXPECT_ANY_THROW(column_variant->compare_at(0, 0, *column_variant, -1));
    std::vector<uint8_t> com_res(column_variant->size());
    EXPECT_ANY_THROW(column_variant->compare_internal(0, *column_variant, 0, 0, com_res, nullptr));
}

TEST_F(ColumnVariantTest, clear) {
    auto tmp_col = column_variant->clone();
    EXPECT_EQ(tmp_col->size(), column_variant->size());

    tmp_col->clear();
    EXPECT_EQ(tmp_col->size(), 0);
}

TEST_F(ColumnVariantTest, convert_column_if_overflow) {
    // convert_column_if_overflow may need impl in ColumnVariant, like ColumnArray?
    auto ret = column_variant->convert_column_if_overflow();
    EXPECT_EQ(ret.get(), column_variant.get());
}

TEST_F(ColumnVariantTest, resize) {
    auto test_func = [](const auto& source_column, size_t add_count) {
        {
            auto source_size = source_column->size();
            auto tmp_col = source_column->clone();
            auto default_col = source_column->clone_empty();
            default_col->insert_default();
            tmp_col->resize(source_size + add_count);
            EXPECT_EQ(tmp_col->size(), source_size + add_count);
            for (size_t i = 0; i != source_size; ++i) {
                checkField(*tmp_col, *source_column, i, i);
            }
            for (size_t i = 0; i != add_count; ++i) {
                checkField(*tmp_col, *default_col, source_size + i, 0);
            }
        }
        {
            // resize in self
            auto ptr = source_column.get();
            source_column->resize(add_count);
            EXPECT_EQ(source_column.get(), ptr);
            EXPECT_EQ(source_column->size(), add_count);
        }
    };
    test_func(column_variant, 0);
    test_func(column_variant, 10);
}

// ================= variant specific interface =================
// meta info related interface
TEST_F(ColumnVariantTest, get_least_common_type) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test get_least_common_type for root column
        const auto& root = source_column->get_subcolumns().get_root();
        EXPECT_TRUE(root != nullptr);
        EXPECT_TRUE(root->data.get_least_common_type() != nullptr);

        // Test get_least_common_type for subcolumns
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);
            EXPECT_TRUE(subcolumn->data.get_least_common_type() != nullptr);
        }
    };
    test_func(column_variant);
}

TEST_F(ColumnVariantTest, get_dimensions) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test get_dimensions for root column
        const auto& root = source_column->get_subcolumns().get_root();
        EXPECT_TRUE(root != nullptr);
        EXPECT_GE(root->data.get_dimensions(), 0);

        // Test get_dimensions for subcolumns
        for (auto& entry : source_column->get_subcolumns()) {
            EXPECT_TRUE(entry != nullptr);
            EXPECT_GE(entry->data.get_dimensions(), 0);
        }
    };
    test_func(column_variant);
}

TEST_F(ColumnVariantTest, get_last_field) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test get_last_field for root column
        const auto& root = source_column->get_subcolumns().get_root();
        EXPECT_TRUE(root != nullptr);
        Field last_field;
        root->data.get_last_field();

        // Test get_last_field for subcolumns
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);
            subcolumn->data.get_last_field();
        }
    };
    test_func(column_variant);
}

// sub column op related interface
TEST_F(ColumnVariantTest, get_finalized_column) {
    auto test_func = [](const auto& source_column) {
        // do not clone and then get , will case heap-after-use-free cause of defined in COW as temporary Ptr
        // auto source_column = assert_cast<ColumnVariant*>(var_column->clone_resized(var_column->size()).get());
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);
        // Test get_finalized_column for root column
        auto root = source_column->get_subcolumns().get_root();
        EXPECT_TRUE(root != nullptr);
        source_column->finalize();
        root = source_column->get_subcolumns().get_root();
        const auto& finalized_col = root->data.get_finalized_column();
        EXPECT_TRUE(source_column->is_finalized());
        Field rf;
        finalized_col.get(0, rf);
        EXPECT_TRUE(std::strlen(rf.get_type_name().c_str()) > 0);

        // Test get_finalized_column for subcolumns
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);
            EXPECT_TRUE(subcolumn->data.is_finalized());
            const auto& subcolumn_finalized = subcolumn->data.get_finalized_column();

            // Verify finalized column data
            Field field;
            subcolumn_finalized.get(0, field);
            EXPECT_TRUE(std::strlen(rf.get_type_name().c_str()) > 0);
            // Verify column size
            EXPECT_EQ(subcolumn_finalized.size(), src_size);
        }
    };

    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnVariant*>(temp.get());
    EXPECT_NE(cloned_object, column_variant.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnVariantTest, get_finalized_column_ptr) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);
        // Test get_finalized_column_ptr for root column
        auto root = source_column->get_subcolumns().get_root();
        EXPECT_TRUE(root != nullptr);
        source_column->finalize();
        // when finalized , the root will be changed
        root = source_column->get_subcolumns().get_root();
        const auto& finalized_col_ptr = root->data.get_finalized_column_ptr();
        EXPECT_TRUE(finalized_col_ptr.get() != nullptr);

        // Test get_finalized_column_ptr for subcolumns
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);
            const auto& subcolumn_finalized_ptr = subcolumn->data.get_finalized_column_ptr();
            EXPECT_TRUE(subcolumn_finalized_ptr.get() != nullptr);
            EXPECT_TRUE(subcolumn->data.is_finalized());

            // Verify finalized column data
            Field field;
            subcolumn_finalized_ptr->get(0, field);
            EXPECT_TRUE(std::strlen(field.get_type_name().c_str()) > 0);
            // Verify column size
            EXPECT_EQ(subcolumn_finalized_ptr->size(), src_size);
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnVariant*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnVariantTest, remove_nullable) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test remove_nullable for subcolumns
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);
            subcolumn->data.finalize();
            auto subcolumn_type_before = subcolumn->data.get_least_common_type();
            subcolumn->data.remove_nullable();
            auto subcolumn_type_after = subcolumn->data.get_least_common_type();
            EXPECT_TRUE(remove_nullable(subcolumn_type_before)->equals(*subcolumn_type_after));
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnVariant*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnVariantTest, add_new_column_part) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test add_new_column_part for subcolumns
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);

            // Store original type before adding new part
            auto original_type = subcolumn->data.get_least_common_type();

            // The add_new_column_part interface must be added to the minimum common type of the data type vector in the current subcolumn,
            // otherwise an error will be reported: [E3] Not implemeted
            subcolumn->data.add_new_column_part(original_type);
            // Verify the type is updated
            auto updated_type = subcolumn->data.get_least_common_type();
            EXPECT_TRUE(updated_type != nullptr);
            // Verify column size
            EXPECT_EQ(subcolumn->data.size(), src_size);
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnVariant*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnVariantTest, get_subcolumn) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test get_subcolumn
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);

            // Verify subcolumn properties
            EXPECT_TRUE(subcolumn->data.get_least_common_type() != nullptr);
            EXPECT_GE(subcolumn->data.get_dimensions(), 0);

            // Verify subcolumn data
            FieldWithDataType field;
            subcolumn->data.get(0, field);
            EXPECT_TRUE(std::strlen(field.field.get_type_name().c_str()) > 0);
            EXPECT_EQ(subcolumn->data.size(), src_size);
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnVariant*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnVariantTest, ensure_root_node_type) {
    ColumnVariant::MutablePtr obj;
    obj = ColumnVariant::create(1);
    MutableColumns cols;
    cols.push_back(obj->get_ptr());
    const auto& json_file_obj = test_data_dir_json + "json_variant/object_boundary.jsonl";
    load_columns_data_from_file(cols, serde, '\n', {0}, json_file_obj);
    EXPECT_TRUE(!obj->empty());
    // Store original root type
    auto root = obj->get_subcolumns().get_root();
    auto original_root_type = root->data.get_least_common_type();
    obj->finalize();

    // Test ensure_root_node_type
    auto new_type =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
    obj->ensure_root_node_type(new_type);

    // Verify root type is updated
    root = obj->get_subcolumns().get_root();
    auto updated_root_type = root->data.get_least_common_type();
    EXPECT_TRUE(updated_root_type->equals(*new_type));
};

TEST_F(ColumnVariantTest, create_root) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test case 1: Create root with string type
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnVariant*>(col.get());
            auto type = DataTypeFactory::instance().create_data_type(
                    FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
            auto column = type->create_column();
            obj->create_root(type, std::move(column));

            // Verify root is created with correct type
            const auto& root = obj->get_subcolumns().get_root();
            EXPECT_TRUE(root != nullptr);
            EXPECT_TRUE(root->data.get_least_common_type()->equals(*type));
        }

        // Test case 2: Create root with int type
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnVariant*>(col.get());
            auto type = DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_INT,
                                                                     0, 0);
            auto column = type->create_column();
            obj->create_root(type, std::move(column));

            // Verify root is created with correct type
            const auto& root = obj->get_subcolumns().get_root();
            EXPECT_TRUE(root != nullptr);
            EXPECT_TRUE(root->data.get_least_common_type()->equals(*type));
        }

        // Test case 3: Create root on existing column
        {
            auto col = source_column->clone();
            auto obj = assert_cast<ColumnVariant*>(col.get());
            auto original_root = obj->get_subcolumns().get_root();
            EXPECT_TRUE(original_root != nullptr);

            // Create root with new type
            auto type = DataTypeFactory::instance().create_data_type(
                    FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
            auto column = type->create_column();
            EXPECT_ANY_THROW(obj->create_root(type, std::move(column)));

            // Verify root is replaced with new type
            const auto& new_root = obj->get_subcolumns().get_root();
            EXPECT_TRUE(new_root != nullptr);
            EXPECT_EQ(new_root, original_root);
        }

        // Test case 4: Create root and verify data operations
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnVariant*>(col.get());
            auto type = DataTypeFactory::instance().create_data_type(
                    FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
            auto column = type->create_column();
            obj->create_root(type, std::move(column));

            // Insert some data
            Field field;
            source_column->get(0, field);
            obj->insert(field);

            // Verify data is inserted correctly
            Field inserted_field;
            obj->get(0, inserted_field);
        }

        // Test case 5: Create root with nullable type
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnVariant*>(col.get());
            auto type = DataTypeFactory::instance().create_data_type(
                    FieldType::OLAP_FIELD_TYPE_STRING, 0, 0);
            auto nullable_type = make_nullable(type);
            auto column = nullable_type->create_column();
            obj->create_root(nullable_type, std::move(column));

            // Verify root is created with nullable type
            const auto& root = obj->get_subcolumns().get_root();
            EXPECT_TRUE(root != nullptr);
            EXPECT_TRUE(root->data.get_least_common_type()->equals(*nullable_type));
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnVariant*>(temp.get());
    test_func(std::move(cloned_object));
}
TEST_F(ColumnVariantTest, get_most_common_type) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test get_most_common_type
        DataTypePtr most_common_type = source_column->get_most_common_type();
        EXPECT_TRUE(most_common_type != nullptr);
    };
    test_func(column_variant);
}

TEST_F(ColumnVariantTest, is_null_root) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test is_null_root
        bool is_null = source_column->is_null_root();
        EXPECT_FALSE(is_null); // Since we have data, root should not be null
    };
    test_func(column_variant);
}

TEST_F(ColumnVariantTest, is_scalar_variant) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test is_scalar_variant
        bool is_scalar = source_column->is_scalar_variant();
        // The result depends on the actual data structure
        EXPECT_FALSE(is_scalar);
    };
    test_func(column_variant);
}

TEST_F(ColumnVariantTest, is_exclusive) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test is_exclusive
        bool is_exclusive = source_column->is_exclusive();
        // The result depends on the actual data structure
        EXPECT_TRUE(is_exclusive);
    };
    test_func(column_variant);
}

TEST_F(ColumnVariantTest, get_root_type) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test get_root_type
        DataTypePtr root_type = source_column->get_root_type();
        EXPECT_TRUE(root_type != nullptr);
    };
    test_func(column_variant);
}

TEST_F(ColumnVariantTest, has_subcolumn) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test has_subcolumn
        for (const auto& subcolumn : source_column->get_subcolumns()) {
            EXPECT_TRUE(subcolumn != nullptr);
            bool has_subcolumn = source_column->has_subcolumn(subcolumn->path);
            EXPECT_TRUE(has_subcolumn);
        }
    };
    test_func(column_variant);
}

TEST_F(ColumnVariantTest, finalize) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test case 1: Test finalize with READ_MODE
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnVariant*>(col.get());

            // Insert data from source column
            for (size_t i = 0; i < src_size; ++i) {
                Field field;
                source_column->get(i, field);
                obj->insert(field);
            }

            // Verify initial state
            EXPECT_FALSE(obj->is_finalized());

            // Finalize in READ_MODE
            obj->finalize(ColumnVariant::FinalizeMode::READ_MODE);
            EXPECT_TRUE(obj->is_finalized());

            // Verify data integrity
            for (size_t i = 0; i < src_size; ++i) {
                Field original_field, finalized_field;
                source_column->get(i, original_field);
                obj->get(i, finalized_field);
                EXPECT_EQ(original_field, finalized_field);
            }
        }

        // Test case 2: Test finalize with WRITE_MODE
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnVariant*>(col.get());

            // Insert data from source column
            for (size_t i = 0; i < src_size; ++i) {
                Field field;
                source_column->get(i, field);
                obj->insert(field);
            }

            // Verify initial state
            EXPECT_FALSE(obj->is_finalized());

            // Finalize in WRITE_MODE
            obj->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
            EXPECT_TRUE(obj->is_finalized());

            // Verify data integrity
            for (size_t i = 0; i < src_size; ++i) {
                Field original_field, finalized_field;
                source_column->get(i, original_field);
                obj->get(i, finalized_field);
                EXPECT_EQ(original_field, finalized_field);
            }
        }

        // Test case 3: Test finalize without mode (default READ_MODE)
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnVariant*>(col.get());

            // Insert data from source column
            for (size_t i = 0; i < src_size; ++i) {
                Field field;
                source_column->get(i, field);
                obj->insert(field);
            }

            // Verify initial state
            EXPECT_FALSE(obj->is_finalized());

            // Finalize without mode
            obj->finalize();
            EXPECT_TRUE(obj->is_finalized());

            // Verify data integrity
            for (size_t i = 0; i < src_size; ++i) {
                Field original_field, finalized_field;
                source_column->get(i, original_field);
                obj->get(i, finalized_field);
                EXPECT_EQ(original_field, finalized_field);
            }
        }

        // Test case 4: Test finalize on empty column
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnVariant*>(col.get());

            // Empty column always finalized
            EXPECT_TRUE(obj->is_finalized());

            // Finalize empty column
            obj->finalize(ColumnVariant::FinalizeMode::READ_MODE);
            EXPECT_TRUE(obj->is_finalized());
            EXPECT_EQ(obj->size(), 0);
        }

        // Test case 5: Test finalize preserves column structure
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnVariant*>(col.get());

            // Insert data from source column
            for (size_t i = 0; i < src_size; ++i) {
                Field field;
                source_column->get(i, field);
                obj->insert(field);
            }

            // Verify initial state
            EXPECT_FALSE(obj->is_finalized());

            // Store original structure
            auto original_subcolumns = obj->get_subcolumns();

            // Finalize
            obj->finalize(ColumnVariant::FinalizeMode::READ_MODE);
            EXPECT_TRUE(obj->is_finalized());

            // Verify structure is preserved
            auto final_subcolumns = obj->get_subcolumns();
            EXPECT_EQ(final_subcolumns.size(), original_subcolumns.size());

            // Verify each subcolumn is finalized
            for (const auto& subcolumn : final_subcolumns) {
                EXPECT_TRUE(subcolumn->data.is_finalized());
            }
        }

        // Test case 6: Test finalize with WRITE_MODE on sparse columns
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnVariant*>(col.get());

            // Insert data from source column
            for (size_t i = 0; i < src_size; ++i) {
                Field field;
                source_column->get(i, field);
                obj->insert(field);
            }

            // Verify initial state
            EXPECT_FALSE(obj->is_finalized());

            // Finalize in WRITE_MODE
            obj->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
            EXPECT_TRUE(obj->is_finalized());

            // Verify sparse columns are handled
            auto sparse_column = obj->get_sparse_column().get();
            EXPECT_TRUE(sparse_column != nullptr);

            // Verify data integrity
            for (size_t i = 0; i < src_size; ++i) {
                Field original_field, finalized_field;
                source_column->get(i, original_field);
                obj->get(i, finalized_field);
                EXPECT_EQ(original_field, finalized_field);
            }
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnVariant*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnVariantTest, clone_finalized) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Store original data for comparison
        auto original_subcolumns = source_column->get_subcolumns();

        // Test clone_finalized
        auto cloned = source_column->clone_finalized();
        EXPECT_TRUE(cloned.get() != nullptr);
        EXPECT_EQ(cloned->size(), src_size);

        // Verify cloned column has same subcolumns
        auto cloned_subcolumns = assert_cast<ColumnVariant*>(cloned.get())->get_subcolumns();
        EXPECT_EQ(cloned_subcolumns.size(), original_subcolumns.size());

        // Verify data integrity
        for (size_t i = 0; i < src_size; ++i) {
            Field original_field, cloned_field;
            source_column->get(i, original_field);
            cloned->get(i, cloned_field);
            EXPECT_EQ(original_field, cloned_field);
        }
    };
    auto cloned_object = VariantUtil::construct_advanced_varint_column();
    test_func(std::move(cloned_object));
}

TEST_F(ColumnVariantTest, sanitize) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Store original data for comparison
        auto original_subcolumns = source_column->get_subcolumns();

        // Test sanitize
        Status status = source_column->sanitize();
        EXPECT_TRUE(status.ok());

        // Verify data integrity after sanitization
        auto subcolumns_after = source_column->get_subcolumns();
        EXPECT_EQ(subcolumns_after.size(), original_subcolumns.size());

        // Verify all subcolumns are valid
        for (const auto& subcolumn : subcolumns_after) {
            EXPECT_TRUE(subcolumn != nullptr);
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnVariant*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnVariantTest, debug_string) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test debug_string
        std::string debug = source_column->debug_string();
        EXPECT_FALSE(debug.empty());
    };
    test_func(column_variant);
}

// used in function_element_at for variant
TEST_F(ColumnVariantTest, find_path_lower_bound_in_sparse_data) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);
        auto* mutable_ptr = assert_cast<ColumnVariant*>(source_column.get());
        //        auto [sparse_data_paths, sparse_data_values] = mutable_ptr->get_sparse_data_paths_and_values();
        // forloop
        PathInData pat("object.array");
        StringRef prefix_ref(pat.get_path());
        std::string_view path_prefix(prefix_ref.data, prefix_ref.size);
        const auto& sparse_data_map =
                assert_cast<const ColumnMap&>(*mutable_ptr->get_sparse_column());
        const auto& src_sparse_data_offsets = sparse_data_map.get_offsets();
        const auto& src_sparse_data_paths =
                assert_cast<const ColumnString&>(sparse_data_map.get_keys());

        for (size_t i = 0; i != src_sparse_data_offsets.size(); ++i) {
            size_t start = src_sparse_data_offsets[ssize_t(i) - 1];
            size_t end = src_sparse_data_offsets[ssize_t(i)];
            size_t lower_bound_index =
                    vectorized::ColumnVariant::find_path_lower_bound_in_sparse_data(
                            prefix_ref, src_sparse_data_paths, start, end);
            for (; lower_bound_index != end; ++lower_bound_index) {
                auto path_ref = src_sparse_data_paths.get_data_at(lower_bound_index);
                std::string_view path(path_ref.data, path_ref.size);
                std::cout << "path : " << path << std::endl;
            }
        }
    };
    ColumnVariant::MutablePtr obj;
    obj = ColumnVariant::create(1);
    MutableColumns cols;
    cols.push_back(obj->get_ptr());
    const auto& json_file_obj = test_data_dir_json + "json_variant/object_boundary.jsonl";
    load_columns_data_from_file(cols, serde, '\n', {0}, json_file_obj);
    EXPECT_TRUE(!obj->empty());
    std::cout << "column variant size: " << obj->size() << std::endl;
    test_func(obj);
}

// used in SparseColumnExtractIterator::_fill_path_column
TEST_F(ColumnVariantTest, fill_path_column_from_sparse_data) {
    ColumnVariant::MutablePtr obj;
    obj = ColumnVariant::create(1);
    MutableColumns cols;
    cols.push_back(obj->get_ptr());
    const auto& json_file_obj = test_data_dir_json + "json_variant/object_boundary.jsonl";
    load_columns_data_from_file(cols, serde, '\n', {0}, json_file_obj);
    EXPECT_TRUE(!obj->empty());
    auto sparse_col = obj->get_sparse_column();
    auto cloned_sparse = sparse_col->clone_empty();
    auto& offsets = obj->serialized_sparse_column_offsets();
    for (size_t i = 0; i != offsets.size(); ++i) {
        auto start = offsets[i - 1];
        auto end = offsets[i];
        vectorized::ColumnVariant::fill_path_column_from_sparse_data(
                *obj->get_subcolumn({}) /*root*/, nullptr, StringRef {"array"},
                cloned_sparse->get_ptr(), start, end);
    }

    EXPECT_NE(cloned_sparse->size(), sparse_col->size());

    vectorized::ColumnVariant::fill_path_column_from_sparse_data(
            *obj->get_subcolumn({}) /*root*/, nullptr, StringRef {"array"}, sparse_col->get_ptr(),
            0, sparse_col->size());
    EXPECT_ANY_THROW(obj->check_consistency());
}

TEST_F(ColumnVariantTest, not_finalized) {
    ColumnVariant::MutablePtr obj;
    obj = ColumnVariant::create(1);
    MutableColumns cols;
    cols.push_back(obj->get_ptr());

    VariantMap map;
    map.try_emplace(PathInData("a"), Field {});
    obj->try_insert(Field::create_field<TYPE_VARIANT>(map));

    map.emplace(PathInData("a"), Field::create_field<TYPE_INT>(Int32(20)));
    obj->try_insert(Field::create_field<TYPE_VARIANT>(map));

    map.emplace(PathInData("a"), Field::create_field<TYPE_STRING>(String("str", 3)));
    obj->try_insert(Field::create_field<TYPE_VARIANT>(map));

    EXPECT_FALSE(obj->is_finalized());
    // test get_finalized_column_ptr/ get_finalized_column for subColumn
    auto subcolumns = obj->get_subcolumns();
    for (const auto& subcolumn : subcolumns) {
        EXPECT_TRUE(subcolumn != nullptr);
        EXPECT_FALSE(subcolumn->data.is_finalized());
        EXPECT_ANY_THROW(subcolumn->data.get_finalized_column_ptr());
        EXPECT_ANY_THROW(subcolumn->data.get_finalized_column());
    }
}

doris::vectorized::Field get_field_v2(std::string_view type, size_t array_element_cnt = 0) {
    static std::unordered_map<std::string_view, doris::vectorized::Field> field_map;
    if (field_map.empty()) {
        doris::vectorized::Field int_field = Field::create_field<TYPE_INT>(Int32(20));
        doris::vectorized::Field str_field = Field::create_field<TYPE_STRING>(String("str", 3));
        doris::vectorized::Field arr_int_field = Field::create_field<TYPE_ARRAY>(Array());
        doris::vectorized::Field arr_str_field = Field::create_field<TYPE_ARRAY>(Array());
        auto& array1 = arr_int_field.get<Array>();
        auto& array2 = arr_str_field.get<Array>();
        for (size_t i = 0; i < array_element_cnt; ++i) {
            array1.emplace_back(int_field);
            array2.emplace_back(str_field);
        }
        field_map["int"] = int_field;
        field_map["string"] = str_field;
        field_map["ai"] = arr_int_field;
        field_map["as"] = arr_str_field;
    }
    return field_map[type];
}

TEST_F(ColumnVariantTest, array_field_operations) {
    auto test_func = [](const auto& source_column) {
        auto src_size = source_column->size();
        EXPECT_TRUE(src_size > 0);

        // Test case 2: Test create_array
        {
            // Test with different types
            std::vector<PrimitiveType> types = {PrimitiveType::TYPE_TINYINT,
                                                PrimitiveType::TYPE_STRING,
                                                PrimitiveType::TYPE_DOUBLE};
            for (const auto& type : types) {
                for (size_t dim = 1; dim <= 3; ++dim) {
                    DataTypePtr array_type = create_array(type, dim);
                    EXPECT_TRUE(array_type != nullptr);
                }
            }
            // Test create_array_of_type with TypeIndex::Nothing
            auto dt_ptr = create_array_of_type(PrimitiveType::TYPE_NULL, 0, false);
            // NULL -> will be created as UInt8 or we should ban for it ?
            EXPECT_TRUE(dt_ptr->get_primitive_type() == PrimitiveType::TYPE_BOOLEAN);
        }

        // Test case 3: Test recreate_column_with_default_values
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnVariant*>(col.get());

            // Create a subcolumn with array type
            PathInData path("array_field");
            auto array_type = create_array(PrimitiveType::TYPE_TINYINT, 2);
            auto column =
                    array_type
                            ->create_column(); // Nullable(Array(Nullable(Array(Nullable(TINYINT)))))
            Field array_field = vectorized::Field::create_field<TYPE_ARRAY>(Array());
            array_field.get<Array>().emplace_back(vectorized::Field::create_field<TYPE_TINYINT>(1));
            Field array_field_o = vectorized::Field::create_field<TYPE_ARRAY>(Array());
            array_field_o.get<Array>().emplace_back(array_field);
            column->insert(array_field_o);
            obj->add_sub_column(path, std::move(column));

            // Get the subcolumn
            const auto* subcolumn = obj->get_subcolumn(path);
            EXPECT_TRUE(subcolumn != nullptr);

            EXPECT_ANY_THROW(subcolumn->get_finalized_column_ptr());

            // Recreate column with default values
            auto new_column = recreate_column_with_default_values(
                    column->convert_to_full_column_if_const(), PrimitiveType::TYPE_TINYINT, 2);
            EXPECT_TRUE(new_column->get_name().find("Array") != std::string::npos);
            EXPECT_EQ(new_column->size(), subcolumn->size());
        }

        // Test case 4: Test clone_with_default_values
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnVariant*>(col.get());

            // Create a subcolumn with array type
            PathInData path("array_field");
            auto array_type = create_array(PrimitiveType::TYPE_TINYINT, 1);
            auto column = array_type->create_column();
            Field array1 = vectorized::Field::create_field<TYPE_ARRAY>(Array());
            array1.get<Array>().emplace_back(vectorized::Field::create_field<TYPE_TINYINT>(1));
            array1.get<Array>().emplace_back(vectorized::Field::create_field<TYPE_TINYINT>(2));
            array1.get<Array>().emplace_back(vectorized::Field::create_field<TYPE_TINYINT>(3));
            column->insert(array1);
            obj->add_sub_column(path, std::move(column), array_type);

            // Get the subcolumn
            const auto* subcolumn = obj->get_subcolumn(path);
            EXPECT_TRUE(subcolumn != nullptr);
            EXPECT_TRUE(subcolumn->size() > 0);
            std::cout << "subcolumn size: " << subcolumn->size() << std::endl;
            Field f = subcolumn->get_last_field();
            EXPECT_TRUE(f.get_type() == PrimitiveType::TYPE_ARRAY);

            // Create field info
            FieldInfo info;
            info.scalar_type_id = PrimitiveType::TYPE_TINYINT;
            info.num_dimensions = 1;
            info.have_nulls = false;
            info.need_convert = false;

            // Clone with default values
            auto cloned = subcolumn->clone_with_default_values(info);
            std::cout << "cloned size: " << cloned.size() << std::endl;
            EXPECT_TRUE(cloned.size() == subcolumn->size());
        }

        // Test case 5: Test Subcolumn::resize
        {
            auto col = source_column->clone_empty();
            auto obj = assert_cast<ColumnVariant*>(col.get());

            // Create a subcolumn
            PathInData path("test_field");
            obj->add_sub_column(path, src_size);

            // Get the subcolumn
            auto* subcolumn = obj->get_subcolumn(path);
            EXPECT_TRUE(subcolumn != nullptr);

            // Test resize to larger size
            size_t new_size = src_size + 10;
            subcolumn->resize(new_size);
            EXPECT_EQ(subcolumn->size(), new_size);

            // Test resize to smaller size
            new_size = src_size / 2;
            subcolumn->resize(new_size);
            EXPECT_EQ(subcolumn->size(), new_size);

            // Test resize to zero
            subcolumn->resize(0);
            EXPECT_EQ(subcolumn->size(), 0);
        }
        {
            // Test wrapp_array_nullable
            // 1. create an empty variant column
            auto variant = ColumnVariant::create(2);

            std::vector<std::pair<std::string, doris::vectorized::Field>> data;

            // 2. subcolumn path
            data.emplace_back("v.ai", get_field_v2("ai", 1));
            data.emplace_back("v.as", get_field_v2("as", 1));

            for (int i = 0; i < 2; ++i) {
                auto field = VariantUtil::construct_variant_map(data);
                variant->try_insert(field);
            }
            EXPECT_FALSE(variant->is_finalized());
            variant->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
            EXPECT_TRUE(variant->is_finalized());
            std::cout << "sub: " << variant->get_subcolumns().size() << std::endl;
            for (auto& entry : variant->get_subcolumns()) {
                std::cout << "entry path: " << entry->path.get_path() << std::endl;
                std::cout << "entry type: " << entry->data.get_least_common_typeBase()->get_name()
                          << std::endl;
                std::cout << "entry dimension " << entry->data.get_dimensions() << std::endl;
            }

            // then clear
            variant->clear();
            EXPECT_TRUE(variant->size() == 0);
        }
    };
    auto temp = column_variant->clone();
    auto cloned_object = assert_cast<ColumnVariant*>(temp.get());
    test_func(std::move(cloned_object));
}

TEST_F(ColumnVariantTest, assert_exception_happen) {
    // Test case 1: Test assert_exception_happen
    {
        // 1. create an empty variant column
        vectorized::ColumnVariant::Subcolumns dynamic_subcolumns;
        dynamic_subcolumns.create_root(
                vectorized::ColumnVariant::Subcolumn(0, true, true /*root*/));
        dynamic_subcolumns.add(vectorized::PathInData("v.f"),
                               vectorized::ColumnVariant::Subcolumn {0, true});
        dynamic_subcolumns.add(vectorized::PathInData("v.e"),
                               vectorized::ColumnVariant::Subcolumn {0, true});
        dynamic_subcolumns.add(vectorized::PathInData("v.b"),
                               vectorized::ColumnVariant::Subcolumn {0, true});
        dynamic_subcolumns.add(vectorized::PathInData("v.b.d"),
                               vectorized::ColumnVariant::Subcolumn {0, true});
        dynamic_subcolumns.add(vectorized::PathInData("v.c.d"),
                               vectorized::ColumnVariant::Subcolumn {0, true});
        std::cout << "dynamic_subcolumns size: " << dynamic_subcolumns.size() << std::endl;
        EXPECT_ANY_THROW(ColumnVariant::create(2, std::move(dynamic_subcolumns)));
    }

    {
        // 1. create an empty variant column
        auto variant = ColumnVariant::create(5);

        std::vector<std::pair<std::string, doris::vectorized::Field>> data;

        // 2. subcolumn path
        data.emplace_back("v.a", get_field_v2("int"));
        data.emplace_back("v.b", get_field_v2("string"));
        data.emplace_back("v.c", get_field_v2("ai", 2));
        data.emplace_back("v.f", get_field_v2("as", 2));
        data.emplace_back("v.e", get_field_v2("string"));

        for (int i = 0; i < 5; ++i) {
            auto field = VariantUtil::construct_variant_map(data);
            variant->try_insert(field);
        }

        // 3. sparse column path
        data.emplace_back("v.d.d", get_field_v2("ai", 2));
        data.emplace_back("v.c.d", get_field_v2("string"));
        data.emplace_back("v.b.d", get_field_v2("ai", 2));
        for (int i = 0; i < 5; ++i) {
            auto field = VariantUtil::construct_variant_map(data);
            variant->try_insert(field);
        }

        data.clear();
        data.emplace_back("v.a", get_field_v2("int"));
        data.emplace_back("v.b", get_field_v2("int"));
        data.emplace_back("v.c", get_field_v2("ai", 2));
        data.emplace_back("v.f", get_field_v2("as", 2));
        data.emplace_back("v.e", get_field_v2("string"));
        data.emplace_back("v.d.d", get_field_v2("as", 2));
        data.emplace_back("v.c.d", get_field_v2("int"));
        data.emplace_back("v.b.d", get_field_v2("as", 2));
        for (int i = 0; i < 5; ++i) {
            auto field = VariantUtil::construct_variant_map(data);
            variant->try_insert(field);
        }
        EXPECT_FALSE(variant->is_finalized());
        for (const auto& column : variant->get_subcolumns()) {
            if (!column->data.is_finalized()) {
                EXPECT_ANY_THROW(column->data.remove_nullable());
                EXPECT_ANY_THROW(column->data.get_finalized_column());
            } else {
                std::cout << "column path: " << column->path.get_path() << std::endl;
                EXPECT_NO_THROW(column->data.remove_nullable());
                EXPECT_NO_THROW(column->data.get_finalized_column());
            }
        }
    }
}

TEST_F(ColumnVariantTest, try_insert_default_from_nested) {
    // 1. create an empty variant column
    vectorized::ColumnVariant::Subcolumns dynamic_subcolumns;
    auto array_type = create_array(PrimitiveType::TYPE_STRING, 1);
    auto column = array_type->create_column();
    Field array1 = vectorized::Field::create_field<TYPE_ARRAY>(Array());
    array1.get<Array>().emplace_back(vectorized::Field::create_field<TYPE_STRING>("amory"));
    array1.get<Array>().emplace_back(vectorized::Field::create_field<TYPE_STRING>("commit"));
    Field array2 = vectorized::Field::create_field<TYPE_ARRAY>(Array());
    array2.get<Array>().emplace_back(vectorized::Field::create_field<TYPE_STRING>("amory"));
    array2.get<Array>().emplace_back(vectorized::Field::create_field<TYPE_STRING>("doris"));
    column->insert(array1);
    column->insert(array2);

    auto array_type2 = create_array(PrimitiveType::TYPE_STRING, 2);
    auto column2 = array_type2->create_column();
    Field array22 = vectorized::Field::create_field<TYPE_ARRAY>(Array());
    array22.get<Array>().emplace_back(array1);
    array22.get<Array>().emplace_back(array2);
    Field array23 = vectorized::Field::create_field<TYPE_ARRAY>(Array());
    array23.get<Array>().emplace_back(array2);
    array23.get<Array>().emplace_back(array1);
    column2->insert(array22);
    column2->insert(array23);

    dynamic_subcolumns.create_root(vectorized::ColumnVariant::Subcolumn(0, true, true /*root*/));
    dynamic_subcolumns.add(vectorized::PathInData("v.f"),
                           vectorized::ColumnVariant::Subcolumn {0, true});
    dynamic_subcolumns.add(
            vectorized::PathInData("v.a"),
            vectorized::ColumnVariant::Subcolumn {std::move(column2), array_type2, false, false});
    dynamic_subcolumns.add(vectorized::PathInData("v.b"),
                           vectorized::ColumnVariant::Subcolumn {0, true});
    dynamic_subcolumns.add(
            vectorized::PathInData("v.b.a"),
            vectorized::ColumnVariant::Subcolumn {std::move(column), array_type, false, false});
    dynamic_subcolumns.add(vectorized::PathInData("v.c.d"),
                           vectorized::ColumnVariant::Subcolumn {0, true});
    std::cout << "dynamic_subcolumns size: " << dynamic_subcolumns.size() << std::endl;
    auto obj = ColumnVariant::create(5, std::move(dynamic_subcolumns));

    for (auto& entry : obj->get_subcolumns()) {
        std::cout << "entry path: " << entry->path.get_path() << std::endl;
        std::cout << "entry type: " << entry->data.get_least_common_typeBase()->get_name()
                  << std::endl;
        std::cout << "entry dimension " << entry->data.get_dimensions() << std::endl;
        bool inserted = obj->try_insert_default_from_nested(entry);
        if (!inserted) {
            entry->data.insert_default();
        }
    }
}

// unnest, clear_column_data
TEST_F(ColumnVariantTest, unnest) {
    // 1. create an empty variant column
    vectorized::ColumnVariant::Subcolumns dynamic_subcolumns;
    auto nested_col = ColumnVariant::NESTED_TYPE->create_column();
    Field array1 = VariantUtil::create_nested_array_field(
            {{{"a", vectorized::Field::create_field<TYPE_STRING>("amory")}},
             {{"b", vectorized::Field::create_field<TYPE_STRING>("commit")}}});
    Field array2 = VariantUtil::create_nested_array_field(
            {{{"a", vectorized::Field::create_field<TYPE_STRING>("amory")}},
             {{"b", vectorized::Field::create_field<TYPE_STRING>("doris")}}});
    std::cout << "array: " << array1.get<Array>().size() << std::endl;
    nested_col->insert(array1);
    nested_col->insert(array2);
    std::cout << nested_col->size() << std::endl;

    // 2. subcolumn path
    dynamic_subcolumns.create_root(vectorized::ColumnVariant::Subcolumn(2, true, true /*root*/));
    dynamic_subcolumns.add(vectorized::PathInData("v.f"),
                           vectorized::ColumnVariant::Subcolumn {2, true});
    dynamic_subcolumns.add(vectorized::PathInData("v.a"),
                           vectorized::ColumnVariant::Subcolumn {
                                   std::move(nested_col), ColumnVariant::NESTED_TYPE, true, false});
    std::cout << "dynamic_subcolumns size: " << dynamic_subcolumns.size() << std::endl;
    auto obj = ColumnVariant::create(2, std::move(dynamic_subcolumns));
    obj->set_num_rows(2);
    EXPECT_TRUE(!obj->empty());
    std::cout << obj->size() << std::endl;
    obj->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
}

TEST_F(ColumnVariantTest, path_in_data_builder_test) {
    // Create a ColumnVariant with nested subcolumns
    auto variant = ColumnVariant::create(5);

    // Test case 1: Build a nested path with PathInDataBuilder
    {
        PathInDataBuilder builder;
        builder.append("v", false); // First part is not array
        builder.append("a", true);  // Second part is array
        builder.append("b", true);  // Third part is array
        builder.append("c", false); // Fourth part is not array

        PathInData path = builder.build();
        EXPECT_TRUE(path.has_nested_part());

        // Create field info for nested type
        FieldInfo field_info;
        field_info.scalar_type_id = PrimitiveType::TYPE_INT;
        field_info.have_nulls = true;
        field_info.need_convert = false;
        field_info.num_dimensions = 2; // Array of Array

        // Test add_nested_subcolumn
        variant->add_nested_subcolumn(path, field_info, 5);

        // Verify the subcolumn was added correctly
        const auto* subcolumn = variant->get_subcolumn(path);
        EXPECT_TRUE(subcolumn != nullptr);

        // then clear
        variant->clear();
        EXPECT_TRUE(variant->size() == 0);
    }
}

TEST_F(ColumnVariantTest, get_field_info_all_types) {
    // Test Int32
    {
        Field field = Field::create_field<TYPE_INT>(Int32(42));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_TINYINT);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test Int64
    {
        Field field = Field::create_field<TYPE_BIGINT>(Int64(42));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_TINYINT);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test UInt64
    {
        Field field = Field::create_field<TYPE_BIGINT>(UInt64(42));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_TINYINT);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test Int64 with different ranges
    {
        // Test Int64 <= Int8::max()
        Field field1 = Field::create_field<TYPE_BIGINT>(Int64(std::numeric_limits<Int8>::max()));
        FieldInfo info1;
        schema_util::get_field_info(field1, &info1);
        EXPECT_EQ(info1.scalar_type_id, PrimitiveType::TYPE_TINYINT);
        EXPECT_FALSE(info1.have_nulls);
        EXPECT_FALSE(info1.need_convert);
        EXPECT_EQ(info1.num_dimensions, 0);

        // Test Int64 <= Int16::max()
        Field field2 = Field::create_field<TYPE_BIGINT>(Int64(std::numeric_limits<Int16>::max()));
        FieldInfo info2;
        schema_util::get_field_info(field2, &info2);
        EXPECT_EQ(info2.scalar_type_id, PrimitiveType::TYPE_SMALLINT);
        EXPECT_FALSE(info2.have_nulls);
        EXPECT_FALSE(info2.need_convert);
        EXPECT_EQ(info2.num_dimensions, 0);

        // Test Int64 <= Int32::max()
        Field field3 = Field::create_field<TYPE_BIGINT>(Int64(std::numeric_limits<Int32>::max()));
        FieldInfo info3;
        schema_util::get_field_info(field3, &info3);
        EXPECT_EQ(info3.scalar_type_id, PrimitiveType::TYPE_INT);
        EXPECT_FALSE(info3.have_nulls);
        EXPECT_FALSE(info3.need_convert);
        EXPECT_EQ(info3.num_dimensions, 0);

        // Test Int64 > Int32::max()
        Field field4 = Field::create_field<TYPE_BIGINT>(
                Int64(static_cast<Int64>(std::numeric_limits<Int32>::max()) + 1));
        FieldInfo info4;
        schema_util::get_field_info(field4, &info4);
        EXPECT_EQ(info4.scalar_type_id, PrimitiveType::TYPE_BIGINT);
        EXPECT_FALSE(info4.have_nulls);
        EXPECT_FALSE(info4.need_convert);
        EXPECT_EQ(info4.num_dimensions, 0);

        // Test Int64 <= Int8::min()
        Field field5 = Field::create_field<TYPE_BIGINT>(Int64(std::numeric_limits<Int8>::min()));
        FieldInfo info5;
        schema_util::get_field_info(field5, &info5);
        EXPECT_EQ(info5.scalar_type_id, PrimitiveType::TYPE_TINYINT);
        EXPECT_FALSE(info5.have_nulls);
        EXPECT_FALSE(info5.need_convert);
        EXPECT_EQ(info5.num_dimensions, 0);

        // Test Int64 <= Int16::min()
        Field field6 = Field::create_field<TYPE_BIGINT>(Int64(std::numeric_limits<Int16>::min()));
        FieldInfo info6;
        schema_util::get_field_info(field6, &info6);
        EXPECT_EQ(info6.scalar_type_id, PrimitiveType::TYPE_SMALLINT);
        EXPECT_FALSE(info6.have_nulls);
        EXPECT_FALSE(info6.need_convert);
        EXPECT_EQ(info6.num_dimensions, 0);

        // Test Int64 <= Int32::min()
        Field field7 = Field::create_field<TYPE_BIGINT>(Int64(std::numeric_limits<Int32>::min()));
        FieldInfo info7;
        schema_util::get_field_info(field7, &info7);
        EXPECT_EQ(info7.scalar_type_id, PrimitiveType::TYPE_INT);
        EXPECT_FALSE(info7.have_nulls);
        EXPECT_FALSE(info7.need_convert);
        EXPECT_EQ(info7.num_dimensions, 0);

        // Test Int64 < Int32::min()
        Field field8 = Field::create_field<TYPE_BIGINT>(
                Int64(static_cast<Int64>(std::numeric_limits<Int32>::min()) - 1));
        FieldInfo info8;
        schema_util::get_field_info(field8, &info8);
        EXPECT_EQ(info8.scalar_type_id, PrimitiveType::TYPE_BIGINT);
    }

    // Test UInt64 with different ranges
    {
        // Test UInt64 <= UInt8::max()
        Field field1 = Field::create_field<TYPE_BIGINT>(UInt64(std::numeric_limits<UInt8>::max()));
        FieldInfo info1;
        schema_util::get_field_info(field1, &info1);
        EXPECT_EQ(info1.scalar_type_id, PrimitiveType::TYPE_SMALLINT);
        EXPECT_FALSE(info1.have_nulls);
        EXPECT_FALSE(info1.need_convert);
        EXPECT_EQ(info1.num_dimensions, 0);

        // Test UInt64 <= UInt16::max()
        Field field2 = Field::create_field<TYPE_BIGINT>(UInt64(std::numeric_limits<UInt16>::max()));
        FieldInfo info2;
        schema_util::get_field_info(field2, &info2);
        EXPECT_EQ(info2.scalar_type_id, PrimitiveType::TYPE_INT);
        EXPECT_FALSE(info2.have_nulls);
        EXPECT_FALSE(info2.need_convert);
        EXPECT_EQ(info2.num_dimensions, 0);

        // Test UInt64 <= UInt32::max()
        Field field3 = Field::create_field<TYPE_BIGINT>(UInt64(std::numeric_limits<UInt32>::max()));
        FieldInfo info3;
        schema_util::get_field_info(field3, &info3);
        EXPECT_EQ(info3.scalar_type_id, PrimitiveType::TYPE_BIGINT);
        EXPECT_FALSE(info3.have_nulls);
        EXPECT_FALSE(info3.need_convert);
        EXPECT_EQ(info3.num_dimensions, 0);

        // Test UInt64 > UInt32::max()
        Field field4 = Field::create_field<TYPE_BIGINT>(
                UInt64(static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1));
        FieldInfo info4;
        schema_util::get_field_info(field4, &info4);
        EXPECT_EQ(info4.scalar_type_id, PrimitiveType::TYPE_BIGINT);
    }

    // Test Float32
    {
        Field field = Field::create_field<TYPE_FLOAT>(Float32(42.0f));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_DOUBLE);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test Float64
    {
        Field field = Field::create_field<TYPE_DOUBLE>(Float64(42.0));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_DOUBLE);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test String
    {
        Field field = Field::create_field<TYPE_STRING>(String("test"));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_STRING);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test Array
    {
        Array array;
        array.push_back(Field::create_field<TYPE_BIGINT>(Int64(1)));
        array.push_back(Field::create_field<TYPE_BIGINT>(Int64(2)));
        Field field = Field::create_field<TYPE_ARRAY>(std::move(array));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_TINYINT);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 1);
    }

    // Test nested Array
    {
        Array inner_array;
        inner_array.push_back(Field::create_field<TYPE_BIGINT>(Int64(1)));
        inner_array.push_back(Field::create_field<TYPE_BIGINT>(Int64(2)));

        Array outer_array;
        outer_array.push_back(Field::create_field<TYPE_ARRAY>(std::move(inner_array)));
        outer_array.push_back(Field::create_field<TYPE_ARRAY>(Array()));

        Field field = Field::create_field<TYPE_ARRAY>(std::move(outer_array));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_TINYINT);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 2);
    }

    // Test VariantMap
    {
        VariantMap variant_map;
        variant_map[PathInData("key1")] =
                FieldWithDataType {.field = Field::create_field<TYPE_BIGINT>(Int64(1)),
                                   .base_scalar_type_id = PrimitiveType::TYPE_BIGINT};
        variant_map[PathInData("key2")] =
                FieldWithDataType {.field = Field::create_field<TYPE_STRING>(String("value")),
                                   .base_scalar_type_id = PrimitiveType::TYPE_STRING};
        Field field = Field::create_field<TYPE_VARIANT>(std::move(variant_map));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_VARIANT);
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test JsonbField
    {
        Slice slice("\"amory is cute\"");
        JsonBinaryValue value;
        Status st = value.from_json_string(slice.data, slice.size);
        EXPECT_TRUE(st.ok()) << st.to_string();
        JsonbField field(value.value(), value.size());

        FieldInfo info;
        schema_util::get_field_info(vectorized::Field::create_field<TYPE_JSONB>(std::move(field)),
                                    &info);
        EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_JSONB);
    }

    // Test Tuple
    {
        Tuple t1;
        t1.push_back(Field::create_field<TYPE_STRING>(String("amory cute")));
        t1.push_back(Field::create_field<TYPE_BIGINT>(Int64(37)));
        t1.push_back(Field::create_field<TYPE_BOOLEAN>(true));
        FieldInfo info;
        schema_util::get_field_info(vectorized::Field::create_field<TYPE_STRUCT>(t1), &info);
        EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_STRUCT)
                << "info.scalar_type_id: " << info.scalar_type_id;
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 0);
    }

    // Test Map which is invalid
    {
        Array k1;
        k1.push_back(Field::create_field<TYPE_STRING>(String("a")));
        k1.push_back(Field::create_field<TYPE_STRING>(String("b")));
        k1.push_back(Field::create_field<TYPE_STRING>(String("c")));

        Array v1;
        v1.push_back(Field::create_field<TYPE_BIGINT>(Int64(1)));
        v1.push_back(Field::create_field<TYPE_BIGINT>(Int64(2)));
        v1.push_back(Field::create_field<TYPE_BIGINT>(Int64(3)));

        Map map;
        map.push_back(Field::create_field<TYPE_ARRAY>(std::move(k1)));
        map.push_back(Field::create_field<TYPE_ARRAY>(std::move(v1)));
        FieldInfo info;
        EXPECT_THROW(
                schema_util::get_field_info(vectorized::Field::create_field<TYPE_MAP>(map), &info),
                doris::Exception);
    }

    // Test Array with different types
    {
        Array array;
        array.push_back(Field::create_field<TYPE_BIGINT>(Int64(1)));
        Field field = Field::create_field<TYPE_ARRAY>(std::move(array));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_TINYINT)
                << "info.scalar_type_id: " << info.scalar_type_id;
        EXPECT_FALSE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 1);
    }

    // Test Array with nulls
    {
        Array array;
        array.push_back(Field::create_field<TYPE_BIGINT>(Int64(1)));
        array.push_back(Field::create_field<TYPE_NULL>(Null()));
        Field field = Field::create_field<TYPE_ARRAY>(std::move(array));
        FieldInfo info;
        schema_util::get_field_info(field, &info);
        EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_TINYINT);
        EXPECT_TRUE(info.have_nulls);
        EXPECT_FALSE(info.need_convert);
        EXPECT_EQ(info.num_dimensions, 1);
    }

    // Test nested Array with Int64 in different ranges
    {// Test nested Array with Int64 <= Int8::max()
     {Array inner_array;
    inner_array.push_back(
            Field::create_field<TYPE_BIGINT>(Int64(std::numeric_limits<Int8>::max())));
    inner_array.push_back(
            Field::create_field<TYPE_BIGINT>(Int64(std::numeric_limits<Int8>::max())));

    Array outer_array;
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(std::move(inner_array)));
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(Array()));

    Field field = Field::create_field<TYPE_ARRAY>(std::move(outer_array));
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_TINYINT);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}

// Test nested Array with Int64 <= Int16::max()
{
    Array inner_array;
    inner_array.push_back(
            Field::create_field<TYPE_BIGINT>(Int64(std::numeric_limits<Int16>::max())));
    inner_array.push_back(
            Field::create_field<TYPE_BIGINT>(Int64(std::numeric_limits<Int16>::max())));

    Array outer_array;
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(std::move(inner_array)));
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(Array()));

    Field field = Field::create_field<TYPE_ARRAY>(std::move(outer_array));
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_SMALLINT);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}

// Test nested Array with Int64 <= Int32::max()
{
    Array inner_array;
    inner_array.push_back(
            Field::create_field<TYPE_BIGINT>(Int64(std::numeric_limits<Int32>::max())));
    inner_array.push_back(
            Field::create_field<TYPE_BIGINT>(Int64(std::numeric_limits<Int32>::max())));

    Array outer_array;
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(std::move(inner_array)));
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(Array()));

    Field field = Field::create_field<TYPE_ARRAY>(std::move(outer_array));
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_INT);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}

// Test nested Array with Int64 > Int32::max()
{
    Array inner_array;
    inner_array.push_back(Field::create_field<TYPE_BIGINT>(
            Int64(static_cast<Int64>(std::numeric_limits<Int32>::max()) + 1)));
    inner_array.push_back(Field::create_field<TYPE_BIGINT>(
            Int64(static_cast<Int64>(std::numeric_limits<Int32>::max()) + 1)));

    Array outer_array;
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(std::move(inner_array)));
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(Array()));

    Field field = Field::create_field<TYPE_ARRAY>(std::move(outer_array));
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_BIGINT);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}
} // namespace doris::vectorized

// Test nested Array with UInt64 in different ranges
{// Test nested Array with UInt64 <= UInt8::max()
 {Array inner_array;
inner_array.push_back(Field::create_field<TYPE_BIGINT>(UInt64(std::numeric_limits<UInt8>::max())));
inner_array.push_back(Field::create_field<TYPE_BIGINT>(UInt64(std::numeric_limits<UInt8>::max())));

Array outer_array;
outer_array.push_back(Field::create_field<TYPE_ARRAY>(std::move(inner_array)));
outer_array.push_back(Field::create_field<TYPE_ARRAY>(Array()));

Field field = Field::create_field<TYPE_ARRAY>(std::move(outer_array));
FieldInfo info;
schema_util::get_field_info(field, &info);
EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_SMALLINT);
EXPECT_FALSE(info.have_nulls);
EXPECT_FALSE(info.need_convert);
EXPECT_EQ(info.num_dimensions, 2);
}

// Test nested Array with UInt64 <= UInt16::max()
{
    Array inner_array;
    inner_array.push_back(
            Field::create_field<TYPE_BIGINT>(UInt64(std::numeric_limits<UInt16>::max())));
    inner_array.push_back(
            Field::create_field<TYPE_BIGINT>(UInt64(std::numeric_limits<UInt16>::max())));

    Array outer_array;
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(std::move(inner_array)));
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(Array()));

    Field field = Field::create_field<TYPE_ARRAY>(std::move(outer_array));
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_INT);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}

// Test nested Array with UInt64 <= UInt32::max()
{
    Array inner_array;
    inner_array.push_back(
            Field::create_field<TYPE_BIGINT>(UInt64(std::numeric_limits<UInt32>::max())));
    inner_array.push_back(
            Field::create_field<TYPE_BIGINT>(UInt64(std::numeric_limits<UInt32>::max())));

    Array outer_array;
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(std::move(inner_array)));
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(Array()));

    Field field = Field::create_field<TYPE_ARRAY>(std::move(outer_array));
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_BIGINT);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}

// Test nested Array with UInt64 > UInt32::max()
{
    Array inner_array;
    inner_array.push_back(Field::create_field<TYPE_BIGINT>(
            UInt64(static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1)));
    inner_array.push_back(Field::create_field<TYPE_BIGINT>(
            UInt64(static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1)));

    Array outer_array;
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(std::move(inner_array)));
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(Array()));

    Field field = Field::create_field<TYPE_ARRAY>(std::move(outer_array));
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_BIGINT);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}
}

// Test nested Array with mixed Int64 and UInt64
{
    Array inner_array1;
    inner_array1.push_back(
            Field::create_field<TYPE_BIGINT>(Int64(std::numeric_limits<Int32>::max())));
    inner_array1.push_back(
            Field::create_field<TYPE_BIGINT>(Int64(std::numeric_limits<Int32>::max())));

    Array inner_array2;
    inner_array2.push_back(
            Field::create_field<TYPE_BIGINT>(UInt64(std::numeric_limits<UInt32>::max())));
    inner_array2.push_back(
            Field::create_field<TYPE_BIGINT>(UInt64(std::numeric_limits<UInt32>::max())));

    Array outer_array;
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(std::move(inner_array1)));
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(std::move(inner_array2)));

    Field field = Field::create_field<TYPE_ARRAY>(std::move(outer_array));
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_BIGINT);
    EXPECT_FALSE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}

// Test nested Array with nulls
{
    Array inner_array1;
    inner_array1.push_back(Field::create_field<TYPE_BIGINT>(Int64(1)));
    inner_array1.push_back(Field::create_field<TYPE_BIGINT>(Int64(2)));

    Array inner_array2;
    inner_array2.push_back(Field::create_field<TYPE_BIGINT>(Int64(3)));
    inner_array2.push_back(Field::create_field<TYPE_NULL>(Null()));

    Array outer_array;
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(std::move(inner_array1)));
    outer_array.push_back(Field::create_field<TYPE_ARRAY>(std::move(inner_array2)));

    Field field = Field::create_field<TYPE_ARRAY>(std::move(outer_array));
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_TINYINT);
    EXPECT_TRUE(info.have_nulls);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 2);
}

// Test Array with JsonbField
{
    Slice slice("\"amory is cute\"");
    JsonBinaryValue value;
    Status st = value.from_json_string(slice.data, slice.size);
    EXPECT_TRUE(st.ok()) << st.to_string();
    JsonbField field(value.value(), value.size());

    Field array = Field::create_field<TYPE_ARRAY>(Array());
    array.get<Array>().push_back(Field::create_field<TYPE_JSONB>(std::move(field)));
    array.get<Array>().push_back(Field::create_field<TYPE_JSONB>(std::move(JsonbField())));
    FieldInfo info;
    schema_util::get_field_info(array, &info);
    // which should support ??!!
    std::cout << std::to_string(info.scalar_type_id) << std::endl;
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_JSONB);
}
}

TEST_F(ColumnVariantTest, field_visitor) {
    // Test replacing scalar values in a flat array
    {
        Array array;
        array.push_back(Field::create_field<TYPE_BIGINT>(Int64(1)));
        array.push_back(Field::create_field<TYPE_BIGINT>(Int64(2)));
        array.push_back(Field::create_field<TYPE_BIGINT>(Int64(3)));

        Field field = Field::create_field<TYPE_ARRAY>(std::move(array));
        Field replacement = Field::create_field<TYPE_BIGINT>(Int64(42));
        Field result = apply_visitor(FieldVisitorReplaceScalars(replacement, 0), field);

        EXPECT_EQ(result.get<Int64>(), 42);

        Field replacement1 = Field::create_field<TYPE_BIGINT>(Int64(42));
        Field result1 = apply_visitor(FieldVisitorReplaceScalars(replacement, 1), field);

        EXPECT_EQ(result1.get<Array>().size(), 3);
        EXPECT_EQ(result1.get<Array>()[0].get<Int64>(), 42);
        EXPECT_EQ(result1.get<Array>()[1].get<Int64>(), 42);
        EXPECT_EQ(result1.get<Array>()[2].get<Int64>(), 42);
    }
}

TEST_F(ColumnVariantTest, subcolumn_operations_coverage) {
    // Test insert_range_from
    {
        auto src_column = VariantUtil::construct_basic_varint_column();
        auto dst_column = VariantUtil::construct_dst_varint_column();

        // Test normal case
        auto* dst_subcolumn = const_cast<ColumnVariant::Subcolumn*>(
                &dst_column->get_subcolumns().get_root()->data);
        dst_subcolumn->insert_range_from(src_column->get_subcolumns().get_root()->data, 0, 2);

        // Test empty range
        dst_subcolumn->insert_range_from(src_column->get_subcolumns().get_root()->data, 0, 0);

        // Test with different types
        auto src_column2 = VariantUtil::construct_advanced_varint_column();
        dst_subcolumn->insert_range_from(src_column2->get_subcolumns().get_root()->data, 0, 1);
    }

    // Test parse_binary_from_sparse_column
    {
        auto column = VariantUtil::construct_basic_varint_column();
        vectorized::Field res;
        FieldInfo field_info;

        // Test String type
        {
            std::string test_str = "test_data";
            std::vector<char> binary_data;
            size_t str_size = test_str.size();
            binary_data.resize(sizeof(size_t) + test_str.size());
            memcpy(binary_data.data(), &str_size, sizeof(size_t));
            memcpy(binary_data.data() + sizeof(size_t), test_str.data(), test_str.size());
            const char* data = binary_data.data();
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_STRING, data, res,
                                            field_info);
            EXPECT_EQ(res.get<String>(), "test_data");
        }

        // Test integer types
        {
            Int8 int8_val = 42;
            const char* data = reinterpret_cast<const char*>(&int8_val);
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_TINYINT, data, res,
                                            field_info);
            EXPECT_EQ(res.get<Int8>(), 42);
        }

        {
            Int16 int16_val = 12345;
            const char* data = reinterpret_cast<const char*>(&int16_val);
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_SMALLINT, data, res,
                                            field_info);
            EXPECT_EQ(res.get<Int16>(), 12345);
        }

        {
            Int32 int32_val = 123456789;
            const char* data = reinterpret_cast<const char*>(&int32_val);
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_INT, data, res, field_info);
            EXPECT_EQ(res.get<Int32>(), 123456789);
        }

        {
            Int64 int64_val = 1234567890123456789LL;
            const char* data = reinterpret_cast<const char*>(&int64_val);
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_BIGINT, data, res,
                                            field_info);
            EXPECT_EQ(res.get<Int64>(), 1234567890123456789LL);
        }

        // Test floating point types
        {
            Float32 float32_val = 3.1415901f;
            const char* data = reinterpret_cast<const char*>(&float32_val);
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_FLOAT, data, res,
                                            field_info);
            EXPECT_FLOAT_EQ(res.get<Float32>(), 0);
        }

        {
            Float64 float64_val = 3.141592653589793;
            const char* data = reinterpret_cast<const char*>(&float64_val);
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_DOUBLE, data, res,
                                            field_info);
            EXPECT_DOUBLE_EQ(res.get<Float64>(), 3.141592653589793);
        }

        // Test JSONB type
        {
            std::string json_str = "{\"key\": \"value\"}";
            std::vector<char> binary_data;
            size_t json_size = json_str.size();
            binary_data.resize(sizeof(size_t) + json_str.size());
            memcpy(binary_data.data(), &json_size, sizeof(size_t));
            memcpy(binary_data.data() + sizeof(size_t), json_str.data(), json_str.size());
            const char* data = binary_data.data();
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_JSONB, data, res,
                                            field_info);
        }

        // Test Array type
        {
            std::vector<char> binary_data;
            size_t array_size = 2;
            binary_data.resize(sizeof(size_t) + 2 * (sizeof(uint8_t) + sizeof(Int32)));
            char* data_ptr = binary_data.data();

            // Write array size
            memcpy(data_ptr, &array_size, sizeof(size_t));
            data_ptr += sizeof(size_t);

            // Write first element (Int32)
            *data_ptr++ = static_cast<uint8_t>(PrimitiveType::TYPE_INT);
            Int32 val1 = 42;
            memcpy(data_ptr, &val1, sizeof(Int32));
            data_ptr += sizeof(Int32);

            // Write second element (Int32)
            *data_ptr++ = static_cast<uint8_t>(PrimitiveType::TYPE_INT);
            Int32 val2 = 43;
            memcpy(data_ptr, &val2, sizeof(Int32));

            const char* data = binary_data.data();
            parse_binary_from_sparse_column(FieldType::OLAP_FIELD_TYPE_ARRAY, data, res,
                                            field_info);
            const Array& array = res.get<Array>();
            EXPECT_EQ(array.size(), 2);
            EXPECT_EQ(array[0].get<Int32>(), 42);
            EXPECT_EQ(array[1].get<Int32>(), 43);
        }
    }

    // Test add_sub_column
    {
        auto column = VariantUtil::construct_basic_varint_column();
        PathInData path("test.path");

        // Test normal case
        column->add_sub_column(path, 10);

        // Test with existing path
        column->add_sub_column(path, 10);

        // Test with max subcolumns limit
        for (int i = 0; i < 1000; i++) {
            PathInData new_path("test.path." + std::to_string(i));
            column->add_sub_column(new_path, 10);
        }
    }

    // Test wrapp_array_nullable
    {
        auto column = VariantUtil::construct_advanced_varint_column();
        column->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
        PathInData path("v.f");
        auto* subcolumn = column->get_subcolumn(path);
        subcolumn->wrapp_array_nullable();
        EXPECT_TRUE(subcolumn->get_least_common_type()->is_nullable());
    }

    // Test is_empty_nested
    {
        vectorized::ColumnVariant container_variant(1, true);
        // v:  {"k": [1,2,3]} ==》 [{"k": 1}, {"k": 2}, {"k": 3}]
        //     {"k": []} => [{}] vs  {"k": null} -> [null]
        //     {"k": [4]} => [{"k": 4}]
        auto col_arr =
                ColumnArray::create(ColumnInt64::create(), ColumnArray::ColumnOffsets::create());
        //        Array array1 = {1, 2, 3};
        //        Array array2 = {4};
        //        col_arr->insert(array1);
        //        col_arr->insert(array2);
        Field an = Field::create_field<TYPE_ARRAY>(Array());
        an.get<Array>().push_back(Field::create_field<TYPE_BIGINT>(Int64(1)));
        col_arr->insert(an);
        col_arr->insert(an);
        col_arr->insert(an);
        MutableColumnPtr nested_object = ColumnVariant::create(
                container_variant.max_subcolumns_count(), col_arr->get_data().size());
        MutableColumnPtr offset = col_arr->get_offsets_ptr()->assume_mutable(); // [3, 3, 4]
        auto* nested_object_ptr = assert_cast<ColumnVariant*>(nested_object.get());
        // flatten nested arrays
        MutableColumnPtr flattend_column = col_arr->get_data_ptr()->assume_mutable();
        DataTypePtr flattend_type = DataTypeFactory::instance().create_data_type(
                FieldType::OLAP_FIELD_TYPE_BIGINT, 0, 0);
        // add sub path without parent prefix
        PathInData sub_path("k");
        nested_object_ptr->add_sub_column(sub_path, std::move(flattend_column),
                                          std::move(flattend_type));
        nested_object = make_nullable(nested_object->get_ptr())->assume_mutable();
        auto array =
                make_nullable(ColumnArray::create(std::move(nested_object), std::move(offset)));
        PathInData path("v.k");
        container_variant.add_sub_column(path, array->assume_mutable(),
                                         container_variant.NESTED_TYPE);
        container_variant.set_num_rows(3);
        for (auto subcolumn : container_variant.get_subcolumns()) {
            if (subcolumn->data.is_root) {
                // Nothing
                EXPECT_TRUE(subcolumn->data.is_empty_nested(0));
                continue;
            }
            for (int i = 0; i < 3; ++i) {
                EXPECT_FALSE(subcolumn->data.is_empty_nested(i));
            }
        }
    }

    // Test is_empty_nested
    {
        auto v = ColumnVariant::create(1);
        auto sub_dt = make_nullable(std::make_unique<DataTypeArray>(
                make_nullable(std::make_unique<DataTypeVariant>(1))));
        auto sub_col = sub_dt->create_column();

        std::vector<std::pair<std::string, doris::vectorized::Field>> data;
        Field an = Field::create_field<TYPE_ARRAY>(Array());
        an.get<Array>().push_back(Field::create_field<TYPE_NULL>(Null()));
        data.emplace_back("v.a", an);
        // 2. subcolumn path
        auto vf = VariantUtil::construct_variant_map(data);
        v->try_insert(vf);

        for (auto subcolumn : v->get_subcolumns()) {
            for (int i = 0; i < v->size(); ++i) {
                if (subcolumn->data.is_root) {
                    EXPECT_TRUE(subcolumn->data.is_empty_nested(i));
                }
                EXPECT_TRUE(subcolumn->data.is_empty_nested(i));
            }
        }
        v->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
        PathInData path("v.a");
        for (auto sub : v->get_subcolumns()) {
            if (sub->data.is_root) {
                continue;
            }
            sub->kind = SubcolumnsTree<ColumnVariant::Subcolumn, false>::Node::NESTED;
            EXPECT_FALSE(v->try_insert_default_from_nested(sub));
        }
    }
}

// Helper function to create ColumnVariant with various types (int, double, array, string)
static auto create_mixed_type_variant_column() {
    auto variant = ColumnVariant::create(1);

    // Create test data with different types
    std::vector<std::pair<std::string, doris::vectorized::Field>> test_data = {
            // int type
            {"data.int_field", doris::vectorized::Field::create_field<TYPE_INT>(42)},
            // double type
            {"data.double_field", doris::vectorized::Field::create_field<TYPE_DOUBLE>(3.14159)},
            // string type
            {"data.string_field",
             doris::vectorized::Field::create_field<TYPE_STRING>(String("hello_world"))},
            // array of ints
            {"data.array_int_field",
             [] {
                 auto array_field = doris::vectorized::Field::create_field<TYPE_ARRAY>(Array());
                 auto& array = array_field.get<Array>();
                 array.emplace_back(doris::vectorized::Field::create_field<TYPE_INT>(1));
                 array.emplace_back(doris::vectorized::Field::create_field<TYPE_INT>(2));
                 array.emplace_back(doris::vectorized::Field::create_field<TYPE_INT>(3));
                 return array_field;
             }()},
            // array of strings
            {"data.array_string_field", [] {
                 auto array_field = doris::vectorized::Field::create_field<TYPE_ARRAY>(Array());
                 auto& array = array_field.get<Array>();
                 array.emplace_back(
                         doris::vectorized::Field::create_field<TYPE_STRING>(String("apple")));
                 array.emplace_back(
                         doris::vectorized::Field::create_field<TYPE_STRING>(String("banana")));
                 return array_field;
             }()}};

    // Insert 5 rows with the same structure
    for (int i = 0; i < 5; ++i) {
        auto variant_map = VariantUtil::construct_variant_map(test_data);
        variant->try_insert(variant_map);
    }

    // Add some additional rows with different structures for sparse column
    std::vector<std::pair<std::string, doris::vectorized::Field>> sparse_data = {
            {"data.int_field", doris::vectorized::Field::create_field<TYPE_INT>(100)},
            {"data.double_field", doris::vectorized::Field::create_field<TYPE_DOUBLE>(2.71828)},
            {"data.nested.sparse_string",
             doris::vectorized::Field::create_field<TYPE_STRING>(String("sparse_data"))},
            {"data.nested.sparse_int", doris::vectorized::Field::create_field<TYPE_INT>(999)}};

    // Insert 3 more rows with sparse structure
    for (int i = 0; i < 3; ++i) {
        auto sparse_variant_map = VariantUtil::construct_variant_map(sparse_data);
        variant->try_insert(sparse_variant_map);
    }

    return variant;
}

// // Test to generate ColumnVariant data and serialize to binary file
// TEST_F(ColumnVariantTest, generate_compatibility_test_data) {
//     // 1. Create ColumnVariant with various types
//     auto variant_column = create_mixed_type_variant_column();
//     variant_column->finalize();
//     EXPECT_TRUE(variant_column->pick_subcolumns_to_sparse_column({}, false).ok());
//     variant_column->finalize();
//     EXPECT_LE(variant_column->get_subcolumns().size(), 2);
//
//     // 2. Create DataTypeVariant for serialization
//     auto data_type_variant = std::make_shared<DataTypeVariant>();
//
//     // 3. Calculate serialized size
//     int be_exec_version = 0; // Use current version
//     int64_t serialized_size =
//             data_type_variant->get_uncompressed_serialized_bytes(*variant_column, be_exec_version);
//     EXPECT_GT(serialized_size, 0);
//
//     // 4. Serialize to buffer
//     std::vector<char> buffer(serialized_size);
//     char* buf_end = data_type_variant->serialize(*variant_column, buffer.data(), be_exec_version);
//     size_t actual_size = buf_end - buffer.data();
//     // EXPECT_EQ(actual_size, serialized_size);
//
//     // 5. Write to binary file
//     std::string test_data_path =
//             std::string(getenv("ROOT")) + "/be/test/util/test_data/column_variant.bin";
//     std::ofstream file(test_data_path, std::ios::binary);
//     ASSERT_TRUE(file.is_open()) << "Failed to open file for writing: " << test_data_path;
//     file.write(buffer.data(), actual_size);
//     file.close();
//
//     std::cout << "Successfully generated test data file: " << test_data_path << std::endl;
//     std::cout << "Serialized size: " << actual_size << " bytes" << std::endl;
// }

// Test to deserialize from binary file and verify data consistency
TEST_F(ColumnVariantTest, compatibility_deserialize_and_verify) {
    // 1. Create reference data for comparison
    auto expected_variant = create_mixed_type_variant_column();
    expected_variant->finalize();

    // 2. Read binary data from file
    std::string test_data_path =
            std::string(getenv("ROOT")) + "/be/test/util/test_data/column_variant.bin";
    std::ifstream file(test_data_path, std::ios::binary);
    ASSERT_TRUE(file.is_open()) << "Failed to open test data file: " << test_data_path;

    // Get file size
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    file.seekg(0, std::ios::beg);
    EXPECT_GT(file_size, 0);

    // Read all data
    std::vector<char> buffer(file_size);
    file.read(buffer.data(), file_size);
    file.close();

    // 3. Deserialize from binary data
    auto data_type_variant = std::make_shared<DataTypeVariant>();
    auto deserialized_column = data_type_variant->create_column();
    int be_exec_version = 0;

    const char* buf_end =
            data_type_variant->deserialize(buffer.data(), &deserialized_column, be_exec_version);
    size_t consumed_size = buf_end - buffer.data();
    EXPECT_EQ(consumed_size, file_size);

    auto variant_column = assert_cast<ColumnVariant*>(deserialized_column.get());
    EXPECT_EQ(variant_column->size(), 8);

    // 4. Use ColumnVariant::get to retrieve VariantMap and verify data
    for (size_t row = 0; row < variant_column->size(); ++row) {
        Field result_field;
        variant_column->get(row, result_field);
        EXPECT_EQ(result_field.get_type(), TYPE_VARIANT);

        const auto& variant_map = result_field.get<VariantMap>();

        if (row < 5) {
            // First 5 rows should have the structured data
            EXPECT_TRUE(variant_map.find(PathInData("data.int_field")) != variant_map.end());
            EXPECT_TRUE(variant_map.find(PathInData("data.double_field")) != variant_map.end());
            EXPECT_TRUE(variant_map.find(PathInData("data.string_field")) != variant_map.end());
            EXPECT_TRUE(variant_map.find(PathInData("data.array_int_field")) != variant_map.end());
            EXPECT_TRUE(variant_map.find(PathInData("data.array_string_field")) !=
                        variant_map.end());

            // Verify specific values
            auto int_iter = variant_map.find(PathInData("data.int_field"));
            if (int_iter != variant_map.end()) {
                EXPECT_EQ(int_iter->second.field.get<Int32>(), 42);
            }

            auto double_iter = variant_map.find(PathInData("data.double_field"));
            if (double_iter != variant_map.end()) {
                EXPECT_DOUBLE_EQ(double_iter->second.field.get<Float64>(), 3.14159);
            }

            auto string_iter = variant_map.find(PathInData("data.string_field"));
            if (string_iter != variant_map.end()) {
                EXPECT_EQ(string_iter->second.field.get<String>(), "hello_world");
            }

            auto array_int_iter = variant_map.find(PathInData("data.array_int_field"));
            if (array_int_iter != variant_map.end()) {
                const auto& array = array_int_iter->second.field.get<Array>();
                EXPECT_EQ(array.size(), 3);
                EXPECT_EQ(array[0].get<Int32>(), 1);
                EXPECT_EQ(array[1].get<Int32>(), 2);
                EXPECT_EQ(array[2].get<Int32>(), 3);
            }

            auto array_string_iter = variant_map.find(PathInData("data.array_string_field"));
            if (array_string_iter != variant_map.end()) {
                const auto& array = array_string_iter->second.field.get<Array>();
                EXPECT_EQ(array.size(), 2);
                EXPECT_EQ(array[0].get<String>(), "apple");
                EXPECT_EQ(array[1].get<String>(), "banana");
            }
        } else {
            // Last 3 rows should have sparse data
            EXPECT_TRUE(variant_map.find(PathInData("data.int_field")) != variant_map.end());
            EXPECT_TRUE(variant_map.find(PathInData("data.double_field")) != variant_map.end());
            EXPECT_TRUE(variant_map.find(PathInData("data.nested.sparse_string")) !=
                        variant_map.end());
            EXPECT_TRUE(variant_map.find(PathInData("data.nested.sparse_int")) !=
                        variant_map.end());

            // Verify sparse data values
            auto int_iter = variant_map.find(PathInData("data.int_field"));
            if (int_iter != variant_map.end()) {
                EXPECT_EQ(int_iter->second.field.get<Int32>(), 100);
            }

            auto double_iter = variant_map.find(PathInData("data.double_field"));
            if (double_iter != variant_map.end()) {
                EXPECT_DOUBLE_EQ(double_iter->second.field.get<Float64>(), 2.71828);
            }

            auto sparse_string_iter = variant_map.find(PathInData("data.nested.sparse_string"));
            if (sparse_string_iter != variant_map.end()) {
                EXPECT_EQ(sparse_string_iter->second.field.get<String>(), "sparse_data");
            }

            auto sparse_int_iter = variant_map.find(PathInData("data.nested.sparse_int"));
            if (sparse_int_iter != variant_map.end()) {
                EXPECT_EQ(sparse_int_iter->second.field.get<Int32>(), 999);
            }
        }
    }

    std::cout << "Successfully verified deserialized data integrity!" << std::endl;
}

TEST_F(ColumnVariantTest, subcolumn_insert_range_from_test) {
    ColumnVariant::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);
    Field int_field = Field::create_field<TYPE_INT>(200000);
    Field string_field = Field::create_field<TYPE_STRING>("hello");

    Array array_int(2);
    array_int[0] = int_field;
    array_int[1] = int_field;
    Field array_int_field = Field::create_field<TYPE_ARRAY>(array_int);
    ColumnVariant::Subcolumn subcolumn2(0, true /* is_nullable */, false /* is_root */);
    subcolumn2.insert(array_int_field);
    subcolumn2.finalize();

    Array array_tiny_int(2);
    Field tiny_int = Field::create_field<TYPE_TINYINT>(100);
    array_tiny_int[0] = tiny_int;
    array_tiny_int[1] = tiny_int;
    Field array_tiny_int_field = Field::create_field<TYPE_ARRAY>(array_tiny_int);
    ColumnVariant::Subcolumn subcolumn1(0, true /* is_nullable */, false /* is_root */);
    subcolumn1.insert(array_tiny_int_field);
    subcolumn1.finalize();

    Array array_string(2);
    array_string[0] = string_field;
    array_string[1] = string_field;
    Field array_string_field = Field::create_field<TYPE_ARRAY>(array_string);
    ColumnVariant::Subcolumn subcolumn3(0, true /* is_nullable */, false /* is_root */);
    subcolumn3.insert(array_string_field);
    subcolumn3.finalize();

    subcolumn.insert_range_from(subcolumn1, 0, 1);
    subcolumn.insert_range_from(subcolumn2, 0, 1);
    subcolumn.insert_range_from(subcolumn3, 0, 1);
    subcolumn.finalize();
    EXPECT_EQ(subcolumn.data.size(), 1);
    std::cout << subcolumn.get_least_common_type()->get_name() << std::endl;
    EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(), PrimitiveType::TYPE_ARRAY);
}

TEST_F(ColumnVariantTest, subcolumn_insert_test) {
    ColumnVariant::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);
    Field int_field = Field::create_field<TYPE_INT>(200000);
    Field string_field = Field::create_field<TYPE_STRING>("hello");
    Array array_int(2);
    array_int[0] = int_field;
    array_int[1] = int_field;
    Field array_int_field = Field::create_field<TYPE_ARRAY>(array_int);

    Array array_int2(2);
    Field tiny_int = Field::create_field<TYPE_TINYINT>(100);
    array_int2[0] = tiny_int;
    array_int2[1] = tiny_int;
    Field array_int2_field = Field::create_field<TYPE_ARRAY>(array_int2);

    Array array_string(2);
    array_string[0] = string_field;
    array_string[1] = string_field;
    Field array_string_field = Field::create_field<TYPE_ARRAY>(array_string);

    subcolumn.insert(array_int2_field);
    subcolumn.insert(array_int_field);
    subcolumn.insert(array_string_field);
    subcolumn.finalize();
    EXPECT_EQ(subcolumn.data.size(), 1);
    EXPECT_EQ(subcolumn.get_least_common_type()->get_primitive_type(), PrimitiveType::TYPE_ARRAY);

    subcolumn.insert(string_field);
    subcolumn.insert(int_field);
    EXPECT_EQ(subcolumn.data.size(), 2);
    EXPECT_EQ(remove_nullable(subcolumn.get_least_common_type())->get_primitive_type(),
              PrimitiveType::TYPE_JSONB);
}

TEST_F(ColumnVariantTest, subcolumn_insert_test_advanced) {
    std::vector<Field> fields;

    fields.emplace_back(Field::create_field<TYPE_NULL>(Null()));

    fields.emplace_back(Field::create_field<TYPE_BOOLEAN>(true));

    fields.emplace_back(Field::create_field<TYPE_BIGINT>(922337203685477588));

    fields.emplace_back(Field::create_field<TYPE_LARGEINT>(922337203685477588));

    fields.emplace_back(Field::create_field<TYPE_DOUBLE>(-3.14159265359));

    fields.emplace_back(Field::create_field<TYPE_STRING>("hello world"));

    Array arr_boolean(2);
    arr_boolean[0] = Field::create_field<TYPE_BOOLEAN>(true);
    arr_boolean[1] = Field::create_field<TYPE_BOOLEAN>(false);
    Field arr_boolean_field = Field::create_field<TYPE_ARRAY>(arr_boolean);
    fields.emplace_back(arr_boolean_field);

    Array arr_int64(2);
    arr_int64[0] = Field::create_field<TYPE_BIGINT>(1232323232323232323);
    arr_int64[1] = Field::create_field<TYPE_BIGINT>(2232323223232323232);
    Field arr_int64_field = Field::create_field<TYPE_ARRAY>(arr_int64);
    fields.emplace_back(arr_int64_field);

    Array arr_double(2);
    arr_double[0] = Field::create_field<TYPE_DOUBLE>(1.1);
    arr_double[1] = Field::create_field<TYPE_DOUBLE>(2.2);
    Field arr_double_field = Field::create_field<TYPE_ARRAY>(arr_double);
    fields.emplace_back(arr_double_field);

    Array arr_string(2);
    arr_string[0] = Field::create_field<TYPE_STRING>("one");
    arr_string[1] = Field::create_field<TYPE_STRING>("two");
    Field arr_string_field = Field::create_field<TYPE_ARRAY>(arr_string);
    fields.emplace_back(arr_string_field);

    Array arr_jsonb(5);
    arr_jsonb[0] = Field::create_field<TYPE_STRING>("one");
    arr_jsonb[1] = Field::create_field<TYPE_DOUBLE>(1.1);
    arr_jsonb[2] = Field::create_field<TYPE_BOOLEAN>(true);
    arr_jsonb[3] = Field::create_field<TYPE_LARGEINT>(1232323232323232323);
    arr_jsonb[4] = Field::create_field<TYPE_BIGINT>(1232323232323232323);
    Field arr_jsonb_field = Field::create_field<TYPE_ARRAY>(arr_jsonb);
    fields.emplace_back(arr_jsonb_field);

    std::random_device rd;
    std::mt19937 g(rd());

    for (int i = 0; i < (1 << fields.size()); i++) {
        std::shuffle(fields.begin(), fields.end(), g);
        auto subcolumn = ColumnVariant::Subcolumn(0, true, false);

        for (const auto& field : fields) {
            subcolumn.insert(field);
        }

        subcolumn.finalize();
        EXPECT_EQ(subcolumn.data.size(), 1);
        // std::cout << "least common type: " << subcolumn.get_least_common_type()->get_name() << std::endl;
        EXPECT_EQ(subcolumn.least_common_type.get_base_type_id(), PrimitiveType::TYPE_JSONB);

        for (const auto& field : fields) {
            subcolumn.insert(field);
        }
        EXPECT_EQ(subcolumn.least_common_type.get_base_type_id(), PrimitiveType::TYPE_JSONB);

        if (i % 1000 == 0) {
            std::cout << "insert count " << i << std::endl;
        }
    }
}

TEST_F(ColumnVariantTest, subcolumn_insert_range_from_test_advanced) {
    std::vector<Field> fields;

    fields.emplace_back(Field::create_field<TYPE_NULL>(Null()));

    fields.emplace_back(Field::create_field<TYPE_BOOLEAN>(true));

    fields.emplace_back(Field::create_field<TYPE_BIGINT>(922337203685477588));

    fields.emplace_back(Field::create_field<TYPE_LARGEINT>(922337203685477588));

    fields.emplace_back(Field::create_field<TYPE_DOUBLE>(-3.14159265359));

    fields.emplace_back(Field::create_field<TYPE_STRING>("hello world"));

    Array arr_boolean(2);
    arr_boolean[0] = Field::create_field<TYPE_BOOLEAN>(true);
    arr_boolean[1] = Field::create_field<TYPE_BOOLEAN>(false);
    Field arr_boolean_field = Field::create_field<TYPE_ARRAY>(arr_boolean);
    fields.emplace_back(arr_boolean_field);

    Array arr_int64(2);
    arr_int64[0] = Field::create_field<TYPE_BIGINT>(1232323232323232323);
    arr_int64[1] = Field::create_field<TYPE_BIGINT>(2232323223232323232);
    Field arr_int64_field = Field::create_field<TYPE_ARRAY>(arr_int64);
    fields.emplace_back(arr_int64_field);

    Array arr_largeint(2);
    arr_largeint[0] = Field::create_field<TYPE_LARGEINT>(1232323232323232323);
    arr_largeint[1] = Field::create_field<TYPE_LARGEINT>(2232323223232323232);
    Field arr_largeint_field = Field::create_field<TYPE_ARRAY>(arr_largeint);
    fields.emplace_back(arr_largeint_field);

    Array arr_double(2);
    arr_double[0] = Field::create_field<TYPE_DOUBLE>(1.1);
    arr_double[1] = Field::create_field<TYPE_DOUBLE>(2.2);
    Field arr_double_field = Field::create_field<TYPE_ARRAY>(arr_double);
    fields.emplace_back(arr_double_field);

    Array arr_string(2);
    arr_string[0] = Field::create_field<TYPE_STRING>("one");
    arr_string[1] = Field::create_field<TYPE_STRING>("two");
    Field arr_string_field = Field::create_field<TYPE_ARRAY>(arr_string);
    fields.emplace_back(arr_string_field);

    Array arr_jsonb(5);
    arr_jsonb[0] = Field::create_field<TYPE_STRING>("one");
    arr_jsonb[1] = Field::create_field<TYPE_DOUBLE>(1.1);
    arr_jsonb[2] = Field::create_field<TYPE_BOOLEAN>(true);
    arr_jsonb[3] = Field::create_field<TYPE_LARGEINT>(1232323232323232323);
    arr_jsonb[4] = Field::create_field<TYPE_BIGINT>(1232323232323232323);
    Field arr_jsonb_field = Field::create_field<TYPE_ARRAY>(arr_jsonb);
    fields.emplace_back(arr_jsonb_field);

    std::random_device rd;
    std::mt19937 g(rd());

    for (int i = 0; i < (1 << fields.size()); i++) {
        std::shuffle(fields.begin(), fields.end(), g);
        auto subcolumn = ColumnVariant::Subcolumn(0, true, false);

        for (const auto& field : fields) {
            auto subcolumn_tmp = ColumnVariant::Subcolumn(0, true, false);
            subcolumn_tmp.insert(field);
            subcolumn.insert_range_from(subcolumn_tmp, 0, 1);
        }

        subcolumn.finalize();
        EXPECT_EQ(subcolumn.data.size(), 1);
        // std::cout << "least common type: " << subcolumn.get_least_common_type()->get_name() << std::endl;
        EXPECT_EQ(subcolumn.least_common_type.get_base_type_id(), PrimitiveType::TYPE_JSONB);

        for (const auto& field : fields) {
            subcolumn.insert(field);
        }
        EXPECT_EQ(subcolumn.least_common_type.get_base_type_id(), PrimitiveType::TYPE_JSONB);

        if (i % 1000 == 0) {
            std::cout << "insert count " << i << std::endl;
        }
    }
}

} // namespace doris::vectorized
