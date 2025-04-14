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

#include "vec/columns/column_object.h"

#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>
#include <rapidjson/prettywriter.h>
#include <stdio.h>

#include "runtime/jsonb_value.h"
#include "testutil/variant_util.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"

using namespace doris::vectorized;

using namespace doris;
// #define ADD_SUB_COLUMN(key) \
//    varaint->add_sub_column(PathInData(std::string_view(key)), 0);

void convert_field_to_rapidjson(const vectorized::Field& field, rapidjson::Value& target,
                                rapidjson::Document::AllocatorType& allocator) {
    switch (field.get_type()) {
    case vectorized::Field::Types::Null:
        target.SetNull();
        break;
    case vectorized::Field::Types::Int64:
        target.SetInt64(field.get<Int64>());
        break;
    case vectorized::Field::Types::Float64:
        target.SetDouble(field.get<Float64>());
        break;
    case vectorized::Field::Types::JSONB: {
        const auto& val = field.get<JsonbField>();
        JsonbValue* json_val = JsonbDocument::createValue(val.get_value(), val.get_size());
        convert_jsonb_to_rapidjson(*json_val, target, allocator);
        break;
    }
    case vectorized::Field::Types::String: {
        const String& val = field.get<String>();
        target.SetString(val.data(), cast_set<rapidjson::SizeType>(val.size()));
        break;
    }
    case vectorized::Field::Types::Array: {
        const vectorized::Array& array = field.get<Array>();
        target.SetArray();
        for (const vectorized::Field& item : array) {
            rapidjson::Value val;
            convert_field_to_rapidjson(item, val, allocator);
            target.PushBack(val, allocator);
        }
        break;
    }
    case vectorized::Field::Types::VariantMap: {
        const vectorized::VariantMap& map = field.get<VariantMap>();
        target.SetObject();
        for (const auto& item : map) {
            if (item.second.is_null()) {
                continue;
            }
            rapidjson::Value key;
            key.SetString(item.first.data(), cast_set<rapidjson::SizeType>(item.first.size()));
            rapidjson::Value val;
            convert_field_to_rapidjson(item.second, val, allocator);
            if (val.IsNull() && item.first.empty()) {
                // skip null value with empty key, indicate the null json value of root in variant map,
                // usally padding in nested arrays
                continue;
            }
            target.AddMember(key, val, allocator);
        }
        break;
    }
    default:
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "unkown field type: {}",
                               field.get_type_name());
        break;
    }
}

void convert_variant_map_to_rapidjson(const vectorized::VariantMap& map, rapidjson::Value& target,
                                      rapidjson::Document::AllocatorType& allocator) {
    target.SetObject();
    for (const auto& item : map) {
        if (item.second.is_null()) {
            continue;
        }
        rapidjson::Value key;
        key.SetString(item.first.data(), cast_set<rapidjson::SizeType>(item.first.size()));
        rapidjson::Value val;
        convert_field_to_rapidjson(item.second, val, allocator);
        if (val.IsNull() && item.first.empty()) {
            // skip null value with empty key, indicate the null json value of root in variant map,
            // usally padding in nested arrays
            continue;
        }
        target.AddMember(key, val, allocator);
    }
}

void convert_array_to_rapidjson(const vectorized::Array& array, rapidjson::Value& target,
                                rapidjson::Document::AllocatorType& allocator) {
    target.SetArray();
    for (const vectorized::Field& item : array) {
        rapidjson::Value val;
        convert_field_to_rapidjson(item, val, allocator);
        target.PushBack(val, allocator);
    }
}

TEST(ColumnVariantTest, insert_try_insert) {
    auto v = VariantUtil::construct_dst_varint_column();
    FieldInfo info;
    info.scalar_type_id = TypeIndex::Nothing;
    info.num_dimensions = 0;
    PathInData path("v.f");
    auto sub = v->get_subcolumn(path);
    Int64 value = 43;
    sub->insert(value, info);

    info.num_dimensions = 1;
    sub->insert(value, info);

    info.num_dimensions = 2;
    sub->insert(value, info);
}

TEST(ColumnVariantTest, basic_finalize) {
    auto variant = VariantUtil::construct_basic_varint_column();
    // 4. finalize
    EXPECT_TRUE(variant->finalize(ColumnObject::FinalizeMode::WRITE_MODE).ok());
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

TEST(ColumnVariantTest, basic_deserialize) {
    auto variant = VariantUtil::construct_basic_varint_column();

    // 4. finalize
    EXPECT_TRUE(variant->finalize(ColumnObject::FinalizeMode::WRITE_MODE).ok());
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

TEST(ColumnVariantTest, basic_inset_range_from) {
    auto src = VariantUtil::construct_basic_varint_column();
    EXPECT_TRUE(src->finalize(ColumnObject::FinalizeMode::WRITE_MODE).ok());
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
    ColumnObject::Subcolumns dst_subcolumns = dst->subcolumns;
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
            TypeIndex::JSONB, false);
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
            TypeIndex::JSONB, false);
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
                TypeIndex::Int8, false);
        DataTypePtr data_type_array_int =
                std::make_shared<doris::vectorized::DataTypeArray>(data_type_int);
        auto array_column_int = data_type_array_int->create_column();
        array_column_int->insert(VariantUtil::get_field("array_int"));
        auto array_serde_int = data_type_array_int->get_serde();
        field_map["array_int"] = convert_to_jsonb_field(array_serde_int, *array_column_int);

        DataTypePtr data_type_str = doris::vectorized::DataTypeFactory::instance().create_data_type(
                TypeIndex::String, false);
        DataTypePtr data_type_array_str =
                std::make_shared<doris::vectorized::DataTypeArray>(data_type_str);
        auto array_column_str = data_type_array_str->create_column();
        array_column_str->insert(VariantUtil::get_field("array_str"));
        auto array_serde_str = data_type_array_str->get_serde();
        field_map["array_str"] = convert_to_jsonb_field(array_serde_str, *array_column_str);

        auto column_int = data_type_int->create_column();
        column_int->insert(VariantUtil::get_field("int"));
        auto serde_int = data_type_int->get_serde();
        field_map["int"] = convert_to_jsonb_field(serde_int, *column_int);

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
    convert_field_to_rapidjson(array, json_value, allocator);
    doc.AddMember("value", json_value, allocator);
    rapidjson::StringBuffer buffer;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    return std::string(buffer.GetString());
}

TEST(ColumnVariantTest, is_null_at) {
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
    vectorized::Field f;
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

TEST(ColumnVariantTest, advanced_finalize) {
    auto variant = VariantUtil::construct_advanced_varint_column();

    // 4. finalize
    EXPECT_TRUE(variant->finalize(ColumnObject::FinalizeMode::WRITE_MODE).ok());
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
        vectorized::ColumnObject::fill_path_column_from_sparse_data(
                *variant->get_subcolumn({}) /*root*/, map.get(), StringRef {"array"},
                variant->get_sparse_column(), 0, 5);
        vectorized::ColumnObject::fill_path_column_from_sparse_data(
                *variant->get_subcolumn({}) /*root*/, map.get(), StringRef {"array"},
                variant->get_sparse_column(), 5, 15);
    }
}

TEST(ColumnVariantTest, advanced_deserialize) {
    auto variant = VariantUtil::construct_advanced_varint_column();

    // 4. finalize
    EXPECT_TRUE(variant->finalize(ColumnObject::FinalizeMode::WRITE_MODE).ok());
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

TEST(ColumnVariantTest, advanced_insert_range_from) {
    auto src = VariantUtil::construct_advanced_varint_column();
    EXPECT_TRUE(src->finalize(ColumnObject::FinalizeMode::WRITE_MODE).ok());
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
    ColumnObject::Subcolumns dst_subcolumns = dst->subcolumns;

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
                                  ->get_type_id(),
                          TypeIndex::JSONB);
            }
        } else if (column->path.get_path().size() == 5) {
            EXPECT_EQ(column->data.get_non_null_value_size(), 10);
            EXPECT_EQ(assert_cast<const DataTypeNullable*>(column->data.data_types[0].get())
                              ->get_nested_type()
                              ->get_type_id(),
                      TypeIndex::JSONB);
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

TEST(ColumnVariantTest, empty_inset_range_from) {
    auto src = VariantUtil::construct_varint_column_only_subcolumns();
    EXPECT_TRUE(src->finalize(ColumnObject::FinalizeMode::WRITE_MODE).ok());
    EXPECT_EQ(src->size(), 6);

    // dst is an empty column
    auto dst = ColumnObject::create(5);

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

    EXPECT_TRUE(
            src_contains_seven_subcolumns->finalize(ColumnObject::FinalizeMode::WRITE_MODE).ok());
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
    EXPECT_TRUE(src_contains_subcoumns_and_sparse_columns
                        ->finalize(ColumnObject::FinalizeMode::WRITE_MODE)
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