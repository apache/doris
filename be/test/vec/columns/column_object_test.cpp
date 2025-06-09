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
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <rapidjson/prettywriter.h>
#include <stdio.h>

#include "common/cast_set.h"
#include "common/exception.h"
#include "runtime/jsonb_value.h"
#include "testutil/variant_util.h"
#include "vec/columns/common_column_test.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris::vectorized {

class ColumnObjectTest : public ::testing::Test {};

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
            key.SetString(item.first.get_path().data(),
                          cast_set<rapidjson::SizeType>(item.first.get_path().size()));
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
        key.SetString(item.first.get_path().data(),
                      cast_set<rapidjson::SizeType>(item.first.get_path().size()));
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
    Int64 value = 43000000;

    try {
        sub->insert(value, info);
        info.num_dimensions = 1;
        sub->insert(value, info);

        info.num_dimensions = 2;
        sub->insert(value, info);
    } catch (const doris::Exception& e) {
        std::cout << "encounter exception: " << e.what() << std::endl;
    }
}

TEST(ColumnVariantTest, basic_finalize) {
    auto variant = VariantUtil::construct_basic_varint_column();
    // 4. finalize
    EXPECT_TRUE(variant->finalize(ColumnObject::FinalizeMode::WRITE_MODE).ok());
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

TEST(ColumnVariantTest, basic_deserialize) {
    auto variant = VariantUtil::construct_basic_varint_column();

    // 4. finalize
    EXPECT_TRUE(variant->finalize(ColumnObject::FinalizeMode::WRITE_MODE).ok());
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

// test ColumnVariant with ColumnNothing using update_hash_with_value
TEST(ColumnVariantTest, updateHashValueWithColumnNothingTest) {
    // Create a ColumnObject with a subcolumn that contains ColumnNothing
    auto variant = ColumnObject::create(3, 3);

    // Create a subcolumn with ColumnNothing type
    PathInData path("v.nothing");
    auto type = DataTypeFactory::instance().create_data_type(TypeIndex::Nothing);
    auto column = type->create_column();
    column->insert_many_defaults(3);
    variant->add_sub_column(path, std::move(column), type);

    // Finalize the variant column to ensure proper structure
    // EXPECT_TRUE(variant->finalize(ColumnObject::FinalizeMode::WRITE_MODE).ok());
    // EXPECT_TRUE(variant->pick_subcolumns_to_sparse_column({}).ok());
    EXPECT_EQ(variant->size(), 3);

    // Test update_hash_with_value with ColumnNothing
    SipHash hash1, hash2, hash3;

    // Test that update_hash_with_value doesn't crash with ColumnNothing
    EXPECT_NO_THROW(variant->update_hash_with_value(0, hash1));
    EXPECT_NO_THROW(variant->update_hash_with_value(1, hash2));
    EXPECT_NO_THROW(variant->update_hash_with_value(2, hash3));

    // For ColumnNothing, the hash should be consistent since it doesn't contain actual data
    // However, the hash might include structural information, so we just verify it doesn't crash
    // and produces some hash value
    EXPECT_NE(hash1.get64(), 0);
    EXPECT_NE(hash2.get64(), 0);
    EXPECT_NE(hash3.get64(), 0);

    // Test update_hashes_with_value with ColumnNothing
    std::vector<uint64_t> hashes(3, 0);
    EXPECT_NO_THROW(variant->update_hashes_with_value(hashes.data()));

    // Verify that hashes are computed (non-zero)
    EXPECT_NE(hashes[0], 0);
    EXPECT_NE(hashes[1], 0);
    EXPECT_NE(hashes[2], 0);

    // Test update_xxHash_with_value with ColumnNothing
    uint64_t xxhash = 0;
    EXPECT_NO_THROW(variant->update_xxHash_with_value(0, 3, xxhash, nullptr));
    EXPECT_NE(xxhash, 0);

    // Test update_crc_with_value with ColumnNothing
    uint32_t crc_hash = 0;
    EXPECT_NO_THROW(variant->update_crc_with_value(0, 3, crc_hash, nullptr));
    EXPECT_NE(crc_hash, 0);

    // Test with null map
    std::vector<uint8_t> null_map(3, 0);
    null_map[1] = 1; // Mark second row as null

    std::vector<uint64_t> hashes_with_null(3, 0);
    EXPECT_NO_THROW(variant->update_hashes_with_value(hashes_with_null.data(), null_map.data()));

    uint64_t xxhash_with_null = 0;
    EXPECT_NO_THROW(variant->update_xxHash_with_value(0, 3, xxhash_with_null, null_map.data()));

    uint32_t crc_hash_with_null = 0;
    EXPECT_NO_THROW(variant->update_crc_with_value(0, 3, crc_hash_with_null, null_map.data()));
}

TEST(ColumnVariantTest, basic_inset_range_from) {
    auto src = VariantUtil::construct_basic_varint_column();
    EXPECT_TRUE(src->finalize(ColumnObject::FinalizeMode::WRITE_MODE).ok());
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
    // DataTypeSerDe::convert_field_to_rapidjson(array, json_value, allocator);
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

TEST(ColumnVariantTest, advanced_insert_range_from) {
    auto src = VariantUtil::construct_advanced_varint_column();
    EXPECT_TRUE(src->finalize(ColumnObject::FinalizeMode::WRITE_MODE).ok());
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
    EXPECT_TRUE(src->pick_subcolumns_to_sparse_column({}, false).ok());
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
    EXPECT_TRUE(src_contains_subcoumns_and_sparse_columns
                        ->finalize(ColumnObject::FinalizeMode::WRITE_MODE)
                        .ok());
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

TEST(ColumnVariantTest, insert_null_to_decimal_column) {
    ColumnObject::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);
    Field null_field;
    subcolumn.insert(null_field);
    subcolumn.finalize();
    EXPECT_EQ(subcolumn.data.size(), 1);
    EXPECT_EQ(subcolumn.data[0]->size(), 1);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.least_common_type.get_base_type_id(), TypeIndex::Nothing);
    Field decimal_field(DecimalField<Decimal128V2>(10, 2));
    subcolumn.insert(decimal_field);
    subcolumn.finalize();
    EXPECT_EQ(subcolumn.get_non_null_value_size(), 1);
    EXPECT_EQ(subcolumn.data.size(), 1);
    EXPECT_EQ(subcolumn.data[0]->size(), 2);
    EXPECT_EQ(subcolumn.data[0]->is_null_at(0), true);
    EXPECT_EQ(subcolumn.data[0]->is_null_at(1), false);
    EXPECT_EQ(subcolumn.data_types.size(), 1);
    EXPECT_EQ(subcolumn.least_common_type.get_base_type_id(), TypeIndex::Decimal128V2);
}

TEST(ColumnVariantTest, subcolumn_insert_range_from_test) {
    ColumnObject::Subcolumn subcolumn(0, true /* is_nullable */, false /* is_root */);
    Field int_field(20);
    Field string_field("hello");
    Field array_int_field = Array(2);
    auto& array_int = array_int_field.get<Array>();
    array_int[0] = int_field;
    array_int[1] = int_field;
    ColumnObject::Subcolumn subcolumn2(0, true /* is_nullable */, false /* is_root */);
    subcolumn2.insert(array_int_field);
    subcolumn2.finalize();

    Field array_string_field = Array(2);
    auto& array_string = array_string_field.get<Array>();
    array_string[0] = string_field;
    array_string[1] = string_field;
    ColumnObject::Subcolumn subcolumn3(0, true /* is_nullable */, false /* is_root */);
    subcolumn3.insert(array_string_field);
    subcolumn3.finalize();

    subcolumn.insert_range_from(subcolumn2, 0, 1);
    subcolumn.insert_range_from(subcolumn3, 0, 1);
    subcolumn.finalize();
    EXPECT_EQ(subcolumn.data.size(), 1);
    ColumnObject::Subcolumn subcolumn4(0, true /* is_nullable */, false /* is_root */);
    subcolumn4.insert(int_field);
    subcolumn4.finalize();

    ColumnObject::Subcolumn subcolumn5(0, true /* is_nullable */, false /* is_root */);
    subcolumn5.insert(string_field);
    subcolumn5.finalize();

    subcolumn.insert_range_from(subcolumn4, 0, 1);
    subcolumn.insert_range_from(subcolumn5, 0, 1);
    subcolumn.finalize();
    EXPECT_EQ(subcolumn.data.size(), 1);
}

TEST(ColumnVariantTest, subcolumn_insert_range_fromtest_variant_field) {
    std::vector<Field> fields;
    fields.emplace_back(
            VariantField(DecimalField<Decimal32>(Decimal32(1234), 2), TypeIndex::Decimal32, 6, 2));
    fields.emplace_back(
            VariantField(DecimalField<Decimal64>(Decimal64(5678), 2), TypeIndex::Decimal64, 8, 2));
    fields.emplace_back(VariantField(DecimalField<Decimal128V2>(Decimal128V2(91011), 2),
                                     TypeIndex::Decimal128V2, 16, 2));
    fields.emplace_back(VariantField(DecimalField<Decimal128V3>(Decimal128V3(121314), 2),
                                     TypeIndex::Decimal128V3, 18, 2));
    fields.emplace_back(VariantField(DecimalField<Decimal256>(Decimal256(151617), 2),
                                     TypeIndex::Decimal256, 32, 2));
    Array arr_decimal32;
    arr_decimal32.push_back(Field(VariantField(DecimalField<Decimal32>(Decimal32(12345678), 2),
                                               TypeIndex::Decimal32, 6, 2)));
    arr_decimal32.push_back(Field(VariantField(DecimalField<Decimal32>(Decimal32(87654321), 2),
                                               TypeIndex::Decimal32, 6, 2)));
    fields.emplace_back(VariantField(arr_decimal32, TypeIndex::Array));

    Array arr_decimal64;
    arr_decimal64.push_back(Field(VariantField(
            DecimalField<Decimal64>(Decimal64(123456789012345), 2), TypeIndex::Decimal64, 18, 2)));
    arr_decimal64.push_back(Field(VariantField(
            DecimalField<Decimal64>(Decimal64(987654321098765), 2), TypeIndex::Decimal64, 18, 2)));
    fields.emplace_back(VariantField(arr_decimal64, TypeIndex::Array));

    Array arr_decimal128v2;
    arr_decimal128v2.push_back(
            Field(VariantField(DecimalField<Decimal128V2>(Decimal128V2(1234567890), 2),
                               TypeIndex::Decimal128V2, 16, 2)));
    arr_decimal128v2.push_back(
            Field(VariantField(DecimalField<Decimal128V2>(Decimal128V2(9876543210), 2),
                               TypeIndex::Decimal128V2, 16, 2)));
    fields.emplace_back(VariantField(arr_decimal128v2, TypeIndex::Array));

    Array arr_decimal128v3;
    arr_decimal128v3.push_back(
            Field(VariantField(DecimalField<Decimal128V3>(Decimal128V3(1234567890), 2),
                               TypeIndex::Decimal128V3, 18, 2)));
    arr_decimal128v3.push_back(
            Field(VariantField(DecimalField<Decimal128V3>(Decimal128V3(9876543210), 2),
                               TypeIndex::Decimal128V3, 18, 2)));
    fields.emplace_back(VariantField(arr_decimal128v3, TypeIndex::Array));

    Array arr_decimal256;
    arr_decimal256.push_back(Field(VariantField(DecimalField<Decimal256>(Decimal256(1234567890), 2),
                                                TypeIndex::Decimal256, 32, 2)));
    arr_decimal256.push_back(Field(VariantField(DecimalField<Decimal256>(Decimal256(9876543210), 2),
                                                TypeIndex::Decimal256, 32, 2)));
    fields.emplace_back(VariantField(arr_decimal256, TypeIndex::Array));

    std::random_device rd;
    std::mt19937 g(rd());
    for (int i = 0; i < 10000; i++) {
        std::vector<Field> fields_copy;
        fields_copy.emplace_back(Field::Types::Null);
        fields_copy.emplace_back(Field::Types::Null);
        std::shuffle(fields.begin(), fields.end(), g);
        fields_copy.emplace_back(fields[0]);
        fields_copy.emplace_back(fields[0]);
        std::shuffle(fields_copy.begin(), fields_copy.end(), g);
        auto subcolumn = ColumnObject::Subcolumn(0, true, false);
        for (const auto& field : fields_copy) {
            auto subcolumn_tmp = ColumnObject::Subcolumn(0, true, false);
            subcolumn_tmp.insert(field);
            subcolumn.insert_range_from(subcolumn_tmp, 0, 1);
        }
        subcolumn.finalize();
        EXPECT_EQ(subcolumn.data.size(), 1);
        EXPECT_EQ(subcolumn.data[0]->size(), 4);
        auto& variant_field = fields[0].get<VariantField>();
        EXPECT_EQ(remove_nullable(subcolumn.get_least_common_type())->get_type_id(),
                  variant_field.get_type_id());
    }
}

TEST(ColumnVariantTest, subcolumn_insert_range_from_test_advanced) {
    std::vector<Field> fields;

    fields.emplace_back(Field::Types::Null);

    fields.emplace_back(Int8(100));

    fields.emplace_back(Int16(10000));

    fields.emplace_back(Int32(1000000000));

    fields.emplace_back(Int64(922337203685477588));

    fields.emplace_back(Float32(3.14159f));

    fields.emplace_back(Float64(3.14159265359));

    fields.emplace_back(String("hello world"));

    Array arr_int8;
    arr_int8.push_back(Field(Int8(1)));
    arr_int8.push_back(Field(Int8(2)));
    fields.emplace_back(arr_int8);

    Array arr_int16;
    arr_int16.push_back(Field(Int16(12323)));
    arr_int16.push_back(Field(Int16(23232)));
    fields.emplace_back(arr_int16);

    Array arr_int32;
    arr_int32.push_back(Field(Int32(123232323)));
    arr_int32.push_back(Field(Int32(232323232)));
    fields.emplace_back(arr_int32);

    Array arr_int64;
    arr_int64.push_back(Field(Int64(1232323232323232323)));
    arr_int64.push_back(Field(Int64(2323232323232323232)));
    fields.emplace_back(arr_int64);

    Array arr_float32;
    arr_float32.push_back(Field(Float32(1.1f)));
    arr_float32.push_back(Field(Float32(2.2f)));
    fields.emplace_back(arr_float32);

    Array arr_float;
    arr_float.push_back(Field(Float64(1.1)));
    arr_float.push_back(Field(Float64(2.2)));
    fields.emplace_back(arr_float);

    Array arr_string;
    arr_string.push_back(Field(String("one")));
    arr_string.push_back(Field(String("two")));
    fields.emplace_back(arr_string);

    std::random_device rd;
    std::mt19937 g(rd());

    for (int i = 0; i < 10000; i++) {
        std::shuffle(fields.begin(), fields.end(), g);
        auto subcolumn = ColumnObject::Subcolumn(0, true, false);

        for (const auto& field : fields) {
            auto subcolumn_tmp = ColumnObject::Subcolumn(0, true, false);
            subcolumn_tmp.insert(field);
            subcolumn.insert_range_from(subcolumn_tmp, 0, 1);
        }

        subcolumn.finalize();
        EXPECT_EQ(subcolumn.data.size(), 1);
        // std::cout << "least common type: " << subcolumn.get_least_common_type()->get_name() << std::endl;
        EXPECT_EQ(subcolumn.get_least_common_base_type_id(), TypeIndex::JSONB);

        for (const auto& field : fields) {
            subcolumn.insert(field);
        }
        EXPECT_EQ(subcolumn.get_least_common_base_type_id(), TypeIndex::JSONB);

        if (i % 1000 == 0) {
            std::cout << "insert count " << i << std::endl;
        }
    }
}

TEST(ColumnVariantTest, test_serialize_to_sparse_column_and_deserialize) {
    Field decimal32_field = VariantField(DecimalField<Decimal32>(Decimal32(1234567890), 2),
                                         TypeIndex::Decimal32, 6, 2);
    Field decimal64_field = VariantField(DecimalField<Decimal64>(Decimal64(1234567890), 3),
                                         TypeIndex::Decimal64, 16, 3);
    Field decimal128v3_field = VariantField(DecimalField<Decimal128V3>(Decimal128V3(1234567890), 4),
                                            TypeIndex::Decimal128V3, 28, 4);
    Field decimal256_field = VariantField(DecimalField<Decimal256>(Decimal256(1234567890), 5),
                                          TypeIndex::Decimal256, 56, 5);

    ColumnObject::Subcolumn decimal32_subcolumn(0, true, false);
    decimal32_subcolumn.insert(decimal32_field);
    ColumnObject::Subcolumn decimal64_subcolumn(0, true, false);
    decimal64_subcolumn.insert(decimal64_field);
    ColumnObject::Subcolumn decimal128v3_subcolumn(0, true, false);
    decimal128v3_subcolumn.insert(decimal128v3_field);
    ColumnObject::Subcolumn decimal256_subcolumn(0, true, false);
    decimal256_subcolumn.insert(decimal256_field);

    auto serialized_sparse_column = ColumnMap::create(
            ColumnString::create(), ColumnString::create(), ColumnArray::ColumnOffsets::create());
    auto& column_map = assert_cast<ColumnMap&>(*serialized_sparse_column);
    auto& sparse_column_keys = assert_cast<ColumnString&>(column_map.get_keys());
    auto& sparse_column_values = assert_cast<ColumnString&>(column_map.get_values());

    decimal32_subcolumn.serialize_to_sparse_column(&sparse_column_keys, "decimal32",
                                                   &sparse_column_values, 0);
    decimal64_subcolumn.serialize_to_sparse_column(&sparse_column_keys, "decimal64",
                                                   &sparse_column_values, 0);
    decimal128v3_subcolumn.serialize_to_sparse_column(&sparse_column_keys, "decimal128v3",
                                                      &sparse_column_values, 0);
    decimal256_subcolumn.serialize_to_sparse_column(&sparse_column_keys, "decimal256",
                                                    &sparse_column_values, 0);

    auto column_object = ColumnObject::create(0);
    const auto& [field, field_info] =
            column_object->deserialize_from_sparse_column(&sparse_column_values, 0);
    EXPECT_EQ(field_info.scalar_type_id, TypeIndex::Decimal32);
    EXPECT_EQ(field_info.have_nulls, false);
    EXPECT_EQ(field_info.need_convert, false);
    EXPECT_EQ(field_info.num_dimensions, 0);
    EXPECT_EQ(field_info.scale, 2);

    const auto& [field2, field_info2] =
            column_object->deserialize_from_sparse_column(&sparse_column_values, 1);
    EXPECT_EQ(field_info2.scalar_type_id, TypeIndex::Decimal64);
    EXPECT_EQ(field_info2.have_nulls, false);
    EXPECT_EQ(field_info2.need_convert, false);
    EXPECT_EQ(field_info2.num_dimensions, 0);
    EXPECT_EQ(field_info2.scale, 3);

    const auto& [field3, field_info3] =
            column_object->deserialize_from_sparse_column(&sparse_column_values, 2);
    EXPECT_EQ(field_info3.scalar_type_id, TypeIndex::Decimal128V3);
    EXPECT_EQ(field_info3.have_nulls, false);
    EXPECT_EQ(field_info3.need_convert, false);
    EXPECT_EQ(field_info3.num_dimensions, 0);
    EXPECT_EQ(field_info3.scale, 4);

    const auto& [field4, field_info4] =
            column_object->deserialize_from_sparse_column(&sparse_column_values, 3);
    EXPECT_EQ(field_info4.scalar_type_id, TypeIndex::Decimal256);
    EXPECT_EQ(field_info4.have_nulls, false);
    EXPECT_EQ(field_info4.need_convert, false);
    EXPECT_EQ(field_info4.num_dimensions, 0);
    EXPECT_EQ(field_info4.scale, 5);

    decimal32_subcolumn.insert(field, field_info);
    decimal32_subcolumn.finalize();
    EXPECT_EQ(decimal32_subcolumn.data.size(), 1);
    EXPECT_EQ(decimal32_subcolumn.data[0]->size(), 2);
    auto tmp_col = ColumnString::create();
    VectorBufferWriter write_buffer(*tmp_col.get());
    decimal32_subcolumn.serialize_text_json(0, write_buffer);
    write_buffer.commit();
    EXPECT_EQ(tmp_col->get_data_at(0), StringRef("12345678.9", 11));
    decimal32_subcolumn.serialize_text_json(1, write_buffer);
    write_buffer.commit();
    EXPECT_EQ(tmp_col->get_data_at(1), StringRef("12345678.9", 11));

    decimal64_subcolumn.insert(field2, field_info2);
    decimal64_subcolumn.finalize();
    EXPECT_EQ(decimal64_subcolumn.data.size(), 1);
    EXPECT_EQ(decimal64_subcolumn.data[0]->size(), 2);
    auto tmp_col2 = ColumnString::create();
    VectorBufferWriter write_buffer2(*tmp_col2.get());
    decimal64_subcolumn.serialize_text_json(0, write_buffer2);
    write_buffer2.commit();
    EXPECT_EQ(tmp_col2->get_data_at(0), StringRef("1234567.890", 11));
    decimal64_subcolumn.serialize_text_json(1, write_buffer2);
    write_buffer2.commit();
    EXPECT_EQ(tmp_col2->get_data_at(1), StringRef("1234567.890", 11));

    decimal128v3_subcolumn.insert(field3, field_info3);
    decimal128v3_subcolumn.finalize();
    EXPECT_EQ(decimal128v3_subcolumn.data.size(), 1);
    EXPECT_EQ(decimal128v3_subcolumn.data[0]->size(), 2);
    auto tmp_col3 = ColumnString::create();
    VectorBufferWriter write_buffer3(*tmp_col3.get());
    decimal128v3_subcolumn.serialize_text_json(0, write_buffer3);
    write_buffer3.commit();
    EXPECT_EQ(tmp_col3->get_data_at(0), StringRef("123456.7890", 11));
    decimal128v3_subcolumn.serialize_text_json(1, write_buffer3);
    write_buffer3.commit();
    EXPECT_EQ(tmp_col3->get_data_at(1), StringRef("123456.7890", 11));

    decimal256_subcolumn.insert(field4, field_info4);
    decimal256_subcolumn.finalize();
    EXPECT_EQ(decimal256_subcolumn.data.size(), 1);
    EXPECT_EQ(decimal256_subcolumn.data[0]->size(), 2);
    auto tmp_col4 = ColumnString::create();
    VectorBufferWriter write_buffer4(*tmp_col4.get());
    decimal256_subcolumn.serialize_text_json(0, write_buffer4);
    write_buffer4.commit();
    EXPECT_EQ(tmp_col4->get_data_at(0), StringRef("12345.67890", 11));
    decimal256_subcolumn.serialize_text_json(1, write_buffer4);
    write_buffer4.commit();
    EXPECT_EQ(tmp_col4->get_data_at(1), StringRef("12345.67890", 11));

    Field string_ipv4_field("192.168.1.1");
    Field string_ipv6_field("2001:db8:85a3:85a2:85a1:8a2e:370:7334");
    Field string_date_field("2021-01-01");
    Field string_datetime_field("2021-01-01 02:09:10");

    vectorized::DataTypePtr data_type_string =
            vectorized::DataTypeFactory::instance().create_data_type(vectorized::TypeIndex::String,
                                                                     true, 0, 0);
    vectorized::DataTypePtr data_type_ipv4 =
            vectorized::DataTypeFactory::instance().create_data_type(vectorized::TypeIndex::IPv4,
                                                                     true, 0, 0);
    vectorized::DataTypePtr data_type_ipv6 =
            vectorized::DataTypeFactory::instance().create_data_type(vectorized::TypeIndex::IPv6,
                                                                     true, 0, 0);
    vectorized::DataTypePtr data_type_date =
            vectorized::DataTypeFactory::instance().create_data_type(vectorized::TypeIndex::DateV2,
                                                                     true, 0, 0);
    vectorized::DataTypePtr data_type_datetime =
            vectorized::DataTypeFactory::instance().create_data_type(
                    vectorized::TypeIndex::DateTimeV2, true, 0, 6);

    ColumnPtr column_string_ipv4 = data_type_string->create_column();
    ColumnPtr column_string_ipv6 = data_type_string->create_column();
    ColumnPtr column_string_date = data_type_string->create_column();
    ColumnPtr column_string_datetime = data_type_string->create_column();

    auto column_nullable_ipv4 = assert_cast<ColumnNullable&>(*column_string_ipv4->assume_mutable());
    auto column_nullable_ipv6 = assert_cast<ColumnNullable&>(*column_string_ipv6->assume_mutable());
    auto column_nullable_date = assert_cast<ColumnNullable&>(*column_string_date->assume_mutable());
    auto column_nullable_datetime =
            assert_cast<ColumnNullable&>(*column_string_datetime->assume_mutable());

    column_nullable_ipv4.insert(string_ipv4_field);
    column_nullable_ipv6.insert(string_ipv6_field);
    column_nullable_date.insert(string_date_field);
    column_nullable_datetime.insert(string_datetime_field);

    vectorized::ColumnPtr expected_ipv4;
    auto status = schema_util::cast_column({column_string_ipv4, data_type_string, ""},
                                           data_type_ipv4, &expected_ipv4);
    EXPECT_TRUE(status.ok());
    vectorized::ColumnPtr expected_ipv6;
    status = schema_util::cast_column({column_string_ipv6, data_type_string, ""}, data_type_ipv6,
                                      &expected_ipv6);
    EXPECT_TRUE(status.ok());
    vectorized::ColumnPtr expected_date;
    status = schema_util::cast_column({column_string_date, data_type_string, ""}, data_type_date,
                                      &expected_date);
    EXPECT_TRUE(status.ok());
    vectorized::ColumnPtr expected_datetime;
    status = schema_util::cast_column({column_string_datetime, data_type_string, ""},
                                      data_type_datetime, &expected_datetime);
    EXPECT_TRUE(status.ok());

    ColumnObject::Subcolumn ipv4_subcolumn(expected_ipv4->assume_mutable(), data_type_ipv4, true,
                                           false);
    ColumnObject::Subcolumn ipv6_subcolumn(expected_ipv6->assume_mutable(), data_type_ipv6, true,
                                           false);
    ColumnObject::Subcolumn date_subcolumn(expected_date->assume_mutable(), data_type_date, true,
                                           false);
    ColumnObject::Subcolumn datetime_subcolumn(expected_datetime->assume_mutable(),
                                               data_type_datetime, true, false);

    ipv4_subcolumn.serialize_to_sparse_column(&sparse_column_keys, "ipv4", &sparse_column_values,
                                              0);
    ipv6_subcolumn.serialize_to_sparse_column(&sparse_column_keys, "ipv6", &sparse_column_values,
                                              0);
    date_subcolumn.serialize_to_sparse_column(&sparse_column_keys, "date", &sparse_column_values,
                                              0);
    datetime_subcolumn.serialize_to_sparse_column(&sparse_column_keys, "datetime",
                                                  &sparse_column_values, 0);

    auto column_object2 = ColumnObject::create(0);
    const auto& [field5, field_info5] =
            column_object2->deserialize_from_sparse_column(&sparse_column_values, 4);
    EXPECT_EQ(field_info5.scalar_type_id, TypeIndex::IPv4);
    EXPECT_EQ(field_info5.have_nulls, false);
    EXPECT_EQ(field_info5.need_convert, false);
    EXPECT_EQ(field_info5.num_dimensions, 0);

    const auto& [field6, field_info6] =
            column_object2->deserialize_from_sparse_column(&sparse_column_values, 5);
    EXPECT_EQ(field_info6.scalar_type_id, TypeIndex::IPv6);
    EXPECT_EQ(field_info6.have_nulls, false);
    EXPECT_EQ(field_info6.need_convert, false);
    EXPECT_EQ(field_info6.num_dimensions, 0);

    const auto& [field7, field_info7] =
            column_object2->deserialize_from_sparse_column(&sparse_column_values, 6);
    EXPECT_EQ(field_info7.scalar_type_id, TypeIndex::DateV2);
    EXPECT_EQ(field_info7.have_nulls, false);
    EXPECT_EQ(field_info7.need_convert, false);
    EXPECT_EQ(field_info7.num_dimensions, 0);

    const auto& [field8, field_info8] =
            column_object2->deserialize_from_sparse_column(&sparse_column_values, 7);
    EXPECT_EQ(field_info8.scalar_type_id, TypeIndex::DateTimeV2);
    EXPECT_EQ(field_info8.have_nulls, false);
    EXPECT_EQ(field_info8.need_convert, false);
    EXPECT_EQ(field_info8.num_dimensions, 0);
    EXPECT_EQ(field_info8.scale, 6);

    ipv4_subcolumn.insert(field5, field_info5);
    ipv4_subcolumn.finalize();
    EXPECT_EQ(ipv4_subcolumn.data.size(), 1);
    EXPECT_EQ(ipv4_subcolumn.data[0]->size(), 2);
    auto tmp_col5 = ColumnString::create();
    VectorBufferWriter write_buffer5(*tmp_col5.get());
    ipv4_subcolumn.serialize_text_json(0, write_buffer5);
    write_buffer5.commit();
    EXPECT_EQ(tmp_col5->get_data_at(0), StringRef("\"192.168.1.1\"", 13));
    ipv4_subcolumn.serialize_text_json(1, write_buffer5);
    write_buffer5.commit();
    EXPECT_EQ(tmp_col5->get_data_at(1), StringRef("\"192.168.1.1\"", 13));

    ipv6_subcolumn.insert(field6, field_info6);
    ipv6_subcolumn.finalize();
    EXPECT_EQ(ipv6_subcolumn.data.size(), 1);
    EXPECT_EQ(ipv6_subcolumn.data[0]->size(), 2);
    auto tmp_col6 = ColumnString::create();
    VectorBufferWriter write_buffer6(*tmp_col6.get());
    ipv6_subcolumn.serialize_text_json(0, write_buffer6);
    write_buffer6.commit();
    EXPECT_EQ(tmp_col6->get_data_at(0), StringRef("\"2001:db8:85a3:85a2:85a1:8a2e:370:7334\"", 39));
    ipv6_subcolumn.serialize_text_json(1, write_buffer6);
    write_buffer6.commit();
    EXPECT_EQ(tmp_col6->get_data_at(1), StringRef("\"2001:db8:85a3:85a2:85a1:8a2e:370:7334\"", 39));

    date_subcolumn.insert(field7, field_info7);
    date_subcolumn.finalize();
    EXPECT_EQ(date_subcolumn.data.size(), 1);
    EXPECT_EQ(date_subcolumn.data[0]->size(), 2);
    auto tmp_col7 = ColumnString::create();
    VectorBufferWriter write_buffer7(*tmp_col7.get());
    date_subcolumn.serialize_text_json(0, write_buffer7);
    write_buffer7.commit();
    EXPECT_EQ(tmp_col7->get_data_at(0), StringRef("\"2021-01-01\"", 12));
    date_subcolumn.serialize_text_json(1, write_buffer7);
    write_buffer7.commit();
    EXPECT_EQ(tmp_col7->get_data_at(1), StringRef("\"2021-01-01\"", 12));

    datetime_subcolumn.insert(field8, field_info8);
    datetime_subcolumn.finalize();
    EXPECT_EQ(datetime_subcolumn.data.size(), 1);
    EXPECT_EQ(datetime_subcolumn.data[0]->size(), 2);
    auto tmp_col8 = ColumnString::create();
    VectorBufferWriter write_buffer8(*tmp_col8.get());
    datetime_subcolumn.serialize_text_json(0, write_buffer8);
    write_buffer8.commit();
    EXPECT_EQ(tmp_col8->get_data_at(0), StringRef("\"2021-01-01 02:09:10\"", 21));
    datetime_subcolumn.serialize_text_json(1, write_buffer8);
    write_buffer8.commit();
    EXPECT_EQ(tmp_col8->get_data_at(1), StringRef("\"2021-01-01 02:09:10\"", 21));
}

} // namespace doris::vectorized
