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

#pragma once

#include "vec/columns/column_string.h"
#include "vec/columns/column_variant.h"
#include "vec/common/schema_util.h"
#include "vec/data_types/data_type_string.h"
#include "vec/json/parse2column.h"

namespace doris {

using namespace vectorized;

class VariantUtil {
public:
    using VariantStringCreator = std::function<void(ColumnString*, size_t)>;

    static doris::vectorized::Field get_field(std::string_view type) {
        static std::unordered_map<std::string_view, doris::vectorized::Field> field_map;
        if (field_map.empty()) {
            auto int_field = doris::vectorized::Field::create_field<TYPE_INT>(20);
            auto str_field = doris::vectorized::Field::create_field<TYPE_STRING>(String("str", 3));
            auto arr_int_field = doris::vectorized::Field::create_field<TYPE_ARRAY>(Array());
            auto& array1 = arr_int_field.get<Array>();
            array1.emplace_back(int_field);
            array1.emplace_back(int_field);
            auto arr_str_field = doris::vectorized::Field::create_field<TYPE_ARRAY>(Array());
            auto& array2 = arr_str_field.get<Array>();
            array2.emplace_back(str_field);
            array2.emplace_back(str_field);
            field_map["int"] = int_field;
            field_map["string"] = str_field;
            field_map["array_int"] = arr_int_field;
            field_map["array_str"] = arr_str_field;

            // add other int value
            field_map["int_16"] = doris::vectorized::Field::create_field<TYPE_SMALLINT>(
                    std::numeric_limits<Int16>::max());
            field_map["int_32"] = doris::vectorized::Field::create_field<TYPE_INT>(
                    std::numeric_limits<Int32>::max());
            field_map["int_64"] = doris::vectorized::Field::create_field<TYPE_BIGINT>(
                    Int64(static_cast<Int64>(std::numeric_limits<Int32>::max()) + 1));
        }
        return field_map[type];
    }

    static doris::vectorized::Field construct_variant_map(
            const std::vector<std::pair<std::string, doris::vectorized::Field>>& key_and_values) {
        doris::vectorized::Field res =
                doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap());
        auto& object = res.get<VariantMap&>();
        for (const auto& [k, v] : key_and_values) {
            PathInData path(k);
            object.try_emplace(path, FieldWithDataType {.field = v});
        }
        return res;
    }

    static auto construct_basic_varint_column() {
        // 1. create an empty variant column
        auto variant = ColumnVariant::create(5);

        std::vector<std::pair<std::string, doris::vectorized::Field>> data;

        // 2. subcolumn path
        data.emplace_back("v.a", doris::vectorized::Field::create_field<TYPE_INT>(20));
        data.emplace_back("v.b",
                          doris::vectorized::Field::create_field<TYPE_STRING>(String("20", 2)));
        data.emplace_back("v.c", doris::vectorized::Field::create_field<TYPE_INT>(20));
        data.emplace_back("v.f", doris::vectorized::Field::create_field<TYPE_INT>(20));
        data.emplace_back("v.e",
                          doris::vectorized::Field::create_field<TYPE_STRING>(String("50", 2)));
        for (int i = 0; i < 5; ++i) {
            auto field = construct_variant_map(data);
            variant->try_insert(field);
        }

        // 3. sparse column path
        data.emplace_back("v.d.d",
                          doris::vectorized::Field::create_field<TYPE_STRING>(String("50", 2)));
        data.emplace_back("v.c.d", doris::vectorized::Field::create_field<TYPE_INT>(30));
        data.emplace_back("v.b.d", doris::vectorized::Field::create_field<TYPE_INT>(30));
        for (int i = 0; i < 5; ++i) {
            auto field = construct_variant_map(data);
            variant->try_insert(field);
        }
        return variant;
    }

    static auto construct_dst_varint_column() {
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
        return ColumnVariant::create(5, std::move(dynamic_subcolumns));
    }

    static auto construct_advanced_varint_column() {
        // 1. create an empty variant column
        auto variant = ColumnVariant::create(5);

        std::vector<std::pair<std::string, doris::vectorized::Field>> data;

        // 2. subcolumn path
        data.emplace_back("v.a", get_field("int"));
        data.emplace_back("v.b", get_field("string"));
        data.emplace_back("v.c", get_field("array_int"));
        data.emplace_back("v.f", get_field("array_str"));
        data.emplace_back("v.e", get_field("string"));

        for (int i = 0; i < 5; ++i) {
            auto field = construct_variant_map(data);
            variant->try_insert(field);
        }

        // 3. sparse column path
        data.emplace_back("v.d.d", get_field("array_int"));
        data.emplace_back("v.c.d", get_field("string"));
        data.emplace_back("v.b.d", get_field("array_int"));
        for (int i = 0; i < 5; ++i) {
            auto field = construct_variant_map(data);
            variant->try_insert(field);
        }

        data.clear();
        data.emplace_back("v.a", get_field("int"));
        data.emplace_back("v.b", get_field("int"));
        data.emplace_back("v.c", get_field("array_int"));
        data.emplace_back("v.f", get_field("array_str"));
        data.emplace_back("v.e", get_field("string"));
        data.emplace_back("v.d.d", get_field("array_str"));
        data.emplace_back("v.c.d", get_field("int"));
        data.emplace_back("v.b.d", get_field("array_str"));
        for (int i = 0; i < 5; ++i) {
            auto field = construct_variant_map(data);
            variant->try_insert(field);
        }
        return variant;
    }

    static auto construct_varint_column_only_subcolumns() {
        // 1. create an empty variant column
        auto variant = ColumnVariant::create(5);

        std::vector<std::pair<std::string, doris::vectorized::Field>> data;

        // 2. subcolumn path
        data.emplace_back("v.a", doris::vectorized::Field::create_field<TYPE_INT>(20));
        data.emplace_back("v.b",
                          doris::vectorized::Field::create_field<TYPE_STRING>(String("20", 2)));
        data.emplace_back("v.c", doris::vectorized::Field::create_field<TYPE_INT>(20));
        data.emplace_back("v.f", doris::vectorized::Field::create_field<TYPE_INT>(20));
        data.emplace_back("v.e",
                          doris::vectorized::Field::create_field<TYPE_STRING>(String("50", 2)));
        for (int i = 0; i < 5; ++i) {
            auto field = construct_variant_map(data);
            variant->try_insert(field);
        }

        // 3. root
        VariantMap root;
        insert_root_scalar_field(*variant, doris::vectorized::Field::create_field<TYPE_INT>(20));
        return variant;
    }

    static doris::vectorized::Field create_nested_array_field(
            std::vector<std::map<std::string, doris::vectorized::Field>> data) {
        doris::vectorized::Field array_field =
                doris::vectorized::Field::create_field<TYPE_ARRAY>(Array());
        auto variant_field = doris::vectorized::Field::create_field<TYPE_VARIANT>(VariantMap());
        for (const auto& entry : data) {
            auto& variant_map = variant_field.get<VariantMap&>();
            for (const auto& [k, v] : entry) {
                variant_map.try_emplace(PathInData(k), FieldWithDataType {.field = v});
            }
            array_field.get<Array>().emplace_back(std::move(variant_field));
        }
        return array_field;
    }

    static void insert_root_scalar_field(ColumnVariant& variant, doris::vectorized::Field&& field) {
        VariantMap root;
        root.try_emplace(PathInData(), FieldWithDataType {.field = field});
        variant.try_insert(doris::vectorized::Field::create_field<TYPE_VARIANT>(std::move(root)));
    }

    static auto construct_varint_column_more_subcolumns() {
        // 1. create an empty variant column
        auto variant = ColumnVariant::create(5);

        std::vector<std::pair<std::string, doris::vectorized::Field>> data;

        // 2. subcolumn path
        data.emplace_back("v.a", doris::vectorized::Field::create_field<TYPE_INT>(20));
        data.emplace_back("v.b",
                          doris::vectorized::Field::create_field<TYPE_STRING>(String("20", 2)));
        data.emplace_back("v.c", doris::vectorized::Field::create_field<TYPE_INT>(20));
        data.emplace_back("v.f", doris::vectorized::Field::create_field<TYPE_INT>(20));
        data.emplace_back("v.e",
                          doris::vectorized::Field::create_field<TYPE_STRING>(String("50", 2)));
        data.emplace_back("v.s",
                          doris::vectorized::Field::create_field<TYPE_STRING>(String("str", 3)));
        data.emplace_back("v.x", get_field("int_16"));
        data.emplace_back("v.y", get_field("int_32"));
        data.emplace_back("v.z", get_field("int_64"));
        for (int i = 0; i < 5; ++i) {
            auto field = construct_variant_map(data);
            variant->try_insert(field);
        }

        return variant;
    }

    static std::unordered_map<std::string, int> fill_string_column_with_test_data(
            auto& column_string, int size, std::unordered_map<int, std::string>* inserted_jsonstr) {
        std::unordered_map<std::string, int> all_path_stats;
        std::srand(42);
        for (int i = 0; i < size; i++) {
            std::string json_str = "{";
            int num_pairs = std::rand() % 10 + 1;
            for (int j = 0; j < num_pairs; j++) {
                std::string key = "key" + std::to_string(j);
                if (j % 2 == 0) {
                    int value = 88;
                    json_str += "\"" + key + "\":" + std::to_string(value);
                } else {
                    std::string value = "str" + std::to_string(99);
                    json_str += "\"" + key + "\":\"" + value + "\"";
                }
                if (j < num_pairs - 1) {
                    json_str += ",";
                }
                all_path_stats[key] += 1;
            }
            json_str += "}";
            column_string->insert_data(json_str.data(), json_str.size());
            (*inserted_jsonstr)[i] = json_str;
        }
        return all_path_stats;
    }

    static std::unordered_map<std::string, int> fill_string_column_with_nested_test_data(
            auto& column_string, int size, std::unordered_map<int, std::string>* inserted_jsonstr) {
        std::unordered_map<std::string, int> all_path_stats;
        std::srand(42);
        for (int i = 0; i < size; i++) {
            std::string json_str = "{";

            int num_paths = std::rand() % 9 + 2;
            int current_path = 0;

            json_str += "\"key0\":{";

            json_str += "\"key1\":{";

            json_str += "\"key2\":" + std::to_string(88) + ",";
            json_str += R"("key3":")" + std::to_string(88) + "\"";
            json_str += "},";
            json_str += "\"key4\":" + std::to_string(88);
            json_str += "},";

            all_path_stats["key0.key1.key2"] += 1;
            all_path_stats["key0.key1.key3"] += 1;
            all_path_stats["key0.key4"] += 1;
            current_path += 3;

            while (current_path < num_paths) {
                std::string key = "key" + std::to_string(current_path);
                if (std::rand() % 2 == 0) {
                    json_str += "\"" + key + "\":{";
                    json_str +=
                            "\"nested" + std::to_string(current_path) + "\":" + std::to_string(88);
                    json_str += "},";
                    all_path_stats[key + ".nested" + std::to_string(current_path)] += 1;
                } else {
                    // 添加简单路径
                    json_str += "\"" + key + "\":\"" + std::to_string(88) + "\",";
                    all_path_stats[key] += 1;
                }
                current_path++;
            }

            json_str = json_str.substr(0, json_str.length() - 1);
            json_str += "}";

            column_string->insert_data(json_str.data(), json_str.size());
            (*inserted_jsonstr)[i] = json_str;
        }
        return all_path_stats;
    }

    static std::unordered_map<std::string, int> fill_object_column_with_test_data(
            auto& column_object, int size, std::unordered_map<int, std::string>* inserted_jsonstr) {
        auto type_string = std::make_shared<vectorized::DataTypeString>();
        auto column = type_string->create_column();
        auto column_string = assert_cast<ColumnString*>(column.get());
        auto res = fill_string_column_with_test_data(column_string, size, inserted_jsonstr);
        vectorized::ParseConfig config;
        config.enable_flatten_nested = false;
        parse_json_to_variant(*column_object, *column_string, config);
        return res;
    }

    static void fill_string_column_with_nested_data(auto& column_string, int size) {
        // insert some nested type test data to json string:  {"a" : {"b" : [{"c" : {"d" : 123, "e": "a@b"}}]}, "x": "y"}
        // {"a" : {"b" : [{"f" : {"d" : 123, "e": "a@b"}}]}, "z": "y"}
        // which
        // nested node path  : a.b(NESTED),
        // tablet_column path_info   : a.b.c.d(SCALAR)
        // parent path node          : a.b.c(TUPLE)
        // leaf path_info      : a.b.c.d(SCALAR)
        for (int i = 0; i < size; ++i) {
            std::string inserted_jsonstr = R"({"a": {"b": [{"c": {"d": )" + std::to_string(i) +
                                           R"(, "e": ")" + std::to_string(i) + R"("}}]}, "x": ")" +
                                           std::to_string(i) + R"("})";
            // add some rand key for sparse column with 'a.b' prefix : {"a" : {"b" : [{"c" : {"d" : 123, "e": "a@b", "f": 111}}]}, "x": "y"}
            if (i % 17 == 0) {
                inserted_jsonstr = R"({"a": {"b": [{"c": {"d": )" + std::to_string(i) +
                                   R"(, "e": ")" + std::to_string(i) + R"(", "f": )" +
                                   std::to_string(i) + R"(}}]}, "x": ")" + std::to_string(i) +
                                   R"("})";
            }
            // add some rand key for spare column without prefix: {"a" : {"b" : [{"c" : {"d" : 123, "e": "a@b", "f": 111}}]}, "x": "y", "z": 11}
            if (i % 177 == 0) {
                inserted_jsonstr = R"({"a": {"b": [{"c": {"d": )" + std::to_string(i) +
                                   R"(, "e": ")" + std::to_string(i) + R"("}}]}, "x": ")" +
                                   std::to_string(i) + R"(", "z": )" + std::to_string(i) + R"("})";
            }
            // insert json string to variant column
            column_string->insert_data(inserted_jsonstr.data(), inserted_jsonstr.size());
        }
    }

    static void fill_variant_column(auto& variant_column, int size, int uid,
                                    bool has_nested = false,
                                    VariantStringCreator* callback_variant_creator = nullptr) {
        auto type_string = std::make_shared<vectorized::DataTypeString>();
        auto column = type_string->create_column();
        auto column_string = assert_cast<ColumnString*>(column.get());
        if (callback_variant_creator != nullptr) {
            (*callback_variant_creator)(column_string, size);
        } else if (has_nested) {
            fill_string_column_with_nested_data(column_string, size);
        } else {
            std::unordered_map<int, std::string> inserted_jsonstr;
            fill_string_column_with_test_data(column_string, size, &inserted_jsonstr);
            assert(inserted_jsonstr.size() == size);
        }
        assert(column_string->size() == size);
        vectorized::ParseConfig config;
        // do not treat array with jsonb field
        config.enable_flatten_nested = has_nested;
        parse_json_to_variant(*variant_column, *column_string, config);
    }

    static std::unordered_map<std::string, int> fill_object_column_with_nested_test_data(
            auto& column_object, int size, std::unordered_map<int, std::string>* inserted_jsonstr) {
        auto type_string = std::make_shared<vectorized::DataTypeString>();
        auto column = type_string->create_column();
        auto column_string = assert_cast<ColumnString*>(column.get());
        auto res = fill_string_column_with_nested_test_data(column_string, size, inserted_jsonstr);
        vectorized::ParseConfig config;
        config.enable_flatten_nested = false;
        parse_json_to_variant(*column_object, *column_string, config);
        return res;
    }
};

} // namespace doris