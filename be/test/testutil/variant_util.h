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

#include "vec/columns/column_object.h"
#include "vec/columns/column_string.h"
#include "vec/common/schema_util.h"
#include "vec/data_types/data_type_string.h"
#include "vec/json/parse2column.h"

namespace doris {

using namespace vectorized;

class VariantUtil {
public:
    static doris::vectorized::Field get_field(std::string_view type) {
        static std::unordered_map<std::string_view, doris::vectorized::Field> field_map;
        if (field_map.empty()) {
            doris::vectorized::Field int_field = 20;
            doris::vectorized::Field str_field(String("str", 3));
            doris::vectorized::Field arr_int_field = Array();
            auto& array1 = arr_int_field.get<Array>();
            array1.emplace_back(int_field);
            array1.emplace_back(int_field);
            doris::vectorized::Field arr_str_field = Array();
            auto& array2 = arr_str_field.get<Array>();
            array2.emplace_back(str_field);
            array2.emplace_back(str_field);
            field_map["int"] = int_field;
            field_map["string"] = str_field;
            field_map["array_int"] = arr_int_field;
            field_map["array_str"] = arr_str_field;

            // add other int value
            field_map["int_16"] = std::numeric_limits<Int16>::max();
            field_map["int_32"] = std::numeric_limits<Int32>::max();
            field_map["int_64"] = Int64(static_cast<Int64>(std::numeric_limits<Int32>::max()) + 1);
        }
        return field_map[type];
    }

    static doris::vectorized::Field construct_variant_map(
            const std::vector<std::pair<std::string, doris::vectorized::Field>>& key_and_values) {
        doris::vectorized::Field res = VariantMap();
        auto& object = res.get<VariantMap&>();
        for (const auto& [k, v] : key_and_values) {
            PathInData path(k);
            object.try_emplace(path, v);
        }
        return res;
    }

    static auto construct_basic_varint_column() {
        // 1. create an empty variant column
        auto variant = ColumnObject::create(5);

        std::vector<std::pair<std::string, doris::vectorized::Field>> data;

        // 2. subcolumn path
        data.emplace_back("v.a", 20);
        data.emplace_back("v.b", "20");
        data.emplace_back("v.c", 20);
        data.emplace_back("v.f", 20);
        data.emplace_back("v.e", "50");
        for (int i = 0; i < 5; ++i) {
            auto field = construct_variant_map(data);
            variant->try_insert(field);
        }

        // 3. sparse column path
        data.emplace_back("v.d.d", "50");
        data.emplace_back("v.c.d", 30);
        data.emplace_back("v.b.d", 30);
        for (int i = 0; i < 5; ++i) {
            auto field = construct_variant_map(data);
            variant->try_insert(field);
        }
        return variant;
    }

    static auto construct_dst_varint_column() {
        // 1. create an empty variant column
        vectorized::ColumnObject::Subcolumns dynamic_subcolumns;
        dynamic_subcolumns.create_root(vectorized::ColumnObject::Subcolumn(0, true, true /*root*/));
        dynamic_subcolumns.add(vectorized::PathInData("v.f"),
                               vectorized::ColumnObject::Subcolumn {0, true});
        dynamic_subcolumns.add(vectorized::PathInData("v.e"),
                               vectorized::ColumnObject::Subcolumn {0, true});
        dynamic_subcolumns.add(vectorized::PathInData("v.b"),
                               vectorized::ColumnObject::Subcolumn {0, true});
        dynamic_subcolumns.add(vectorized::PathInData("v.b.d"),
                               vectorized::ColumnObject::Subcolumn {0, true});
        dynamic_subcolumns.add(vectorized::PathInData("v.c.d"),
                               vectorized::ColumnObject::Subcolumn {0, true});
        return ColumnObject::create(5, std::move(dynamic_subcolumns));
    }

    static auto construct_advanced_varint_column() {
        // 1. create an empty variant column
        auto variant = ColumnObject::create(5);

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
        auto variant = ColumnObject::create(5);

        std::vector<std::pair<std::string, doris::vectorized::Field>> data;

        // 2. subcolumn path
        data.emplace_back("v.a", 20);
        data.emplace_back("v.b", "20");
        data.emplace_back("v.c", 20);
        data.emplace_back("v.f", 20);
        data.emplace_back("v.e", "50");
        for (int i = 0; i < 5; ++i) {
            auto field = construct_variant_map(data);
            variant->try_insert(field);
        }

        // 3. root
        variant->try_insert(doris::vectorized::Field(20));
        return variant;
    }

    static auto construct_varint_column_more_subcolumns() {
        // 1. create an empty variant column
        auto variant = ColumnObject::create(5);

        std::vector<std::pair<std::string, doris::vectorized::Field>> data;

        // 2. subcolumn path
        data.emplace_back("v.a", 20);
        data.emplace_back("v.b", "20");
        data.emplace_back("v.c", 20);
        data.emplace_back("v.f", 20);
        data.emplace_back("v.e", "50");
        data.emplace_back("v.s", "str");
        data.emplace_back("v.x", get_field("int_16"));
        data.emplace_back("v.y", get_field("int_32"));
        data.emplace_back("v.z", get_field("int_64"));
        for (int i = 0; i < 5; ++i) {
            auto field = construct_variant_map(data);
            variant->try_insert(field);
        }

        return variant;
    }

    static schema_util::PathToNoneNullValues fill_string_column_with_test_data(
            auto& column_string, int size, std::unordered_map<int, std::string>* inserted_jsonstr) {
        schema_util::PathToNoneNullValues all_path_stats;
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
            vectorized::Field str(json_str);
            column_string->insert_data(json_str.data(), json_str.size());
            (*inserted_jsonstr)[i] = json_str;
        }
        return all_path_stats;
    }

    static schema_util::PathToNoneNullValues fill_string_column_with_nested_test_data(
            auto& column_string, int size, std::unordered_map<int, std::string>* inserted_jsonstr) {
        schema_util::PathToNoneNullValues all_path_stats;
        std::srand(42);
        for (int i = 0; i < size; i++) {
            std::string json_str = "{";

            int num_paths = std::rand() % 9 + 2;
            int current_path = 0;

            json_str += "\"key0\":{";

            json_str += "\"key1\":{";

            json_str += "\"key2\":" + std::to_string(88) + ",";
            json_str += "\"key3\":\"" + std::to_string(88) + "\"";
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

            vectorized::Field str(json_str);
            column_string->insert_data(json_str.data(), json_str.size());
            (*inserted_jsonstr)[i] = json_str;
        }
        return all_path_stats;
    }

    static schema_util::PathToNoneNullValues fill_object_column_with_test_data(
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

    static schema_util::PathToNoneNullValues fill_object_column_with_nested_test_data(
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
