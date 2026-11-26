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

#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "exec/common/variant_util.h"

namespace doris {

class VariantUtil {
public:
    using VariantStringCreator = std::function<void(ColumnString*, size_t)>;

    static doris::Field get_field(std::string_view type) {
        static std::unordered_map<std::string_view, doris::Field> field_map;
        if (field_map.empty()) {
            auto int_field = doris::Field::create_field<TYPE_INT>(20);
            auto str_field = doris::Field::create_field<TYPE_STRING>(String("str", 3));
            auto arr_int_field = doris::Field::create_field<TYPE_ARRAY>(Array());
            auto& array1 = arr_int_field.get<TYPE_ARRAY>();
            array1.emplace_back(int_field);
            array1.emplace_back(int_field);
            auto arr_str_field = doris::Field::create_field<TYPE_ARRAY>(Array());
            auto& array2 = arr_str_field.get<TYPE_ARRAY>();
            array2.emplace_back(str_field);
            array2.emplace_back(str_field);
            field_map["int"] = int_field;
            field_map["string"] = str_field;
            field_map["array_int"] = arr_int_field;
            field_map["array_str"] = arr_str_field;

            // add other int value
            field_map["int_16"] =
                    doris::Field::create_field<TYPE_SMALLINT>(std::numeric_limits<Int16>::max());
            field_map["int_32"] =
                    doris::Field::create_field<TYPE_INT>(std::numeric_limits<Int32>::max());
            field_map["int_64"] = doris::Field::create_field<TYPE_BIGINT>(
                    Int64(static_cast<Int64>(std::numeric_limits<Int32>::max()) + 1));
        }
        return field_map[type];
    }

    static auto construct_basic_varint_column() {
        auto variant = ColumnVariantV2::create();
        std::vector<std::string> rows;
        for (int i = 0; i < 5; ++i) {
            rows.emplace_back(R"({"v":{"a":20,"b":"20","c":20,"f":20,"e":"50"}})");
        }
        for (int i = 0; i < 5; ++i) {
            rows.emplace_back(
                    R"({"v":{"a":20,"b":{"d":30},"c":{"d":30},"f":20,"e":"50","d":{"d":"50"}}})");
        }
        insert_json_rows(*variant, rows);
        return variant;
    }

    static auto construct_dst_varint_column() { return ColumnVariantV2::create(); }

    static auto construct_advanced_varint_column() {
        auto variant = ColumnVariantV2::create();
        std::vector<std::string> rows;
        for (int i = 0; i < 5; ++i) {
            rows.emplace_back(
                    R"({"v":{"a":20,"b":"str","c":[20,20],"f":["str","str"],"e":"str"}})");
        }
        for (int i = 0; i < 5; ++i) {
            rows.emplace_back(
                    R"({"v":{"a":20,"b":{"d":[20,20]},"c":{"d":"str"},"f":["str","str"],"e":"str","d":{"d":[20,20]}}})");
        }
        for (int i = 0; i < 5; ++i) {
            rows.emplace_back(
                    R"({"v":{"a":20,"b":{"d":["str","str"]},"c":{"d":20},"f":["str","str"],"e":"str","d":{"d":["str","str"]}}})");
        }
        insert_json_rows(*variant, rows);
        return variant;
    }

    static auto construct_varint_column_only_subcolumns() {
        auto variant = ColumnVariantV2::create();
        std::vector<std::string> rows;
        for (int i = 0; i < 5; ++i) {
            rows.emplace_back(R"({"v":{"a":20,"b":"20","c":20,"f":20,"e":"50"}})");
        }
        rows.emplace_back("20");
        insert_json_rows(*variant, rows);
        return variant;
    }

    static void insert_root_scalar_field(ColumnVariantV2& variant, doris::Field&& field) {
        std::string json;
        switch (field.get_type()) {
        case TYPE_STRING:
            json = "\"" + field.get<TYPE_STRING>() + "\"";
            break;
        case TYPE_INT:
            json = std::to_string(field.get<TYPE_INT>());
            break;
        default:
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "unsupported test variant root scalar type {}",
                                   field.get_type());
        }
        insert_json_rows(variant, {json});
    }

    static auto construct_varint_column_more_subcolumns() {
        auto variant = ColumnVariantV2::create();
        std::vector<std::string> rows;
        for (int i = 0; i < 5; ++i) {
            rows.emplace_back(
                    R"({"v":{"a":20,"b":"20","c":20,"f":20,"e":"50","s":"str","x":32767,"y":2147483647,"z":2147483648}})");
        }
        insert_json_rows(*variant, rows);
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
        auto type_string = std::make_shared<DataTypeString>();
        auto column = type_string->create_column();
        auto column_string = assert_cast<ColumnString*>(column.get());
        auto res = fill_string_column_with_test_data(column_string, size, inserted_jsonstr);
        insert_json_rows(assert_cast<ColumnVariantV2&>(*column_object), *column_string);
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
        auto type_string = std::make_shared<DataTypeString>();
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
        insert_json_rows(assert_cast<ColumnVariantV2&>(*variant_column), *column_string);
    }

    static std::unordered_map<std::string, int> fill_object_column_with_nested_test_data(
            auto& column_object, int size, std::unordered_map<int, std::string>* inserted_jsonstr) {
        auto type_string = std::make_shared<DataTypeString>();
        auto column = type_string->create_column();
        auto column_string = assert_cast<ColumnString*>(column.get());
        auto res = fill_string_column_with_nested_test_data(column_string, size, inserted_jsonstr);
        insert_json_rows(assert_cast<ColumnVariantV2&>(*column_object), *column_string);
        return res;
    }

    static void insert_json_rows(ColumnVariantV2& variant, const ColumnString& rows) {
        DataTypeVariantV2SerDe serde;
        DataTypeSerDe::FormatOptions options;
        for (size_t i = 0; i < rows.size(); ++i) {
            StringRef value = rows.get_data_at(i);
            Slice slice(value.data, value.size);
            auto st = serde.deserialize_one_cell_from_json(variant, slice, options);
            if (!st.ok()) {
                throw doris::Exception(st);
            }
        }
    }

    static void insert_json_rows(ColumnVariantV2& variant, const std::vector<std::string>& rows) {
        DataTypeVariantV2SerDe serde;
        DataTypeSerDe::FormatOptions options;
        for (const auto& row : rows) {
            Slice slice(row.data(), row.size());
            auto st = serde.deserialize_one_cell_from_json(variant, slice, options);
            if (!st.ok()) {
                throw doris::Exception(st);
            }
        }
    }
};

} // namespace doris
