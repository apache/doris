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

#include <memory>
#include <string>
#include <vector>

#include "function_test_util.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/complex_hash_map_dictionary.h"
#include "vec/functions/dictionary.h"

namespace doris::vectorized {

template <typename DataType>
ColumnPtr create_column_with_data(std::vector<typename DataType::FieldType> datas) {
    auto column = DataType::ColumnType::create();
    if constexpr (std::is_same_v<DataType, DataTypeString>) {
        for (auto data : datas) {
            column->insert_data(data.data(), data.size());
        }
    } else {
        for (auto data : datas) {
            column->insert_value(data);
        }
    }
    return std::move(column);
}

template <typename DataType>
ColumnWithTypeAndName create_column_with_data_and_name(
        std::vector<typename DataType::FieldType> datas, std::string name) {
    auto column = create_column_with_data<DataType>(datas);
    return ColumnWithTypeAndName(std::move(column), std::make_shared<DataType>(), name);
}

void test_complex_hash_map_dict(ColumnsWithTypeAndName key_data, ColumnsWithTypeAndName values_data,
                                const std::string dict_name) {
    ColumnPtrs key_columns;
    DataTypes key_types;
    for (auto column : key_data) {
        auto key_column = column.column;
        auto add_column = column.type->create_column();
        add_column->insert_default();
        add_column->insert_range_from(*key_column, 0, key_column->size());
        key_columns.push_back(std::move(add_column));
        key_types.push_back(column.type);
    }

    std::vector<DictionaryAttribute> attributes;
    for (const auto& att : values_data) {
        // attributes do not handle nullable DataType
        attributes.push_back({att.name, remove_nullable(att.type)});
    }

    std::vector<std::string> attribute_names;
    DataTypes attribute_types;
    for (auto column : values_data) {
        attribute_names.push_back(column.name);
        attribute_types.push_back(column.type);
    }

    auto dict = create_complex_hash_map_dict_from_column("dict1", key_data, values_data);

    auto result = dict->get_tuple_columns(attribute_names, attribute_types, key_columns, key_types);

    const auto rows = result[0]->size();

    for (int j = 0; j < attribute_names.size(); j++) {
        std::cout << attribute_names[j] << "\t";
    }
    std::cout << std::endl;

    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < attribute_types.size(); j++) {
            std::cout << attribute_types[j]->to_string(*remove_nullable(result[j]), i) << "\t";
        }
        std::cout << std::endl;
    }
}

TEST(ComplexHashMapDictTest, Test1) {
    test_complex_hash_map_dict(
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt32>({1, 2, 3}, "key"),
                    create_column_with_data_and_name<DataTypeInt64>({1, 1, 3}, "key"),
            },
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt32>({11, 45, 14}, "value1"),
                    create_column_with_data_and_name<DataTypeInt64>({19, 19, 810}, "value2"),
                    create_column_with_data_and_name<DataTypeString>({"a", "b", "c"}, "value3"),
            },
            "dict1");

    test_complex_hash_map_dict(
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt32>({1, 2, 3, 34}, "key"),
                    create_column_with_data_and_name<DataTypeInt64>({1, 1, 3, 1231231}, "key"),
            },
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt32>({11, 45, 14, 123123}, "value1"),
                    create_column_with_data_and_name<DataTypeInt64>({19, 19, 810, 32123213},
                                                                    "value2"),
                    create_column_with_data_and_name<DataTypeString>({"a", "b", "c", "sadawe"},
                                                                     "value3"),
            },
            "dict1");

    test_complex_hash_map_dict(
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt32>({1, 2}, "key"),
                    create_column_with_data_and_name<DataTypeString>({"abc", "ABC"}, "key"),
            },
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeFloat32>({1, 2}, "value1"),
                    create_column_with_data_and_name<DataTypeString>({"def", "DEF"}, "value2")},
            "dict1");
}

} // namespace doris::vectorized
