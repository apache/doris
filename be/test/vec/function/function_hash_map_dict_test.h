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

#include <iterator>
#include <map>
#include <memory>
#include <ostream>
#include <random>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include "function_test_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/dictionary.h"
#include "vec/functions/hash_map_dictionary.h"
#include "vec/functions/ip_address_dictionary.h"
namespace doris::vectorized {

template <typename DataType, bool use_string64>
auto random_column(size_t size) {
    if constexpr (std::is_same_v<DataType, DataTypeString>) {
        auto column = std::conditional_t<use_string64, ColumnString64,
                                         typename DataType::ColumnType>::create();
        std::vector<std::string> strings = {"a",
                                            "b",
                                            "c",
                                            "d",
                                            "e",
                                            "f",
                                            "g",
                                            "h",
                                            "i",
                                            "j",
                                            "213132",
                                            "879asd798a",
                                            "120937193272891238917389127389"};
        for (size_t i = 0; i < size; ++i) {
            auto string = strings[i % strings.size()];
            string[0] = 'a' + i % 26;
            column->insert_value(string);
        }
        return column;
    } else {
        using FieldType = DataType::FieldType;
        auto column = ColumnVector<FieldType>::create();
        auto& data = column->get_data();
        data.resize(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<UInt8> dis;
        for (size_t i = 0; i < size; ++i) {
            data[i] = dis(gen);
        }
        return column;
    }
}

template <typename KeyDataType, typename AttributeDataType, size_t size = 10,
          bool use_string64 = false>
void test_hash_map_dict() {
    using KeyFieldType = typename KeyDataType::FieldType;
    using AttributeFieldType = typename AttributeDataType::FieldType;

    std::map<KeyFieldType, AttributeFieldType> key_to_attribute;

    auto key_column = random_column<KeyDataType, use_string64>(size);
    auto attribute_column = random_column<AttributeDataType, use_string64>(size);

    if (!key_column || !attribute_column) {
        throw std::runtime_error("Unsupported data type for random_column");
    }

    for (size_t i = 0; i < size; i++) {
        key_to_attribute[key_column->get_element(i)] = attribute_column->get_element(i);
    }

    ColumnsWithTypeAndName attribute_data {
            {attribute_column->clone(), std::make_shared<AttributeDataType>(), "attribute"}};

    ColumnPtr input_key = key_column->clone();

    auto debug_string = [](auto& column) -> std::string {
        return column->get_name() + std::string("  , is col str 64 { ") +
               std::to_string(column->is_column_string64()) + "}";
    };

    std::cout << "key col : " + debug_string(input_key) + "\t" + "val col : "
              << debug_string(attribute_column) << std::endl;

    auto dict =
            HashMapDictionary<KeyDataType>::create_hash_map_dict("test", input_key, attribute_data);

    {
        auto query_key = random_column<KeyDataType, false>(size);

        auto result = dict->get_column("attribute", std::make_shared<AttributeDataType>(),
                                       query_key->clone(), std::make_shared<KeyDataType>());

        const auto* real_result_column =
                assert_cast<const typename AttributeDataType::ColumnType*>(result.get());

        std::cout << "real_result_column size : " << real_result_column->size() << std::endl;
        for (size_t i = 0; i < size; i++) {
            auto key = query_key->get_element(i);
            EXPECT_EQ(key_to_attribute[key], real_result_column->get_element(i));
        }
    }
    if constexpr (!use_string64) {
        auto& query_key = key_column;

        auto result = dict->get_column("attribute", std::make_shared<AttributeDataType>(),
                                       query_key->clone(), std::make_shared<KeyDataType>());

        const auto* real_result_column =
                assert_cast<const typename AttributeDataType::ColumnType*>(result.get());

        for (size_t i = 0; i < size; i++) {
            auto key = query_key->get_element(i);
            EXPECT_EQ(key_to_attribute[key], real_result_column->get_element(i));
        }
    }
}

} // namespace doris::vectorized
