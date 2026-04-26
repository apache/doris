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

#include <gtest/gtest.h>

#include <functional>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"

namespace doris::reader_test {

inline const std::vector<std::string>& default_nested_reader_column_names() {
    static const std::vector<std::string> names = {"name", "profile"};
    return names;
}

inline void create_complex_user_profile_types(
        DataTypePtr& coordinates_struct_type, DataTypePtr& address_struct_type,
        DataTypePtr& phone_struct_type, DataTypePtr& contact_struct_type,
        DataTypePtr& hobby_element_struct_type, DataTypePtr& hobbies_array_type,
        DataTypePtr& profile_struct_type, DataTypePtr& name_type) {
    name_type = make_nullable(std::make_shared<DataTypeString>());

    std::vector<DataTypePtr> coordinates_types = {
            make_nullable(std::make_shared<DataTypeFloat64>()),
            make_nullable(std::make_shared<DataTypeFloat64>())};
    std::vector<std::string> coordinates_names = {"lat", "lng"};
    coordinates_struct_type =
            make_nullable(std::make_shared<DataTypeStruct>(coordinates_types, coordinates_names));

    std::vector<DataTypePtr> address_types = {make_nullable(std::make_shared<DataTypeString>()),
                                              make_nullable(std::make_shared<DataTypeString>()),
                                              coordinates_struct_type};
    std::vector<std::string> address_names = {"street", "city", "coordinates"};
    address_struct_type =
            make_nullable(std::make_shared<DataTypeStruct>(address_types, address_names));

    std::vector<DataTypePtr> phone_types = {make_nullable(std::make_shared<DataTypeString>()),
                                            make_nullable(std::make_shared<DataTypeString>())};
    std::vector<std::string> phone_names = {"country_code", "number"};
    phone_struct_type = make_nullable(std::make_shared<DataTypeStruct>(phone_types, phone_names));

    std::vector<DataTypePtr> contact_types = {make_nullable(std::make_shared<DataTypeString>()),
                                              phone_struct_type};
    std::vector<std::string> contact_names = {"email", "phone"};
    contact_struct_type =
            make_nullable(std::make_shared<DataTypeStruct>(contact_types, contact_names));

    std::vector<DataTypePtr> hobby_element_types = {
            make_nullable(std::make_shared<DataTypeString>()),
            make_nullable(std::make_shared<DataTypeInt32>())};
    std::vector<std::string> hobby_element_names = {"name", "level"};
    hobby_element_struct_type = make_nullable(
            std::make_shared<DataTypeStruct>(hobby_element_types, hobby_element_names));

    hobbies_array_type = make_nullable(std::make_shared<DataTypeArray>(hobby_element_struct_type));

    std::vector<DataTypePtr> profile_types = {address_struct_type, contact_struct_type,
                                              hobbies_array_type};
    std::vector<std::string> profile_names = {"address", "contact", "hobbies"};
    profile_struct_type =
            make_nullable(std::make_shared<DataTypeStruct>(profile_types, profile_names));
}

inline Block make_name_profile_block(const DataTypePtr& name_type,
                                     const DataTypePtr& profile_struct_type) {
    Block block;
    block.insert(ColumnWithTypeAndName(name_type->create_column(), name_type, "name"));
    block.insert(ColumnWithTypeAndName(profile_struct_type->create_column(), profile_struct_type,
                                       "profile"));
    return block;
}

inline void verify_nested_reader_block(Block& block, size_t read_rows,
                                       const std::vector<std::string>& expected_column_names =
                                               default_nested_reader_column_names()) {
    EXPECT_GT(read_rows, 0) << "Should read at least one row";
    EXPECT_EQ(block.rows(), read_rows);
    EXPECT_EQ(block.columns(), expected_column_names.size());

    auto columns_with_names = block.get_columns_with_type_and_name();
    for (size_t i = 0; i < expected_column_names.size(); ++i) {
        EXPECT_EQ(columns_with_names[i].name, expected_column_names[i]);
    }

    EXPECT_TRUE(columns_with_names[0].type->get_name().find("String") != std::string::npos);
    EXPECT_TRUE(columns_with_names[1].type->get_name().find("Struct") != std::string::npos);

    std::function<void(const ColumnPtr&, const DataTypePtr&, const std::string&, bool)>
            print_column_rows = [&](const ColumnPtr& col, const DataTypePtr& type,
                                    const std::string& name, bool allow_empty) {
                if (!allow_empty) {
                    EXPECT_GT(col->size(), 0) << name << " column/subcolumn size should be > 0";
                }

                if (const auto* nullable_col = typeid_cast<const ColumnNullable*>(col.get())) {
                    auto nested_type =
                            assert_cast<const DataTypeNullable*>(type.get())->get_nested_type();
                    bool is_complex_type =
                            typeid_cast<const DataTypeStruct*>(nested_type.get()) != nullptr ||
                            typeid_cast<const DataTypeArray*>(nested_type.get()) != nullptr ||
                            typeid_cast<const DataTypeMap*>(nested_type.get()) != nullptr;
                    std::string nested_name = is_complex_type ? name + ".nested" : name;
                    print_column_rows(nullable_col->get_nested_column_ptr(), nested_type,
                                      nested_name, allow_empty);
                    return;
                }

                if (const auto* struct_col = typeid_cast<const ColumnStruct*>(col.get())) {
                    auto struct_type = assert_cast<const DataTypeStruct*>(type.get());
                    for (size_t i = 0; i < struct_col->tuple_size(); ++i) {
                        print_column_rows(
                                struct_col->get_column_ptr(i), struct_type->get_element(i),
                                name + "." + struct_type->get_element_name(i), allow_empty);
                    }
                    return;
                }

                if (const auto* array_col = typeid_cast<const ColumnArray*>(col.get())) {
                    auto array_type = assert_cast<const DataTypeArray*>(type.get());
                    print_column_rows(array_col->get_data_ptr(), array_type->get_nested_type(),
                                      name + ".data", true);
                }
            };

    for (size_t i = 0; i < block.columns(); ++i) {
        const auto& column_with_name = block.get_by_position(i);
        print_column_rows(column_with_name.column, column_with_name.type, column_with_name.name,
                          false);
        EXPECT_EQ(column_with_name.column->size(), block.rows())
                << "Column " << column_with_name.name << " size mismatch";
    }
}

} // namespace doris::reader_test
