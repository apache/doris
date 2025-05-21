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

#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/complex_dict_hash_map.h"
#include "vec/functions/dictionary.h"

namespace doris::vectorized {

// ComplexHashMapDictionary is a dictionary used to store multiple keys and multiple values
// Currently, it is in a relatively simple state. In the future, it will reuse the hashmap of hashjoin
// In fact, you can think of this class as a hashjoin that will not be cleared (but the specific hashmap used is different from that used by hashjoin)
class ComplexHashMapDictionary : public IDictionary {
public:
    ComplexHashMapDictionary(std::string name, std::vector<DictionaryAttribute> attributes)
            : IDictionary(std::move(name), std::move(attributes)) {}

    ~ComplexHashMapDictionary() override;
    static DictionaryPtr create_complex_hash_map_dict(const std::string& name,
                                                      const ColumnPtrs& key_columns,
                                                      const DataTypes& key_types,
                                                      const ColumnsWithTypeAndName& values_data) {
        std::vector<DictionaryAttribute> attributes;
        std::vector<ColumnPtr> values_column;
        for (const auto& att : values_data) {
            attributes.push_back({att.name, att.type});
            values_column.push_back(att.column);
        }
        auto dict = std::make_shared<ComplexHashMapDictionary>(name, attributes);
        dict->load_data(key_columns, key_types, values_column);
        return dict;
    }

    ColumnPtr get_column(const std::string& attribute_name, const DataTypePtr& attribute_type,
                         const ColumnPtr& key_column, const DataTypePtr& key_type) const override {
        return get_tuple_columns({attribute_name}, {attribute_type}, {key_column}, {key_type})[0];
    }

    ColumnPtrs get_tuple_columns(const std::vector<std::string>& attribute_names,
                                 const DataTypes& attribute_types, const ColumnPtrs& key_columns,
                                 const DataTypes& key_types) const override;

    size_t allocated_bytes() const override;

private:
    void load_data(const ColumnPtrs& key_columns, const DataTypes& key_types,
                   const std::vector<ColumnPtr>& values_column);

    ColumnPtr get_single_value_column(const IColumn::Selector& value_index, const NullMap& null_map,
                                      const std::string& attribute_name,
                                      const DataTypePtr& attribute_type) const;

    void init_find_hash_map(DictionaryHashMapMethod& find_hash_map_method,
                            const DataTypes& key_types) const;

    DictionaryHashMapMethod _hash_map_method;

    // Used to save key columns, because some types of hashmaps do not hold key columns, such as MethodStringNoCache
    ColumnPtrs _key_columns;
};

inline DictionaryPtr create_complex_hash_map_dict_from_column(
        const std::string& name, const ColumnsWithTypeAndName& key_data,
        const ColumnsWithTypeAndName& values_data) {
    ColumnPtrs key_columns;
    DataTypes key_types;
    for (const auto& key : key_data) {
        if (key.column->is_nullable()) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "ComplexHashMapDictionary key column should not be nullable");
        }
        key_columns.push_back(key.column);
        key_types.push_back(key.type);
    }
    auto dict = ComplexHashMapDictionary::create_complex_hash_map_dict(name, key_columns, key_types,
                                                                       values_data);
    return dict;
}
} // namespace doris::vectorized
