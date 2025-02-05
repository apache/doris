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

#include "vec/functions/complex_hash_map_dictionary.h" // for ComplexHashMapDictionary

namespace doris::vectorized {
void ComplexHashMapDictionary::load_data(const ColumnPtrs& key_columns,
                                         const std::vector<ColumnPtr>& values_column) {
    // load key column

    Arena arena;
    const size_t rows = key_columns[0]->size();
    for (size_t i = 0; i < rows; i++) {
        auto key_data = serialize_keys_to_arena(i, key_columns, arena);
        _key_hash_map[key_data.to_string()] = i;
    }
    // load value column
    load_values(values_column);
}

ColumnPtrs ComplexHashMapDictionary::get_tuple_columns(
        const std::vector<std::string>& attribute_names, const DataTypes& attribute_types,
        const ColumnPtrs& key_columns, const DataTypes& key_types) const {
    const size_t rows = key_columns[0]->size();
    IColumn::Selector selector = IColumn::Selector(rows);
    NullMap null_map = NullMap(rows, false);
    Arena arena;
    for (size_t i = 0; i < key_columns[0]->size(); i++) {
        auto key_data = serialize_keys_to_arena(i, key_columns, arena);
        auto it = _key_hash_map.find(key_data.to_string());
        if (it != _key_hash_map.end()) {
            selector[i] = it->second;
        } else {
            null_map[i] = true;
        }
    }
    ColumnPtrs columns;
    for (size_t i = 0; i < attribute_names.size(); ++i) {
        columns.push_back(get_single_value_column(selector, null_map, attribute_names[i],
                                                  attribute_types[i]));
    }
    return columns;
}

ColumnPtr ComplexHashMapDictionary::get_single_value_column(
        const IColumn::Selector& selector, const NullMap& null_map,
        const std::string& attribute_name, const DataTypePtr& attribute_type) const {
    MutableColumnPtr res_column = attribute_type->create_column();
    const auto& value_data = _values_data[attribute_index(attribute_name)];
    std::visit(
            [&](auto&& arg) {
                using ValueDataType = std::decay_t<decltype(arg)>;
                using OutputColumnType = ValueDataType::OutputColumnType;
                auto* res_real_column = assert_cast<OutputColumnType*>(res_column.get());
                const auto* value_column = arg.get();
                for (size_t i = 0; i < selector.size(); i++) {
                    if (null_map[i]) {
                        res_real_column->insert_default();
                    } else {
                        const auto idx = selector[i];
                        set_value_data(res_real_column, value_column, idx);
                    }
                }
            },
            value_data);

    return res_column;
}

} // namespace doris::vectorized
