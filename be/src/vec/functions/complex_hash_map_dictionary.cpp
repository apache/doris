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

#include <type_traits>
#include <vector>

#include "common/status.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h" // IWYU pragma: keep
#include "vec/functions/dictionary.h"

namespace doris::vectorized {

ComplexHashMapDictionary::~ComplexHashMapDictionary() {
    if (_mem_tracker) {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
        _hash_map_method.method_variant.emplace<std::monostate>();
        ColumnPtrs {}.swap(_key_columns);
    }
}

size_t ComplexHashMapDictionary::allocated_bytes() const {
    size_t bytes = 0;
    std::visit(vectorized::Overload {[&](const std::monostate& arg) { bytes = 0; },
                                     [&](const auto& dict_method) {
                                         bytes = dict_method.hash_table->get_buffer_size_in_bytes();
                                     }},
               _hash_map_method.method_variant);

    for (const auto& column : _key_columns) {
        bytes += column->allocated_bytes();
    }
    return bytes + IDictionary::allocated_bytes();
}
void ComplexHashMapDictionary::load_data(const ColumnPtrs& key_columns, const DataTypes& key_types,
                                         const std::vector<ColumnPtr>& values_column) {
    // load key column
    THROW_IF_ERROR(init_hash_method<DictionaryHashMapMethod>(&_hash_map_method, key_types, true));

    // save key columns
    _key_columns = key_columns;

    std::visit(vectorized::Overload {
                       [&](std::monostate& arg) {
                           throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                       },
                       [&](auto&& dict_method) {
                           using HashMethodType = std::decay_t<decltype(dict_method)>;
                           using State = typename HashMethodType::State;

                           ColumnRawPtrs key_raw_columns;
                           for (const auto& column : key_columns) {
                               key_raw_columns.push_back(column.get());
                           }
                           State state(key_raw_columns);

                           const size_t rows = key_columns[0]->size();
                           dict_method.init_serialized_keys(key_raw_columns, rows);
                           size_t input_rows = 0;
                           for (int i = 0; i < rows; i++) {
                               auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                                   ctor(key, i);
                                   input_rows++;
                               };

                               auto creator_for_null_key = [&](auto& mapped) {
                                   throw doris::Exception(ErrorCode::INTERNAL_ERROR, "no null key");
                               };
                               dict_method.lazy_emplace(state, i, creator, creator_for_null_key);
                           }
                           if (input_rows < rows) {
                               throw doris::Exception(
                                       ErrorCode::INVALID_ARGUMENT,
                                       DICT_DATA_ERROR_TAG +
                                               "The key has duplicate data in HashMapDictionary");
                           }
                       }},
               _hash_map_method.method_variant);

    // load value column
    load_values(values_column);
}

void ComplexHashMapDictionary::init_find_hash_map(DictionaryHashMapMethod& find_hash_map_method,
                                                  const DataTypes& key_types) const {
    THROW_IF_ERROR(
            init_hash_method<DictionaryHashMapMethod>(&find_hash_map_method, key_types, true));

    std::visit(vectorized::Overload {
                       [&](const std::monostate& arg) {
                           throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                       },
                       [&](auto&& dict_method) {
                           using HashMethodType =
                                   std::remove_cvref_t<std::decay_t<decltype(dict_method)>>;
                           if (!std::holds_alternative<HashMethodType>(
                                       find_hash_map_method.method_variant)) {
                               throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                      "key column not match");
                           }
                           auto& find_hash_map =
                                   std::get<HashMethodType>(find_hash_map_method.method_variant);
                           find_hash_map.hash_table = dict_method.hash_table;
                       }},
               _hash_map_method.method_variant);
}

ColumnPtrs ComplexHashMapDictionary::get_tuple_columns(
        const std::vector<std::string>& attribute_names, const DataTypes& attribute_types,
        const ColumnPtrs& key_columns, const DataTypes& key_types) const {
    if (have_nullable(attribute_types) || have_nullable(key_types)) {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "ComplexHashMapDictionary get_column attribute_type or key_type must "
                "not nullable type");
    }

    const size_t rows = key_columns[0]->size();
    IColumn::Selector value_index = IColumn::Selector(rows);
    // if key is not found, or key is null , wiil set true
    NullMap key_not_found = NullMap(rows, false);

    DictionaryHashMapMethod find_hash_map;
    // In init_find_hash_map, hashtable will be shared, similar to shared_hashtable in join
    init_find_hash_map(find_hash_map, key_types);

    bool key_hash_nullable = false;

    ColumnRawPtrs key_raw_columns;

    std::vector<const ColumnNullable*> nullable_key_raw_columns;
    for (const auto& column : key_columns) {
        key_raw_columns.push_back(remove_nullable(column).get());
        if (column->is_nullable()) {
            key_hash_nullable = true;
            nullable_key_raw_columns.push_back(assert_cast<const ColumnNullable*>(column.get()));
        }
    }

    auto has_null_key = [&](size_t i) {
        for (const auto* nullable_column : nullable_key_raw_columns) {
            if (nullable_column->is_null_at(i)) {
                return true;
            }
        }
        return false;
    };

    std::visit(vectorized::Overload {

                       [&](std::monostate& arg, auto key_hash_nullable) {
                           throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                       },
                       [&](auto&& dict_method, auto key_hash_nullable) {
                           using HashMethodType = std::decay_t<decltype(dict_method)>;
                           using State = typename HashMethodType::State;
                           State state(key_raw_columns);
                           dict_method.init_serialized_keys(key_raw_columns, rows);
                           for (size_t i = 0; i < rows; ++i) {
                               if constexpr (key_hash_nullable) {
                                   // if any key is null, we will not find it in the hash table
                                   if (has_null_key(i)) {
                                       key_not_found[i] = true;
                                       continue;
                                   }
                               }
                               auto find_result = dict_method.find(state, i);
                               if (find_result.is_found()) {
                                   value_index[i] = find_result.get_mapped();
                               } else {
                                   key_not_found[i] = true;
                               }
                           }
                       }},
               find_hash_map.method_variant, make_bool_variant(key_hash_nullable));

    ColumnPtrs columns;
    for (size_t i = 0; i < attribute_names.size(); ++i) {
        columns.push_back(get_single_value_column(value_index, key_not_found, attribute_names[i],
                                                  attribute_types[i]));
    }
    return columns;
}

ColumnPtr ComplexHashMapDictionary::get_single_value_column(
        const IColumn::Selector& value_index, const NullMap& key_not_found,
        const std::string& attribute_name, const DataTypePtr& attribute_type) const {
    const auto rows = value_index.size();
    MutableColumnPtr res_column = attribute_type->create_column();
    ColumnUInt8::MutablePtr res_null = ColumnUInt8::create(rows, false);
    auto& res_null_map = res_null->get_data();
    const auto& value_data = _values_data[attribute_index(attribute_name)];
    std::visit(
            [&](auto&& arg, auto value_is_nullable) {
                using ValueDataType = std::decay_t<decltype(arg)>;
                using OutputColumnType = ValueDataType::OutputColumnType;
                auto* res_real_column = assert_cast<OutputColumnType*>(res_column.get());
                const auto* value_column = arg.get();
                const auto* value_null_map = arg.get_null_map();
                for (size_t i = 0; i < rows; i++) {
                    if (key_not_found[i]) {
                        // if input key is not found, set the result column to null
                        res_real_column->insert_default();
                        res_null_map[i] = true;
                    } else {
                        set_value_data<value_is_nullable>(res_real_column, res_null_map[i],
                                                          value_column, value_null_map,
                                                          value_index[i]);
                    }
                }
            },
            value_data, attribute_nullable_variant(attribute_index(attribute_name)));

    return ColumnNullable::create(std::move(res_column), std::move(res_null));
}

} // namespace doris::vectorized
