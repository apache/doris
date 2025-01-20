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

#include "vec/functions/hash_map_dictionary.h"

#include <ostream>

#include "runtime/primitive_type.h"
#include "runtime/thread_context.h"
#include "vec/functions/dictionary.h"

namespace doris::vectorized {

template <typename DataType>
HashMapDictionary<DataType>::~HashMapDictionary() {
    if (_mem_tracker) {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
        phmap::flat_hash_map<HashMapKeyType, IColumn::ColumnIndex> {}.swap(_key_hash_map);
        _key_string_store.reset();
        std::vector<ColumnPtr> {}.swap(_value_null_maps);
        _key_type.reset();
    }
}

template <typename DataType>
size_t HashMapDictionary<DataType>::allocated_bytes() const {
    size_t hash_map_bytes =
            _key_hash_map.capacity() +
            (_key_hash_map.capacity() * sizeof(std::pair<HashMapKeyType, IColumn::ColumnIndex>));

    size_t null_map_bytes = 0;

    for (auto column : _value_null_maps) {
        if (column) {
            null_map_bytes += column->allocated_bytes();
        }
    }

    return IDictionary::allocated_bytes() + hash_map_bytes + _key_string_store->allocated_bytes() +
           null_map_bytes;
}

template <typename DataType>
ColumnPtr HashMapDictionary<DataType>::get_column(const std::string& attribute_name,
                                                  const DataTypePtr& attribute_type,
                                                  const ColumnPtr& key_column,
                                                  const DataTypePtr& key_type) const {
    if (key_type->get_type_id() != _key_type->get_type_id()) {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                               "HashMapDictionary({}) key type is : {}  , but input is : {}",
                               dict_name(), _key_type->get_name(), key_type->get_name());
    }
    MutableColumnPtr res_column = attribute_type->create_column();
    const auto& value_data = _values_data[attribute_index(attribute_name)];
    const auto& value_null_map = _value_null_maps[attribute_index(attribute_name)];
    const auto* key_real_column = assert_cast<const KeyRealColumnType*>(key_column.get());
    std::visit(
            [&](auto&& arg) {
                using ValueDataType = std::decay_t<decltype(arg)>;
                using OutputColumnType = ValueDataType::OutputColumnType;
                auto* res_real_column = assert_cast<OutputColumnType*>(res_column.get());
                const auto* value_column = arg.get();
                if (value_null_map) {
                    // value is nullable
                    const auto& null_map =
                            assert_cast<const ColumnUInt8&, TypeCheckOnRelease::DISABLE>(
                                    *value_null_map)
                                    .get_data();
                    for (size_t i = 0; i < key_real_column->size(); i++) {
                        auto it = find_key_iter(key_real_column, i);
                        if (it == _key_hash_map.end()) {
                            res_real_column->insert_default();
                        } else {
                            const auto idx = it->second;
                            if (null_map[idx]) {
                                res_real_column->insert_default();
                            } else {
                                set_value_data(res_real_column, value_column, idx);
                            }
                        }
                    }
                } else {
                    // value without nullable
                    for (size_t i = 0; i < key_real_column->size(); i++) {
                        auto it = find_key_iter(key_real_column, i);
                        if (it == _key_hash_map.end()) {
                            res_real_column->insert_default();
                        } else {
                            const auto idx = it->second;
                            set_value_data(res_real_column, value_column, idx);
                        }
                    }
                }
            },
            value_data);

    return res_column;
}

template <typename DataType>
void HashMapDictionary<DataType>::load_data(const ColumnPtr& key_column,
                                            const std::vector<ColumnPtr>& values_column) {
    // load key column
    if constexpr (is_string_key) {
        _key_string_store = key_column;
        auto insert_key = [&](const auto* key_real_column) {
            for (size_t i = 0; i < key_real_column->size(); i++) {
                auto str_ref = key_real_column->get_data_at(i);
                _key_hash_map[str_ref] = i;
            }
        };
        if (key_column->is_column_string64()) {
            insert_key(assert_cast<const ColumnString64*>(_key_string_store.get()));
        } else {
            insert_key(assert_cast<const ColumnString*>(_key_string_store.get()));
        }
    } else {
        // No need for a structure similar to _key_string_store here, as the data is already fully stored in the HashMap
        const auto* key_real_column = assert_cast<const KeyRealColumnType*>(key_column.get());
        for (size_t i = 0; i < key_real_column->size(); i++) {
            auto key_str = key_real_column->get_element(i);
            _key_hash_map[key_str] = i;
        }
    }
    // load value column
    load_values(values_column);

    // load value nullable
    _value_null_maps.resize(values_column.size());
    for (size_t i = 0; i < values_column.size(); i++) {
        const DataTypePtr att_type = _attributes[i].type;
        ColumnPtr column = values_column[i];
        if (column->is_nullable()) {
            _value_null_maps[i] =
                    assert_cast<const ColumnNullable*>(column.get())->get_null_map_column_ptr();
        }
    }
}

template class HashMapDictionary<DataTypeUInt8>;
template class HashMapDictionary<DataTypeInt8>;
template class HashMapDictionary<DataTypeInt16>;
template class HashMapDictionary<DataTypeInt32>;
template class HashMapDictionary<DataTypeInt64>;
template class HashMapDictionary<DataTypeInt128>;
template class HashMapDictionary<DataTypeFloat32>;
template class HashMapDictionary<DataTypeFloat64>;

template class HashMapDictionary<DataTypeIPv4>;
template class HashMapDictionary<DataTypeIPv6>;

template class HashMapDictionary<DataTypeString>;

template class HashMapDictionary<DataTypeDateV2>;
template class HashMapDictionary<DataTypeDateTimeV2>;

template class HashMapDictionary<DataTypeDecimal<Decimal32>>;
template class HashMapDictionary<DataTypeDecimal<Decimal64>>;
template class HashMapDictionary<DataTypeDecimal<Decimal128V3>>;
template class HashMapDictionary<DataTypeDecimal<Decimal256>>;

} // namespace doris::vectorized
