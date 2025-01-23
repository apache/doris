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

#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/dictionary.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

// HashMapDictionary is a regular dictionary.
// Its underlying implementation uses a hashmap.
// It maintains a map<Key, RowIndex>. For a given query key, it first retrieves the corresponding RowIndex,
// and then fetches the value from value_data.
// This way, if multiple attributes are queried simultaneously, we only need to query the hashmap once.
template <typename KeyDataType>
class HashMapDictionary : public IDictionary {
public:
    using Self = HashMapDictionary<KeyDataType>;

    constexpr static bool is_string_key = std::is_same_v<KeyDataType, DataTypeString>;

    // When storing column string, the key of the HashMap is a StringRef to reduce copy during query.
    // TODO: We might need something like StringRefWithHash to avoid calculating the hash every time.
    using HashMapKeyType =
            std::conditional_t<is_string_key, StringRef, typename KeyDataType::FieldType>;
    using KeyRealColumnType = KeyDataType::ColumnType;

    HashMapDictionary(std::string name, std::vector<DictionaryAttribute> attributes)
            : IDictionary(std::move(name), std::move(attributes)),
              _key_type(std::make_shared<const KeyDataType>()) {}

    ~HashMapDictionary() override;
    static DictionaryPtr create_hash_map_dict(const std::string& name, const ColumnPtr& key_column,
                                              const ColumnsWithTypeAndName& values_data) {
        std::vector<DictionaryAttribute> attributes;
        std::vector<ColumnPtr> values_column;
        for (const auto& att : values_data) {
            // attributes do not handle nullable DataType
            attributes.push_back({att.name, remove_nullable(att.type)});
            values_column.push_back(att.column);
        }
        auto dict = std::make_shared<Self>(name, attributes);
        dict->load_data(key_column, values_column);
        return dict;
    }

    ColumnPtr get_column(const std::string& attribute_name, const DataTypePtr& attribute_type,
                         const ColumnPtr& key_column, const DataTypePtr& key_type) const override;

    size_t allocated_bytes() const override;

private:
    ALWAYS_INLINE auto find_key_iter(const KeyRealColumnType* key_real_column, size_t i) const {
        if constexpr (is_string_key) {
            const auto& key_value = key_real_column->get_data_at(i);
            return _key_hash_map.find(key_value);
        } else {
            const auto& key_value = key_real_column->get_element(i);
            return _key_hash_map.find(key_value);
        }
    }

    void load_data(const ColumnPtr& key_column, const std::vector<ColumnPtr>& values_column);

    // map<Key, RowIndex>
    phmap::flat_hash_map<HashMapKeyType, IColumn::ColumnIndex> _key_hash_map;

    // use to store string when key type in StringRef
    ColumnPtr _key_string_store;

    // If _value_null_maps[index] is not nullptr, the original value is a nullable column and needs to be handled.
    std::vector<ColumnPtr> _value_null_maps;

    // The type of the key, used for checking
    DataTypePtr _key_type;
};

inline DictionaryPtr create_hash_map_dict_from_column(const std::string& name,
                                                      const ColumnWithTypeAndName& key_data,
                                                      const ColumnsWithTypeAndName& values_data) {
    auto key_column = key_data.column;
    auto key_type = key_data.type;
    DictionaryPtr dict;
    bool valid = IDictionary::cast_type(key_type.get(), [&](const auto& type) {
        using KeyRealDataType = std::decay_t<decltype(type)>;
        dict = HashMapDictionary<KeyRealDataType>::create_hash_map_dict(name, key_column,
                                                                        values_data);
        return true;
    });

    if (!valid) {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                               "HashMapDictionary Unsupported key type : {}", key_type->get_name());
    }
    return dict;
}

} // namespace doris::vectorized
