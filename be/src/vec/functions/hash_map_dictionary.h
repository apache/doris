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
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/functions/dictionary.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

template <typename KeyDataType>
class HashMapDictionary : public IDictionary {
public:
    using Self = HashMapDictionary<KeyDataType>;
    using KeyType = KeyDataType::FieldType;
    using KeyRealColumnType = KeyDataType::ColumnType;

    HashMapDictionary(std::string name, std::vector<DictionaryAttribute> attributes)
            : IDictionary(std::move(name), std::move(attributes)),
              _key_type(std::make_shared<const KeyDataType>()) {}

    static DictionaryPtr create_hash_map_dict(const std::string& name, ColumnPtr& key_column,
                                              ColumnsWithTypeAndName& attribute_data) {
        std::vector<DictionaryAttribute> attributes;
        std::vector<ColumnPtr> attributes_column;
        for (const auto& att : attribute_data) {
            /// TODO: remove "remove nullable"
            attributes.push_back({att.name, remove_nullable(att.type)});
            attributes_column.push_back(att.column);
        }
        auto dict = std::make_shared<Self>(name, attributes);
        dict->load_data(key_column, attributes_column);
        return dict;
    }

    ColumnPtr getColumn(const std::string& attribute_name, const DataTypePtr& attribute_type,
                        const ColumnPtr& key_column, const DataTypePtr& key_type) const override {
        if (key_type->get_type_id() != _key_type->get_type_id()) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "HashMapDictionary({}) key type is : {}  , but input is : {}",
                                   dict_name(), _key_type->get_name(), key_type->get_name());
        }
        MutableColumnPtr res_column = attribute_type->create_column();
        auto& attribute = _hashmap_attributes[attribute_index(attribute_name)];
        const auto* real_key_column = assert_cast<const KeyRealColumnType*>(key_column.get());

        std::visit(
                [&](auto&& arg) {
                    using HashTableType = std::decay_t<decltype(arg)>;
                    using AttributeRealDataType = HashTableType::DataType;
                    using AttributeRealColumnType = AttributeRealDataType::ColumnType;

                    auto* res_real_column = assert_cast<AttributeRealColumnType*>(res_column.get());
                    const auto& attributes_column = arg.column;

                    if (attribute.null_map) {
                        // att is nullable
                        const auto& null_map = attribute.null_map->get_data();
                        for (size_t i = 0; i < real_key_column->size(); i++) {
                            const auto& key_value = real_key_column->get_element(i);
                            auto it = _key_hash_map.find(key_value);
                            if (it == _key_hash_map.end()) {
                                res_real_column->insert_default();
                            } else {
                                const auto idx = it->second;
                                if (null_map[idx]) {
                                    res_real_column->insert_default();
                                } else {
                                    res_real_column->insert_value(
                                            attributes_column->get_element(idx));
                                }
                            }
                        }

                    } else {
                        // att without nullable
                        for (size_t i = 0; i < real_key_column->size(); i++) {
                            const auto& key_value = real_key_column->get_element(i);
                            auto it = _key_hash_map.find(key_value);
                            if (it == _key_hash_map.end()) {
                                res_real_column->insert_default();
                            } else {
                                const auto idx = it->second;
                                res_real_column->insert_value(attributes_column->get_element(idx));
                            }
                        }
                    }
                },
                attribute.containers);

        return res_column;
    }

private:
    void load_data(ColumnPtr& key_column, std::vector<ColumnPtr>& attributes_column) {
        const auto* key_real_column = assert_cast<const KeyRealColumnType*>(key_column.get());
        for (size_t i = 0; i < key_real_column->size(); i++) {
            auto key_str = key_real_column->get_element(i);
            _key_hash_map[key_str] = i;
        }

        _hashmap_attributes.resize(attributes_column.size());
        for (size_t i = 0; i < attributes_column.size(); i++) {
            const DataTypePtr att_type = _attributes[i].type;
            ColumnPtr column = attributes_column[i];

            auto remove_nullable_data_type = remove_nullable(att_type);
            auto remove_nullable_column = remove_nullable(column);

            // Set containers
            bool valid = cast_type(remove_nullable_data_type.get(), [&](const auto& type) {
                using AttributeRealDataType = std::decay_t<decltype(type)>;
                using AttributeRealColumnType = AttributeRealDataType::ColumnType;
                const auto* res_real_column =
                        typeid_cast<const AttributeRealColumnType*>(remove_nullable_column.get());
                if (!res_real_column) {
                    return false;
                }
                auto& att = _hashmap_attributes[i];
                ColumnWithType<AttributeRealDataType> column_with_type;
                column_with_type.column = AttributeRealColumnType::create(*res_real_column);
                att.containers = column_with_type;
                return true;
            });
            if (!valid) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "HashMapDictionary({}) att type is : {} , but input column is : {}",
                        dict_name(), att_type->get_name(), column->get_name());
            }

            // Set other content, such as nullmap (no need to visit).

            if (column->is_nullable()) {
                auto& att = _hashmap_attributes[i];
                att.null_map = ColumnUInt8::create(
                        assert_cast<const ColumnNullable*>(column.get())->get_null_map_column());
            }
        }
    }

    struct ColumnWithNullAttribute final {
        ColumnUInt8::Ptr null_map;
        ColumnData containers;
    };

    phmap::flat_hash_map<KeyType, IColumn::ColumnIndex> _key_hash_map;

    std::vector<ColumnWithNullAttribute> _hashmap_attributes;

    DataTypePtr _key_type;
};

inline DictionaryPtr create_hash_map_dict_from_column(const std::string& name,
                                                      ColumnWithTypeAndName key_data,
                                                      ColumnsWithTypeAndName attribute_data) {
    auto key_column = key_data.column;
    auto key_type = key_data.type;
    DictionaryPtr dict;
    bool valid = IDictionary::cast_type(key_type.get(), [&](const auto& type) {
        using KeyRealDataType = std::decay_t<decltype(type)>;
        dict = HashMapDictionary<KeyRealDataType>::create_hash_map_dict(name, key_column,
                                                                        attribute_data);
        return true;
    });

    if (!valid) {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT, " Unsupported key type : {}",
                               key_type->get_name());
    }
    return dict;
}

} // namespace doris::vectorized
