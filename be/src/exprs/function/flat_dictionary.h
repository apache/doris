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
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "exprs/function/dictionary.h"

namespace doris {

// FlatDictionary stores a single UInt64 key used directly as an array index.
// It mirrors ClickHouse's flat dictionary layout: value attributes are stored
// column-wise (via IDictionary::_values_data), a presence bitmap distinguishes
// absent keys from present-with-default, and the key-indexed structure grows to
// key + 1. A key that is greater than or equal to max_array_size is rejected at
// load time, before any allocation, to prevent a sparse huge key from exploding
// memory (the per-dictionary memory_limit check happens only after allocation).
class FlatDictionary : public IDictionary {
public:
    // ClickHouse default flat dictionary maximum array size.
    static constexpr size_t MAX_ARRAY_SIZE = 500000;

    FlatDictionary(std::string name, std::vector<DictionaryAttribute> attributes)
            : IDictionary(std::move(name), std::move(attributes)) {}

    ~FlatDictionary() override;

    ColumnPtr get_column(const std::string& attribute_name, const DataTypePtr& attribute_type,
                         const ColumnPtr& key_column, const DataTypePtr& key_type) const override;

    static DictionaryPtr create_flat_dict(const std::string& name, const ColumnPtr& key_column,
                                          const DataTypePtr& key_type,
                                          const ColumnsWithTypeAndName& values_data) {
        std::vector<DictionaryAttribute> attributes;
        std::vector<ColumnPtr> values_column;
        for (const auto& att : values_data) {
            attributes.push_back({att.name, att.type});
            values_column.push_back(att.column);
        }
        auto dict = std::make_shared<FlatDictionary>(name, attributes);
        dict->load_data(key_column, key_type, values_column);
        return dict;
    }

    size_t allocated_bytes() const override;

private:
    void load_data(const ColumnPtr& key_column, const DataTypePtr& key_type,
                   const std::vector<ColumnPtr>& values_column);

    // _value_row_index[key] gives the source row index of the value for that key.
    // Only meaningful when _loaded_keys[key] is true.
    std::vector<size_t> _value_row_index;

    // _loaded_keys[key] marks whether the key was present in the source data.
    std::vector<UInt8> _loaded_keys;
};

inline DictionaryPtr create_flat_dict_from_column(const std::string& name,
                                                  const ColumnWithTypeAndName& key_data,
                                                  const ColumnsWithTypeAndName& values_data) {
    auto key_column = key_data.column;
    auto key_type = key_data.type;
    if (!is_int(key_type->get_primitive_type())) {
        throw doris::Exception(
                ErrorCode::INVALID_ARGUMENT,
                DICT_DATA_ERROR_TAG +
                        "FlatDictionary only support integer key , input key type is {} ",
                key_type->get_name());
    }

    DictionaryPtr dict = FlatDictionary::create_flat_dict(name, key_column, key_type, values_data);
    return dict;
}
} // namespace doris
