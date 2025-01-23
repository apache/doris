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

#include "vec/functions/dictionary.h"

#include <variant>

#include "runtime/thread_context.h"
#include "vec/columns/column.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

IDictionary::IDictionary(std::string name, std::vector<DictionaryAttribute> attributes)
        : _dict_name(std::move(name)), _attributes(std::move(attributes)) {
    for (size_t i = 0; i < _attributes.size(); i++) {
        const auto& name = _attributes[i].name;
        if (_name_to_attributes_index.contains(name)) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "The names of attributes should not have duplicates : {}", name);
        }
        if (_attributes[i].type->is_nullable()) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Dictionary attribute should not be nullable : {}", name);
        }
        _name_to_attributes_index[name] = i;
    }
}

IDictionary::~IDictionary() {
    if (_mem_tracker) {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
        std::vector<ValueData> {}.swap(_values_data);
        std::string {}.swap(_dict_name);
        std::vector<DictionaryAttribute> {}.swap(_attributes);
        _name_to_attributes_index.clear();
    }
}

bool IDictionary::has_attribute(const std::string& name) const {
    return _name_to_attributes_index.contains(name);
}

size_t IDictionary::attribute_index(const std::string& name) const {
    auto it = _name_to_attributes_index.find(name);
    if (it == _name_to_attributes_index.end()) {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "no this attribute : {}", name);
    }
    size_t idx = it->second;
    return idx;
}

DataTypePtr IDictionary::get_attribute_type(const std::string& name) const {
    auto it = _name_to_attributes_index.find(name);
    if (it == _name_to_attributes_index.end()) {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "no this attribute : {}", name);
    }
    size_t idx = it->second;
    return _attributes[idx].type;
}

size_t IDictionary::allocated_bytes() const {
    size_t bytes = 0;
    for (const auto& value_data : _values_data) {
        std::visit(
                [&](auto&& arg) {
                    const auto* column = arg.get();
                    bytes += column->allocated_bytes();
                },
                value_data);
    }
    return bytes;
}

void IDictionary::load_values(const std::vector<ColumnPtr>& values_column) {
    // load value column
    _values_data.resize(values_column.size());
    for (size_t i = 0; i < values_column.size(); i++) {
        const DataTypePtr value_type = remove_nullable(_attributes[i].type);
        ColumnPtr value_column = remove_nullable(values_column[i]);
        bool valid = IDictionary::cast_type(value_type.get(), [&](const auto& type) {
            using ValueRealDataType = std::decay_t<decltype(type)>;
            auto& att = _values_data[i];
            if (value_column->is_column_string64()) {
                ColumnWithType<DictDataTypeString64> column_with_type;
                column_with_type.column = value_column;
                att = column_with_type;
            } else {
                ColumnWithType<ValueRealDataType> column_with_type;
                column_with_type.column = value_column;
                att = column_with_type;
            }
            return true;
        });
        if (!valid) {
            throw doris::Exception(
                    ErrorCode::INVALID_ARGUMENT,
                    "Dictionary({}) value type is : {} , column is : {}  Not supported",
                    dict_name(), value_type->get_name(), value_column->get_name());
        }
    }
}

} // namespace doris::vectorized
