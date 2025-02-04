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

void IDictionary::load_attributes(std::vector<ColumnPtr>& attributes_column) {
    // load att column
    _attribute_data.resize(attributes_column.size());
    for (size_t i = 0; i < attributes_column.size(); i++) {
        const DataTypePtr att_type = _attributes[i].type;
        ColumnPtr column = attributes_column[i];
        auto remove_nullable_data_type = remove_nullable(att_type);
        auto remove_nullable_column = remove_nullable(column);

        bool valid = IDictionary::cast_type(remove_nullable_data_type.get(), [&](const auto& type) {
            using AttributeRealDataType = std::decay_t<decltype(type)>;
            using AttributeRealColumnType = AttributeRealDataType::ColumnType;
            const auto* res_real_column =
                    typeid_cast<const AttributeRealColumnType*>(remove_nullable_column.get());
            if (!res_real_column) {
                return false;
            }
            auto& att = _attribute_data[i];
            ColumnWithType<AttributeRealDataType> column_with_type;
            column_with_type.column = AttributeRealColumnType::create(*res_real_column);
            att = column_with_type;
            return true;
        });
        if (!valid) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Dictionary({}) att type is : {} , but input column is : {}",
                                   dict_name(), att_type->get_name(), column->get_name());
        }
    }
}

} // namespace doris::vectorized
