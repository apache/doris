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

#include "format/table/iceberg/schema.h"

namespace doris::iceberg {

const std::string Schema::ALL_COLUMNS = "*";
const int Schema::DEFAULT_SCHEMA_ID = 0;

Schema::Schema(int schema_id, std::vector<NestedField> columns)
        : _schema_id(schema_id), _root_struct(std::move(columns)) {
    _id_to_field.reserve(_root_struct.fields().size());
    for (const auto& field : _root_struct.fields()) {
        _index_field(field, false);
    }
}
Schema::Schema(std::vector<NestedField> columns) : Schema(DEFAULT_SCHEMA_ID, std::move(columns)) {}

void Schema::_index_field(const NestedField& field, bool nested_in_list_or_map) {
    _id_to_field[field.field_id()] = &field;
    if (nested_in_list_or_map) {
        _field_ids_nested_in_list_or_map.insert(field.field_id());
    }

    Type* field_type = field.field_type();
    if (field_type->is_struct_type()) {
        for (const auto& child : field_type->as_struct_type()->fields()) {
            _index_field(child, nested_in_list_or_map);
        }
    } else if (field_type->is_list_type()) {
        _index_field(field_type->as_list_type()->element_field(), true);
    } else if (field_type->is_map_type()) {
        _index_field(field_type->as_map_type()->key_field(), true);
        _index_field(field_type->as_map_type()->value_field(), true);
    }
}

Type* Schema::find_type(int id) const {
    auto it = _id_to_field.find(id);
    if (it != _id_to_field.end()) {
        return it->second->field_type();
    }
    return nullptr;
}

const NestedField* Schema::find_field(int id) const {
    auto it = _id_to_field.find(id);
    if (it != _id_to_field.end()) {
        return it->second;
    }
    return nullptr;
}

bool Schema::is_nested_in_list_or_map(int id) const {
    return _field_ids_nested_in_list_or_map.contains(id);
}

} // namespace doris::iceberg
