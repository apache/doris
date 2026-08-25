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

#include <functional>

namespace doris::iceberg {
#include "common/compile_check_begin.h"

const std::string Schema::ALL_COLUMNS = "*";
const int Schema::DEFAULT_SCHEMA_ID = 0;

Schema::Schema(int schema_id, std::vector<NestedField> columns)
        : _schema_id(schema_id), _root_struct(std::move(columns)) {
    FieldPath path;
    std::function<void(const NestedField&)> index_field = [&](const NestedField& field) {
        path.push_back(&field);
        _id_to_field[field.field_id()] = &field;
        _id_to_field_path[field.field_id()] = path;
        Type* type = field.field_type();
        if (type->is_struct_type()) {
            for (const auto& child : type->as_struct_type()->fields()) {
                index_field(child);
            }
        } else if (type->is_list_type()) {
            index_field(type->as_list_type()->element_field());
        } else if (type->is_map_type()) {
            index_field(type->as_map_type()->key_field());
            index_field(type->as_map_type()->value_field());
        }
        path.pop_back();
    };
    for (const auto& field : _root_struct.fields()) {
        index_field(field);
    }
}
Schema::Schema(std::vector<NestedField> columns) : Schema(DEFAULT_SCHEMA_ID, std::move(columns)) {}

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

const Schema::FieldPath* Schema::find_field_path(int id) const {
    auto it = _id_to_field_path.find(id);
    return it == _id_to_field_path.end() ? nullptr : &it->second;
}

#include "common/compile_check_end.h"
} // namespace doris::iceberg
