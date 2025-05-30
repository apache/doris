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

#include "vec/exec/format/table/iceberg/schema.h"

namespace doris::iceberg {
#include "common/compile_check_begin.h"

const std::string Schema::ALL_COLUMNS = "*";
const int Schema::DEFAULT_SCHEMA_ID = 0;

Schema::Schema(int schema_id, std::vector<NestedField> columns)
        : _schema_id(schema_id), _root_struct(std::move(columns)) {
    _id_to_field.reserve(_root_struct.fields().size());
    for (const auto& field : _root_struct.fields()) {
        int field_id = field.field_id();
        _id_to_field[field_id] = &field;
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

#include "common/compile_check_end.h"
} // namespace doris::iceberg
