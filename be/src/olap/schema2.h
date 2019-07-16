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

#include "olap/field_info.h"
#include "olap/types.h"

namespace doris {

class RowBlockRow;

// Column schema informaton
class ColumnSchemaV2 {
public:
    ColumnSchemaV2(const FieldInfo& field_info)
        : _field_info(field_info), _type_info(get_type_info(field_info.type)) { }

    const FieldInfo& field_info() const { return _field_info; }
    const TypeInfo* type_info() const { return _type_info; }
    FieldType type() const { return _field_info.type; }
    bool is_nullable() const { return _field_info.is_allow_null; }
private:
    FieldInfo _field_info;
    TypeInfo* _type_info;
};

// The class is used to represent row's format in memory.
// One row contains some columns, within these columns there may be key columns which
// must be the first few columns.
class SchemaV2 {
public:
    SchemaV2(const std::vector<ColumnSchemaV2>& column_schemas, size_t num_key_columns)
        : _column_schemas(column_schemas), _num_key_columns(num_key_columns) {
    }

    const std::vector<ColumnSchemaV2>& column_schemas() const { return _column_schemas; }
    const ColumnSchemaV2& column(int idx) const { return _column_schemas[idx]; }
    size_t num_columns() const { return _column_schemas.size(); }
    int compare(const RowBlockRow& lhs, const RowBlockRow& rhs) const;
private:
    std::vector<ColumnSchemaV2> _column_schemas;
    size_t _num_key_columns;
};

}
