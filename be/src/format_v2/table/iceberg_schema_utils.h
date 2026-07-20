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

#include <vector>

#include "format/table/iceberg_scan_semantics.h"
#include "format_v2/column_data.h"

namespace doris::format::iceberg {

inline bool schema_has_any_field_id(const std::vector<ColumnDefinition>& schema) {
    for (const auto& field : schema) {
        // Iceberg's hasIds contract is existential for the whole file. Ignore synthesized columns
        // and retain ID projection when any real field, including a nested field, carries an ID.
        if (field.column_type != ColumnType::DATA_COLUMN) {
            continue;
        }
        if (field.has_identifier_field_id() || schema_has_any_field_id(field.children)) {
            return true;
        }
    }
    return false;
}

inline bool schema_has_all_field_ids(const std::vector<ColumnDefinition>& schema) {
    for (const auto& field : schema) {
        if (field.column_type != ColumnType::DATA_COLUMN) {
            continue;
        }
        if (!field.has_identifier_field_id() || !schema_has_all_field_ids(field.children)) {
            return false;
        }
    }
    return true;
}

} // namespace doris::format::iceberg
