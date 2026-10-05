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

#include <glog/logging.h>

#include <string>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/data_type_serde/data_type_serde.h"
#include "runtime/descriptors.h"
#include "util/slice.h"

namespace doris {

inline Status fill_partition_column_from_path_value(
        IColumn& column, const SlotDescriptor& slot_desc, const std::string& value, size_t rows,
        bool explicit_null_marker, DataTypeSerDe::FormatOptions text_format_options = {}) {
    auto data_type = slot_desc.get_data_type_ptr();
    auto text_serde = data_type->get_serde();
    uint64_t num_deserialized = 0;

    if (explicit_null_marker) {
        DCHECK(data_type->is_nullable());
        column.insert_many_defaults(rows);
        return Status::OK();
    }

    IColumn* value_column = &column;
    DataTypeSerDeSPtr value_serde = text_serde;
    ColumnNullable* nullable_column = nullptr;
    if (data_type->is_nullable()) {
        nullable_column = assert_cast<ColumnNullable*>(&column);
        value_column = &nullable_column->get_nested_column();
        value_serde = text_serde->get_nested_serdes()[0];
    }

    const size_t old_size = value_column->size();
    Slice slice(value.data(), value.size());
    Status status = value_serde->deserialize_column_from_fixed_json(
            *value_column, slice, rows, &num_deserialized, text_format_options);
    if (!status.ok()) {
        value_column->resize(old_size);
        return Status::InternalError("Failed to fill partition column: {}={}", slot_desc.col_name(),
                                     value);
    }
    if (num_deserialized != rows) {
        value_column->resize(old_size);
        return Status::InternalError(
                "Failed to fill partition column: {}={}. Expected rows: {}, actual: {}",
                slot_desc.col_name(), value, rows, num_deserialized);
    }
    if (nullable_column != nullptr) {
        nullable_column->get_null_map_column().insert_many_vals(0, rows);
    }
    return Status::OK();
}

} // namespace doris
