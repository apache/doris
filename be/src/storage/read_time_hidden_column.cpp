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

#include "storage/read_time_hidden_column.h"

#include "common/logging.h"
#include "core/column/column.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {

ReadTimeHiddenColumnType get_read_time_hidden_column_type(const TabletColumn& column) {
    const auto& column_name = column.name();
    if (column_name == VERSION_COL) {
        return ReadTimeHiddenColumnType::VERSION;
    }
    if (column_name == COMMIT_TSO_COL) {
        return ReadTimeHiddenColumnType::COMMIT_TSO;
    }
    if (column_name == BINLOG_TSO_COL) {
        return ReadTimeHiddenColumnType::BINLOG_TSO;
    }
    return ReadTimeHiddenColumnType::NONE;
}

ReadTimeHiddenColumnType get_read_time_hidden_column_type(const TabletSchema& schema,
                                                          int32_t column_unique_id) {
    const int32_t column_idx = schema.field_index(column_unique_id);
    if (column_idx < 0) {
        return ReadTimeHiddenColumnType::NONE;
    }
    return get_read_time_hidden_column_type(schema.column(column_idx));
}

std::optional<Field> get_read_time_hidden_column_value(ReadTimeHiddenColumnType column_type,
                                                       const Version& version,
                                                       const TsoRange& commit_tso,
                                                       bool read_row_binlog) {
    if (version.first != version.second) {
        return std::nullopt;
    }
    switch (column_type) {
    case ReadTimeHiddenColumnType::VERSION:
        return Field::create_field<TYPE_BIGINT>(version.second);
    case ReadTimeHiddenColumnType::COMMIT_TSO:
        if (commit_tso.end_tso() != -1) {
            return Field::create_field<TYPE_BIGINT>(commit_tso.end_tso());
        }
        return std::nullopt;
    case ReadTimeHiddenColumnType::BINLOG_TSO:
        if (read_row_binlog) {
            const int64_t value = commit_tso.end_tso() == -1 ? 0 : commit_tso.end_tso();
            return Field::create_field<TYPE_BIGINT>(value);
        }
        return std::nullopt;
    case ReadTimeHiddenColumnType::NONE:
        return std::nullopt;
    }
    __builtin_unreachable();
}

void replace_suffix_with_read_time_hidden_column(ReadTimeHiddenColumnType column_type,
                                                 const Version& version, const TsoRange& commit_tso,
                                                 size_t num_rows, IColumn& column) {
    auto value = get_read_time_hidden_column_value(column_type, version, commit_tso, false);
    if (!value.has_value()) {
        return;
    }
    DORIS_CHECK_GE(column.size(), num_rows);
    column.pop_back(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        column.insert(*value);
    }
}

} // namespace doris
