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

bool has_load_placeholder_hidden_column(const TabletSchema& schema) {
    // by name, like the readers: TabletMeta does not fill version_col_idx for FE-created tablets
    return schema.field_index(VERSION_COL) != -1 || schema.field_index(COMMIT_TSO_COL) != -1;
}

namespace {

const std::string& hidden_column_name(ReadTimeHiddenColumnType column_type) {
    switch (column_type) {
    case ReadTimeHiddenColumnType::VERSION:
        return VERSION_COL;
    case ReadTimeHiddenColumnType::COMMIT_TSO:
        return COMMIT_TSO_COL;
    case ReadTimeHiddenColumnType::BINLOG_TSO:
        return BINLOG_TSO_COL;
    case ReadTimeHiddenColumnType::NONE:
        break;
    }
    __builtin_unreachable();
}

} // namespace

Status resolve_read_time_hidden_column_value(ReadTimeHiddenColumnType column_type,
                                             const Version& version, const TsoRange& commit_tso,
                                             std::optional<Field>* value) {
    value->reset();
    switch (column_type) {
    case ReadTimeHiddenColumnType::VERSION:
        // A singleton rowset exposes its rowset version for every row. Segment writers store a
        // placeholder because the publish version is not known when the segment is written.
        if (version.first == version.second) {
            value->emplace(Field::create_field<TYPE_BIGINT>(version.second));
        }
        return Status::OK();
    case ReadTimeHiddenColumnType::COMMIT_TSO:
    case ReadTimeHiddenColumnType::BINLOG_TSO: {
        // Version [0-0] identifies a pre-publish physical read, such as segment compaction while a
        // RowsetWriter is still open. Its TSO is intentionally [-1--1], so preserve the on-disk
        // COMMIT_TSO_COL=0 or BINLOG_TSO_COL=NULL placeholder instead of synthesizing a value.
        if (version == Version(0, 0) && commit_tso.start_tso() == -1 &&
            commit_tso.end_tso() == -1) {
            return Status::OK();
        }
        if (version.first != version.second) {
            // Range rowsets already contain materialized TSOs: compacting the initial empty [0-1]
            // rowset with [2-2] and [3-3] can leave a valid output with TSO range [-1, T3].
            return Status::OK();
        }
        // A singleton needs its publish TSO to replace the on-disk NULL/0 placeholder.
        if (commit_tso.start_tso() < 0 || commit_tso.end_tso() < commit_tso.start_tso()) {
            return Status::InternalError(
                    "reading {} requires a valid commit tso, rowset version={}, commit tso={}",
                    hidden_column_name(column_type), version.to_string(), commit_tso.to_string());
        }
        if (commit_tso.start_tso() != commit_tso.end_tso()) {
            return Status::InternalError(
                    "singleton rowset {} requires a singleton commit tso when reading {}, got {}",
                    version.to_string(), hidden_column_name(column_type), commit_tso.to_string());
        }
        value->emplace(Field::create_field<TYPE_BIGINT>(commit_tso.end_tso()));
        return Status::OK();
    }
    case ReadTimeHiddenColumnType::NONE:
        return Status::OK();
    }
    __builtin_unreachable();
}

Status get_read_time_hidden_column_value(ReadTimeHiddenColumnType column_type,
                                         const Version& version, const TsoRange& commit_tso,
                                         std::optional<Field>* value) {
    if (column_type == ReadTimeHiddenColumnType::BINLOG_TSO) {
        // Direct reads leave BINLOG_TSO to the segment reader, which synthesizes it together with
        // the other read-time hidden columns.
        value->reset();
        return Status::OK();
    }
    return resolve_read_time_hidden_column_value(column_type, version, commit_tso, value);
}

} // namespace doris
