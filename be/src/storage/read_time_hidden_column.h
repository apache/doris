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

#include <cstdint>
#include <optional>

#include "core/field.h"
#include "storage/olap_common.h"

namespace doris {

class TabletColumn;
class TabletSchema;

enum class ReadTimeHiddenColumnType { NONE, VERSION, COMMIT_TSO, BINLOG_TSO };

ReadTimeHiddenColumnType get_read_time_hidden_column_type(const TabletColumn& column);

ReadTimeHiddenColumnType get_read_time_hidden_column_type(const TabletSchema& schema,
                                                          int32_t column_unique_id);

// Loads write VERSION and COMMIT_TSO as placeholders into both column storage and the row-store
// JSONB. Compaction materializes the per-row values in column storage but copies the JSONB
// unchanged, so a direct read never takes these two columns from the JSONB: a singleton rowset
// answers with its own version/TSO (get_read_time_hidden_column_value), any other rowset with the
// materialized column storage.
inline bool row_store_value_may_be_stale(ReadTimeHiddenColumnType column_type) {
    return column_type == ReadTimeHiddenColumnType::VERSION ||
           column_type == ReadTimeHiddenColumnType::COMMIT_TSO;
}

// The logical value every row of a single-version rowset carries for `column_type`, or nullopt
// when the stored value is the one to read: a multi-version rowset, an unassigned commit TSO, or
// BINLOG_TSO outside a row-binlog read. Point-query and row-ID direct reads are not row-binlog
// reads; SegmentIterator materializes BINLOG_TSO when StorageReadOptions::read_row_binlog is set.
std::optional<Field> get_read_time_hidden_column_value(ReadTimeHiddenColumnType column_type,
                                                       const Version& version,
                                                       const TsoRange& commit_tso,
                                                       bool read_row_binlog);

} // namespace doris
