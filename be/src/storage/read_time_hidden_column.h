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

#include "common/status.h"
#include "core/field.h"
#include "storage/olap_common.h"

namespace doris {

class TabletColumn;
class TabletSchema;

enum class ReadTimeHiddenColumnType { NONE, VERSION, COMMIT_TSO, BINLOG_TSO };

ReadTimeHiddenColumnType get_read_time_hidden_column_type(const TabletColumn& column);

ReadTimeHiddenColumnType get_read_time_hidden_column_type(const TabletSchema& schema,
                                                          int32_t column_unique_id);

// Loads write VERSION and COMMIT_TSO as the placeholder 0 into column storage, because the
// publish version and TSO are unknown while the segment is written; compaction materializes the
// per-row values. BINLOG_TSO is a NULL placeholder that row-binlog reads synthesize themselves.
inline bool is_load_placeholder_hidden_column(ReadTimeHiddenColumnType column_type) {
    return column_type == ReadTimeHiddenColumnType::VERSION ||
           column_type == ReadTimeHiddenColumnType::COMMIT_TSO;
}

// Loads write the VERSION and COMMIT_TSO placeholders into the row-store JSONB as well.
// Compaction materializes the per-row values in column storage but copies the JSONB unchanged, so
// a direct read never takes these two columns from the JSONB: a singleton rowset answers with its
// own version/TSO (get_read_time_hidden_column_value), any other rowset with the materialized
// column storage.
inline bool row_store_value_may_be_stale(ReadTimeHiddenColumnType column_type) {
    return is_load_placeholder_hidden_column(column_type);
}

// Whether `schema` has a column that loads store as a placeholder (VERSION or COMMIT_TSO, see
// is_load_placeholder_hidden_column). A single-version rowset of such a schema may still hold the
// placeholders in column storage, so a compaction has to rewrite it rather than hard-link its
// segments under a multi-version output, where get_read_time_hidden_column_value stops
// substituting. BINLOG_TSO lives only on row-binlog tablets, whose link path never reaches this.
bool has_load_placeholder_hidden_column(const TabletSchema& schema);

// The single VERSION/COMMIT_TSO/BINLOG_TSO resolution policy shared by Segment and direct reads.
// On success, `value` receives the logical constant every row of the rowset carries, or nullopt
// when the stored value is the one to read: a multi-version rowset, the pre-publish placeholder
// rowset, or (for BINLOG_TSO) a value direct reads leave to the segment. A singleton rowset whose
// TSO range is unassigned or not a singleton is rejected, exactly as a segment scan rejects it.
Status resolve_read_time_hidden_column_value(ReadTimeHiddenColumnType column_type,
                                             const Version& version, const TsoRange& commit_tso,
                                             std::optional<Field>* value);

// Direct-read entry point for point-query and row-ID reads. VERSION/COMMIT_TSO resolve from rowset
// metadata without opening a segment; BINLOG_TSO keeps its separate semantics and is always left
// to the segment reader.
Status get_read_time_hidden_column_value(ReadTimeHiddenColumnType column_type,
                                         const Version& version, const TsoRange& commit_tso,
                                         std::optional<Field>* value);

} // namespace doris
