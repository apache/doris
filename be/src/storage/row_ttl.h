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

#include <cctz/time_zone.h>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "storage/olap_common.h"

namespace doris {

class TabletSchema;
class TabletColumn;
class TabletMetaPB;

inline constexpr int32_t MIN_ROW_TTL_TIME_ZONE_OFFSET_SECONDS = -12 * 60 * 60;
inline constexpr int32_t MAX_ROW_TTL_TIME_ZONE_OFFSET_SECONDS = 14 * 60 * 60;

inline constexpr bool is_valid_row_ttl_time_zone_offset_seconds(int32_t offset_seconds) {
    return offset_seconds >= MIN_ROW_TTL_TIME_ZONE_OFFSET_SECONDS &&
           offset_seconds <= MAX_ROW_TTL_TIME_ZONE_OFFSET_SECONDS && offset_seconds % 60 == 0;
}

// Converts one non-NULL DATE/DATETIME-family cell to its final expiration. TIMESTAMPTZ is
// interpreted as an absolute instant; other source types use time_zone.
Status calculate_row_ttl_expiration_us(const IColumn& source, FieldType source_type, size_t row,
                                       const cctz::time_zone& time_zone, int64_t duration_us,
                                       int64_t* expiration_us);

// Converts a temporal value to Unix epoch microseconds using a validated fixed UTC offset.
// TIMESTAMPTZ keeps its absolute-time semantics and requires an offset of zero.
Result<std::optional<int64_t>> convert_row_ttl_time_to_epoch_us(const TabletColumn& source_column,
                                                                const std::string& source_value,
                                                                int32_t time_zone_offset_seconds);

bool row_ttl_uses_source_time(const TabletSchema& tablet_schema);
// DATE/DATETIME-family values need the table's fixed UTC offset. TIMESTAMPTZ is already absolute.
bool row_ttl_requires_time_zone(const TabletSchema& tablet_schema);
bool row_ttl_requires_time_zone(FieldType ttl_type);

// Verifies the part of a restored schema's row TTL policy that is persisted in BE metadata. The
// hidden column is matched by its stable unique id and type, not its ordinal, because schema change
// may insert ordinary columns before it. The FE must separately verify the source column identity
// because TabletSchema only stores the hidden TTL column and its expiration policy.
Status check_row_ttl_restore_schema_compatible(const TabletSchema& source_schema,
                                               const TabletSchema& target_schema);

// Validates both the tablet-level policy and every rowset-embedded schema before a snapshot is
// rewritten for an existing target tablet.
Status check_row_ttl_restore_tablet_meta_compatible(const TabletMetaPB& source_meta,
                                                    const TabletSchema& target_schema);

struct RowVisibilityFilter {
    IColumn::Filter selection;
    size_t rows_deleted = 0;
};

// Builds the post-merge row-visibility mask. TTL is a conditional delete sign: an existing
// delete sign wins, otherwise NULL is kept and an expiration at or before now_us is removed.
Status build_row_visibility_filter(const Block& block, const TabletSchema& tablet_schema,
                                   bool apply_delete_sign, bool apply_row_ttl, int64_t now_us,
                                   RowVisibilityFilter* filter);

// Applies a previously built row-visibility mask to every column in the block.
Status filter_block_by_row_visibility(Block* block, const IColumn::Filter& filter);

// Copies restored source-time cells into the hidden column after a partial update. Unselected
// rows retain their stored source time.
Status copy_row_ttl_source(Block* block, const TabletSchema& tablet_schema, int32_t source_cid,
                           const std::vector<bool>& rows_to_copy, size_t row_pos = 0);

// TTL GC is safe for DUP and UNIQUE-MOW in ordinary compactions. UNIQUE-MOR may only remove rows
// when all historical versions that could otherwise reappear are covered. Row-binlog compaction
// must keep every binlog row intact. Legacy temporal schemas without a duration or fixed UTC
// offset keep compacting normally but skip the TTL visibility filter.
bool should_gc_row_ttl(const TabletSchema& tablet_schema, bool enable_unique_key_merge_on_write,
                       bool is_row_binlog_tablet, ReaderType reader_type, const Version& version);

} // namespace doris
