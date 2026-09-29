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
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "storage/olap_common.h"

namespace doris {

class TabletSchema;

// Converts one non-NULL DATE/DATETIME-family cell to its final expiration. TIMESTAMPTZ is
// interpreted as an absolute instant; other source types use time_zone.
Status calculate_row_ttl_expiration_us(const IColumn& source, FieldType source_type, size_t row,
                                       const cctz::time_zone& time_zone, int64_t duration_us,
                                       int64_t* expiration_us);

bool row_ttl_uses_source_time(const TabletSchema& tablet_schema);

struct RowVisibilityFilter {
    IColumn::Filter selection;
    size_t rows_deleted = 0;
};

// Builds the post-merge row-visibility mask. TTL is a conditional delete sign: an existing
// delete sign wins, otherwise NULL is kept and an expiration at or before now_us is removed.
Status build_row_visibility_filter(const Block& block, const TabletSchema& tablet_schema,
                                   bool apply_delete_sign, bool apply_row_ttl, int64_t now_us,
                                   RowVisibilityFilter* filter);

// Copies restored source-time cells into the hidden column after a partial update. Unselected
// rows retain their stored source time.
void copy_row_ttl_source(Block* block, const TabletSchema& tablet_schema, int32_t source_cid,
                         const std::vector<bool>& rows_to_copy, size_t row_pos = 0);

// TTL GC is safe for DUP and UNIQUE-MOW in ordinary compactions. UNIQUE-MOR may only remove rows
// when all historical versions that could otherwise reappear are covered. Row-binlog compaction
// must keep every binlog row intact. Temporal policies are validated when tablets are created.
bool should_gc_row_ttl(const TabletSchema& tablet_schema, bool enable_unique_key_merge_on_write,
                       bool is_row_binlog_tablet, ReaderType reader_type, const Version& version);

} // namespace doris
