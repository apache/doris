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

#include <utility>

#include "load/memtable/memtable_aggregating.h"

namespace doris {

// AGG_KEYS merges rows with equal keys using the aggregate functions declared in the tablet
// schema, and additionally pre-aggregates in memory once the aggregation buffer is full.
class AggKeysMemTable final : public AggregatingMemTable {
public:
    AggKeysMemTable(int64_t tablet_id, std::shared_ptr<TabletSchema> tablet_schema,
                    const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                    PartialUpdateInfo* partial_update_info,
                    const std::shared_ptr<ResourceContext>& resource_ctx, bool need_lsn)
            : AggregatingMemTable(tablet_id, std::move(tablet_schema), slot_descs, tuple_desc,
                                  partial_update_info, resource_ctx, need_lsn) {}

    bool need_agg() const override;

protected:
    // AGG_KEYS never sets _skip_bitmap_col_idx, so the flexible partial update path is unreachable.
    void _aggregate_flexible_partial_update(
            bool is_final, MutableBlock& mutable_block,
            DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks) override;
};

} // namespace doris
