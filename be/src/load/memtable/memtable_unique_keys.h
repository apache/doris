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

#include <memory>
#include <vector>

#include "load/memtable/memtable_aggregating.h"

namespace doris {

// UNIQUE_KEYS keeps the last row of each key. Merge-on-write tables replace value columns
// directly, merge-on-read tables use the schema aggregate functions, and partial updates may
// carry a sequence column, a skip bitmap and a delete sign. Merge-on-write tables with cluster
// keys are re-sorted after the merge.
class UniqueKeysMemTable final : public AggregatingMemTable {
public:
    UniqueKeysMemTable(int64_t tablet_id, std::shared_ptr<TabletSchema> tablet_schema,
                       const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                       bool enable_unique_key_mow, PartialUpdateInfo* partial_update_info,
                       const std::shared_ptr<ResourceContext>& resource_ctx, bool need_lsn);

protected:
    void _init_aggregation_columns() override;
    AggregateFunctionPtr _make_agg_function(int cid) override;
    void _aggregate_flexible_partial_update(
            bool is_final, MutableBlock& mutable_block,
            DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks) override;
    Status _after_aggregate() override;
    int64_t _flush_buffer_size() const override;

private:
    template <bool is_final>
    void _aggregate_for_flexible_partial_update_without_seq_col(
            MutableBlock& mutable_block,
            DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks);

    template <bool is_final>
    void _aggregate_for_flexible_partial_update_with_seq_col(
            MutableBlock& mutable_block,
            DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks);

    Status _sort_by_cluster_keys();

    bool _enable_unique_key_mow = false;
    UniqueKeyUpdateModePB _partial_update_mode {UniqueKeyUpdateModePB::UPSERT};
    int32_t _delete_sign_col_idx {-1};
    int32_t _delete_sign_col_unique_id {-1};
    int32_t _seq_col_unique_id {-1};
};

} // namespace doris
