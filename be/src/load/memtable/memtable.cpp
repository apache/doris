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

#include "load/memtable/memtable.h"

#include <gen_cpp/olap_file.pb.h>

#include <memory>
#include <vector>

#include "bvar/bvar.h"
#include "common/config.h"
#include "core/column/column_vector.h"
#include "load/delta_writer/delta_writer_context.h"
#include "load/memtable/memtable_agg_keys.h"
#include "load/memtable/memtable_dup_keys.h"
#include "load/memtable/memtable_memory_limiter.h"
#include "load/memtable/memtable_unique_keys.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/olap_define.h"
#include "storage/tablet/tablet_schema.h"
#include "util/debug_points.h"

namespace doris {

bvar::Adder<int64_t> g_memtable_cnt("memtable_cnt");
bvar::Adder<uint64_t> g_flush_cuz_memtable_full("flush_cuz_memtable_full");

using namespace ErrorCode;

MemTable::MemTable(int64_t tablet_id, std::shared_ptr<TabletSchema> tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   PartialUpdateInfo* partial_update_info,
                   const std::shared_ptr<ResourceContext>& resource_ctx, bool need_lsn)
        : _mem_type(MemType::ACTIVE),
          _tablet_id(tablet_id),
          _tablet_schema(tablet_schema),
          _resource_ctx(resource_ctx),
          _need_lsn(need_lsn),
          _is_first_insertion(true) {
    g_memtable_cnt << 1;
    _mem_tracker = std::make_shared<MemTracker>();
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    _init_columns_offset_by_slot_descs(slot_descs, tuple_desc, partial_update_info);
    _row_lsn_col_pos = tablet_schema->row_lsn_col_idx();
    _load_mem_limit = MemInfo::mem_limit() * config::load_process_max_memory_limit_percent / 100;
}

std::shared_ptr<MemTable> MemTable::create(int64_t tablet_id,
                                           std::shared_ptr<TabletSchema> tablet_schema,
                                           const std::vector<SlotDescriptor*>* slot_descs,
                                           TupleDescriptor* tuple_desc, bool enable_unique_key_mow,
                                           PartialUpdateInfo* partial_update_info,
                                           const std::shared_ptr<ResourceContext>& resource_ctx,
                                           bool need_lsn) {
    switch (tablet_schema->keys_type()) {
    case KeysType::DUP_KEYS:
        return std::make_shared<DupKeysMemTable>(tablet_id, std::move(tablet_schema), slot_descs,
                                                 tuple_desc, partial_update_info, resource_ctx,
                                                 need_lsn);
    case KeysType::AGG_KEYS:
        return std::make_shared<AggKeysMemTable>(tablet_id, std::move(tablet_schema), slot_descs,
                                                 tuple_desc, partial_update_info, resource_ctx,
                                                 need_lsn);
    case KeysType::UNIQUE_KEYS:
        return std::make_shared<UniqueKeysMemTable>(tablet_id, std::move(tablet_schema), slot_descs,
                                                    tuple_desc, enable_unique_key_mow,
                                                    partial_update_info, resource_ctx, need_lsn);
    }
    DORIS_CHECK(false);
    return nullptr;
}

void MemTable::_init_columns_offset_by_slot_descs(const std::vector<SlotDescriptor*>* slot_descs,
                                                  const TupleDescriptor* tuple_desc,
                                                  PartialUpdateInfo* partial_update_info) {
    for (auto slot_desc : *slot_descs) {
        const auto& slots = tuple_desc->slots();
        for (int j = 0; j < slots.size(); ++j) {
            if (slot_desc->id() == slots[j]->id()) {
                _column_offset.emplace_back(j);
                break;
            }
        }
    }
    // A fixed partial update that omits an auto-increment column appends it to the input block.
    // This only changes the column mapping, so it stays in the shared block layout.
    if (partial_update_info != nullptr &&
        partial_update_info->update_mode() == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS &&
        partial_update_info->is_schema_contains_auto_inc_column &&
        !partial_update_info->is_input_columns_contains_auto_inc_column) {
        _column_offset.emplace_back(_column_offset.size());
    }
    _num_columns = _column_offset.size();
}

MemTable::~MemTable() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    {
        SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
        g_memtable_cnt << -1;
        // Model-specific rows, aggregation states and arena are released by the derived
        // destructor. It runs before this one and installs the same tracker scopes, so those
        // releases stay accounted to this memtable.
        _input_mutable_block.clear();
        _output_mutable_block.clear();
        // Reset the LSN sidecar to release its capacity tracked by the memtable tracker.
        _output_allocated_lsns = std::make_shared<std::vector<int64_t>>();
    }
    if (_is_flush_success) {
        // If the memtable is flush success, then its memtracker's consumption should be 0
        if (_mem_tracker->consumption() != 0 && config::crash_in_memory_tracker_inaccurate) {
            LOG(FATAL) << "memtable flush success but cosumption is not 0, it is "
                       << _mem_tracker->consumption();
        }
    }
}

int RowInBlockComparator::operator()(const MutableBlock& block, const RowInBlock* left,
                                     const RowInBlock* right) const {
    return block.compare_at(left->_row_pos, right->_row_pos, _tablet_schema->num_key_columns(),
                            block, -1);
}

Status MemTable::insert(const Block* input_block, const TabletAddRowsPayload& rows) {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    const auto& row_idxs = rows.row_idxs;
    const auto& allocated_lsns = rows.allocated_lsns;

    if (_need_lsn) {
        if (allocated_lsns.empty()) {
            return Status::InternalError(
                    "allocated lsn is missing for memtable insert, "
                    "tablet_id={}",
                    _tablet_id);
        }
        // The row metadata of a batch is derived from the same payload, so a length mismatch would
        // read past the LSN sidecar later. Assert it here rather than only under NDEBUG.
        DORIS_CHECK_EQ(allocated_lsns.size(), row_idxs.size());
    } else if (!allocated_lsns.empty()) {
        return Status::InternalError(
                "allocated lsn is unexpectedly provided for memtable insert, tablet_id={}",
                _tablet_id);
    }

    if (_is_first_insertion) {
        _is_first_insertion = false;
        _input_mutable_block = MutableBlock::build_mutable_block(
                input_block->clone_without_columns(&_column_offset));
        _output_mutable_block = MutableBlock::build_mutable_block(
                input_block->clone_without_columns(&_column_offset));
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_on_first_insertion());
    }

    auto num_rows = row_idxs.size();
    size_t cursor_in_mutableblock = _input_mutable_block.rows();
    // The row limit is a precondition documented on insert(): MemTableWriter::write flushes the
    // memtable before raw rows reach it, so a batch always fits.
    DORIS_CHECK(num_rows <= MAX_MEMTABLE_ROWS &&
                cursor_in_mutableblock <= MAX_MEMTABLE_ROWS - num_rows);
    // Reserve the row metadata of this batch before appending its columns, so a failed reservation
    // leaves the block untouched. DUP_KEYS then fills that reservation without allocating again;
    // the aggregating models still allocate their RowInBlock batch in _append_row_metadata, so a
    // failure there leaves the block ahead of the row tracking and relies on the caller discarding
    // the memtable instead of unwinding it, as documented on insert().
    RETURN_IF_CATCH_EXCEPTION(_reserve_rows(num_rows, cursor_in_mutableblock));
    RETURN_IF_ERROR(_input_mutable_block.add_rows(input_block, row_idxs.data(),
                                                  row_idxs.data() + num_rows, &_column_offset));
    if (_need_lsn && _row_lsn_col_pos >= 0) {
        auto lsn_column = ColumnInt64::create();
        lsn_column->get_data().assign(allocated_lsns.begin(), allocated_lsns.end());
        _input_mutable_block.get_column_by_position(_row_lsn_col_pos)
                ->replace_column_data_range(*lsn_column, 0, num_rows, cursor_in_mutableblock);
    }
    _append_row_metadata(num_rows, cursor_in_mutableblock, allocated_lsns);

    _stat.raw_rows += num_rows;
    return Status::OK();
}

bool MemTable::need_flush() const {
    DBUG_EXECUTE_IF("MemTable.need_flush", { return true; });
    auto max_size = _flush_buffer_size();
    if (memory_usage() >= max_size) {
        g_flush_cuz_memtable_full << 1;
        return true;
    }
    return false;
}

int64_t MemTable::_flush_buffer_size() const {
    return _adaptive_write_buffer_size();
}

int64_t MemTable::_adaptive_write_buffer_size() const {
    if (!config::enable_adaptive_write_buffer_size) [[unlikely]] {
        return config::write_buffer_size;
    }
    auto* memtable_limiter = ExecEnv::GetInstance()->memtable_memory_limiter();
    int64_t memtable_mem =
            (memtable_limiter != nullptr && memtable_limiter->mem_tracker() != nullptr)
                    ? memtable_limiter->mem_tracker()->consumption()
                    : 0;
    int64_t factor = 4;
    // Memory usage intervals:
    // (80 %, 100 %] → 1× buffer
    // (50 %, 80 %]  → 2× buffer
    // [0 %, 50 %]   → 4× buffer
    if (memtable_mem > (_load_mem_limit * 4) / 5) { // > 80 %
        factor = 1;
    } else if (memtable_mem > _load_mem_limit / 2) { // > 50 %
        factor = 2;
    }
    return config::write_buffer_size * factor;
}

Status MemTable::to_block(std::unique_ptr<Block>* res) {
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_to_block(res));
    return Status::OK();
}

} // namespace doris
