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

#include "load/memtable/memtable_dup_keys.h"

#include <limits>
#include <numeric>
#include <utility>
#include <vector>

#include "load/memtable/memtable_sort.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/tablet/tablet_schema.h"
#include "util/stopwatch.hpp"

namespace doris {

DupKeysMemTable::DupKeysMemTable(int64_t tablet_id, std::shared_ptr<TabletSchema> tablet_schema,
                                 const std::vector<SlotDescriptor*>* slot_descs,
                                 TupleDescriptor* tuple_desc,
                                 PartialUpdateInfo* partial_update_info,
                                 const std::shared_ptr<ResourceContext>& resource_ctx,
                                 bool need_lsn)
        : MemTable(tablet_id, std::move(tablet_schema), slot_descs, tuple_desc, partial_update_info,
                   resource_ctx, need_lsn) {}

DupKeysMemTable::~DupKeysMemTable() {
    // The permutation is a plain member, so release its tracked capacity here: derived members are
    // destroyed before the base destructor installs its tracker scope.
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    DorisVector<uint32_t>().swap(_row_positions);
    DorisVector<int64_t>().swap(_allocated_lsns);
}

void DupKeysMemTable::_reserve_rows(size_t num_rows, size_t cursor) {
    DORIS_CHECK(_row_positions.size() == cursor);
    reserve_for_append(_row_positions, num_rows, MAX_MEMTABLE_ROWS);
    if (_need_lsn) {
        DORIS_CHECK(_allocated_lsns.size() == cursor);
        reserve_for_append(_allocated_lsns, num_rows, MAX_MEMTABLE_ROWS);
    }
}

void DupKeysMemTable::_append_row_metadata(size_t num_rows, size_t cursor,
                                           const DorisVector<int64_t>& allocated_lsns) {
    // The mutable block appends rows contiguously, so the initial permutation for this batch is
    // exactly [cursor, cursor + num_rows). _reserve_rows() already made room, so filling the
    // reserved range cannot reallocate.
    //
    // resize() + iota() rather than an emplace_back() loop: resize() value-initialises the new
    // range and iota() immediately overwrites it, but the memset that costs is still cheaper than
    // the per-element capacity check of emplace_back(). BM_MemTableScenario's
    // dup_zero_key_columns and dup_no_dup_keys are dominated by this fill and show the difference.
    DORIS_CHECK(cursor + num_rows <= _row_positions.capacity());
    _row_positions.resize(cursor + num_rows);
    std::iota(_row_positions.begin() + cursor, _row_positions.end(), static_cast<uint32_t>(cursor));
    if (_need_lsn) {
        DORIS_CHECK(cursor + num_rows <= _allocated_lsns.capacity());
        _allocated_lsns.insert(_allocated_lsns.end(), allocated_lsns.begin(), allocated_lsns.end());
    }
}

size_t DupKeysMemTable::_sort() {
    SCOPED_RAW_TIMER(&_stat.sort_ns);
    _stat.sort_times++;
    return memtable_detail::sort_rows(
            _input_mutable_block, *_tablet_schema, _last_sorted_pos, _row_positions,
            [](uint32_t row_pos) { return row_pos; }, true);
}

Status DupKeysMemTable::_put_into_output(Block& in_block) {
    SCOPED_RAW_TIMER(&_stat.put_into_output_ns);
    DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
    DCHECK_EQ(_row_positions.size(), in_block.rows());
    if (_need_lsn) {
        _output_allocated_lsns->reserve(_output_allocated_lsns->size() + in_block.rows());
        // LSNs are stored in physical input order. Apply the row permutation here so each sorted
        // output row keeps the LSN assigned to its original position.
        DCHECK_EQ(_allocated_lsns.size(), in_block.rows());
        for (const auto row_pos : _row_positions) {
            _output_allocated_lsns->emplace_back(_allocated_lsns[row_pos]);
        }
    }
    return _output_mutable_block.add_rows(&in_block, _row_positions.data(),
                                          _row_positions.data() + _row_positions.size());
}

Status DupKeysMemTable::_to_block(std::unique_ptr<Block>* res) {
    _output_allocated_lsns = std::make_shared<std::vector<int64_t>>();
    if (_tablet_schema->num_key_columns() == 0) {
        // There is no key to sort. Keep the physical block and LSN sidecar in insertion order and
        // avoid building Tie state or sorting a permutation that would be discarded immediately.
        _output_mutable_block.swap(_input_mutable_block);
        if (_need_lsn) {
            DCHECK_EQ(_allocated_lsns.size(), _output_mutable_block.rows());
            _output_allocated_lsns->assign(_allocated_lsns.begin(), _allocated_lsns.end());
        }
        _input_mutable_block.clear();
        *res = Block::create_unique(_output_mutable_block.to_block());
        return Status::OK();
    }

    _sort();
    Block in_block = _input_mutable_block.to_block();
    RETURN_IF_ERROR(_put_into_output(in_block));
    if (_need_lsn) {
        DCHECK_EQ(_output_allocated_lsns->size(), _output_mutable_block.rows());
    }
    _input_mutable_block.clear();
    *res = Block::create_unique(_output_mutable_block.to_block());
    return Status::OK();
}

} // namespace doris
