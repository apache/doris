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

#include "storage/iterator/heap_merge_iterator.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "common/cast_set.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "runtime/runtime_state.h"
#include "storage/iterators.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
using namespace ErrorCode;

VHeapMergeIterator::VHeapMergeIterator(std::vector<RowwiseIteratorUPtr> children,
                                       ReadSchemaSPtr read_schema, MergeDecision decision)
        : _read_schema(std::move(read_schema)), _decision(std::move(decision)) {
    _children.reserve(children.size());
    for (auto& child : children) {
        _children.emplace_back();
        _children.back().iter = std::move(child);
    }
}

Status VHeapMergeIterator::init(const StorageReadOptions& opts) {
    DORIS_CHECK_GT(opts.block_row_max, 0);
    _limits = OutputBlockLimits {.max_rows = opts.block_row_max,
                                 .preferred_bytes = opts.preferred_block_size_bytes};
    _runtime_state = opts.runtime_state;
    _tablet_id = opts.tablet_id;

    _heap.reserve(_children.size());
    for (size_t index = 0; index < _children.size(); ++index) {
        Child& child = _children[index];
        RETURN_IF_ERROR(child.iter->init(opts));
        child.block = std::make_shared<Block>(_read_schema->create_read_block());
        bool exhausted = false;
        RETURN_IF_ERROR(_fill(child, &exhausted));
        if (exhausted) {
            continue;
        }
        RETURN_IF_ERROR(_validate_compare_contract(*child.block));
        _heap.push_back(cast_set<uint32_t>(index));
    }
    if (_heap.empty()) {
        return Status::OK();
    }
    std::ranges::make_heap(_heap, _heap_order());
    std::ranges::pop_heap(_heap, _heap_order());
    _lead = _heap.back();
    _heap.pop_back();
    return Status::OK();
}

// The read-schema ordinals the rows are compared by must exist in the merged Blocks and, when
// they are the whole key, be key columns: a projection that puts a value column ahead of a
// key would otherwise be merged by that value (issue #66390).
Status VHeapMergeIterator::_validate_compare_contract(const Block& block) const {
    const size_t block_columns = block.columns();
    auto contract_error = [&](const std::string& detail) {
        return Status::InternalError(
                "merge iterator compare contract violated: {}, tablet_id={}, block_columns={}, "
                "read_columns={}, num_key_columns={}, sequence_ordinal={}",
                detail, _tablet_id, block_columns, _read_schema->num_block_columns(),
                _read_schema->num_key_columns(), _decision.sequence_ordinal);
    };
    for (uint32_t ordinal : _decision.compare_ordinals) {
        if (ordinal >= block_columns) {
            return contract_error("compare column ordinal " + std::to_string(ordinal) +
                                  " out of range");
        }
        if (_decision.compare_whole_key && !_read_schema->column(ordinal)->is_key()) {
            return contract_error("read schema ordinal " + std::to_string(ordinal) +
                                  " is not a key column");
        }
    }
    if (_decision.sequence_ordinal != -1 &&
        std::cmp_greater_equal(_decision.sequence_ordinal, block_columns)) {
        return contract_error("sequence column ordinal out of range");
    }
    return Status::OK();
}

Status VHeapMergeIterator::_fill(Child& child, bool* exhausted) {
    child.block->clear_column_data();
    Status st = child.iter->next_batch(child.block.get());
    if (st.is<END_OF_FILE>()) {
        *exhausted = true;
        child.pos = 0;
        child.rows = 0;
        return Status::OK();
    }
    RETURN_IF_ERROR(st);
    if (_runtime_state != nullptr && _runtime_state->is_cancelled()) [[unlikely]] {
        return _runtime_state->cancel_reason();
    }
    DCHECK_GT(child.block->rows(), 0);
    DCHECK_EQ(child.block->columns(), _read_schema->num_block_columns());
    child.pos = 0;
    child.rows = child.block->rows();
    *exhausted = false;
    return Status::OK();
}

bool VHeapMergeIterator::_less(uint32_t a, uint32_t b) const {
    const Child& a_child = _children[a];
    const Child& b_child = _children[b];
    const Block& a_block = *a_child.block;
    const Block& b_block = *b_child.block;
    const int keys =
            a_block.compare_at(a_child.pos, b_child.pos, &_decision.compare_ordinals, b_block, -1);
    if (keys != 0) {
        return _decision.reverse ? keys > 0 : keys < 0;
    }
    if (_decision.sequence_ordinal != -1) {
        const int sequence = a_block.compare_column_at(a_child.pos, b_child.pos,
                                                       _decision.sequence_ordinal, b_block, -1);
        if (sequence != 0) {
            return _decision.small_sequence_first ? sequence < 0 : sequence > 0;
        }
    }
    // Same key and sequence: the later child holds the newer row.
    return _decision.older_first ? a < b : a > b;
}

Status VHeapMergeIterator::_take_lead_row(MutableColumns& columns,
                                          std::vector<uint32_t>* row_children) {
    const uint32_t lead = *_lead;
    const size_t pos = _children[lead].pos;
    if (!_taken.empty() && _taken.back().child == lead &&
        _taken.back().pos + _taken.back().rows == pos) {
        ++_taken.back().rows;
    } else {
        _taken.push_back({.child = lead, .pos = pos, .rows = 1});
    }
    if (row_children != nullptr) {
        row_children->push_back(lead);
    }
    return _advance_lead(columns);
}

Status VHeapMergeIterator::_advance_lead(MutableColumns& columns) {
    Child& lead = _children[*_lead];
    ++lead.pos;
    if (lead.pos == lead.rows) {
        // _fill() refills this Block, which the taken rows may point into.
        RETURN_IF_CATCH_EXCEPTION(_copy_taken(columns));
        bool exhausted = false;
        RETURN_IF_ERROR(_fill(lead, &exhausted));
        if (exhausted) {
            // The child is done: release its segment and its Block now, not at the end of the
            // merge.
            lead.iter.reset();
            lead.block.reset();
            if (_heap.empty()) {
                _lead.reset();
                return Status::OK();
            }
            std::ranges::pop_heap(_heap, _heap_order());
            _lead = _heap.back();
            _heap.pop_back();
            return Status::OK();
        }
    }
    if (_heap.empty() || _less(*_lead, _heap.front())) {
        return Status::OK();
    }
    // The heap's top goes first now: it becomes the lead, and the lead goes into the heap.
    const uint32_t previous_lead = *_lead;
    std::ranges::pop_heap(_heap, _heap_order());
    _lead = _heap.back();
    _heap.back() = previous_lead;
    std::ranges::push_heap(_heap, _heap_order());
    return Status::OK();
}

void VHeapMergeIterator::_copy_taken(MutableColumns& columns) {
    for (size_t ordinal = 0; ordinal < columns.size(); ++ordinal) {
        IColumn& column = *columns[ordinal];
        for (const TakenRows& taken : _taken) {
            const IColumn& source = *_children[taken.child].block->get_by_position(ordinal).column;
            if (taken.rows == 1) {
                column.insert_from(source, taken.pos);
            } else {
                column.insert_range_from(source, taken.pos, taken.rows);
            }
        }
    }
    _taken.clear();
}

Status VHeapMergeIterator::_next_batch(Block* block, std::vector<uint32_t>* row_children) {
    DCHECK(!_eof) << "next_batch() called after EndOfFile";
    DCHECK_EQ(block->columns(), _read_schema->num_block_columns());
    DCHECK(_taken.empty());
    size_t rows = 0;
    size_t rows_at_last_check = 0;
    {
        auto columns_guard = block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        while (rows < _limits.max_rows && _lead.has_value()) {
            RETURN_IF_ERROR(_take_lead_row(columns, row_children));
            ++rows;
            if (_limits.has_byte_budget() &&
                rows - rows_at_last_check >= OutputBlockLimits::BYTE_BUDGET_CHECK_INTERVAL_ROWS) {
                rows_at_last_check = rows;
                // The budget measures the copied columns.
                RETURN_IF_CATCH_EXCEPTION(_copy_taken(columns));
                if (_limits.reached_byte_budget(columns)) {
                    break;
                }
            }
        }
        RETURN_IF_CATCH_EXCEPTION(_copy_taken(columns));
    }
    if (rows == 0) {
        DCHECK(!_lead.has_value());
        _eof = true;
        return Status::EndOfFile("no more rows to merge");
    }
    return Status::OK();
}

Status VHeapMergeIterator::next_batch(Block* block) {
    return _next_batch(block, nullptr);
}

Status VHeapMergeIterator::next_batch_with_children(Block* block,
                                                    std::vector<uint32_t>* row_children) {
    row_children->clear();
    return _next_batch(block, row_children);
}

void VHeapMergeIterator::update_profile(RuntimeProfile* profile) {
    for (auto& child : _children) {
        if (child.iter != nullptr) {
            child.iter->update_profile(profile);
        }
    }
}

RowwiseIteratorUPtr new_heap_merge_iterator(std::vector<RowwiseIteratorUPtr> children,
                                            ReadSchemaSPtr read_schema,
                                            const MergeDecision& decision) {
    return std::make_unique<VHeapMergeIterator>(std::move(children), std::move(read_schema),
                                                decision);
}

} // namespace doris
