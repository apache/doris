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
#include "common/config.h"
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
    _block_row_max = opts.block_row_max;
    _preferred_block_size_bytes = opts.preferred_block_size_bytes;
    _runtime_state = opts.runtime_state;
    _tablet_id = opts.tablet_id;

    _heap.reserve(_children.size());
    for (size_t index = 0; index < _children.size(); ++index) {
        Child& child = _children[index];
        child.block = std::make_shared<Block>(_read_schema->create_read_block());
        bool exhausted = false;
        RETURN_IF_ERROR(_fill(child, &exhausted));
        if (exhausted) {
            child.iter.reset();
            child.block.reset();
            continue;
        }
        RETURN_IF_ERROR(_validate_compare_contract(*child.block));
        _heap.push_back(cast_set<uint32_t>(index));
    }
    std::ranges::make_heap(_heap, _heap_order());
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
    // A segment returns an empty batch when its predicates filter out every row it read.
    do {
        child.block->clear_column_data();
        Status st = child.iter->next_batch(child.block.get());
        if (st.is<END_OF_FILE>()) {
            DCHECK_EQ(child.block->rows(), 0);
            *exhausted = true;
            child.pos = 0;
            child.rows = 0;
            return Status::OK();
        }
        RETURN_IF_ERROR(st);
        if (_runtime_state != nullptr && _runtime_state->is_cancelled()) [[unlikely]] {
            return _runtime_state->cancel_reason();
        }
    } while (child.block->rows() == 0);
    DCHECK_EQ(child.block->columns(), _read_schema->num_block_columns());
    child.pos = 0;
    child.rows = child.block->rows();
    child.bytes = child.block->bytes();
    *exhausted = false;
    return Status::OK();
}

bool VHeapMergeIterator::_less(uint32_t a, size_t a_pos, uint32_t b) const {
    const Block& a_block = *_children[a].block;
    const Child& b_child = _children[b];
    const Block& b_block = *b_child.block;
    const int keys =
            a_block.compare_at(a_pos, b_child.pos, &_decision.compare_ordinals, b_block, -1);
    if (keys != 0) {
        return _decision.reverse ? keys > 0 : keys < 0;
    }
    if (_decision.sequence_ordinal != -1) {
        const int sequence = a_block.compare_column_at(a_pos, b_child.pos,
                                                       _decision.sequence_ordinal, b_block, -1);
        if (sequence != 0) {
            return _decision.small_sequence_first ? sequence < 0 : sequence > 0;
        }
    }
    // Same key and sequence: the later child holds the newer row.
    return _decision.older_first ? a < b : a > b;
}

size_t VHeapMergeIterator::_max_rows() const {
    if (!config::enable_adaptive_batch_size || _preferred_block_size_bytes == 0) {
        return _block_row_max;
    }
    size_t bytes = 0;
    size_t rows = 0;
    for (uint32_t index : _heap) {
        bytes += _children[index].bytes;
        rows += _children[index].rows;
    }
    const size_t row_bytes = std::max<size_t>(1, bytes / rows);
    return std::clamp<size_t>(_preferred_block_size_bytes / row_bytes, 1, _block_row_max);
}

void VHeapMergeIterator::_append(MutableColumns& columns, const Child& child, size_t rows) {
    for (size_t ordinal = 0; ordinal < columns.size(); ++ordinal) {
        const IColumn& source = *child.block->get_by_position(ordinal).column;
        if (rows == 1) {
            columns[ordinal]->insert_from(source, child.pos);
        } else {
            columns[ordinal]->insert_range_from(source, child.pos, rows);
        }
    }
}

Status VHeapMergeIterator::next_batch(Block* block) {
    DCHECK(!_eof) << "next_batch() called after EndOfFile";
    DCHECK_EQ(block->columns(), _read_schema->num_block_columns());
    if (_heap.empty()) {
        _eof = true;
        return Status::EndOfFile("no more rows to merge");
    }
    const size_t max_rows = _max_rows();
    size_t rows = 0;
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    while (rows < max_rows && !_heap.empty()) {
        // The child whose current row goes first moves to the back of `_heap`; the rest stays a
        // heap, with the child whose row goes next on top.
        std::ranges::pop_heap(_heap, _heap_order());
        const uint32_t index = _heap.back();
        Child& child = _children[index];
        // Its run: the rows that go before the current row of every other child.
        const size_t limit = std::min(child.rows - child.pos, max_rows - rows);
        size_t run = 1;
        if (_heap.size() == 1) {
            run = limit;
        } else {
            while (run < limit && _less(index, child.pos + run, _heap.front())) {
                ++run;
            }
        }
        RETURN_IF_CATCH_EXCEPTION(_append(columns, child, run));
        child.pos += run;
        rows += run;
        if (child.pos == child.rows) {
            bool exhausted = false;
            RETURN_IF_ERROR(_fill(child, &exhausted));
            if (exhausted) {
                // The child is done: release its segment and its Block now, not at the end of the
                // merge.
                child.iter.reset();
                child.block.reset();
                _heap.pop_back();
                continue;
            }
        }
        std::ranges::push_heap(_heap, _heap_order());
    }
    return Status::OK();
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
