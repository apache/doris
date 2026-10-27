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

#include "storage/iterator/vgeneric_iterators.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "common/exception.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "runtime/runtime_state.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
class RuntimeProfile;
using namespace ErrorCode;

Status VStatisticsIterator::init(const StorageReadOptions& opts) {
    if (!_init) {
        _push_down_agg_type_opt = opts.push_down_agg_type_opt;

        if (_push_down_agg_type_opt != TPushAggOp::COUNT) {
            _column_iterators.resize(_schema.num_block_columns());
            for (size_t ordinal = 0; ordinal < _schema.num_block_columns(); ++ordinal) {
                RETURN_IF_ERROR(_segment->new_column_iterator(*_schema.column(ordinal),
                                                              &_column_iterators[ordinal], &opts));
            }
        }

        _target_rows = _push_down_agg_type_opt == TPushAggOp::MINMAX ? 2 : _segment->num_rows();
        _init = true;
    }
    return Status::OK();
}

Status VStatisticsIterator::next_batch(Block* block) {
    DCHECK_EQ(block->columns(), _schema.num_block_columns());
    if (_output_rows < _target_rows) {
        block->clear_column_data();
        auto columns_guard = block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();

        size_t size = _push_down_agg_type_opt == TPushAggOp::MINMAX
                              ? 2
                              : std::min(_target_rows - _output_rows, MAX_ROW_SIZE_IN_COUNT);
        if (_push_down_agg_type_opt == TPushAggOp::COUNT) {
            for (auto& column : columns) {
                column->insert_many_defaults(size);
            }
        } else {
            for (size_t i = 0; i < columns.size(); ++i) {
                RETURN_IF_ERROR(_column_iterators[i]->next_batch_of_zone_map(&size, columns[i]));
            }
        }
        _output_rows += size;
        return Status::OK();
    }
    return Status::EndOfFile("End of VStatisticsIterator");
}

VMergeIterator::VMergeIterator(std::vector<RowwiseIteratorUPtr> children,
                               ReadSchemaSPtr read_schema, MergeDecision decision)
        : _read_schema(std::move(read_schema)), _decision(std::move(decision)) {
    DCHECK(!_decision.drop_older_versions || _decision.compare_whole_key);
    _children.reserve(children.size());
    for (auto& child : children) {
        _children.emplace_back();
        _children.back().iter = std::move(child);
    }
}

Status VMergeIterator::init(const StorageReadOptions& opts) {
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
    for (size_t index = _heap.size(); index-- > 0;) {
        _sift_down(index);
    }
    _update_second();
    return Status::OK();
}

// The read-schema ordinals the rows are compared by must exist in the merged Blocks and, when
// they are the whole key, be key columns: a projection that puts a value column ahead of a
// key would otherwise be merged by that value (issue #66390).
Status VMergeIterator::_validate_compare_contract(const Block& block) const {
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

Status VMergeIterator::_fill(Child& child, bool* exhausted) {
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

int VMergeIterator::_compare_keys(const Block& a, size_t a_pos, const Block& b,
                                  size_t b_pos) const {
    return a.compare_at(a_pos, b_pos, &_decision.compare_ordinals, b, -1);
}

bool VMergeIterator::_less(uint32_t a, size_t a_pos, uint32_t b, size_t b_pos) const {
    const Block& a_block = *_children[a].block;
    const Block& b_block = *_children[b].block;
    const int keys = _compare_keys(a_block, a_pos, b_block, b_pos);
    if (keys != 0) {
        return _decision.reverse ? keys > 0 : keys < 0;
    }
    if (_decision.sequence_ordinal != -1) {
        const int sequence =
                a_block.compare_column_at(a_pos, b_pos, _decision.sequence_ordinal, b_block, -1);
        if (sequence != 0) {
            return _decision.small_sequence_first ? sequence < 0 : sequence > 0;
        }
    }
    // Same key and sequence: the later child holds the newer row.
    return _decision.older_first ? a < b : a > b;
}

void VMergeIterator::_sift_down(size_t index) {
    const size_t size = _heap.size();
    while (true) {
        size_t smallest = index;
        const size_t left = 2 * index + 1;
        const size_t right = left + 1;
        if (left < size && _less_current(_heap[left], _heap[smallest])) {
            smallest = left;
        }
        if (right < size && _less_current(_heap[right], _heap[smallest])) {
            smallest = right;
        }
        if (smallest == index) {
            return;
        }
        std::swap(_heap[index], _heap[smallest]);
        index = smallest;
    }
}

void VMergeIterator::_pop_top() {
    _heap[0] = _heap.back();
    _heap.pop_back();
    if (!_heap.empty()) {
        _sift_down(0);
    }
}

void VMergeIterator::_update_second() {
    if (_heap.size() <= 1) {
        return;
    }
    // In a binary min-heap the second-best element is one of the root's two children.
    _second = _heap[1];
    if (_heap.size() > 2 && _less_current(_heap[2], _heap[1])) {
        _second = _heap[2];
    }
}

void VMergeIterator::_remember_key(const Block& block, size_t pos) {
    if (_last_key.empty()) {
        for (uint32_t ordinal : _decision.compare_ordinals) {
            _last_key.push_back(block.get_by_position(ordinal).column->clone_empty());
        }
    }
    for (size_t i = 0; i < _decision.compare_ordinals.size(); ++i) {
        _last_key[i]->clear();
        _last_key[i]->insert_from(*block.get_by_position(_decision.compare_ordinals[i]).column,
                                  pos);
    }
}

bool VMergeIterator::_key_equals_remembered(const Block& block, size_t pos) const {
    DCHECK(!_last_key.empty());
    for (size_t i = 0; i < _decision.compare_ordinals.size(); ++i) {
        if (block.get_by_position(_decision.compare_ordinals[i])
                    .column->compare_at(pos, 0, *_last_key[i], -1) != 0) {
            return false;
        }
    }
    return true;
}

Status VMergeIterator::_advance_top(bool* same) {
    const uint32_t top_index = _heap[0];
    Child& top = _children[top_index];
    const size_t prev_pos = top.pos;
    top.pos++;
    if (top.pos < top.rows &&
        (_heap.size() == 1 || _less(top_index, top.pos, _second, _children[_second].pos))) {
        // The run goes on, and the row left behind is still in the same Block.
        *same = _compare_keys(*top.block, prev_pos, *top.block, top.pos) == 0;
        return Status::OK();
    }
    // The run ends: the top child reads on or drops out, and the Block of the row left
    // behind may be refilled, so its key is kept aside first.
    _remember_key(*top.block, prev_pos);
    RETURN_IF_ERROR(_top_moved());
    if (_heap.empty()) {
        *same = false;
        return Status::OK();
    }
    _update_second();
    const Child& next = _children[_heap[0]];
    *same = _key_equals_remembered(*next.block, next.pos);
    return Status::OK();
}

Status VMergeIterator::next_row(IteratorRowRef* ref) {
    DCHECK(!_eof) << "next_row() called after EndOfFile";
    bool same = false;
    if (_row_handed_out) {
        while (true) {
            RETURN_IF_ERROR(_advance_top(&same));
            if (_heap.empty()) {
                _eof = true;
                return Status::EndOfFile("no more rows to merge");
            }
            if (_decision.drop_older_versions && same) {
                _merged_rows++;
                continue;
            }
            break;
        }
    } else {
        if (_heap.empty()) {
            _eof = true;
            return Status::EndOfFile("no more rows to merge");
        }
        _row_handed_out = true;
    }
    const Child& top = _children[_heap[0]];
    *ref = IteratorRowRef {.block = top.block, .row_pos = cast_set<int>(top.pos), .is_same = same};
    return Status::OK();
}

Status VMergeIterator::_top_moved() {
    Child& top = _children[_heap[0]];
    if (top.pos == top.rows) {
        bool exhausted = false;
        RETURN_IF_ERROR(_fill(top, &exhausted));
        if (exhausted) {
            // The child is done: release its segment and its Block now, not at the end of
            // the merge. A row handed out of this Block keeps the Block alive on its own.
            _pop_top();
            top.iter.reset();
            top.block.reset();
            return Status::OK();
        }
    }
    _sift_down(0);
    return Status::OK();
}

size_t VMergeIterator::_run_end(uint32_t top_index, size_t cap) const {
    if (_heap.size() == 1) {
        return cap;
    }
    const Child& second = _children[_second];
    size_t end = _children[top_index].pos + 1;
    while (end < cap && _less(top_index, end, _second, second.pos)) {
        ++end;
    }
    return end;
}

Status VMergeIterator::_take_run(MutableColumns& columns, size_t max_rows, size_t* rows,
                                 std::vector<uint8_t>* same_key) {
    const uint32_t top_index = _heap[0];
    Child& top = _children[top_index];
    const size_t end = _run_end(top_index, std::min(top.rows, top.pos + max_rows));
    size_t start = top.pos;
    // Only the first row of the run can repeat the key taken last, since a child holds every
    // key once.
    const bool track_key = _decision.drop_older_versions || same_key != nullptr;
    const bool repeats_last_key =
            track_key && !_last_key.empty() && _key_equals_remembered(*top.block, start);
    if (_decision.drop_older_versions && repeats_last_key) {
        ++start;
        ++_merged_rows;
    }
    if (end > start) {
        _taken_runs.push_back({.child = top_index, .pos = start, .rows = end - start});
        *rows += end - start;
        if (same_key != nullptr) {
            same_key->push_back(repeats_last_key ? 1 : 0);
            same_key->resize(same_key->size() + (end - start - 1), 0);
        }
        if (track_key) {
            _remember_key(*top.block, end - 1);
        }
    }
    top.pos = end;
    if (top.pos == top.rows) {
        // _top_moved() refills or releases this Block, which the taken runs may point into.
        RETURN_IF_CATCH_EXCEPTION(_copy_taken_runs(columns));
    }
    RETURN_IF_ERROR(_top_moved());
    _update_second();
    return Status::OK();
}

void VMergeIterator::_copy_taken_runs(MutableColumns& columns) {
    for (size_t ordinal = 0; ordinal < columns.size(); ++ordinal) {
        IColumn& column = *columns[ordinal];
        for (const TakenRun& run : _taken_runs) {
            const IColumn& source = *_children[run.child].block->get_by_position(ordinal).column;
            if (run.rows == 1) {
                column.insert_from(source, run.pos);
            } else {
                column.insert_range_from(source, run.pos, run.rows);
            }
        }
    }
    _taken_runs.clear();
}

Status VMergeIterator::_next_batch(Block* block, std::vector<uint8_t>* same_key) {
    DCHECK(!_eof) << "next_batch() called after EndOfFile";
    DCHECK(!_row_handed_out) << "a merge is consumed either by next_batch() or by next_row()";
    DCHECK_EQ(block->columns(), _read_schema->num_block_columns());
    DCHECK(_taken_runs.empty());
    size_t rows = 0;
    size_t rows_at_last_check = 0;
    {
        auto columns_guard = block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        while (rows < _limits.max_rows && !_heap.empty()) {
            RETURN_IF_ERROR(_take_run(columns, _limits.max_rows - rows, &rows, same_key));
            if (_limits.has_byte_budget() &&
                rows - rows_at_last_check >= OutputBlockLimits::BYTE_BUDGET_CHECK_INTERVAL_ROWS) {
                rows_at_last_check = rows;
                // The budget measures the copied columns.
                RETURN_IF_CATCH_EXCEPTION(_copy_taken_runs(columns));
                if (_limits.reached_byte_budget(columns)) {
                    break;
                }
            }
        }
        RETURN_IF_CATCH_EXCEPTION(_copy_taken_runs(columns));
    }
    if (rows == 0) {
        DCHECK(_heap.empty());
        _eof = true;
        return Status::EndOfFile("no more rows to merge");
    }
    return Status::OK();
}

Status VMergeIterator::next_batch(Block* block) {
    return _next_batch(block, nullptr);
}

Status VMergeIterator::next_batch(Block* block, std::vector<uint8_t>* same_key) {
    DORIS_CHECK(!_decision.drop_older_versions);
    same_key->clear();
    return _next_batch(block, same_key);
}

void VMergeIterator::update_profile(RuntimeProfile* profile) {
    for (auto& child : _children) {
        if (child.iter != nullptr) {
            child.iter->update_profile(profile);
        }
    }
}

// VUnionIterator reads input iterators one by one. Every input writes the same
// visible slot prefix; a SegmentIterator may retain delete-predicate columns
// privately after that prefix.
class VUnionIterator : public RowwiseIterator {
public:
    // Iterators' ownership it transferred to this class.
    // This class will delete all iterators when destructs
    // Client should not use iterators anymore.
    VUnionIterator(std::vector<RowwiseIteratorUPtr>&& v, ReadSchemaSPtr read_schema)
            : _read_schema(std::move(read_schema)), _origin_iters(std::move(v)) {}

    ~VUnionIterator() override = default;

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override;

    // A union does not compare keys, the way next_row() never flags a row: every flag is 0.
    // The AGG_KEYS fold reads a union only over rowsets whose keys do not overlap.
    Status next_batch(Block* block, std::vector<uint8_t>* same_key) override;

    Status next_row(IteratorRowRef* ref) override;

    const ReadSchema& schema() const override { return *_read_schema; }

    void update_profile(RuntimeProfile* profile) override {
        if (_cur_iter != nullptr) {
            _cur_iter->update_profile(profile);
        }
    }

private:
    const ReadSchemaSPtr _read_schema;
    RowwiseIteratorUPtr _cur_iter = nullptr;
    std::optional<StorageReadOptions> _read_options;
    std::vector<RowwiseIteratorUPtr> _origin_iters;
    // next_row(): the Block the handed-out rows reference and the row handed out last.
    std::shared_ptr<Block> _row_block;
    size_t _row_pos = 0;
};

Status VUnionIterator::init(const StorageReadOptions& opts) {
    _read_options.emplace(opts);
    if (_origin_iters.empty()) {
        return Status::OK();
    }

    // we use back() and pop_back() of std::vector to handle each iterator,
    // so reverse the vector here to keep result block of next_batch to be
    // in the same order as the original segments.
    std::ranges::reverse(_origin_iters);

    _cur_iter = std::move(_origin_iters.back());
    RETURN_IF_ERROR(_cur_iter->init(*_read_options));
    return Status::OK();
}

Status VUnionIterator::next_batch(Block* block) {
    while (_cur_iter != nullptr) {
        auto st = _cur_iter->next_batch(block);
        if (st.is<END_OF_FILE>()) {
            _origin_iters.pop_back();
            if (!_origin_iters.empty()) {
                _cur_iter = std::move(_origin_iters.back());
                RETURN_IF_ERROR(_cur_iter->init(*_read_options));
            } else {
                _cur_iter = nullptr;
            }
        } else {
            return st;
        }
    }
    return Status::EndOfFile("End of VUnionIterator");
}

Status VUnionIterator::next_batch(Block* block, std::vector<uint8_t>* same_key) {
    same_key->clear();
    RETURN_IF_ERROR(next_batch(block));
    same_key->resize(block->rows(), 0);
    return Status::OK();
}

Status VUnionIterator::next_row(IteratorRowRef* ref) {
    if (_row_block == nullptr) {
        _row_block = std::make_shared<Block>(_read_schema->create_read_block());
    }
    if (_row_pos + 1 < _row_block->rows()) {
        ++_row_pos;
    } else {
        _row_block->clear_column_data();
        RETURN_IF_ERROR(next_batch(_row_block.get()));
        _row_pos = 0;
    }
    *ref = IteratorRowRef {
            .block = _row_block, .row_pos = cast_set<int>(_row_pos), .is_same = false};
    return Status::OK();
}

RowwiseIteratorUPtr new_merge_iterator(std::vector<RowwiseIteratorUPtr> children,
                                       ReadSchemaSPtr read_schema, const MergeDecision& decision) {
    return std::make_unique<VMergeIterator>(std::move(children), std::move(read_schema), decision);
}

RowwiseIteratorUPtr new_union_iterator(std::vector<RowwiseIteratorUPtr>&& inputs,
                                       ReadSchemaSPtr read_schema) {
    return std::make_unique<VUnionIterator>(std::move(inputs), std::move(read_schema));
}

RowwiseIterator* new_vstatistics_iterator(std::shared_ptr<Segment> segment,
                                          const ReadSchema& schema) {
    return new VStatisticsIterator(segment, schema);
}

} // namespace doris
