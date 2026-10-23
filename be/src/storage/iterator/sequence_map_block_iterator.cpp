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

#include "storage/iterator/sequence_map_block_iterator.h"

#include <utility>

#include "common/exception.h"
#include "core/block/block.h"
#include "storage/iterators.h"

namespace doris {

SequenceMapBlockIterator::SequenceMapBlockIterator(RowwiseIterator* rows, OutputBlockLimits limits,
                                                   ReadSchemaSPtr read_schema,
                                                   uint64_t* merged_rows)
        : _cursor(rows),
          _limits(limits),
          _read_schema(std::move(read_schema)),
          _merged_rows(merged_rows) {}

Status SequenceMapBlockIterator::next_block(Block* block, bool* eof) {
    if (UNLIKELY(_cursor.eof())) {
        *eof = true;
        return Status::OK();
    }
    *eof = false;

    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();
    size_t rows = 0;
    size_t merged_rows = 0;
    while (rows < _limits.max_rows && !_cursor.eof()) {
        // The cursor is on the newest version of a key: it becomes the output row.
        const IteratorRowRef& row = _cursor.row();
        RETURN_IF_CATCH_EXCEPTION({
            for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
                target_columns[ordinal]->insert_from(*row.block->get_by_position(ordinal).column,
                                                     row.row_pos);
            }
        });
        rows++;

        // Fold the older versions of the same key into it.
        while (true) {
            RETURN_IF_ERROR(_cursor.advance());
            if (_cursor.eof()) {
                break;
            }
            if (!_cursor.row().is_same) {
                break;
            }
            merged_rows++;
            _replace_by_sequence(target_columns);
        }

        // The cursor is now on the next key (or eof), so stopping here repeats no row.
        if (_limits.reached_byte_budget_at(rows, target_columns)) {
            break;
        }
    }
    *_merged_rows += merged_rows;
    return Status::OK();
}

void SequenceMapBlockIterator::_replace_by_sequence(MutableColumns& columns) {
    const IteratorRowRef& src_row = _cursor.row();
    const Block& src_block = *src_row.block;
    const size_t src_pos = src_row.row_pos;

    for (const auto& [seq_ordinal, value_ordinals] : _read_schema->sequence_map()) {
        auto* dst_seq_column = columns[seq_ordinal].get();
        const size_t dst_pos = dst_seq_column->size() - 1;
        const auto& src_seq_column = src_block.get_by_position(seq_ordinal).column;
        // The output row comes from the newer rowset, so it wins ties.
        if (dst_seq_column->compare_at(dst_pos, src_pos, *src_seq_column, -1) >= 0) {
            continue;
        }

        for (auto value_ordinal : value_ordinals) {
            auto* dst_column = columns[value_ordinal].get();
            dst_column->pop_back(1);
            dst_column->insert_from(*src_block.get_by_position(value_ordinal).column, src_pos);
        }
        dst_seq_column->pop_back(1);
        dst_seq_column->insert_from(*src_seq_column, src_pos);
    }
}

} // namespace doris
