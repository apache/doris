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

#include "storage/iterator/unique_key_block_iterator.h"

#include <memory>

#include "common/exception.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "storage/iterator/vcollect_iterator.h"
#include "storage/olap_common.h"

namespace doris {

UniqueKeyBlockIterator::UniqueKeyBlockIterator(VCollectIterator* collect_iter,
                                               OutputBlockLimits limits,
                                               int32_t delete_sign_ordinal,
                                               OlapReaderStatistics* stats)
        : _cursor(collect_iter),
          _limits(limits),
          _delete_sign_ordinal(delete_sign_ordinal),
          _stats(stats) {
    if (_delete_sign_ordinal >= 0) {
        _delete_filter_column = ColumnUInt8::create();
    }
}

Status UniqueKeyBlockIterator::next_block(Block* block, bool* eof) {
    if (UNLIKELY(_cursor.eof())) {
        *eof = true;
        return Status::OK();
    }

    size_t rows = 0;
    {
        auto target_columns_guard = block->mutate_columns_scoped();
        auto& target_columns = target_columns_guard.mutable_columns();
        do {
            const IteratorRowRef& row = _cursor.row();
            RETURN_IF_CATCH_EXCEPTION({
                for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
                    target_columns[ordinal]->insert_from(
                            *row.block->get_by_position(ordinal).column, row.row_pos);
                }
            });
            rows++;

            // The copied row is consumed: advance before any stop so the next call starts
            // with an unread row.
            RETURN_IF_ERROR(_cursor.advance());
            if (_cursor.eof()) {
                *eof = true;
                break;
            }
        } while (rows < _limits.max_rows && !_limits.reached_byte_budget_at(rows, target_columns));
    }

    if (_delete_sign_ordinal >= 0) {
        RETURN_IF_ERROR(_filter_deleted_rows(block, rows));
    }
    return Status::OK();
}

Status UniqueKeyBlockIterator::_filter_deleted_rows(Block* block, size_t rows) {
    DORIS_CHECK_GT(_delete_sign_ordinal, 0);
    DORIS_CHECK_LT(static_cast<size_t>(_delete_sign_ordinal), block->columns());

    auto delete_filter_column = IColumn::mutate(std::move(_delete_filter_column));
    auto* filter_column = reinterpret_cast<ColumnUInt8*>(delete_filter_column.get());
    filter_column->resize(rows);
    auto* __restrict filter_data = filter_column->get_data().data();
    const auto* __restrict delete_data =
            reinterpret_cast<const ColumnInt8*>(
                    block->get_by_position(_delete_sign_ordinal).column.get())
                    ->get_data()
                    .data();
    for (size_t i = 0; i < rows; ++i) {
        filter_data[i] = delete_data[i] == 0;
    }
    _delete_filter_column = std::move(delete_filter_column);

    const size_t columns_to_keep = block->columns();
    block->insert(ColumnWithTypeAndName {_delete_filter_column, std::make_shared<DataTypeUInt8>(),
                                         "__DORIS_COMPACTION_FILTER__"});
    RETURN_IF_ERROR(Block::filter_block(block, columns_to_keep, columns_to_keep));
    _stats->rows_del_filtered += rows - block->rows();
    return Status::OK();
}

} // namespace doris
