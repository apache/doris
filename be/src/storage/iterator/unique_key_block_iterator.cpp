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

#include "common/exception.h"
#include "core/block/block.h"
#include "storage/iterator/vcollect_iterator.h"

namespace doris {

UniqueKeyBlockIterator::UniqueKeyBlockIterator(VCollectIterator* collect_iter,
                                               OutputBlockLimits limits)
        : _cursor(collect_iter), _limits(limits) {}

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

    return Status::OK();
}

} // namespace doris
