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

#include "storage/iterator/merged_row_cursor.h"

#include <glog/logging.h>

#include <utility>

#include "core/block/block.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {

Status MergedRowCursor::init(IteratorRowRef* row) {
    DCHECK(_block == nullptr);
    _block = std::make_shared<Block>(_rows->schema().create_read_block());
    return _read_block(row);
}

Status MergedRowCursor::next(IteratorRowRef* row) {
    ++_row_pos;
    if (std::cmp_less(_row_pos, _block->rows())) {
        *row = {.block = _block,
                .row_pos = _row_pos,
                .is_same = static_cast<bool>(_same_key[_row_pos])};
        return Status::OK();
    }
    return _read_block(row);
}

Status MergedRowCursor::_read_block(IteratorRowRef* row) {
    _block->clear_column_data();
    RETURN_IF_ERROR(_rows->next_batch(_block.get()));
    DCHECK_GT(_block->rows(), 0);
    if (_flags.has_value()) {
        _flags->compute(*_block, 0, &_same_key);
    } else {
        _same_key.assign(_block->rows(), 0);
    }
    _row_pos = 0;
    *row = {.block = _block,
            .row_pos = _row_pos,
            .is_same = static_cast<bool>(_same_key[_row_pos])};
    return Status::OK();
}

} // namespace doris
