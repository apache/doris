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

#include "storage/iterator/tablet_block_iterator.h"

#include <glog/logging.h>

#include <memory>
#include <utility>

#include "core/block/block.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {
using namespace ErrorCode;

Status MergedRowCursor::init() {
    DCHECK(!_eof);
    _row.block = std::make_shared<Block>(_rows->schema().create_read_block());
    return _read_block();
}

Status MergedRowCursor::advance() {
    DCHECK(!_eof);
    ++_row.row_pos;
    if (std::cmp_less(_row.row_pos, _row.block->rows())) {
        _row.is_same = _same_key[_row.row_pos];
        return Status::OK();
    }
    return _read_block();
}

Status MergedRowCursor::_read_block() {
    _row.block->clear_column_data();
    auto st = _rows->next_batch(_row.block.get(), &_same_key);
    if (UNLIKELY(st.is<END_OF_FILE>())) {
        _eof = true;
        _row.reset();
        return Status::OK();
    }
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << "next_batch failed: " << st;
        return st;
    }
    DCHECK_EQ(_same_key.size(), _row.block->rows());
    _row.row_pos = 0;
    _row.is_same = _same_key[0];
    return Status::OK();
}

} // namespace doris
