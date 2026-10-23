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

#include "storage/iterators.h"

namespace doris {
using namespace ErrorCode;

Status MergedRowCursor::init() {
    DCHECK(!_eof);
    return advance();
}

Status MergedRowCursor::advance() {
    DCHECK(!_eof);
    auto st = _rows->next_row(&_row);
    if (UNLIKELY(st.is<END_OF_FILE>())) {
        _eof = true;
        _row.reset();
        return Status::OK();
    }
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << "next_row failed: " << st;
        return st;
    }
    return Status::OK();
}

} // namespace doris
