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

#include "vec/sink/group_commit_vtablet_sink.h"

#include "vec/core/future_block.h"

namespace doris {

namespace stream_load {

GroupCommitVOlapTableSink::GroupCommitVOlapTableSink(ObjectPool* pool,
                                                     const RowDescriptor& row_desc,
                                                     const std::vector<TExpr>& texprs,
                                                     Status* status)
        : VOlapTableSink(pool, row_desc, texprs, status) {}

void GroupCommitVOlapTableSink::handle_block(vectorized::Block* input_block, int64_t rows,
                                             int64_t filter_rows) {
    auto* future_block = dynamic_cast<vectorized::FutureBlock*>(input_block);
    std::unique_lock<doris::Mutex> l(*(future_block->lock));
    future_block->set_result(Status::OK(), rows, rows - filter_rows);
    future_block->cv->notify_all();
}
} // namespace stream_load
} // namespace doris
