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

#include "vec/sink/group_commit_block_sink.h"

#include <gen_cpp/DataSinks_types.h>

#include <future>
#include <shared_mutex>

#include "common/exception.h"
#include "common/status.h"
#include "runtime/exec_env.h"
#include "runtime/group_commit_mgr.h"
#include "runtime/runtime_state.h"
#include "util/debug_points.h"
#include "util/doris_metrics.h"
#include "vec/exprs/vexpr.h"
#include "vec/sink/async_writer_sink.h"
#include "vec/sink/vtablet_finder.h"

namespace doris {

namespace vectorized {

GroupCommitBlockSink::GroupCommitBlockSink(ObjectPool* pool, const RowDescriptor& row_desc,
                                           const std::vector<TExpr>& texprs)
        : AsyncWriterSink<VGroupCommitBlockWriter, GROUP_COMMIT_BLOCK_SINK>(row_desc, texprs) {}

GroupCommitBlockSink::~GroupCommitBlockSink() = default;

Status GroupCommitBlockSink::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(AsyncWriterSink::init(t_sink));
    return Status::OK();
}

Status GroupCommitBlockSink::close(RuntimeState* state, Status close_status) {
    RETURN_IF_ERROR(AsyncWriterSink::close(state, close_status));
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
