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

#pragma once

#include <memory>
#include <vector>

#include "common/status.h"
#include "exec/data_sink.h"
#include "runtime/result_queue_mgr.h"
#include "vec/exprs/vexpr_fwd.h"

namespace arrow {

class Schema;

} // namespace arrow

namespace doris {

class ObjectPool;
class RuntimeState;
class RuntimeProfile;
class RowDescriptor;
class TExpr;
class TMemoryScratchSink;

namespace vectorized {
class Block;

// used to push data to blocking queue
class MemoryScratchSink final : public DataSink {
public:
    MemoryScratchSink(const RowDescriptor& row_desc, const std::vector<TExpr>& t_output_expr);

    ~MemoryScratchSink() override = default;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    // send data in 'batch' to this backend queue mgr
    // Blocks until all rows in batch are pushed to the queue
    Status send(RuntimeState* state, Block* batch, bool eos) override;

    Status close(RuntimeState* state, Status exec_status) override;

    bool can_write() override;

private:
    Status _prepare_vexpr(RuntimeState* state);

    BlockQueueSharedPtr _queue;

    // Owned by the RuntimeState.
    const std::vector<TExpr>& _t_output_expr;
    VExprContextSPtrs _output_vexpr_ctxs;
};
} // namespace vectorized
} // namespace doris
