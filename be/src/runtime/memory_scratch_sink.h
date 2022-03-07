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

#include "common/status.h"
#include "exec/data_sink.h"
#include "gen_cpp/DorisExternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/result_queue_mgr.h"
#include "util/blocking_queue.hpp"

namespace arrow {

class MemoryPool;
class RecordBatch;
class Schema;

} // namespace arrow

namespace doris {

class ObjectPool;
class RowBatch;
class ObjectPool;
class RuntimeState;
class RuntimeProfile;
class BufferControlBlock;
class ExprContext;
class ResultWriter;
class MemTracker;
class TupleRow;

// used to push data to blocking queue
class MemoryScratchSink : public DataSink {
public:
    MemoryScratchSink(const RowDescriptor& row_desc, const std::vector<TExpr>& select_exprs,
                      const TMemoryScratchSink& sink);

    virtual ~MemoryScratchSink();

    virtual Status prepare(RuntimeState* state);

    virtual Status open(RuntimeState* state);

    // send data in 'batch' to this backend queue mgr
    // Blocks until all rows in batch are pushed to the queue
    virtual Status send(RuntimeState* state, RowBatch* batch);

    virtual Status close(RuntimeState* state, Status exec_status);

    virtual RuntimeProfile* profile() { return _profile; }

private:
    Status prepare_exprs(RuntimeState* state);

    // Owned by the RuntimeState.
    const RowDescriptor& _row_desc;
    std::shared_ptr<arrow::Schema> _arrow_schema;

    BlockQueueSharedPtr _queue;

    RuntimeProfile* _profile; // Allocated from _pool

    // Owned by the RuntimeState.
    const std::vector<TExpr>& _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;
};
} // namespace doris
