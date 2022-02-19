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

#ifndef DORIS_BE_RUNTIME_RESULT_SINK_H
#define DORIS_BE_RUNTIME_RESULT_SINK_H

#include "common/status.h"
#include "exec/data_sink.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"

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
struct ResultFileOptions;

namespace vectorized {
class VExprContext;
}

class ResultSink : public DataSink {
public:
    ResultSink(const RowDescriptor& row_desc, const std::vector<TExpr>& select_exprs,
               const TResultSink& sink, int buffer_size);
    virtual ~ResultSink();
    virtual Status prepare(RuntimeState* state) override;
    virtual Status open(RuntimeState* state) override;
    // send data in 'batch' to this backend stream mgr
    // Blocks until all rows in batch are placed in the buffer
    virtual Status send(RuntimeState* state, RowBatch* batch) override;
    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    virtual Status close(RuntimeState* state, Status exec_status) override;
    virtual RuntimeProfile* profile() override { return _profile; }

    void set_query_statistics(std::shared_ptr<QueryStatistics> statistics) override;

private:
    Status prepare_exprs(RuntimeState* state);
    TResultSinkType::type _sink_type;
    // set file options when sink type is FILE
    std::unique_ptr<ResultFileOptions> _file_opts;

    // Owned by the RuntimeState.
    const RowDescriptor& _row_desc;

    // Owned by the RuntimeState.
    const std::vector<TExpr>& _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;

    std::shared_ptr<BufferControlBlock> _sender;
    std::shared_ptr<ResultWriter> _writer;
    RuntimeProfile* _profile; // Allocated from _pool
    int _buf_size;            // Allocated from _pool
};

} // namespace doris
#endif
