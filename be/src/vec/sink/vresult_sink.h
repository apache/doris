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
#include "exec/data_sink.h"
#include "vec/sink/vresult_writer.h"

namespace doris {
class ObjectPool;
class RowBatch;
class RuntimeState;
class RuntimeProfile;
class BufferControlBlock;
class ExprContext;
class ResultWriter;
class MemTracker;
struct ResultFileOptions;
namespace pipeline {
class ResultSinkOperator;
}
namespace vectorized {
class VExprContext;

class VResultSink : public DataSink {
public:
    friend class pipeline::ResultSinkOperator;
    VResultSink(const RowDescriptor& row_desc, const std::vector<TExpr>& select_exprs,
                const TResultSink& sink, int buffer_size);

    virtual ~VResultSink();

    virtual Status prepare(RuntimeState* state) override;
    virtual Status open(RuntimeState* state) override;

    // not implement
    virtual Status send(RuntimeState* state, RowBatch* batch) override;
    virtual Status send(RuntimeState* state, Block* block, bool eos = false) override;
    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    virtual Status close(RuntimeState* state, Status exec_status) override;
    virtual RuntimeProfile* profile() override { return _profile; }

    void set_query_statistics(std::shared_ptr<QueryStatistics> statistics) override;

    const RowDescriptor& row_desc() { return _row_desc; }

private:
    Status prepare_exprs(RuntimeState* state);
    TResultSinkType::type _sink_type;
    // set file options when sink type is FILE
    std::unique_ptr<ResultFileOptions> _file_opts;

    // Owned by the RuntimeState.
    const RowDescriptor& _row_desc;

    // Owned by the RuntimeState.
    const std::vector<TExpr>& _t_output_expr;
    std::vector<vectorized::VExprContext*> _output_vexpr_ctxs;

    std::shared_ptr<BufferControlBlock> _sender;
    std::shared_ptr<VResultWriter> _writer;
    RuntimeProfile* _profile; // Allocated from _pool
    int _buf_size;            // Allocated from _pool
};
} // namespace vectorized

} // namespace doris
