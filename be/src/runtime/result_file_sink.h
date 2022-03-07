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

#ifndef DORIS_BE_RUNTIME_RESULT_FILE_SINK_H
#define DORIS_BE_RUNTIME_RESULT_FILE_SINK_H

#include "common/status.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/data_stream_sender.h"
#include "runtime/descriptors.h"

namespace doris {

class RowBatch;
class ObjectPool;
class RuntimeState;
class RuntimeProfile;
class BufferControlBlock;
class ExprContext;
class ResultWriter;
class MemTracker;
struct ResultFileOptions;

class ResultFileSink : public DataStreamSender {
public:
    ResultFileSink(const RowDescriptor& row_desc, const std::vector<TExpr>& select_exprs,
                   const TResultFileSink& sink);
    ResultFileSink(const RowDescriptor& row_desc, const std::vector<TExpr>& select_exprs,
                   const TResultFileSink& sink,
                   const std::vector<TPlanFragmentDestination>& destinations, ObjectPool* pool,
                   int sender_id, DescriptorTbl& descs);
    virtual ~ResultFileSink();
    virtual Status init(const TDataSink& thrift_sink) override;
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
    // set file options when sink type is FILE
    std::unique_ptr<ResultFileOptions> _file_opts;
    TStorageBackendType::type _storage_type;

    // Owned by the RuntimeState.
    const std::vector<TExpr>& _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;
    RowDescriptor _output_row_descriptor;

    std::shared_ptr<BufferControlBlock> _sender;
    std::shared_ptr<ResultWriter> _writer;
    RowBatch* _output_batch = nullptr;
    int _buf_size = 1024; // Allocated from _pool
    bool _is_top_sink = true;
};

} // namespace doris
#endif
