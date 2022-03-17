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
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/data_stream_sender.h"
#include "runtime/descriptors.h"
#include "vec/core/block.h"
#include "vec/sink/result_writer.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris {

class RowBatch;
class ObjectPool;
class RuntimeState;
class RuntimeProfile;
class BufferControlBlock;
class MemTracker;
class ResultFileOptions;

namespace vectorized {
class VExprContext;
class Block;
class VResultFileSink : public VDataStreamSender {
    
public:
    VResultFileSink(const RowDescriptor& row_desc, const std::vector<TExpr>& select_exprs,
                    const TResultFileSink& sink);

    VResultFileSink(const RowDescriptor& row_desc, const std::vector<TExpr>& select_exprs,
                    const TResultFileSink& sink,
                    const std::vector<TPlanFragmentDestination>& destinations, ObjectPool* pool,
                    int sender_id, DescriptorTbl& descs);
    ~VResultFileSink() = default;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    // send data in 'batch' to this backend stream mgr
    // Blocks until all rows in batch are placed in the buffer
    Status send(RuntimeState* state, RowBatch* batch) override;
    Status send(RuntimeState* state, Block* block) override;
    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    Status close(RuntimeState* state, Status exec_status) override;

    RuntimeProfile* profile() override { return _profile; }

    void set_query_statistics(std::shared_ptr<QueryStatistics> statistics) override;

private:
    Status prepare_exprs(RuntimeState* state);
    // set file options when sink type is FILE
    std::unique_ptr<ResultFileOptions> _file_opts;
    TStorageBackendType::type _storage_type;

    // Owned by the RuntimeState.
    const std::vector<TExpr>& _t_output_expr;
    std::vector<VExprContext*> _output_vexpr_ctxs;
    RowDescriptor _output_row_descriptor;

    std::shared_ptr<BufferControlBlock> _sender;
    std::shared_ptr<VResultWriter> _writer;
    std::shared_ptr<MutableBlock> _mutable_block;
    Block* _output_block;

    int _buf_size = 1024; // Allocated from _pool
    bool _is_top_sink = true;
};

} // namespace vectorized

} // namespace doris