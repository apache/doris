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

#include <gen_cpp/Types_types.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/data_sink.h"
#include "runtime/descriptors.h"
#include "vec/core/block.h"
#include "vec/sink/vdata_stream_sender.h"
#include "vec/sink/vresult_sink.h"

namespace doris {
class BufferControlBlock;
class ObjectPool;
class RuntimeProfile;
class RuntimeState;
class TDataSink;
class TExpr;
class TPlanFragmentDestination;
class TResultFileSink;

namespace vectorized {
class VExprContext;

class VResultFileSink : public DataSink {
public:
    VResultFileSink(ObjectPool* pool, const RowDescriptor& row_desc, const TResultFileSink& sink,
                    int per_channel_buffer_size, const std::vector<TExpr>& t_output_expr);
    VResultFileSink(ObjectPool* pool, int sender_id, const RowDescriptor& row_desc,
                    const TResultFileSink& sink,
                    const std::vector<TPlanFragmentDestination>& destinations,
                    int per_channel_buffer_size, const std::vector<TExpr>& t_output_expr,
                    DescriptorTbl& descs);
    ~VResultFileSink() override = default;
    Status init(const TDataSink& thrift_sink) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    // send data in 'batch' to this backend stream mgr
    // Blocks until all rows in batch are placed in the buffer
    Status send(RuntimeState* state, Block* block, bool eos = false) override;
    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    Status close(RuntimeState* state, Status exec_status) override;
    RuntimeProfile* profile() override { return _profile; }

    const RowDescriptor& row_desc() const { return _row_desc; }

private:
    Status prepare_exprs(RuntimeState* state);
    // set file options when sink type is FILE
    std::unique_ptr<ResultFileOptions> _file_opts;
    TStorageBackendType::type _storage_type;

    // Owned by the RuntimeState.
    const std::vector<TExpr>& _t_output_expr;
    VExprContextSPtrs _output_vexpr_ctxs;
    RowDescriptor _output_row_descriptor;

    std::unique_ptr<Block> _output_block = nullptr;
    std::shared_ptr<BufferControlBlock> _sender;
    std::unique_ptr<VDataStreamSender> _stream_sender;
    std::shared_ptr<ResultWriter> _writer;
    int _buf_size = 1024; // Allocated from _pool
    bool _is_top_sink = true;
    std::string _header;
    std::string _header_type;

    RowDescriptor _row_desc;
    RuntimeProfile* _profile;
};
} // namespace vectorized
} // namespace doris
