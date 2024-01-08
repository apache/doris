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
#include "runtime/descriptors.h"
#include "vec/core/block.h"
#include "vec/sink/async_writer_sink.h"
#include "vec/sink/vdata_stream_sender.h"
#include "vec/sink/vresult_sink.h"
#include "vec/sink/writer/vfile_result_writer.h"

namespace doris {
class BufferControlBlock;
class ObjectPool;
class QueryStatistics;
class RuntimeProfile;
class RuntimeState;
class TDataSink;
class TExpr;
class TPlanFragmentDestination;
class TResultFileSink;

namespace vectorized {
class VExprContext;

inline constexpr char VRESULT_FILE_SINK[] = "VResultFileSink";

class VResultFileSink : public AsyncWriterSink<VFileResultWriter, VRESULT_FILE_SINK> {
public:
    VResultFileSink(const RowDescriptor& row_desc, const std::vector<TExpr>& t_output_expr);

    VResultFileSink(RuntimeState* state, ObjectPool* pool, int sender_id,
                    const RowDescriptor& row_desc, const TResultFileSink& sink,
                    const std::vector<TPlanFragmentDestination>& destinations,
                    bool send_query_statistics_with_every_batch,
                    const std::vector<TExpr>& t_output_expr, DescriptorTbl& descs);

    Status init(const TDataSink& thrift_sink) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    Status close(RuntimeState* state, Status exec_status) override;

    void set_query_statistics(std::shared_ptr<QueryStatistics> statistics) override;

private:
    // set file options when sink type is FILE
    std::unique_ptr<ResultFileOptions> _file_opts;
    TStorageBackendType::type _storage_type;

    // Owned by the RuntimeState.
    RowDescriptor _output_row_descriptor;

    std::unique_ptr<Block> _output_block;
    std::shared_ptr<BufferControlBlock> _sender;
    std::unique_ptr<VDataStreamSender> _stream_sender;
    int _buf_size = 1024; // Allocated from _pool
    bool _is_top_sink = true;
    std::string _header;
    std::string _header_type;
};
} // namespace vectorized
} // namespace doris
