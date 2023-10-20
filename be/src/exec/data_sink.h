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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/data-sink.h
// and modified by Doris

#pragma once

#include <opentelemetry/trace/span.h>
#include <stddef.h>
// IWYU pragma: no_include <opentelemetry/nostd/shared_ptr.h>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"
#include "util/telemetry/telemetry.h"

namespace doris {

class ObjectPool;
class RuntimeState;
class TPlanFragmentExecParams;
class DescriptorTbl;
class QueryStatistics;
class TDataSink;
class TExpr;
class TPipelineFragmentParams;
class TOlapTableSink;

namespace vectorized {
class Block;
}

// Superclass of all data sinks.
class DataSink {
public:
    DataSink(const RowDescriptor& desc) : _row_desc(desc) {}
    virtual ~DataSink() {}

    virtual Status init(const TDataSink& thrift_sink);

    // Setup. Call before send(), Open(), or Close().
    // Subclasses must call DataSink::Prepare().
    virtual Status prepare(RuntimeState* state);

    // Setup. Call before send() or close().
    virtual Status open(RuntimeState* state) = 0;

    // Send a Block into this sink.
    virtual Status send(RuntimeState* state, vectorized::Block* block, bool eos = false) {
        return Status::NotSupported("Not support send block");
    }

    // Send a Block into this sink, not blocked thredd API only use in pipeline exec engine
    virtual Status sink(RuntimeState* state, vectorized::Block* block, bool eos = false) {
        return send(state, block, eos);
    }

    [[nodiscard]] virtual Status try_close(RuntimeState* state, Status exec_status) {
        return Status::OK();
    }

    virtual bool is_close_done() { return true; }

    // Releases all resources that were allocated in prepare()/send().
    // Further send() calls are illegal after calling close().
    // It must be okay to call this multiple times. Subsequent calls should
    // be ignored.
    virtual Status close(RuntimeState* state, Status exec_status) {
        profile()->add_to_span(_span);
        _closed = true;
        return Status::OK();
    }

    // Creates a new data sink from thrift_sink. A pointer to the
    // new sink is written to *sink, and is owned by the caller.
    static Status create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
                                   const std::vector<TExpr>& output_exprs,
                                   const TPlanFragmentExecParams& params,
                                   const RowDescriptor& row_desc, RuntimeState* state,
                                   std::unique_ptr<DataSink>* sink, DescriptorTbl& desc_tbl);

    static Status create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
                                   const std::vector<TExpr>& output_exprs,
                                   const TPipelineFragmentParams& params,
                                   const size_t& local_param_idx, const RowDescriptor& row_desc,
                                   RuntimeState* state, std::unique_ptr<DataSink>* sink,
                                   DescriptorTbl& desc_tbl);

    // Returns the runtime profile for the sink.
    RuntimeProfile* profile() { return _profile; }

    virtual void set_query_statistics(std::shared_ptr<QueryStatistics> statistics) {
        _query_statistics = statistics;
    }

    const RowDescriptor& row_desc() { return _row_desc; }

    virtual bool can_write() { return true; }

private:
    static bool _has_inverted_index(TOlapTableSink sink);

protected:
    // Set to true after close() has been called. subclasses should check and set this in
    // close().
    bool _closed = false;
    std::string _name;
    const RowDescriptor& _row_desc;

    RuntimeProfile* _profile = nullptr; // Allocated from _pool

    // Maybe this will be transferred to BufferControlBlock.
    std::shared_ptr<QueryStatistics> _query_statistics;

    OpentelemetrySpan _span {};
};

} // namespace doris
