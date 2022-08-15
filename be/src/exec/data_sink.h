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

#include <vector>

#include "common/status.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/descriptors.h"
#include "runtime/query_statistics.h"
#include "util/telemetry/telemetry.h"

namespace doris {

class ObjectPool;
class RowBatch;
class RuntimeProfile;
class RuntimeState;
class TPlanFragmentExecParams;
class RowDescriptor;

namespace vectorized {
class Block;
}

// Superclass of all data sinks.
class DataSink {
public:
    DataSink() : _closed(false) {}
    virtual ~DataSink() {}

    virtual Status init(const TDataSink& thrift_sink);

    // Setup. Call before send(), Open(), or Close().
    // Subclasses must call DataSink::Prepare().
    virtual Status prepare(RuntimeState* state);

    // Setup. Call before send() or close().
    virtual Status open(RuntimeState* state) = 0;

    // Send a row batch into this sink.
    // eos should be true when the last batch is passed to send()
    virtual Status send(RuntimeState* state, RowBatch* batch) = 0;

    // Send a Block into this sink.
    virtual Status send(RuntimeState* state, vectorized::Block* block) {
        return Status::NotSupported("Not support send block");
    };
    // Releases all resources that were allocated in prepare()/send().
    // Further send() calls are illegal after calling close().
    // It must be okay to call this multiple times. Subsequent calls should
    // be ignored.
    virtual Status close(RuntimeState* state, Status exec_status) {
        profile()->add_to_span();
        _closed = true;
        return Status::OK();
    }

    // Creates a new data sink from thrift_sink. A pointer to the
    // new sink is written to *sink, and is owned by the caller.
    static Status create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
                                   const std::vector<TExpr>& output_exprs,
                                   const TPlanFragmentExecParams& params,
                                   const RowDescriptor& row_desc, bool is_vec,
                                   std::unique_ptr<DataSink>* sink, DescriptorTbl& desc_tbl);

    // Returns the runtime profile for the sink.
    virtual RuntimeProfile* profile() = 0;

    virtual void set_query_statistics(std::shared_ptr<QueryStatistics> statistics) {
        _query_statistics = statistics;
    }

    void end_send_span() {
        if (_send_span) {
            _send_span->End();
        }
    }

protected:
    // Set to true after close() has been called. subclasses should check and set this in
    // close().
    bool _closed;
    std::string _name;

    // Maybe this will be transferred to BufferControlBlock.
    std::shared_ptr<QueryStatistics> _query_statistics;

    OpentelemetrySpan _send_span {};
};

} // namespace doris
