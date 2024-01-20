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

#include <stddef.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"

namespace doris {

class ObjectPool;
class RuntimeState;
class TPlanFragmentExecParams;
class DescriptorTbl;
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

    [[nodiscard]] virtual bool is_pending_finish() const { return false; }

    // Releases all resources that were allocated in prepare()/send().
    // Further send() calls are illegal after calling close().
    // It must be okay to call this multiple times. Subsequent calls should
    // be ignored.
    virtual Status close(RuntimeState* state, Status exec_status) {
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

    const RowDescriptor& row_desc() { return _row_desc; }

    virtual bool can_write() { return true; }

private:
    static bool _has_inverted_index_or_partial_update(TOlapTableSink sink);

protected:
    // Set to true after close() has been called. subclasses should check and set this in
    // close().
    bool _closed = false;
    std::string _name;
    const RowDescriptor& _row_desc;

    RuntimeProfile* _profile = nullptr; // Allocated from _pool

    RuntimeProfile::Counter* _exec_timer = nullptr;
    RuntimeProfile::Counter* _blocks_sent_counter = nullptr;
    RuntimeProfile::Counter* _output_rows_counter = nullptr;

    void init_sink_common_profile() {
        _exec_timer = ADD_TIMER_WITH_LEVEL(_profile, "ExecTime", 1);
        _output_rows_counter = ADD_COUNTER_WITH_LEVEL(_profile, "RowsProduced", TUnit::UNIT, 1);
        _blocks_sent_counter = ADD_COUNTER_WITH_LEVEL(_profile, "BlocksProduced", TUnit::UNIT, 1);
    }
};

} // namespace doris
