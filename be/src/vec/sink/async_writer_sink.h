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

#include <string>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "runtime/runtime_state.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/sink/writer/vjdbc_table_writer.h"
#include "vec/sink/writer/vmysql_table_writer.h"
#include "vec/sink/writer/vodbc_table_writer.h"

namespace doris {

class RowDescriptor;
class TExpr;
class RuntimeState;
class RuntimeProfile;
class ObjectPool;
class TDataSink;

namespace vectorized {
class Block;

template <typename Writer, const char* Name>
    requires(std::is_base_of_v<AsyncResultWriter, Writer>)
class AsyncWriterSink : public DataSink {
public:
    AsyncWriterSink(const RowDescriptor& row_desc, const std::vector<TExpr>& t_exprs)
            : DataSink(row_desc), _t_output_expr(t_exprs) {
        _name = Name;
    }

    Status init(const TDataSink& thrift_sink) override {
        RETURN_IF_ERROR(DataSink::init(thrift_sink));
        // From the thrift expressions create the real exprs.
        RETURN_IF_ERROR(VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
        _writer.reset(new Writer(thrift_sink, _output_vexpr_ctxs));
        return Status::OK();
    }

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(DataSink::prepare(state));
        // Prepare the exprs to run.
        RETURN_IF_ERROR(VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
        // create profile
        _profile = state->obj_pool()->add(new RuntimeProfile(_name));
        init_sink_common_profile();
        return Status::OK();
    }

    Status open(RuntimeState* state) override {
        // Prepare the exprs to run.
        RETURN_IF_ERROR(VExpr::open(_output_vexpr_ctxs, state));
        RETURN_IF_ERROR(_writer->open(state, _profile));
        return Status::OK();
    }

    // Non-pipeline engine will call this api to send data to sink destination
    Status send(RuntimeState* state, vectorized::Block* block, bool eos = false) override {
        SCOPED_TIMER(_exec_timer);
        COUNTER_UPDATE(_blocks_sent_counter, 1);
        COUNTER_UPDATE(_output_rows_counter, block->rows());
        Status st = _writer->write(*block);
        // Should also check !state->is_cancelled()???, do not know which scenario?
        if (st.ok() && eos) {
            // If this is the last block, then call finish to flush the buffer or commit transctions.
            st = _writer->finish(state);
        }
        return st;
    }

    // Pipeline engine will call this api to send data to destination. This is an async API.
    Status sink(RuntimeState* state, vectorized::Block* block, bool eos = false) override {
        return _writer->sink(block, eos);
    }

    bool can_write() override { return _writer->can_write(); }

    Status close(RuntimeState* state, Status exec_status) override {
        // if the init failed, the _writer may be nullptr. so here need check
        if (_writer) {
            RETURN_IF_ERROR(_writer->close(exec_status));
        }
        return DataSink::close(state, exec_status);
    }

    [[nodiscard]] bool is_pending_finish() const override { return _writer->is_pending_finish(); }

protected:
    const std::vector<TExpr>& _t_output_expr;
    VExprContextSPtrs _output_vexpr_ctxs;
    std::unique_ptr<Writer> _writer;
};

inline constexpr char VJDBC_TABLE_SINK_NAME[] = "VJdbcTableSink";
inline constexpr char VODBC_TABLE_SINK_NAME[] = "VOdbcTableSink";
inline constexpr char VMYSQL_TABLE_SINK_NAME[] = "VMysqlTableSink";

class VJdbcTableSink : public AsyncWriterSink<VJdbcTableWriter, VJDBC_TABLE_SINK_NAME> {
public:
    VJdbcTableSink(const RowDescriptor& row_desc, const std::vector<TExpr>& t_exprs)
            : AsyncWriterSink<VJdbcTableWriter, VJDBC_TABLE_SINK_NAME>(row_desc, t_exprs) {};
};

class VOdbcTableSink : public AsyncWriterSink<VOdbcTableWriter, VODBC_TABLE_SINK_NAME> {
public:
    VOdbcTableSink(const RowDescriptor& row_desc, const std::vector<TExpr>& t_exprs)
            : AsyncWriterSink<VOdbcTableWriter, VODBC_TABLE_SINK_NAME>(row_desc, t_exprs) {};
};

class VMysqlTableSink : public AsyncWriterSink<VMysqlTableWriter, VMYSQL_TABLE_SINK_NAME> {
public:
    VMysqlTableSink(const RowDescriptor& row_desc, const std::vector<TExpr>& t_exprs)
            : AsyncWriterSink<VMysqlTableWriter, VMYSQL_TABLE_SINK_NAME>(row_desc, t_exprs) {};
};
} // namespace vectorized
} // namespace doris
