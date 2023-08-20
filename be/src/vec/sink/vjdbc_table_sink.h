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
#include <memory>
#include <vector>

#include "common/status.h"
#include "vec/sink/vtable_sink.h"
#include "vec/sink/writer/vjdbc_table_writer.h"

namespace doris {
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class TDataSink;
class TExpr;

namespace vectorized {
class Block;

// This class is a sinker, which put input data to jdbc table
class VJdbcTableSink : public VTableSink {
public:
    VJdbcTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                   const std::vector<TExpr>& t_exprs);

    Status init(const TDataSink& thrift_sink) override;

    Status open(RuntimeState* state) override;

    Status send(RuntimeState* state, vectorized::Block* block, bool eos = false) override;

    Status sink(RuntimeState* state, vectorized::Block* block, bool eos = false) override {
        return _writer->sink(block, eos);
    }

    Status close(RuntimeState* state, Status exec_status) override;

    Status try_close(RuntimeState* state, Status exec_status) override;

    bool is_close_done() override { return !_writer->is_pending_finish(); }

    bool can_write() override { return _writer->can_write(); }

private:
    JdbcConnectorParam _jdbc_param;
    std::unique_ptr<VJdbcTableWriter> _writer;
};
} // namespace vectorized
} // namespace doris
