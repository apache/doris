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

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <stdint.h>

#include <memory>

#include "operator.h"
#include "runtime/result_block_buffer.h"
#include "vec/core/block.h"
#include "vec/sink/vmysql_result_writer.h"

namespace doris {

class TDataStreamSink;
class TPlanFragmentDestination;
class TDataSink;

namespace vectorized {
class Block;
}

namespace pipeline {

// Forward declaration
class BlackholeSinkOperatorX;

class BlackholeSinkLocalState final : public PipelineXSinkLocalState<FakeSharedState> {
    ENABLE_FACTORY_CREATOR(BlackholeSinkLocalState);

public:
    using Parent = BlackholeSinkOperatorX;
    using Base = PipelineXSinkLocalState<FakeSharedState>;
    BlackholeSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

    int64_t _rows_processed = 0;
    int64_t _bytes_processed = 0;

    // Cache metrics for WARM UP SELECT result reporting
    int64_t _scan_rows = 0;
    int64_t _scan_bytes = 0;
    int64_t _scan_bytes_from_local_storage = 0;
    int64_t _scan_bytes_from_remote_storage = 0;

    RuntimeProfile::Counter* _rows_processed_timer = nullptr;
    RuntimeProfile::Counter* _bytes_processed_timer = nullptr;

private:
    friend class BlackholeSinkOperatorX;

    // Result buffer for sending cache metrics to FE
    std::shared_ptr<vectorized::MySQLResultBlockBuffer> _sender = nullptr;
};

class BlackholeSinkOperatorX final : public DataSinkOperatorX<BlackholeSinkLocalState> {
public:
    using Base = DataSinkOperatorX<BlackholeSinkLocalState>;

    BlackholeSinkOperatorX(int operator_id, const int dest_id, const TDataStreamSink& sink,
                           const std::vector<TPlanFragmentDestination>& destinations);
    Status prepare(RuntimeState* state) override;

    Status init(const TDataSink& tsink) override;

    Status sink(RuntimeState* state, vectorized::Block* block, bool eos) override;

    Status close(RuntimeState* state) override;

private:
    friend class BlackholeSinkLocalState;

    /**
     * Process a data block by discarding it and collecting metrics.
     * This simulates a "/dev/null" sink - data goes in but nothing comes out.
     */
    Status _process_block(RuntimeState* state, vectorized::Block* block);

    void _collect_cache_metrics(RuntimeState* state, BlackholeSinkLocalState& local_state);

    Status _send_cache_metrics_batch(RuntimeState* state, BlackholeSinkLocalState& local_state);

    TDataStreamSink _t_data_stream_sink;
    std::vector<TPlanFragmentDestination> _destinations;
    std::shared_ptr<vectorized::MySQLResultBlockBuffer> _sender = nullptr;
};

} // namespace pipeline
} // namespace doris
