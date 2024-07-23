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

#include "io/fs/kafka_consumer_pipe.h"
#include "io/fs/multi_table_pipe.h"
#include "runtime/stream_load/stream_load_context.h"

namespace doris {
namespace io {

class MultiTablePipe;
using AppendFunc = Status (KafkaConsumerPipe::*)(const char* data, size_t size);
using KafkaConsumerPipePtr = std::shared_ptr<io::KafkaConsumerPipe>;

class MultiTablePipe : public KafkaConsumerPipe {
public:
    MultiTablePipe(std::shared_ptr<StreamLoadContext> ctx, size_t max_buffered_bytes = 1024 * 1024,
                   size_t min_chunk_size = 64 * 1024)
            : KafkaConsumerPipe(max_buffered_bytes, min_chunk_size), _ctx(ctx.get()) {}

    ~MultiTablePipe() override = default;

    Status append_with_line_delimiter(const char* data, size_t size) override;

    Status append_json(const char* data, size_t size) override;

    // for pipe consumers, i.e. scanners, to get underlying KafkaConsumerPipes
    KafkaConsumerPipePtr get_pipe_by_table(const std::string& table);

    // request and execute plans for unplanned pipes
    Status request_and_exec_plans();

    void handle_consume_finished() {
        _set_consume_finished();
        auto inflight_cnt = _inflight_cnt.fetch_sub(1);
        if (inflight_cnt == 1) {
            _handle_consumer_finished();
        }
    }

    bool is_consume_finished() { return _consume_finished.load(std::memory_order_acquire); }

    Status finish() override;

    void cancel(const std::string& reason) override;

private:
    // parse table name from data
    std::string parse_dst_table(const char* data, size_t size);

    // [thread-unsafe] dispatch data to corresponding KafkaConsumerPipe
    Status dispatch(const std::string& table, const char* data, size_t size, AppendFunc cb);

    template <typename ExecParam>
    Status exec_plans(ExecEnv* exec_env, std::vector<ExecParam> params);

    void _set_consume_finished() { _consume_finished.store(true, std::memory_order_release); }

    void _handle_consumer_finished();

private:
    std::unordered_map<std::string /*table*/, std::shared_ptr<StreamLoadContext>> _planned_tables;
    std::unordered_map<std::string /*table*/, std::shared_ptr<StreamLoadContext>> _unplanned_tables;
    std::atomic<uint64_t> _unplanned_row_cnt {0}; // trigger plan request when exceed threshold
    // inflight count, when it is zero, means consume and all plans is finished
    std::atomic<uint64_t> _inflight_cnt {1};
    std::atomic<bool> _consume_finished {false};
    // note: Use raw pointer here to avoid cycle reference with StreamLoadContext.
    // Life cycle of MultiTablePipe is under control of StreamLoadContext, which means StreamLoadContext is created
    // before NultiTablePipe and released after it. It is safe to use raw pointer here.
    StreamLoadContext* _ctx = nullptr;
    Status _status; // save the first error status of all executing plan fragment

    std::mutex _tablet_commit_infos_lock;
    std::vector<TTabletCommitInfo> _tablet_commit_infos; // collect from each plan fragment
    std::atomic<int64_t> _number_total_rows {0};
    std::atomic<int64_t> _number_loaded_rows {0};
    std::atomic<int64_t> _number_filtered_rows {0};
    std::atomic<int64_t> _number_unselected_rows {0};

    std::mutex _pipe_map_lock;
    std::unordered_map<TUniqueId /*instance id*/, std::shared_ptr<io::StreamLoadPipe>> _pipe_map;

    uint32_t _row_threshold = config::multi_table_batch_plan_threshold;
    uint32_t _wait_tables_threshold = config::multi_table_max_wait_tables;
};
} // namespace io
} // end namespace doris
