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

#include <arrow/type.h>
#include <cctz/time_zone.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/status.h"
#include "runtime/runtime_state.h"

namespace google::protobuf {
class Closure;
} // namespace google::protobuf

namespace brpc {
class Controller;
}

namespace doris {

class Dependency;

class GetArrowResultBatchCtx;
class Block;

class PFetchDataResult;

class ResultBlockBufferBase {
public:
    ResultBlockBufferBase() = default;
    virtual ~ResultBlockBufferBase() = default;

    // Close one fragment instance's contribution to this buffer.  When the last
    // registered instance calls close(), |is_fully_closed| is set to true,
    // indicating that no more producers will write to this buffer and callers may
    // safely schedule deferred cleanup.  The buffer is keyed in ResultBufferMgr
    // under buffer_id(); use that id (not the per-instance fragment_instance_id)
    // when scheduling cancel_at_time() for the deferred cleanup.
    virtual Status close(const TUniqueId& id, Status exec_status, int64_t num_rows,
                         bool& is_fully_closed) = 0;
    virtual void cancel(const Status& reason) = 0;

    // The id under which this buffer was registered in ResultBufferMgr.
    // In parallel result-sink mode this equals query_id; in non-parallel mode
    // it equals fragment_instance_id.
    [[nodiscard]] virtual const TUniqueId& buffer_id() const = 0;

    [[nodiscard]] virtual std::shared_ptr<MemTrackerLimiter> mem_tracker() = 0;
    virtual void set_dependency(const TUniqueId& id,
                                std::shared_ptr<Dependency> result_sink_dependency) = 0;
};

// This is used to serialize a result block by normal queries / arrow flight queries / point queries.
template <typename ResultCtxType>
class ResultBlockBuffer : public ResultBlockBufferBase {
public:
    using InBlockType = typename ResultCtxType::ResultType;
    ResultBlockBuffer(TUniqueId id, RuntimeState* state, int buffer_size);
    ~ResultBlockBuffer() override = default;

    Status add_batch(RuntimeState* state, std::shared_ptr<InBlockType>& result);
    Status get_batch(std::shared_ptr<ResultCtxType> ctx);
    Status close(const TUniqueId& id, Status exec_status, int64_t num_rows,
                 bool& is_fully_closed) override;
    void cancel(const Status& reason) override;

    [[nodiscard]] const TUniqueId& buffer_id() const override { return _fragment_id; }
    [[nodiscard]] std::shared_ptr<MemTrackerLimiter> mem_tracker() override { return _mem_tracker; }
    void set_dependency(const TUniqueId& id,
                        std::shared_ptr<Dependency> result_sink_dependency) override;

protected:
    friend class GetArrowResultBatchCtx;
    ResultBlockBuffer(RuntimeState* state)
            : ResultBlockBuffer<ResultCtxType>(TUniqueId(), state, 0) {}
    void _update_dependency();

    using ResultQueue = std::list<std::shared_ptr<InBlockType>>;

    // result's query id
    TUniqueId _fragment_id;
    bool _is_close;
    Status _status;
    // Producer. blocking queue for result batch waiting to sent to FE by _waiting_rpc.
    ResultQueue _result_batch_queue;
    // protects all subsequent data in this block
    std::mutex _lock;

    // The last batch size in bytes.
    // Determine whether to merge multiple batches based on the size of each batch to avoid getting an excessively large batch after merging.
    size_t _last_batch_bytes = 0;

    // get arrow flight result is a sync method, need wait for data ready and return result.
    // TODO, waiting for data will block pipeline, so use a request pool to save requests waiting for data.
    std::condition_variable _arrow_data_arrival;
    // Consumer. RPCs which FE waiting for result. when _fe_result_batch_queue filled, the rpc could be sent.
    std::deque<std::shared_ptr<ResultCtxType>> _waiting_rpc;

    std::atomic<int64_t> _returned_rows = 0;
    // instance id to dependency
    std::unordered_map<TUniqueId, std::shared_ptr<Dependency>> _result_sink_dependencies;
    std::unordered_map<TUniqueId, size_t> _instance_rows;
    std::list<std::unordered_map<TUniqueId, size_t>> _instance_rows_in_queue;
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
    int _packet_num = 0;
    const int _batch_size;
    const std::string _timezone;
    const int _be_exec_version;
    const segment_v2::CompressionTypePB _fragment_transmission_compression_type;
    const int _buffer_limit;
};

} // namespace doris
