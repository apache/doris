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

#include <brpc/controller.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <parallel_hashmap/phmap.h>

#include <atomic>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <stack>
#include <string>

#include "common/global_types.h"
#include "common/status.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "util/brpc_closure.h"

namespace doris {
#include "common/compile_check_begin.h"
class PTransmitDataParams;
class TUniqueId;

using InstanceLoId = int64_t;

namespace pipeline {
class Dependency;
class ExchangeSinkLocalState;
} // namespace pipeline

namespace vectorized {
class Channel;

// We use BroadcastPBlockHolder to hold a broadcasted PBlock. For broadcast shuffle, one PBlock
// will be shared between different channel, so we have to use a ref count to mark if this
// PBlock is available for next serialization.
class BroadcastPBlockHolderMemLimiter;
class BroadcastPBlockHolder {
    ENABLE_FACTORY_CREATOR(BroadcastPBlockHolder);

public:
    BroadcastPBlockHolder() { _pblock = std::make_unique<PBlock>(); }
    ~BroadcastPBlockHolder();

    PBlock* get_block() { return _pblock.get(); }

    void reset_block() { _pblock->Clear(); }

private:
    friend class BroadcastPBlockHolderMemLimiter;
    std::unique_ptr<PBlock> _pblock;
    std::weak_ptr<BroadcastPBlockHolderMemLimiter> _parent_creator;
    void set_parent_creator(std::shared_ptr<BroadcastPBlockHolderMemLimiter> parent_creator) {
        _parent_creator = parent_creator;
    }
};

class BroadcastPBlockHolderMemLimiter
        : public std::enable_shared_from_this<BroadcastPBlockHolderMemLimiter> {
    ENABLE_FACTORY_CREATOR(BroadcastPBlockHolderMemLimiter);

public:
    BroadcastPBlockHolderMemLimiter() = delete;

    BroadcastPBlockHolderMemLimiter(std::shared_ptr<pipeline::Dependency>& broadcast_dependency)
            : _total_queue_buffer_size_limit(config::exchg_node_buffer_size_bytes),
              _total_queue_blocks_count_limit(config::num_broadcast_buffer) {
        _broadcast_dependency = broadcast_dependency;
    }

    void set_low_memory_mode() {
        _total_queue_buffer_size_limit = 1024 * 1024;
        _total_queue_blocks_count_limit = 8;
    }

    void acquire(BroadcastPBlockHolder& holder);
    void release(const BroadcastPBlockHolder& holder);

private:
    std::atomic_int64_t _total_queue_buffer_size_limit {0};
    std::atomic_int64_t _total_queue_blocks_count_limit {0};
    std::atomic_int64_t _total_queue_buffer_size {0};
    std::atomic_int64_t _total_queue_blocks_count {0};
    std::shared_ptr<pipeline::Dependency> _broadcast_dependency;
    std::mutex _holders_lock;
};

} // namespace vectorized

namespace pipeline {
struct TransmitInfo {
    std::unique_ptr<PBlock> block;
    bool eos;
};

struct BroadcastTransmitInfo {
    std::shared_ptr<vectorized::BroadcastPBlockHolder> block_holder = nullptr;
    bool eos;
};

struct RpcInstanceStatistics {
    int64_t rpc_count = 0;
    int64_t max_time = 0;
    int64_t min_time = INT64_MAX;
    int64_t sum_time = 0;
};

// Consolidated structure for RPC instance data
struct RpcInstance {
    // Constructor initializes the instance with the given ID
    RpcInstance(InstanceLoId id) : id(id) {}

    // Unique identifier for this RPC instance
    InstanceLoId id;

    // Mutex for thread-safe access to this instance's data
    std::unique_ptr<std::mutex> mutex;

    // Sequence number for RPC packets, incremented for each packet sent
    int64_t seq = 0;

    // Queue for regular data transmission requests
    std::unordered_map<vectorized::Channel*, std::queue<TransmitInfo, std::list<TransmitInfo>>>
            package_queue;

    // Queue for broadcast data transmission requests
    std::unordered_map<vectorized::Channel*,
                       std::queue<BroadcastTransmitInfo, std::list<BroadcastTransmitInfo>>>
            broadcast_package_queue;

    // RPC request parameters for data transmission
    std::shared_ptr<PTransmitDataParams> request;

    // Flag indicating if the RPC channel is currently idle (no active RPC)
    bool rpc_channel_is_idle = true;

    // Flag indicating if the RPC channel has been turned off (no more RPCs will be sent)
    bool rpc_channel_is_turn_off = false;

    // Statistics for monitoring RPC performance (latency, counts, etc.)
    RpcInstanceStatistics stats;

    // Count of active exchange sinks using this RPC instance
    int64_t running_sink_count = 0;
};

template <typename Response>
class ExchangeSendCallback : public ::doris::DummyBrpcCallback<Response> {
    ENABLE_FACTORY_CREATOR(ExchangeSendCallback);

public:
    ExchangeSendCallback() = default;

    void init(pipeline::RpcInstance* ins, bool eos) {
        _ins = ins;
        _eos = eos;
    }

    ~ExchangeSendCallback() override = default;
    ExchangeSendCallback(const ExchangeSendCallback& other) = delete;
    ExchangeSendCallback& operator=(const ExchangeSendCallback& other) = delete;
    void addFailedHandler(const std::function<void(RpcInstance*, const std::string&)>& fail_fn) {
        _fail_fn = fail_fn;
    }
    void addSuccessHandler(const std::function<void(RpcInstance*, const bool&, const Response&,
                                                    const int64_t&)>& suc_fn) {
        _suc_fn = suc_fn;
    }

    void call() noexcept override {
        try {
            if (::doris::DummyBrpcCallback<Response>::cntl_->Failed()) {
                std::string err = fmt::format(
                        "failed to send brpc when exchange, error={}, error_text={}, client: {}, "
                        "latency = {}",
                        berror(::doris::DummyBrpcCallback<Response>::cntl_->ErrorCode()),
                        ::doris::DummyBrpcCallback<Response>::cntl_->ErrorText(),
                        BackendOptions::get_localhost(),
                        ::doris::DummyBrpcCallback<Response>::cntl_->latency_us());
                _fail_fn(_ins, err);
            } else {
                _suc_fn(_ins, _eos, *(::doris::DummyBrpcCallback<Response>::response_),
                        start_rpc_time);
            }
        } catch (const std::exception& exp) {
            LOG(FATAL) << "brpc callback error: " << exp.what();
        } catch (...) {
            LOG(FATAL) << "brpc callback error.";
            __builtin_unreachable();
        }
    }
    int64_t start_rpc_time;

private:
    std::function<void(RpcInstance*, const std::string&)> _fail_fn;
    std::function<void(RpcInstance*, const bool&, const Response&, const int64_t&)> _suc_fn;
    RpcInstance* _ins;
    bool _eos;
};

// ExchangeSinkBuffer can either be shared among multiple ExchangeSinkLocalState instances
// or be individually owned by each ExchangeSinkLocalState.
// The following describes the scenario where ExchangeSinkBuffer is shared among multiple ExchangeSinkLocalState instances.
// Of course, individual ownership can be seen as a special case where only one ExchangeSinkLocalState shares the buffer.

// A sink buffer contains multiple rpc_channels.
// Each rpc_channel corresponds to a target instance on the receiving side.
// Data is sent using a ping-pong mode within each rpc_channel,
// meaning that at most one RPC can exist in a single rpc_channel at a time.
// The next RPC can only be sent after the previous one has completed.
//
// Each exchange sink sends data to all target instances on the receiving side.
// If the concurrency is 3, a single rpc_channel will be used simultaneously by three exchange sinks.

/*                                                                                                                                                                                                                                                                                                                          
                          +-----------+          +-----------+        +-----------+      
                          |dest ins id|          |dest ins id|        |dest ins id|      
                          |           |          |           |        |           |      
                          +----+------+          +-----+-----+        +------+----+      
                               |                       |                     |           
                               |                       |                     |           
                      +----------------+      +----------------+     +----------------+  
                      |                |      |                |     |                |  
 sink buffer -------- |   rpc_channel  |      |  rpc_channel   |     |  rpc_channel   |  
                      |                |      |                |     |                |  
                      +-------+--------+      +----------------+     +----------------+  
                              |                        |                      |          
                              |------------------------+----------------------+          
                              |                        |                      |          
                              |                        |                      |          
                     +-----------------+       +-------+---------+    +-------+---------+
                     |                 |       |                 |    |                 |
                     |  exchange sink  |       |  exchange sink  |    |  exchange sink  |
                     |                 |       |                 |    |                 |
                     +-----------------+       +-----------------+    +-----------------+
*/

#if defined(BE_TEST) && !defined(BE_BENCHMARK)
void transmit_blockv2(PBackendService_Stub* stub,
                      std::unique_ptr<AutoReleaseClosure<PTransmitDataParams,
                                                         ExchangeSendCallback<PTransmitDataResult>>>
                              closure);
#endif
class ExchangeSinkBuffer : public HasTaskExecutionCtx {
public:
    ExchangeSinkBuffer(PUniqueId query_id, PlanNodeId dest_node_id, PlanNodeId node_id,
                       RuntimeState* state, const std::vector<InstanceLoId>& sender_ins_ids);
#ifdef BE_TEST
    ExchangeSinkBuffer(RuntimeState* state, int64_t sinknum)
            : HasTaskExecutionCtx(state), _state(state), _exchange_sink_num(sinknum) {};
#endif

    ~ExchangeSinkBuffer() override = default;

    void construct_request(TUniqueId);

    Status add_block(vectorized::Channel* channel, TransmitInfo&& request);
    Status add_block(vectorized::Channel* channel, BroadcastTransmitInfo&& request);
    void close();
    void update_rpc_time(RpcInstance& ins, int64_t start_rpc_time, int64_t receive_rpc_time);
    void update_profile(RuntimeProfile* profile);

    void set_dependency(InstanceLoId sender_ins_id, std::shared_ptr<Dependency> queue_dependency,
                        ExchangeSinkLocalState* local_state) {
        std::lock_guard l(_m);
        _queue_deps.push_back(queue_dependency);
        _parents.push_back(local_state);
    }

    void set_low_memory_mode() { _queue_capacity = 8; }
    std::string debug_each_instance_queue_size();
#ifdef BE_TEST
public:
#else
private:
#endif
    friend class ExchangeSinkLocalState;

    // Single map to store all RPC instance data
    phmap::flat_hash_map<InstanceLoId, std::unique_ptr<RpcInstance>> _rpc_instances;
    std::atomic<size_t> _queue_capacity;

    // It is set to true only when an RPC fails. Currently, we do not have an error retry mechanism.
    // If an RPC error occurs, the query will be canceled.
    std::atomic<bool> _is_failed;
    PUniqueId _query_id;
    PlanNodeId _dest_node_id;

    PlanNodeId _node_id;
    std::atomic<int64_t> _rpc_count = 0;
    // The state may be from PipelineFragmentContext if it is shared among multi instances.
    RuntimeState* _state = nullptr;
    QueryContext* _context = nullptr;

    Status _send_rpc(RpcInstance& ins);

#ifndef BE_TEST
    inline void _ended(RpcInstance& ins);
    inline void _failed(InstanceLoId id, const std::string& err);
    inline void _set_receiver_eof(RpcInstance& ins);
    inline void _turn_off_channel(RpcInstance& ins, std::unique_lock<std::mutex>& with_lock);

#else
    virtual void _ended(RpcInstance& ins);
    virtual void _failed(InstanceLoId id, const std::string& err);
    virtual void _set_receiver_eof(RpcInstance& ins);
    virtual void _turn_off_channel(RpcInstance& ins, std::unique_lock<std::mutex>& with_lock);
#endif

    void get_max_min_rpc_time(int64_t* max_time, int64_t* min_time);
    int64_t get_sum_rpc_time();

    // _total_queue_size is the sum of the sizes of all instance_to_package_queues.
    // Any modification to instance_to_package_queue requires a corresponding modification to _total_queue_size.
    std::atomic<int> _total_queue_size = 0;

    // protected the `_queue_deps` and `_parents`
    std::mutex _m;
    // _queue_deps is used for memory control.
    std::vector<std::shared_ptr<Dependency>> _queue_deps;
    // The ExchangeSinkLocalState in _parents is only used in _turn_off_channel.
    std::vector<ExchangeSinkLocalState*> _parents;
    const int64_t _exchange_sink_num;
    bool _send_multi_blocks = false;
    int _send_multi_blocks_byte_size = 256 * 1024;
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris
