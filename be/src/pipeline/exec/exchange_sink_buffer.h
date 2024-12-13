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
#include "runtime/query_statistics.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "util/ref_count_closure.h"

namespace doris {
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

    BroadcastPBlockHolderMemLimiter(std::shared_ptr<pipeline::Dependency>& broadcast_dependency) {
        _broadcast_dependency = broadcast_dependency;
    }

    void acquire(BroadcastPBlockHolder& holder);
    void release(const BroadcastPBlockHolder& holder);

private:
    std::atomic_int64_t _total_queue_buffer_size {0};
    std::atomic_int64_t _total_queue_blocks_count {0};
    std::shared_ptr<pipeline::Dependency> _broadcast_dependency;
    std::mutex _holders_lock;
};

} // namespace vectorized

namespace pipeline {
struct TransmitInfo {
    vectorized::Channel* channel = nullptr;
    std::unique_ptr<PBlock> block;
    bool eos;
    Status exec_status;
};

struct BroadcastTransmitInfo {
    vectorized::Channel* channel = nullptr;
    std::shared_ptr<vectorized::BroadcastPBlockHolder> block_holder = nullptr;
    bool eos;
};

template <typename Response>
class ExchangeSendCallback : public ::doris::DummyBrpcCallback<Response> {
    ENABLE_FACTORY_CREATOR(ExchangeSendCallback);

public:
    ExchangeSendCallback() = default;

    void init(InstanceLoId id, bool eos) {
        _id = id;
        _eos = eos;
    }

    ~ExchangeSendCallback() override = default;
    ExchangeSendCallback(const ExchangeSendCallback& other) = delete;
    ExchangeSendCallback& operator=(const ExchangeSendCallback& other) = delete;
    void addFailedHandler(
            const std::function<void(const InstanceLoId&, const std::string&)>& fail_fn) {
        _fail_fn = fail_fn;
    }
    void addSuccessHandler(const std::function<void(const InstanceLoId&, const bool&,
                                                    const Response&, const int64_t&)>& suc_fn) {
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
                _fail_fn(_id, err);
            } else {
                _suc_fn(_id, _eos, *(::doris::DummyBrpcCallback<Response>::response_),
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
    std::function<void(const InstanceLoId&, const std::string&)> _fail_fn;
    std::function<void(const InstanceLoId&, const bool&, const Response&, const int64_t&)> _suc_fn;
    InstanceLoId _id;
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

#ifdef BE_TEST
void transmit_blockv2(PBackendService_Stub& stub,
                      std::unique_ptr<AutoReleaseClosure<PTransmitDataParams,
                                                         ExchangeSendCallback<PTransmitDataResult>>>
                              closure);
#endif
class ExchangeSinkBuffer : public HasTaskExecutionCtx {
public:
    ExchangeSinkBuffer(PUniqueId query_id, PlanNodeId dest_node_id, RuntimeState* state,
                       const std::vector<InstanceLoId>& sender_ins_ids);

#ifdef BE_TEST
    ExchangeSinkBuffer(RuntimeState* state, int64_t sinknum)
            : HasTaskExecutionCtx(state), _exchange_sink_num(sinknum) {};
#endif
    ~ExchangeSinkBuffer() override = default;

    void construct_request(TUniqueId);

    Status add_block(TransmitInfo&& request);
    Status add_block(BroadcastTransmitInfo&& request);
    void close();
    void update_rpc_time(InstanceLoId id, int64_t start_rpc_time, int64_t receive_rpc_time);
    void update_profile(RuntimeProfile* profile);

    void set_dependency(InstanceLoId sender_ins_id, std::shared_ptr<Dependency> queue_dependency,
                        ExchangeSinkLocalState* local_state) {
        DCHECK(_queue_deps.contains(sender_ins_id));
        DCHECK(_parents.contains(sender_ins_id));
        _queue_deps[sender_ins_id] = queue_dependency;
        _parents[sender_ins_id] = local_state;
    }
#ifdef BE_TEST
public:
#else
private:
#endif
    friend class ExchangeSinkLocalState;

    phmap::flat_hash_map<InstanceLoId, std::unique_ptr<std::mutex>>
            _instance_to_package_queue_mutex;
    // store data in non-broadcast shuffle
    phmap::flat_hash_map<InstanceLoId, std::queue<TransmitInfo, std::list<TransmitInfo>>>
            _instance_to_package_queue;
    size_t _queue_capacity;
    // store data in broadcast shuffle
    phmap::flat_hash_map<InstanceLoId,
                         std::queue<BroadcastTransmitInfo, std::list<BroadcastTransmitInfo>>>
            _instance_to_broadcast_package_queue;
    using PackageSeq = int64_t;
    // must init zero
    // TODO: make all flat_hash_map to a STRUT
    phmap::flat_hash_map<InstanceLoId, PackageSeq> _instance_to_seq;
    phmap::flat_hash_map<InstanceLoId, std::shared_ptr<PTransmitDataParams>> _instance_to_request;
    // One channel is corresponding to a downstream instance.
    phmap::flat_hash_map<InstanceLoId, bool> _rpc_channel_is_idle;

    // There could be multiple situations that cause an rpc_channel to be turned off,
    // such as receiving the eof, manual cancellation by the user, or all sinks reaching eos.
    // Therefore, it is necessary to prevent an rpc_channel from being turned off multiple times.
    phmap::flat_hash_map<InstanceLoId, bool> _rpc_channel_is_turn_off;
    struct RpcInstanceStatistics {
        RpcInstanceStatistics(InstanceLoId id) : inst_lo_id(id) {}
        InstanceLoId inst_lo_id;
        int64_t rpc_count = 0;
        int64_t max_time = 0;
        int64_t min_time = INT64_MAX;
        int64_t sum_time = 0;
    };
    std::vector<std::shared_ptr<RpcInstanceStatistics>> _instance_to_rpc_stats_vec;
    phmap::flat_hash_map<InstanceLoId, RpcInstanceStatistics*> _instance_to_rpc_stats;

    // It is set to true only when an RPC fails. Currently, we do not have an error retry mechanism.
    // If an RPC error occurs, the query will be canceled.
    std::atomic<bool> _is_failed;
    PUniqueId _query_id;
    PlanNodeId _dest_node_id;
    std::atomic<int64_t> _rpc_count = 0;
    RuntimeState* _state = nullptr;
    QueryContext* _context = nullptr;

    Status _send_rpc(InstanceLoId);

#ifndef BE_TEST
    inline void _ended(InstanceLoId id);
    inline void _failed(InstanceLoId id, const std::string& err);
    inline void _set_receiver_eof(InstanceLoId id);
    inline void _turn_off_channel(InstanceLoId id, std::unique_lock<std::mutex>& with_lock);

#else
    virtual void _ended(InstanceLoId id);
    virtual void _failed(InstanceLoId id, const std::string& err);
    virtual void _set_receiver_eof(InstanceLoId id);
    virtual void _turn_off_channel(InstanceLoId id, std::unique_lock<std::mutex>& with_lock);
#endif

    void get_max_min_rpc_time(int64_t* max_time, int64_t* min_time);
    int64_t get_sum_rpc_time();

    std::atomic<int> _total_queue_size = 0;

    // _running_sink_count is used to track how many sinks have not finished yet.
    // It is only decremented when eos is reached.
    phmap::flat_hash_map<InstanceLoId, int64_t> _running_sink_count;
    // _queue_deps is used for memory control.
    phmap::flat_hash_map<InstanceLoId, std::shared_ptr<Dependency>> _queue_deps;
    // The ExchangeSinkLocalState in _parents is only used in _turn_off_channel.
    phmap::flat_hash_map<InstanceLoId, ExchangeSinkLocalState*> _parents;
    const int64_t _exchange_sink_num;
};

} // namespace pipeline
} // namespace doris
