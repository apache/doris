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
#include <stdint.h>

#include <atomic>
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
class PipChannel;

template <typename T>
struct AtomicWrapper {
    std::atomic<T> _value;

    AtomicWrapper() : _value() {}

    AtomicWrapper(const std::atomic<T>& a) : _value(a.load()) {}

    AtomicWrapper(const AtomicWrapper& other) : _value(other._value.load()) {}

    AtomicWrapper& operator=(const AtomicWrapper& other) { _value.store(other._a.load()); }
};

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
    vectorized::PipChannel* channel = nullptr;
    std::unique_ptr<PBlock> block;
    bool eos;
    Status exec_status;
};

struct BroadcastTransmitInfo {
    vectorized::PipChannel* channel = nullptr;
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

struct ExchangeRpcContext {
    std::shared_ptr<ExchangeSendCallback<PTransmitDataResult>> _send_callback;
    bool is_cancelled = false;
};

// Each ExchangeSinkOperator have one ExchangeSinkBuffer
class ExchangeSinkBuffer final : public HasTaskExecutionCtx {
public:
    ExchangeSinkBuffer(PUniqueId query_id, PlanNodeId dest_node_id, int send_id, int be_number,
                       RuntimeState* state, ExchangeSinkLocalState* parent);
    ~ExchangeSinkBuffer() override = default;
    void register_sink(TUniqueId);

    Status add_block(TransmitInfo&& request);
    Status add_block(BroadcastTransmitInfo&& request);
    void close();
    void set_rpc_time(InstanceLoId id, int64_t start_rpc_time, int64_t receive_rpc_time);
    void update_profile(RuntimeProfile* profile);

    void set_dependency(std::shared_ptr<Dependency> queue_dependency,
                        std::shared_ptr<Dependency> finish_dependency) {
        _queue_dependency = queue_dependency;
        _finish_dependency = finish_dependency;
    }

    void set_broadcast_dependency(std::shared_ptr<Dependency> broadcast_dependency) {
        _broadcast_dependency = broadcast_dependency;
    }

    void set_should_stop() {
        _should_stop = true;
        _set_ready_to_finish(_busy_channels == 0);
    }

private:
    friend class ExchangeSinkLocalState;
    void _set_ready_to_finish(bool all_done);

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
    // Number of busy channels;
    std::atomic<int> _busy_channels = 0;
    phmap::flat_hash_map<InstanceLoId, bool> _instance_to_receiver_eof;
    phmap::flat_hash_map<InstanceLoId, int64_t> _instance_to_rpc_time;
    phmap::flat_hash_map<InstanceLoId, ExchangeRpcContext> _instance_to_rpc_ctx;

    std::atomic<bool> _is_finishing;
    PUniqueId _query_id;
    PlanNodeId _dest_node_id;
    // Sender instance id, unique within a fragment. StreamSender save the variable
    int _sender_id;
    int _be_number;
    std::atomic<int64_t> _rpc_count = 0;
    RuntimeState* _state = nullptr;
    QueryContext* _context = nullptr;

    Status _send_rpc(InstanceLoId);
    // must hold the _instance_to_package_queue_mutex[id] mutex to opera
    void _construct_request(InstanceLoId id, PUniqueId);
    inline void _ended(InstanceLoId id);
    inline void _failed(InstanceLoId id, const std::string& err);
    inline void _set_receiver_eof(InstanceLoId id);
    inline bool _is_receiver_eof(InstanceLoId id);
    inline void _turn_off_channel(InstanceLoId id, bool cleanup = false);
    void get_max_min_rpc_time(int64_t* max_time, int64_t* min_time);
    int64_t get_sum_rpc_time();

    std::atomic<int> _total_queue_size = 0;
    std::shared_ptr<Dependency> _queue_dependency = nullptr;
    std::shared_ptr<Dependency> _finish_dependency = nullptr;
    std::shared_ptr<Dependency> _broadcast_dependency = nullptr;
    std::atomic<bool> _should_stop = false;
    ExchangeSinkLocalState* _parent = nullptr;
};

} // namespace pipeline
} // namespace doris
