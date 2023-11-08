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
#include <string>

#include "common/global_types.h"
#include "common/status.h"
#include "runtime/query_statistics.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"

namespace doris {
class PTransmitDataParams;
class TUniqueId;

using InstanceLoId = int64_t;

namespace pipeline {
class BroadcastDependency;
class ExchangeSinkQueueDependency;
class FinishDependency;
} // namespace pipeline

namespace vectorized {
class VDataStreamSender;
template <typename>
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
class BroadcastPBlockHolder {
public:
    BroadcastPBlockHolder() : _ref_count(0), _dep(nullptr) {}
    BroadcastPBlockHolder(pipeline::BroadcastDependency* dep) : _ref_count(0), _dep(dep) {}
    ~BroadcastPBlockHolder() noexcept = default;

    void ref(int delta) noexcept { _ref_count._value.fetch_add(delta); }
    void unref() noexcept;
    void ref() noexcept { ref(1); }

    bool available() { return _ref_count._value == 0; }

    PBlock* get_block() { return &pblock; }

private:
    AtomicWrapper<int32_t> _ref_count;
    PBlock pblock;
    pipeline::BroadcastDependency* _dep;
};
} // namespace vectorized

namespace pipeline {
template <typename Parent>
struct TransmitInfo {
    vectorized::PipChannel<Parent>* channel;
    std::unique_ptr<PBlock> block;
    bool eos;
    Status exec_status;
};

template <typename Parent>
struct BroadcastTransmitInfo {
    vectorized::PipChannel<Parent>* channel;
    vectorized::BroadcastPBlockHolder* block_holder;
    bool eos;
};

template <typename T>
class SelfDeleteClosure : public google::protobuf::Closure {
public:
    SelfDeleteClosure() = default;

    void init(InstanceLoId id, bool eos, vectorized::BroadcastPBlockHolder* data) {
        _id = id;
        _eos = eos;
        _data = data;
    }

    ~SelfDeleteClosure() override = default;
    SelfDeleteClosure(const SelfDeleteClosure& other) = delete;
    SelfDeleteClosure& operator=(const SelfDeleteClosure& other) = delete;
    void addFailedHandler(
            const std::function<void(const InstanceLoId&, const std::string&)>& fail_fn) {
        _fail_fn = fail_fn;
    }
    void addSuccessHandler(const std::function<void(const InstanceLoId&, const bool&, const T&,
                                                    const int64_t&)>& suc_fn) {
        _suc_fn = suc_fn;
    }

    void Run() noexcept override {
        try {
            if (_data) {
                _data->unref();
            }
            if (cntl.Failed()) {
                std::string err = fmt::format(
                        "failed to send brpc when exchange, error={}, error_text={}, client: {}, "
                        "latency = {}",
                        berror(cntl.ErrorCode()), cntl.ErrorText(), BackendOptions::get_localhost(),
                        cntl.latency_us());
                _fail_fn(_id, err);
            } else {
                _suc_fn(_id, _eos, result, start_rpc_time);
            }
        } catch (const std::exception& exp) {
            LOG(FATAL) << "brpc callback error: " << exp.what();
        } catch (...) {
            LOG(FATAL) << "brpc callback error.";
        }
    }

    brpc::Controller cntl;
    T result;
    int64_t start_rpc_time;

private:
    std::function<void(const InstanceLoId&, const std::string&)> _fail_fn;
    std::function<void(const InstanceLoId&, const bool&, const T&, const int64_t&)> _suc_fn;
    InstanceLoId _id;
    bool _eos;
    vectorized::BroadcastPBlockHolder* _data;
};

struct ExchangeRpcContext {
    SelfDeleteClosure<PTransmitDataResult>* _closure = nullptr;
    bool is_cancelled = false;
};

// Each ExchangeSinkOperator have one ExchangeSinkBuffer
template <typename Parent>
class ExchangeSinkBuffer {
public:
    ExchangeSinkBuffer(PUniqueId, int, PlanNodeId, int, QueryContext*);
    ~ExchangeSinkBuffer();
    void register_sink(TUniqueId);

    Status add_block(TransmitInfo<Parent>&& request);
    Status add_block(BroadcastTransmitInfo<Parent>&& request);
    bool can_write() const;
    bool is_pending_finish();
    void close();
    void set_rpc_time(InstanceLoId id, int64_t start_rpc_time, int64_t receive_rpc_time);
    void update_profile(RuntimeProfile* profile);

    void set_dependency(std::shared_ptr<ExchangeSinkQueueDependency> queue_dependency,
                        std::shared_ptr<FinishDependency> finish_dependency) {
        _queue_dependency = queue_dependency;
        _finish_dependency = finish_dependency;
    }
    void set_query_statistics(QueryStatistics* statistics) { _statistics = statistics; }

private:
    phmap::flat_hash_map<InstanceLoId, std::unique_ptr<std::mutex>>
            _instance_to_package_queue_mutex;
    // store data in non-broadcast shuffle
    phmap::flat_hash_map<InstanceLoId,
                         std::queue<TransmitInfo<Parent>, std::list<TransmitInfo<Parent>>>>
            _instance_to_package_queue;
    // store data in broadcast shuffle
    phmap::flat_hash_map<InstanceLoId, std::queue<BroadcastTransmitInfo<Parent>,
                                                  std::list<BroadcastTransmitInfo<Parent>>>>
            _instance_to_broadcast_package_queue;
    using PackageSeq = int64_t;
    // must init zero
    // TODO: make all flat_hash_map to a STRUT
    phmap::flat_hash_map<InstanceLoId, PackageSeq> _instance_to_seq;
    phmap::flat_hash_map<InstanceLoId, std::unique_ptr<PTransmitDataParams>> _instance_to_request;
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
    QueryContext* _context;

    Status _send_rpc(InstanceLoId);
    // must hold the _instance_to_package_queue_mutex[id] mutex to opera
    void _construct_request(InstanceLoId id, PUniqueId);
    inline void _ended(InstanceLoId id);
    inline void _failed(InstanceLoId id, const std::string& err);
    inline void _set_receiver_eof(InstanceLoId id);
    inline bool _is_receiver_eof(InstanceLoId id);
    void get_max_min_rpc_time(int64_t* max_time, int64_t* min_time);
    int64_t get_sum_rpc_time();

    std::atomic<int> _total_queue_size = 0;
    static constexpr int QUEUE_CAPACITY_FACTOR = 64;
    int _queue_capacity = 0;
    std::shared_ptr<ExchangeSinkQueueDependency> _queue_dependency = nullptr;
    std::shared_ptr<FinishDependency> _finish_dependency = nullptr;
    QueryStatistics* _statistics = nullptr;
};

} // namespace pipeline
} // namespace doris
