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
class BroadcastPBlockHolder {
public:
    BroadcastPBlockHolder() : _ref_count(0) {}
    ~BroadcastPBlockHolder() noexcept = default;

    void unref() noexcept {
        DCHECK_GT(_ref_count._value, 0);
        _ref_count._value.fetch_sub(1);
    }
    void ref() noexcept { _ref_count._value.fetch_add(1); }

    bool available() { return _ref_count._value == 0; }

    PBlock* get_block() { return &pblock; }

private:
    AtomicWrapper<uint32_t> _ref_count;
    PBlock pblock;
};
} // namespace vectorized

namespace pipeline {
struct TransmitInfo {
    vectorized::PipChannel* channel;
    std::unique_ptr<PBlock> block;
    bool eos;
};

struct BroadcastTransmitInfo {
    vectorized::PipChannel* channel;
    vectorized::BroadcastPBlockHolder* block_holder;
    bool eos;
};

class PipelineFragmentContext;

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
class ExchangeSinkBuffer {
public:
    ExchangeSinkBuffer(PUniqueId, int, PlanNodeId, int, PipelineFragmentContext*);
    ~ExchangeSinkBuffer();
    void register_sink(TUniqueId);
    Status add_block(TransmitInfo&& request);
    Status add_block(BroadcastTransmitInfo&& request);
    bool can_write() const;
    bool is_pending_finish();
    void close();
    void set_rpc_time(InstanceLoId id, int64_t start_rpc_time, int64_t receive_rpc_time);
    void update_profile(RuntimeProfile* profile);

    void set_query_statistics(QueryStatistics* statistics) { _statistics = statistics; }

private:
    phmap::flat_hash_map<InstanceLoId, std::unique_ptr<std::mutex>>
            _instance_to_package_queue_mutex;
    // store data in non-broadcast shuffle
    phmap::flat_hash_map<InstanceLoId, std::queue<TransmitInfo, std::list<TransmitInfo>>>
            _instance_to_package_queue;
    // store data in broadcast shuffle
    phmap::flat_hash_map<InstanceLoId,
                         std::queue<BroadcastTransmitInfo, std::list<BroadcastTransmitInfo>>>
            _instance_to_broadcast_package_queue;
    using PackageSeq = int64_t;
    // must init zero
    // TODO: make all flat_hash_map to a STRUT
    phmap::flat_hash_map<InstanceLoId, PackageSeq> _instance_to_seq;
    phmap::flat_hash_map<InstanceLoId, std::unique_ptr<PTransmitDataParams>> _instance_to_request;
    phmap::flat_hash_map<InstanceLoId, bool> _instance_to_sending_by_pipeline;
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
    PipelineFragmentContext* _context;

    Status _send_rpc(InstanceLoId);
    // must hold the _instance_to_package_queue_mutex[id] mutex to opera
    void _construct_request(InstanceLoId id, PUniqueId);
    inline void _ended(InstanceLoId id);
    inline void _failed(InstanceLoId id, const std::string& err);
    inline void _set_receiver_eof(InstanceLoId id);
    inline bool _is_receiver_eof(InstanceLoId id);
    void get_max_min_rpc_time(int64_t* max_time, int64_t* min_time);
    int64_t get_sum_rpc_time();

    QueryStatistics* _statistics = nullptr;
};

} // namespace pipeline
} // namespace doris