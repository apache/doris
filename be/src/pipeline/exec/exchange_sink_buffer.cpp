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

#include "exchange_sink_buffer.h"

#include <google/protobuf/stubs/common.h>

#include <atomic>
#include <memory>

#include "common/status.h"
#include "pipeline/pipeline_fragment_context.h"
#include "service/brpc.h"
#include "util/proto_util.h"
#include "util/time.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris::pipeline {
template <typename T>
class SelfDeleteClosure : public google::protobuf::Closure {
public:
    SelfDeleteClosure(InstanceLoId id, bool eos) : _id(id), _eos(eos) {}
    ~SelfDeleteClosure() override = default;
    SelfDeleteClosure(const SelfDeleteClosure& other) = delete;
    SelfDeleteClosure& operator=(const SelfDeleteClosure& other) = delete;
    void addFailedHandler(std::function<void(const InstanceLoId&, const std::string&)> fail_fn) {
        _fail_fn = std::move(fail_fn);
    }
    void addSuccessHandler(std::function<void(const InstanceLoId&, const bool&, const T&)> suc_fn) {
        _suc_fn = suc_fn;
    }

    void Run() noexcept override {
        std::unique_ptr<SelfDeleteClosure> self_guard(this);
        try {
            if (cntl.Failed()) {
                std::string err = fmt::format(
                        "failed to send brpc when exchange, error={}, error_text={}, client: {}, "
                        "latency = {}",
                        berror(cntl.ErrorCode()), cntl.ErrorText(), BackendOptions::get_localhost(),
                        cntl.latency_us());
                _fail_fn(_id, err);
            } else {
                _suc_fn(_id, _eos, result);
            }
        } catch (const std::exception& exp) {
            LOG(FATAL) << "brpc callback error: " << exp.what();
        } catch (...) {
            LOG(FATAL) << "brpc callback error.";
        }
    }

public:
    brpc::Controller cntl;
    T result;

private:
    std::function<void(const InstanceLoId&, const std::string&)> _fail_fn;
    std::function<void(const InstanceLoId&, const bool&, const T&)> _suc_fn;
    InstanceLoId _id;
    bool _eos;
};

ExchangeSinkBuffer::ExchangeSinkBuffer(PUniqueId query_id, PlanNodeId dest_node_id, int send_id,
                                       int be_number, PipelineFragmentContext* context)
        : _is_finishing(false),
          _query_id(query_id),
          _dest_node_id(dest_node_id),
          _sender_id(send_id),
          _be_number(be_number),
          _context(context) {}

ExchangeSinkBuffer::~ExchangeSinkBuffer() = default;

void ExchangeSinkBuffer::close() {
    for (const auto& pair : _instance_to_request) {
        if (pair.second) {
            pair.second->release_finst_id();
            pair.second->release_query_id();
        }
    }
}

bool ExchangeSinkBuffer::can_write() const {
    size_t max_package_size = 64 * _instance_to_package_queue.size();
    size_t total_package_size = 0;
    for (auto& [_, q] : _instance_to_package_queue) {
        total_package_size += q.size();
    }
    return total_package_size <= max_package_size;
}

bool ExchangeSinkBuffer::is_pending_finish() const {
    for (auto& pair : _instance_to_package_queue_mutex) {
        std::unique_lock<std::mutex> lock(*(pair.second));
        auto& id = pair.first;
        if (!_instance_to_sending_by_pipeline.at(id)) {
            return true;
        }
    }
    return false;
}

void ExchangeSinkBuffer::register_sink(TUniqueId fragment_instance_id) {
    if (_is_finishing) {
        return;
    }
    auto low_id = fragment_instance_id.lo;
    if (_instance_to_package_queue_mutex.count(low_id)) {
        return;
    }
    _instance_to_package_queue_mutex[low_id] = std::make_unique<std::mutex>();
    _instance_to_seq[low_id] = 0;
    _instance_to_package_queue[low_id] = std::queue<TransmitInfo, std::list<TransmitInfo>>();
    PUniqueId finst_id;
    finst_id.set_hi(fragment_instance_id.hi);
    finst_id.set_lo(fragment_instance_id.lo);
    _instance_to_finst_id[low_id] = finst_id;
    _instance_to_sending_by_pipeline[low_id] = true;
}

Status ExchangeSinkBuffer::add_block(TransmitInfo&& request) {
    if (_is_finishing) {
        return Status::OK();
    }
    TUniqueId ins_id = request.channel->_fragment_instance_id;
    bool send_now = false;
    {
        std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[ins_id.lo]);
        // Do not have in process rpc, directly send
        if (_instance_to_sending_by_pipeline[ins_id.lo]) {
            send_now = true;
            _instance_to_sending_by_pipeline[ins_id.lo] = false;
        }
        _instance_to_package_queue[ins_id.lo].emplace(std::move(request));
    }
    if (send_now) {
        RETURN_IF_ERROR(_send_rpc(ins_id.lo));
    }

    return Status::OK();
}

Status ExchangeSinkBuffer::_send_rpc(InstanceLoId id) {
    std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[id]);

    std::queue<TransmitInfo, std::list<TransmitInfo>>& q = _instance_to_package_queue[id];
    if (q.empty() || _is_finishing) {
        _instance_to_sending_by_pipeline[id] = true;
        return Status::OK();
    }

    TransmitInfo& request = q.front();

    if (!_instance_to_request[id]) {
        _construct_request(id);
    }

    auto& brpc_request = _instance_to_request[id];
    brpc_request->set_eos(request.eos);
    brpc_request->set_packet_seq(_instance_to_seq[id]++);
    if (request.block) {
        brpc_request->set_allocated_block(request.block.get());
    }

    auto* _closure = new SelfDeleteClosure<PTransmitDataResult>(id, request.eos);
    _closure->cntl.set_timeout_ms(request.channel->_brpc_timeout_ms);
    _closure->addFailedHandler(
            [&](const InstanceLoId& id, const std::string& err) { _failed(id, err); });
    _closure->addSuccessHandler([&](const InstanceLoId& id, const bool& eos,
                                    const PTransmitDataResult& result) {
        Status s = Status(result.status());
        if (!s.ok()) {
            _failed(id,
                    fmt::format("exchange req success but status isn't ok: {}", s.get_error_msg()));
        } else if (eos) {
            _ended(id);
        } else {
            _send_rpc(id);
        }
    });

    {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
        if (enable_http_send_block(*brpc_request)) {
            RETURN_IF_ERROR(transmit_block_http(_context->get_runtime_state(), _closure,
                                                *brpc_request, request.channel->_brpc_dest_addr));
        } else {
            transmit_block(*request.channel->_brpc_stub, _closure, *brpc_request);
        }
    }

    if (request.block) {
        brpc_request->release_block();
    }
    q.pop();

    return Status::OK();
}

void ExchangeSinkBuffer::_construct_request(InstanceLoId id) {
    _instance_to_request[id] = std::make_unique<PTransmitDataParams>();
    _instance_to_request[id]->set_allocated_finst_id(&_instance_to_finst_id[id]);
    _instance_to_request[id]->set_allocated_query_id(&_query_id);

    _instance_to_request[id]->set_node_id(_dest_node_id);
    _instance_to_request[id]->set_sender_id(_sender_id);
    _instance_to_request[id]->set_be_number(_be_number);
}

void ExchangeSinkBuffer::_ended(InstanceLoId id) {
    std::unique_lock<std::mutex> lock(*_instance_to_package_queue_mutex[id]);
    _instance_to_sending_by_pipeline[id] = true;
}

void ExchangeSinkBuffer::_failed(InstanceLoId id, const std::string& err) {
    _is_finishing = true;
    _context->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, err);
    _ended(id);
};

} // namespace doris::pipeline
