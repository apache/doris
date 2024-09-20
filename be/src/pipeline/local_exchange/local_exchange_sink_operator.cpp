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

#include "pipeline/local_exchange/local_exchange_sink_operator.h"

#include "pipeline/local_exchange/local_exchanger.h"
#include "vec/runtime/partitioner.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris::pipeline {

LocalExchangeSinkLocalState::~LocalExchangeSinkLocalState() = default;

std::vector<Dependency*> LocalExchangeSinkLocalState::dependencies() const {
    auto deps = Base::dependencies();

    auto dep = _shared_state->get_sink_dep_by_channel_id(_channel_id);
    if (dep != nullptr) {
        deps.push_back(dep);
    }
    return deps;
}

Status LocalExchangeSinkOperatorX::init(ExchangeType type, const int num_buckets,
                                        const bool should_disable_bucket_shuffle,
                                        const std::map<int, int>& shuffle_idx_to_instance_idx) {
    _name = "LOCAL_EXCHANGE_SINK_OPERATOR (" + get_exchange_type_name(type) + ")";
    _type = type;
    if (_type == ExchangeType::HASH_SHUFFLE) {
        // For shuffle join, if data distribution has been broken by previous operator, we
        // should use a HASH_SHUFFLE local exchanger to shuffle data again. To be mentioned,
        // we should use map shuffle idx to instance idx because all instances will be
        // distributed to all BEs. Otherwise, we should use shuffle idx directly.
        if (should_disable_bucket_shuffle) {
            std::for_each(shuffle_idx_to_instance_idx.begin(), shuffle_idx_to_instance_idx.end(),
                          [&](const auto& item) {
                              DCHECK(item.first != -1);
                              _shuffle_idx_to_instance_idx.push_back({item.first, item.second});
                          });
        } else {
            _shuffle_idx_to_instance_idx.resize(_num_partitions);
            for (int i = 0; i < _num_partitions; i++) {
                _shuffle_idx_to_instance_idx[i] = {i, i};
            }
        }
        _partitioner.reset(new vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>(
                _num_partitions));
        RETURN_IF_ERROR(_partitioner->init(_texprs));
    } else if (_type == ExchangeType::BUCKET_HASH_SHUFFLE) {
        _partitioner.reset(
                new vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>(num_buckets));
        RETURN_IF_ERROR(_partitioner->init(_texprs));
    }
    return Status::OK();
}

Status LocalExchangeSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<LocalExchangeSinkLocalState>::open(state));
    if (_type == ExchangeType::HASH_SHUFFLE || _type == ExchangeType::BUCKET_HASH_SHUFFLE) {
        RETURN_IF_ERROR(_partitioner->prepare(state, _child->row_desc()));
        RETURN_IF_ERROR(_partitioner->open(state));
    }

    return Status::OK();
}

Status LocalExchangeSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _compute_hash_value_timer = ADD_TIMER(profile(), "ComputeHashValueTime");
    _distribute_timer = ADD_TIMER(profile(), "DistributeDataTime");
    _channel_id = info.task_idx;
    return Status::OK();
}

Status LocalExchangeSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    _exchanger = _shared_state->exchanger.get();
    DCHECK(_exchanger != nullptr);

    if (_exchanger->get_type() == ExchangeType::HASH_SHUFFLE ||
        _exchanger->get_type() == ExchangeType::BUCKET_HASH_SHUFFLE) {
        auto& p = _parent->cast<LocalExchangeSinkOperatorX>();
        RETURN_IF_ERROR(p._partitioner->clone(state, _partitioner));
    }

    return Status::OK();
}

Status LocalExchangeSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    RETURN_IF_ERROR(Base::close(state, exec_status));
    if (exec_status.ok()) {
        DCHECK(_release_count) << "Do not finish correctly! " << debug_string(0)
                               << " state: { cancel = " << state->is_cancelled() << ", "
                               << state->cancel_reason().to_string() << "} query ctx: { cancel = "
                               << state->get_query_ctx()->is_cancelled() << ", "
                               << state->get_query_ctx()->exec_status().to_string() << "}";
    }
    return Status::OK();
}

std::string LocalExchangeSinkLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "{}, _channel_id: {}, _num_partitions: {}, _num_senders: {}, _num_sources: {}, "
                   "_running_sink_operators: {}, _running_source_operators: {}, _release_count: {}",
                   Base::debug_string(indentation_level), _channel_id, _exchanger->_num_partitions,
                   _exchanger->_num_senders, _exchanger->_num_sources,
                   _exchanger->_running_sink_operators, _exchanger->_running_source_operators,
                   _release_count);
    return fmt::to_string(debug_string_buffer);
}

Status LocalExchangeSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                        bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    RETURN_IF_ERROR(local_state._exchanger->sink(state, in_block, eos, local_state));

    // If all exchange sources ended due to limit reached, current task should also finish
    if (local_state._exchanger->_running_source_operators == 0) {
        local_state._release_count = true;
        local_state._shared_state->sub_running_sink_operators();
        return Status::EndOfFile("receiver eof");
    }
    if (eos) {
        local_state._shared_state->sub_running_sink_operators();
        local_state._release_count = true;
    }

    return Status::OK();
}

} // namespace doris::pipeline
