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

#include "dependency.h"

#include "runtime/memory/mem_tracker.h"

namespace doris::pipeline {

Status AggDependency::reset_hash_table() {
    return std::visit(
            [&](auto&& agg_method) {
                auto& hash_table = agg_method.data;
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using HashTableType = std::decay_t<decltype(hash_table)>;

                if constexpr (vectorized::ColumnsHashing::IsPreSerializedKeysHashMethodTraits<
                                      HashMethodType>::value) {
                    agg_method.reset();
                }

                hash_table.for_each_mapped([&](auto& mapped) {
                    if (mapped) {
                        destroy_agg_status(mapped);
                        mapped = nullptr;
                    }
                });

                _agg_state.aggregate_data_container.reset(new vectorized::AggregateDataContainer(
                        sizeof(typename HashTableType::key_type),
                        ((_total_size_of_aggregate_states + _align_aggregate_states - 1) /
                         _align_aggregate_states) *
                                _align_aggregate_states));
                hash_table = HashTableType();
                _agg_state.agg_arena_pool.reset(new vectorized::Arena);
                return Status::OK();
            },
            _agg_state.agg_data->_aggregated_method_variant);
}

Status AggDependency::destroy_agg_status(vectorized::AggregateDataPtr data) {
    for (int i = 0; i < _agg_state.aggregate_evaluators.size(); ++i) {
        _agg_state.aggregate_evaluators[i]->function()->destroy(data +
                                                                _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status AggDependency::create_agg_status(vectorized::AggregateDataPtr data) {
    for (int i = 0; i < _agg_state.aggregate_evaluators.size(); ++i) {
        try {
            _agg_state.aggregate_evaluators[i]->create(data + _offsets_of_aggregate_states[i]);
        } catch (...) {
            for (int j = 0; j < i; ++j) {
                _agg_state.aggregate_evaluators[j]->destroy(data + _offsets_of_aggregate_states[j]);
            }
            throw;
        }
    }
    return Status::OK();
}

Status AggDependency::merge_spilt_data() {
    CHECK(!_agg_state.spill_context.stream_ids.empty());

    for (auto& reader : _agg_state.spill_context.readers) {
        CHECK_LT(_agg_state.spill_context.read_cursor, reader->block_count());
        reader->seek(_agg_state.spill_context.read_cursor);
        vectorized::Block block;
        bool eos;
        RETURN_IF_ERROR(reader->read(&block, &eos));

        // TODO
        //        if (!block.empty()) {
        //            auto st = _merge_with_serialized_key_helper<false /* limit */, true /* for_spill */>(
        //                    &block);
        //            RETURN_IF_ERROR(st);
        //        }
    }
    _agg_state.spill_context.read_cursor++;
    return Status::OK();
}

void AggDependency::release_tracker() {
    mem_tracker()->release(_mem_usage_record.used_in_state + _mem_usage_record.used_in_arena);
}

} // namespace doris::pipeline
