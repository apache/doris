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

#include <cstdint>
#include <type_traits>

#include "operator.h"
#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/operator.h"
#include "runtime/block_spill_manager.h"
#include "runtime/exec_env.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {

namespace pipeline {
template <typename Derived, typename OperatorX>
class AggLocalStateHelper {
public:
    ~AggLocalStateHelper() = default;

    AggLocalStateHelper(Derived* derived) : _derived_class(derived) {}

    void _find_in_hash_table(vectorized::AggregateDataPtr* places,
                             vectorized::ColumnRawPtrs& key_columns, size_t num_rows) {
        std::visit(
                [&](auto&& agg_method) -> void {
                    using HashMethodType = std::decay_t<decltype(agg_method)>;
                    using AggState = typename HashMethodType::State;
                    AggState state(key_columns);
                    agg_method.init_serialized_keys(key_columns, num_rows);

                    /// For all rows.
                    for (size_t i = 0; i < num_rows; ++i) {
                        auto find_result = agg_method.find(state, i);

                        if (find_result.is_found()) {
                            places[i] = find_result.get_mapped();
                        } else {
                            places[i] = nullptr;
                        }
                    }
                },
                _shared_state()->agg_data->method_variant);
    }
    void _emplace_into_hash_table(vectorized::AggregateDataPtr* places,
                                  vectorized::ColumnRawPtrs& key_columns, size_t num_rows) {
        std::visit(
                [&](auto&& agg_method) -> void {
                    SCOPED_TIMER(_derived()->_hash_table_compute_timer);
                    using HashMethodType = std::decay_t<decltype(agg_method)>;
                    using AggState = typename HashMethodType::State;
                    AggState state(key_columns);
                    agg_method.init_serialized_keys(key_columns, num_rows);

                    auto creator = [this](const auto& ctor, auto& key, auto& origin) {
                        HashMethodType::try_presis_key_and_origin(key, origin,
                                                                  *_shared_state()->agg_arena_pool);
                        auto mapped =
                                _shared_state()->aggregate_data_container->append_data(origin);
                        auto st = _create_agg_status(mapped);
                        if (!st) {
                            throw Exception(st.code(), st.to_string());
                        }
                        ctor(key, mapped);
                    };

                    auto creator_for_null_key = [&](auto& mapped) {
                        mapped = _shared_state()->agg_arena_pool->aligned_alloc(
                                _shared_state()->total_size_of_aggregate_states,
                                _shared_state()->align_aggregate_states);
                        auto st = _create_agg_status(mapped);
                        if (!st) {
                            throw Exception(st.code(), st.to_string());
                        }
                    };

                    SCOPED_TIMER(_derived()->_hash_table_emplace_timer);
                    for (size_t i = 0; i < num_rows; ++i) {
                        places[i] =
                                agg_method.lazy_emplace(state, i, creator, creator_for_null_key);
                    }

                    COUNTER_UPDATE(_derived()->_hash_table_input_counter, num_rows);
                },
                _shared_state()->agg_data->method_variant);
    }

    size_t _get_hash_table_size() const {
        return std::visit([&](auto&& agg_method) { return agg_method.hash_table->size(); },
                          _shared_state()->agg_data->method_variant);
    }

    Status _create_agg_status(vectorized::AggregateDataPtr data) {
        auto& shared_state = *_shared_state();
        for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
            try {
                shared_state.aggregate_evaluators[i]->create(
                        data + shared_state.offsets_of_aggregate_states[i]);
            } catch (...) {
                for (int j = 0; j < i; ++j) {
                    shared_state.aggregate_evaluators[j]->destroy(
                            data + shared_state.offsets_of_aggregate_states[j]);
                }
                throw;
            }
        }
        return Status::OK();
    }

    Status _destroy_agg_status(vectorized::AggregateDataPtr data) {
        auto& shared_state = *_shared_state();
        for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
            shared_state.aggregate_evaluators[i]->function()->destroy(
                    data + shared_state.offsets_of_aggregate_states[i]);
        }
        return Status::OK();
    }

    void _init_hash_method(const vectorized::VExprContextSPtrs& probe_exprs) {
        DCHECK(!probe_exprs.empty());
        auto& agg_data = _shared_state()->agg_data;
        using Type = vectorized::AggregatedDataVariants::Type;
        Type t(Type::serialized);

        if (probe_exprs.size() == 1) {
            auto is_nullable = probe_exprs[0]->root()->is_nullable();
            PrimitiveType type = probe_exprs[0]->root()->result_type();
            switch (type) {
            case TYPE_TINYINT:
            case TYPE_BOOLEAN:
            case TYPE_SMALLINT:
            case TYPE_INT:
            case TYPE_FLOAT:
            case TYPE_DATEV2:
            case TYPE_BIGINT:
            case TYPE_DOUBLE:
            case TYPE_DATE:
            case TYPE_DATETIME:
            case TYPE_DATETIMEV2:
            case TYPE_LARGEINT:
            case TYPE_DECIMALV2:
            case TYPE_DECIMAL32:
            case TYPE_DECIMAL64:
            case TYPE_DECIMAL128I: {
                size_t size = get_primitive_type_size(type);
                if (size == 1) {
                    t = Type::int8_key;
                } else if (size == 2) {
                    t = Type::int16_key;
                } else if (size == 4) {
                    t = Type::int32_key;
                } else if (size == 8) {
                    t = Type::int64_key;
                } else if (size == 16) {
                    t = Type::int128_key;
                } else {
                    throw Exception(ErrorCode::INTERNAL_ERROR,
                                    "meet invalid type size, size={}, type={}", size,
                                    type_to_string(type));
                }
                break;
            }
            case TYPE_CHAR:
            case TYPE_VARCHAR:
            case TYPE_STRING: {
                t = Type::string_key;
                break;
            }
            default:
                t = Type::serialized;
            }

            agg_data->init(get_hash_key_type_with_phase(t, !_operator()._is_first_phase),
                           is_nullable);
        } else {
            if (!try_get_hash_map_context_fixed<PHNormalHashMap, HashCRC32,
                                                vectorized::AggregateDataPtr>(
                        agg_data->method_variant, probe_exprs)) {
                agg_data->init(Type::serialized);
            }
        }
    }

protected:
    static constexpr bool is_shared =
            !std::is_same_v<typename Derived::SharedStateType, doris::pipeline::FakeSharedState>;

    auto _shared_state() const {
        if constexpr (is_shared) {
            return _derived_class->_shared_state;
        } else {
            return _derived_class;
        }
    }
    Derived* _derived() { return _derived_class; }
    auto _derived() const { return _derived_class; }

    OperatorX& _operator() { return _derived()->parent()->template cast<OperatorX>(); }

    Derived* const _derived_class;
};
}; // namespace pipeline

}; // namespace doris
