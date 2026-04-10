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

#include "exec/common/hash_table/join_hash_table_context.h"

// hash_map_context.h and join_hash_table.h are already transitively included
// via join_hash_table_context.h -> join_hash_table_types.h

namespace doris {

// Forward declarations — full definitions provided in hashjoin_build_sink.h and
// process_hash_table_probe.h respectively. Only the forward declaration is needed
// here because the names appear in dependent context (Phase 1 of two-phase lookup).
// Actual instantiation happens in join_utils.cpp where the full definitions are available.
template <class HashTableContext>
struct ProcessHashTableBuild;

template <int JoinOpType>
struct ProcessHashTableProbe;

// Forward declaration — defined later in this file.
template <typename ConcreteMethod, int JoinOp>
struct ProbeCtxImpl;

// ─────────────────────────────────────────────────────────────────────────────
// JoinHashTableContextImplBase<ConcreteMethod>
//
// Common implementation for all concrete hash table context types.
// Inherits both IJoinHashTableContext (virtual interface) and ConcreteMethod
// (the actual hash table context with data members).
// ─────────────────────────────────────────────────────────────────────────────
template <typename ConcreteMethod>
class JoinHashTableContextImplBase : public IJoinHashTableContext, public ConcreteMethod {
public:
    using ConcreteMethod::ConcreteMethod;

    // ── Key serialization ──
    // Note: Both IJoinHashTableContext and MethodBaseInner<HashMap> declare
    // estimated_size / serialized_keys_size as virtual methods with the same signature.
    // This override satisfies both base classes (C++ merged override).
    // Signatures must match exactly: estimated_size is non-const, serialized_keys_size is const.
    size_t estimated_size(const ColumnRawPtrs& key_columns, uint32_t num_rows, bool is_join,
                          bool is_build, uint32_t bucket_size) override {
        return ConcreteMethod::estimated_size(key_columns, num_rows, is_join, is_build,
                                              bucket_size);
    }

    size_t serialized_keys_size(bool is_build) const override {
        return ConcreteMethod::serialized_keys_size(is_build);
    }

    // ── Hash table metadata ──
    void get_table_metadata(uint32_t& bucket_size, const uint32_t*& first_array,
                            const uint32_t*& next_array, size_t& build_rows) const override {
        auto* ht = ConcreteMethod::hash_table.get();
        DORIS_CHECK(ht);
        bucket_size = ht->get_bucket_size();
        first_array = ht->get_first().data();
        next_array = ht->get_next().data();
        build_rows = ht->size();
    }

    size_t hash_table_byte_size() const override {
        return ConcreteMethod::hash_table->get_byte_size();
    }

    // ── Build phase ──
    Status process_build(uint32_t rows, ColumnRawPtrs& raw_ptrs,
                         HashJoinBuildSinkLocalState* local_state, int batch_size,
                         RuntimeState* state, const JoinOpVariants& join_op_variants,
                         const ColumnUInt8::Container* null_map, bool* has_null_in_build_side,
                         bool short_circuit_for_null_in_build_side,
                         bool have_other_join_conjunct) override {
        // Single-dimension visit on JoinOpVariants (~13 alternatives).
        // Uses ProcessHashTableBuild<ConcreteMethod> to reuse existing template instantiation.
        return std::visit(
                [&](auto join_op) -> Status {
                    using JoinOpType = std::decay_t<decltype(join_op)>;
                    ProcessHashTableBuild<ConcreteMethod> builder(rows, raw_ptrs, local_state,
                                                                  batch_size, state);
                    return builder.template run<JoinOpType::value>(
                            static_cast<ConcreteMethod&>(*this), null_map, has_null_in_build_side,
                            short_circuit_for_null_in_build_side, have_other_join_conjunct);
                },
                join_op_variants);
    }

    void build_asof_index_groups_impl(AsofIndexVariant& asof_index_groups,
                                      std::vector<uint32_t>& asof_build_row_to_bucket,
                                      uint32_t bucket_size, const uint32_t* first_array,
                                      const uint32_t* next_array, size_t build_rows,
                                      const ColumnNullable* nullable_col,
                                      const IColumn* build_col_nested,
                                      RuntimeProfile::Counter* asof_index_group_timer,
                                      RuntimeProfile::Counter* asof_index_sort_timer) override {
        // Dispatch on ASOF column type to create typed AsofIndexGroups with inline values.
        asof_column_dispatch(build_col_nested, [&](const auto* typed_col) {
            using ColType = std::remove_const_t<std::remove_pointer_t<decltype(typed_col)>>;

            if constexpr (std::is_same_v<ColType, IColumn>) {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "Unsupported ASOF column type for inline optimization");
            } else {
                using IntType = typename ColType::value_type::underlying_value;
                const auto& col_data = typed_col->get_data();

                auto& groups = asof_index_groups.emplace<std::vector<AsofIndexGroup<IntType>>>();

                // Direct access to hash_table via ConcreteMethod — no visit needed.
                auto* hash_table = ConcreteMethod::hash_table.get();
                DORIS_CHECK(hash_table);
                const auto* build_keys = hash_table->get_build_keys();
                using KeyType = std::remove_const_t<std::remove_pointer_t<decltype(build_keys)>>;
                uint32_t next_group_id = 0;

                struct KeyGroupEntry {
                    KeyType key;
                    uint32_t group_id;
                };
                std::vector<KeyGroupEntry> bucket_key_groups;

                {
                    SCOPED_TIMER(asof_index_group_timer);
                    for (uint32_t bucket = 0; bucket <= bucket_size; ++bucket) {
                        uint32_t row_idx = first_array[bucket];
                        if (row_idx == 0) {
                            continue;
                        }

                        bucket_key_groups.clear();
                        while (row_idx != 0) {
                            DCHECK(row_idx <= build_rows);
                            const auto& key = build_keys[row_idx];

                            uint32_t group_id = UINT32_MAX;
                            for (const auto& entry : bucket_key_groups) {
                                if (entry.key == key) {
                                    group_id = entry.group_id;
                                    break;
                                }
                            }
                            if (group_id == UINT32_MAX) {
                                group_id = next_group_id++;
                                DCHECK(group_id == groups.size());
                                groups.emplace_back();
                                bucket_key_groups.push_back({key, group_id});
                            }

                            asof_build_row_to_bucket[row_idx] = group_id;
                            if (!(nullable_col && nullable_col->is_null_at(row_idx))) {
                                groups[group_id].add_row(col_data[row_idx].to_date_int_val(),
                                                         row_idx);
                            }

                            row_idx = next_array[row_idx];
                        }
                    }
                }

                {
                    SCOPED_TIMER(asof_index_sort_timer);
                    for (auto& group : groups) {
                        group.sort_and_finalize();
                    }
                }
            }
        });
    }

    void share_hash_table_from(const IJoinHashTableContext& src) override {
        DORIS_CHECK(dynamic_cast<const JoinHashTableContextImplBase<ConcreteMethod>*>(&src));
        const auto& typed = static_cast<const JoinHashTableContextImplBase<ConcreteMethod>&>(src);
        ConcreteMethod::hash_table = typed.ConcreteMethod::hash_table;
    }

    // ── Probe phase ──
    void init_probe_ctx(HashJoinProbeLocalState* probe_state, int batch_size,
                        const JoinOpVariants& join_op_variants) override {
        std::visit(
                [&](auto join_op) {
                    using JoinOpType = std::decay_t<decltype(join_op)>;
                    if constexpr (JoinOpType::value == TJoinOp::CROSS_JOIN) {
                        throw Exception(
                                Status::InternalError("hash join does not support CROSS_JOIN"));
                    } else {
                        _probe_ctx =
                                std::make_unique<ProbeCtxImpl<ConcreteMethod, JoinOpType::value>>(
                                        probe_state, batch_size);
                    }
                },
                join_op_variants);
    }

    Status process_probe(const uint8_t* null_map, MutableBlock& out, Block* block, uint32_t rows,
                         bool is_mark_join) override {
        DORIS_CHECK(_probe_ctx);
        return _probe_ctx->process(*this, null_map, out, block, rows, is_mark_join);
    }

    void process_direct_return(MutableBlock& out, Block* block, uint32_t rows) override {
        DORIS_CHECK(_probe_ctx);
        _probe_ctx->process_direct_return(*this, out, block, rows);
    }

    Status finish_probing(MutableBlock& out, Block* block, bool* eos, bool is_mark_join) override {
        DORIS_CHECK(_probe_ctx);
        return _probe_ctx->finish_probing(*this, out, block, eos, is_mark_join);
    }

    void close_probe_ctx() override {
        if (_probe_ctx) {
            _probe_ctx->close();
            _probe_ctx.reset();
        }
    }

private:
    std::unique_ptr<IProbeCtxBase> _probe_ctx;
};

// ─────────────────────────────────────────────────────────────────────────────
// JoinHashTableContextHelper<ConcreteMethod> — main template
//
// Empty class body: all implementation inherited from ImplBase.
// ─────────────────────────────────────────────────────────────────────────────
template <typename ConcreteMethod>
class JoinHashTableContextHelper final : public JoinHashTableContextImplBase<ConcreteMethod> {
    using Base = JoinHashTableContextImplBase<ConcreteMethod>;

public:
    using Base::Base;
};

// ─────────────────────────────────────────────────────────────────────────────
// JoinHashTableContextHelper<PrimaryTypeHashTableContext<T>> — partial specialization
//
// Overrides try_convert_to_direct() to attempt direct-mapping optimization
// when the key range is small enough (< 2^23).
// ─────────────────────────────────────────────────────────────────────────────
template <typename T>
class JoinHashTableContextHelper<PrimaryTypeHashTableContext<T>> final
        : public JoinHashTableContextImplBase<PrimaryTypeHashTableContext<T>> {
    using Base = JoinHashTableContextImplBase<PrimaryTypeHashTableContext<T>>;

public:
    using Base::Base;

    bool try_convert_to_direct(
            const ColumnRawPtrs& key_columns,
            std::vector<std::shared_ptr<JoinDataVariants>>& all_variants) override;
};

// try_convert_to_direct implementation for PrimaryType is in join_utils.cpp.
// It needs JoinDataVariants complete type (for make_unique) which is only
// available after join_utils.h defines JoinDataVariants.

// ─────────────────────────────────────────────────────────────────────────────
// ProbeCtxImpl<ConcreteMethod, JoinOp>
//
// Type-erases the JoinOp template parameter for the probe path.
// Bridges IJoinHashTableContext& back to ConcreteMethod& via static_cast chain,
// ensuring ProcessHashTableProbe is instantiated with ConcreteMethod (not Helper).
// ─────────────────────────────────────────────────────────────────────────────
template <typename ConcreteMethod, int JoinOp>
struct ProbeCtxImpl : public IProbeCtxBase {
    ProcessHashTableProbe<JoinOp> inner;

    explicit ProbeCtxImpl(HashJoinProbeLocalState* state, int batch_size)
            : inner(state, batch_size) {}

    Status process(IJoinHashTableContext& ht_ctx, const uint8_t* null_map, MutableBlock& out,
                   Block* block, uint32_t rows, bool is_mark_join) override {
        auto& typed = static_cast<ConcreteMethod&>(
                static_cast<JoinHashTableContextImplBase<ConcreteMethod>&>(ht_ctx));
        return inner.process(typed, null_map, out, block, rows, is_mark_join);
    }

    void process_direct_return(IJoinHashTableContext& ht_ctx, MutableBlock& out, Block* block,
                               uint32_t rows) override {
        auto& typed = static_cast<ConcreteMethod&>(
                static_cast<JoinHashTableContextImplBase<ConcreteMethod>&>(ht_ctx));
        inner.process_direct_return(typed, out, block, rows);
    }

    Status finish_probing(IJoinHashTableContext& ht_ctx, MutableBlock& out, Block* block, bool* eos,
                          bool is_mark_join) override {
        auto& typed = static_cast<ConcreteMethod&>(
                static_cast<JoinHashTableContextImplBase<ConcreteMethod>&>(ht_ctx));
        return inner.finish_probing(typed, out, block, eos, is_mark_join);
    }

    void close() override {
        if (inner._arena) {
            inner._arena.reset();
        }
    }
};

} // namespace doris
