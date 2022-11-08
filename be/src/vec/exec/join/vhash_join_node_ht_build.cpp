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

#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_filter_mgr.h"
#include "util/defer_op.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/template_helpers.hpp"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
static constexpr int PREFETCH_STEP = 64;
using ProfileCounter = RuntimeProfile::Counter;
template <class HashTableContext>
struct ProcessHashTableBuild {
    ProcessHashTableBuild(int rows, Block& acquired_block, ColumnRawPtrs& build_raw_ptrs,
                          HashJoinNode* join_node, int batch_size, uint8_t offset)
            : _rows(rows),
              _skip_rows(0),
              _acquired_block(acquired_block),
              _build_raw_ptrs(build_raw_ptrs),
              _join_node(join_node),
              _batch_size(batch_size),
              _offset(offset),
              _build_side_compute_hash_timer(join_node->_build_side_compute_hash_timer) {}

    template <bool need_null_map_for_build, bool ignore_null, bool build_unique,
              bool has_runtime_filter, bool short_circuit_for_null>
    void run(HashTableContext& hash_table_ctx, ConstNullMapPtr null_map, bool* has_null_key) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;
        int64_t old_bucket_bytes = hash_table_ctx.hash_table.get_buffer_size_in_bytes();

        Defer defer {[&]() {
            int64_t bucket_size = hash_table_ctx.hash_table.get_buffer_size_in_cells();
            int64_t bucket_bytes = hash_table_ctx.hash_table.get_buffer_size_in_bytes();
            _join_node->_mem_used += bucket_bytes - old_bucket_bytes;
            COUNTER_SET(_join_node->_build_buckets_counter, bucket_size);
        }};

        KeyGetter key_getter(_build_raw_ptrs, _join_node->_build_key_sz, nullptr);

        SCOPED_TIMER(_join_node->_build_table_insert_timer);
        // only not build_unique, we need expanse hash table before insert data
        if constexpr (!build_unique) {
            // _rows contains null row, which will cause hash table resize to be large.
            hash_table_ctx.hash_table.expanse_for_add_elem(_rows);
        }
        hash_table_ctx.hash_table.reset_resize_timer();

        vector<int>& inserted_rows = _join_node->_inserted_rows[&_acquired_block];
        if constexpr (has_runtime_filter) {
            inserted_rows.reserve(_batch_size);
        }

        _build_side_hash_values.resize(_rows);
        auto& arena = _join_node->_arena;
        {
            SCOPED_TIMER(_build_side_compute_hash_timer);
            for (size_t k = 0; k < _rows; ++k) {
                if constexpr (ignore_null && need_null_map_for_build) {
                    if ((*null_map)[k]) {
                        continue;
                    }
                }
                // If apply short circuit strategy for null value (e.g. join operator is
                // NULL_AWARE_LEFT_ANTI_JOIN), we build hash table until we meet a null value.
                if constexpr (short_circuit_for_null && need_null_map_for_build) {
                    if ((*null_map)[k]) {
                        DCHECK(has_null_key);
                        *has_null_key = true;
                        return;
                    }
                }
                if constexpr (IsSerializedHashTableContextTraits<KeyGetter>::value) {
                    _build_side_hash_values[k] =
                            hash_table_ctx.hash_table.hash(key_getter.get_key_holder(k, arena).key);
                } else {
                    _build_side_hash_values[k] =
                            hash_table_ctx.hash_table.hash(key_getter.get_key_holder(k, arena));
                }
            }
        }

        for (size_t k = 0; k < _rows; ++k) {
            if constexpr (ignore_null && need_null_map_for_build) {
                if ((*null_map)[k]) {
                    continue;
                }
            }

            auto emplace_result = key_getter.emplace_key(hash_table_ctx.hash_table,
                                                         _build_side_hash_values[k], k, arena);
            if (k + PREFETCH_STEP < _rows) {
                key_getter.template prefetch_by_hash<false>(
                        hash_table_ctx.hash_table, _build_side_hash_values[k + PREFETCH_STEP]);
            }

            if (emplace_result.is_inserted()) {
                new (&emplace_result.get_mapped()) Mapped({k, _offset});
                if constexpr (has_runtime_filter) {
                    inserted_rows.push_back(k);
                }
            } else {
                if constexpr (!build_unique) {
                    /// The first element of the list is stored in the value of the hash table, the rest in the pool.
                    emplace_result.get_mapped().insert({k, _offset}, _join_node->_arena);
                    if constexpr (has_runtime_filter) {
                        inserted_rows.push_back(k);
                    }
                } else {
                    _skip_rows++;
                }
            }
        }

        COUNTER_UPDATE(_join_node->_build_table_expanse_timer,
                       hash_table_ctx.hash_table.get_resize_timer_value());
    }

private:
    const int _rows;
    int _skip_rows;
    Block& _acquired_block;
    ColumnRawPtrs& _build_raw_ptrs;
    HashJoinNode* _join_node;
    int _batch_size;
    uint8_t _offset;

    ProfileCounter* _build_side_compute_hash_timer;
    std::vector<size_t> _build_side_hash_values;
};

void HashJoinNode::_hash_table_build(RuntimeState* state, Block& block, ColumnRawPtrs& raw_ptrs,
                                     uint8_t offset, ColumnUInt8::MutablePtr& null_map_val) {
    size_t rows = block.rows();
    bool has_runtime_filter = !_runtime_filter_descs.empty();

    std::visit(
            [&](auto&& arg, auto has_null_value, auto build_unique, auto has_runtime_filter_value,
                auto need_null_map_for_build, auto short_circuit_for_null_in_build_side) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    ProcessHashTableBuild<HashTableCtxType> hash_table_build_process(
                            rows, block, raw_ptrs, this, state->batch_size(), offset);
                    hash_table_build_process.template run<need_null_map_for_build, has_null_value,
                                                          build_unique, has_runtime_filter_value,
                                                          short_circuit_for_null_in_build_side>(
                            arg, need_null_map_for_build ? &null_map_val->get_data() : nullptr,
                            &_short_circuit_for_null_in_probe_side);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants, make_bool_variant(_build_side_ignore_null),
            make_bool_variant(_build_unique), make_bool_variant(has_runtime_filter),
            make_bool_variant(_need_null_map_for_build),
            make_bool_variant(_short_circuit_for_null_in_build_side));
}

} // namespace doris::vectorized