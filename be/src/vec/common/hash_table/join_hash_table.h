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

#include <gen_cpp/PlanNodes_types.h>

#include "vec/columns/column_filter_helper.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/common/hash_table/hash_table_allocator.h"

namespace doris {
template <typename Key, typename Hash = DefaultHash<Key>>
class JoinHashTable {
public:
    using key_type = Key;
    using mapped_type = void*;
    using value_type = void*;
    size_t hash(const Key& x) const { return Hash()(x); }

    static uint32_t calc_bucket_size(size_t num_elem) {
        size_t expect_bucket_size = num_elem + (num_elem - 1) / 7;
        return phmap::priv::NormalizeCapacity(expect_bucket_size) + 1;
    }

    size_t get_byte_size() const {
        auto cal_vector_mem = [](const auto& vec) { return vec.capacity() * sizeof(vec[0]); };
        return cal_vector_mem(visited) + cal_vector_mem(first) + cal_vector_mem(next);
    }

    template <int JoinOpType>
    void prepare_build(size_t num_elem, int batch_size, bool has_null_key) {
        _has_null_key = has_null_key;

        // the first row in build side is not really from build side table
        _empty_build_side = num_elem <= 1;
        max_batch_size = batch_size;
        bucket_size = calc_bucket_size(num_elem + 1);
        first.resize(bucket_size + 1);
        next.resize(num_elem);

        if constexpr (JoinOpType == TJoinOp::FULL_OUTER_JOIN ||
                      JoinOpType == TJoinOp::RIGHT_OUTER_JOIN ||
                      JoinOpType == TJoinOp::RIGHT_ANTI_JOIN ||
                      JoinOpType == TJoinOp::RIGHT_SEMI_JOIN) {
            visited.resize(num_elem);
        }
    }

    uint32_t get_bucket_size() const { return bucket_size; }

    size_t size() const { return next.size(); }

    std::vector<uint8_t>& get_visited() { return visited; }

    void build(const Key* __restrict keys, const uint32_t* __restrict bucket_nums,
               size_t num_elem) {
        build_keys = keys;
        for (size_t i = 1; i < num_elem; i++) {
            uint32_t bucket_num = bucket_nums[i];
            next[i] = first[bucket_num];
            first[bucket_num] = i;
        }
        first[bucket_size] = 0; // index = bucket_num means null
    }

    template <int JoinOpType, bool with_other_conjuncts, bool is_mark_join, bool need_judge_null>
    auto find_batch(const Key* __restrict keys, const uint32_t* __restrict build_idx_map,
                    int probe_idx, uint32_t build_idx, int probe_rows,
                    uint32_t* __restrict probe_idxs, bool& probe_visited,
                    uint32_t* __restrict build_idxs, vectorized::ColumnFilterHelper* mark_column) {
        if constexpr (JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            if (_empty_build_side) {
                return _process_null_aware_left_anti_join_for_empty_build_side<
                        JoinOpType, with_other_conjuncts, is_mark_join>(
                        probe_idx, probe_rows, probe_idxs, build_idxs, mark_column);
            }
        }

        if constexpr (with_other_conjuncts || is_mark_join) {
            return _find_batch_conjunct<JoinOpType, need_judge_null>(
                    keys, build_idx_map, probe_idx, build_idx, probe_rows, probe_idxs, build_idxs);
        }

        if constexpr (JoinOpType == TJoinOp::INNER_JOIN || JoinOpType == TJoinOp::FULL_OUTER_JOIN ||
                      JoinOpType == TJoinOp::LEFT_OUTER_JOIN ||
                      JoinOpType == TJoinOp::RIGHT_OUTER_JOIN) {
            return _find_batch_inner_outer_join<JoinOpType>(keys, build_idx_map, probe_idx,
                                                            build_idx, probe_rows, probe_idxs,
                                                            probe_visited, build_idxs);
        }
        if constexpr (JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                      JoinOpType == TJoinOp::LEFT_SEMI_JOIN ||
                      JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            return _find_batch_left_semi_anti<JoinOpType, need_judge_null>(
                    keys, build_idx_map, probe_idx, probe_rows, probe_idxs);
        }
        if constexpr (JoinOpType == TJoinOp::RIGHT_ANTI_JOIN ||
                      JoinOpType == TJoinOp::RIGHT_SEMI_JOIN) {
            return _find_batch_right_semi_anti(keys, build_idx_map, probe_idx, probe_rows);
        }
        return std::tuple {0, 0U, 0};
    }

    /**
     * Because the equality comparison result of null with any value is null,
     * in null aware join, if the probe key of a row in the left table(probe side) is null,
     * then this row will match all rows on the right table(build side) (the match result is null).
     * If the probe key of a row in the left table does not match any row in right table,
     * this row will match all rows with null key in the right table.
     * select 'a' in ('b', null) => 'a' = 'b' or 'a' = null => false or null => null
     * select 'a' in ('a', 'b', null) => true
     * select 'a' not in ('b', null) => null => 'a' != 'b' and 'a' != null => true and null => null
     * select 'a' not in ('a', 'b', null) => false
     */
    auto find_null_aware_with_other_conjuncts(
            const Key* __restrict keys, const uint32_t* __restrict build_idx_map, int probe_idx,
            uint32_t build_idx, int probe_rows, uint32_t* __restrict probe_idxs,
            uint32_t* __restrict build_idxs, std::set<uint32_t>& null_result,
            const std::vector<uint32_t>& build_indexes_null, const size_t build_block_count) {
        auto matched_cnt = 0;
        const auto batch_size = max_batch_size;

        bool has_matched = false;
        auto do_the_probe = [&]() {
            while (build_idx && matched_cnt < batch_size) {
                if (build_idx == bucket_size) {
                    /// All rows in build side should be executed with other join conjuncts.
                    for (size_t i = 1; i != build_block_count; ++i) {
                        build_idxs[matched_cnt] = i;
                        probe_idxs[matched_cnt] = probe_idx;
                        matched_cnt++;
                    }
                    null_result.emplace(probe_idx);
                    build_idx = 0;
                    has_matched = true;
                    break;
                } else if (keys[probe_idx] == build_keys[build_idx]) {
                    build_idxs[matched_cnt] = build_idx;
                    probe_idxs[matched_cnt] = probe_idx;
                    matched_cnt++;
                    has_matched = true;
                }

                build_idx = next[build_idx];
            }

            // may over batch_size when emplace 0 into build_idxs
            if (!build_idx) {
                if (!has_matched) { // has no any row matched
                    for (auto index : build_indexes_null) {
                        build_idxs[matched_cnt] = index;
                        probe_idxs[matched_cnt] = probe_idx;
                        matched_cnt++;
                    }
                }
                probe_idxs[matched_cnt] = probe_idx;
                build_idxs[matched_cnt] = 0;
                matched_cnt++;
                has_matched = false;
            }

            probe_idx++;
        };

        if (build_idx) {
            do_the_probe();
        }

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            build_idx = build_idx_map[probe_idx];
            do_the_probe();
        }

        probe_idx -= (build_idx != 0);
        return std::tuple {probe_idx, build_idx, matched_cnt};
    }

    template <int JoinOpType>
    bool iterate_map(std::vector<uint32_t>& build_idxs) const {
        const auto batch_size = max_batch_size;
        const auto elem_num = visited.size();
        int count = 0;
        build_idxs.resize(batch_size);

        while (count < batch_size && iter_idx < elem_num) {
            const auto matched = visited[iter_idx];
            build_idxs[count] = iter_idx;
            if constexpr (JoinOpType != TJoinOp::RIGHT_SEMI_JOIN) {
                count += !matched;
            } else {
                count += matched;
            }
            iter_idx++;
        }

        build_idxs.resize(count);
        return iter_idx >= elem_num;
    }

    bool has_null_key() { return _has_null_key; }

    void pre_build_idxs(std::vector<uint32>& bucksets, const uint8_t* null_map) {
        if (null_map) {
            first[bucket_size] = bucket_size; // distinguish between not matched and null
        }

        for (uint32_t i = 0; i < bucksets.size(); i++) {
            bucksets[i] = first[bucksets[i]];
        }
    }

private:
    template <int JoinOpType, bool with_other_conjuncts, bool is_mark_join>
    auto _process_null_aware_left_anti_join_for_empty_build_side(
            int probe_idx, int probe_rows, uint32_t* __restrict probe_idxs,
            uint32_t* __restrict build_idxs, vectorized::ColumnFilterHelper* mark_column) {
        static_assert(JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN);
        auto matched_cnt = 0;
        const auto batch_size = max_batch_size;

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            probe_idxs[matched_cnt] = probe_idx++;
            build_idxs[matched_cnt] = 0;
            ++matched_cnt;
        }

        if constexpr (is_mark_join && !with_other_conjuncts) {
            // we will flip the mark column later for anti join, so here set 0 into mark column.
            mark_column->resize_fill(matched_cnt, 0);
        }

        return std::tuple {probe_idx, 0U, matched_cnt};
    }

    auto _find_batch_right_semi_anti(const Key* __restrict keys,
                                     const uint32_t* __restrict build_idx_map, int probe_idx,
                                     int probe_rows) {
        while (probe_idx < probe_rows) {
            auto build_idx = build_idx_map[probe_idx];

            while (build_idx) {
                if (!visited[build_idx] && keys[probe_idx] == build_keys[build_idx]) {
                    visited[build_idx] = 1;
                }
                build_idx = next[build_idx];
            }
            probe_idx++;
        }
        return std::tuple {probe_idx, 0U, 0};
    }

    template <int JoinOpType, bool need_judge_null>
    auto _find_batch_left_semi_anti(const Key* __restrict keys,
                                    const uint32_t* __restrict build_idx_map, int probe_idx,
                                    int probe_rows, uint32_t* __restrict probe_idxs) {
        auto matched_cnt = 0;
        const auto batch_size = max_batch_size;

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            if constexpr (need_judge_null) {
                if (build_idx_map[probe_idx] == bucket_size) {
                    probe_idx++;
                    continue;
                }
            }

            auto build_idx = build_idx_map[probe_idx];

            while (build_idx && keys[probe_idx] != build_keys[build_idx]) {
                build_idx = next[build_idx];
            }
            bool matched = JoinOpType == TJoinOp::LEFT_SEMI_JOIN ? build_idx != 0 : build_idx == 0;
            probe_idxs[matched_cnt] = probe_idx++;
            matched_cnt += matched;
        }
        return std::tuple {probe_idx, 0U, matched_cnt};
    }

    template <int JoinOpType, bool need_judge_null>
    auto _find_batch_conjunct(const Key* __restrict keys, const uint32_t* __restrict build_idx_map,
                              int probe_idx, uint32_t build_idx, int probe_rows,
                              uint32_t* __restrict probe_idxs, uint32_t* __restrict build_idxs) {
        auto matched_cnt = 0;
        const auto batch_size = max_batch_size;

        auto do_the_probe = [&]() {
            while (build_idx && matched_cnt < batch_size) {
                if constexpr (JoinOpType == TJoinOp::RIGHT_ANTI_JOIN ||
                              JoinOpType == TJoinOp::RIGHT_SEMI_JOIN) {
                    if (!visited[build_idx] && keys[probe_idx] == build_keys[build_idx]) {
                        probe_idxs[matched_cnt] = probe_idx;
                        build_idxs[matched_cnt] = build_idx;
                        matched_cnt++;
                    }
                } else if constexpr (need_judge_null) {
                    if (build_idx == bucket_size) {
                        build_idxs[matched_cnt] = build_idx;
                        probe_idxs[matched_cnt] = probe_idx;
                        build_idx = 0;
                        matched_cnt++;
                        break;
                    }
                }

                if (keys[probe_idx] == build_keys[build_idx]) {
                    build_idxs[matched_cnt] = build_idx;
                    probe_idxs[matched_cnt] = probe_idx;
                    matched_cnt++;
                }
                build_idx = next[build_idx];
            }

            if constexpr (JoinOpType == TJoinOp::LEFT_OUTER_JOIN ||
                          JoinOpType == TJoinOp::FULL_OUTER_JOIN ||
                          JoinOpType == TJoinOp::LEFT_SEMI_JOIN ||
                          JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                          JoinOpType == doris::TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                          JoinOpType == doris::TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN) {
                // may over batch_size when emplace 0 into build_idxs
                if (!build_idx) {
                    probe_idxs[matched_cnt] = probe_idx;
                    build_idxs[matched_cnt] = 0;
                    matched_cnt++;
                }
            }

            probe_idx++;
        };

        if (build_idx) {
            do_the_probe();
        }

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            build_idx = build_idx_map[probe_idx];
            do_the_probe();
        }

        probe_idx -= (build_idx != 0);
        return std::tuple {probe_idx, build_idx, matched_cnt};
    }

    template <int JoinOpType>
    auto _find_batch_inner_outer_join(const Key* __restrict keys,
                                      const uint32_t* __restrict build_idx_map, int probe_idx,
                                      uint32_t build_idx, int probe_rows,
                                      uint32_t* __restrict probe_idxs, bool& probe_visited,
                                      uint32_t* __restrict build_idxs) {
        auto matched_cnt = 0;
        const auto batch_size = max_batch_size;

        auto do_the_probe = [&]() {
            while (build_idx && matched_cnt < batch_size) {
                if (keys[probe_idx] == build_keys[build_idx]) {
                    probe_idxs[matched_cnt] = probe_idx;
                    build_idxs[matched_cnt] = build_idx;
                    matched_cnt++;
                    if constexpr (JoinOpType == TJoinOp::RIGHT_OUTER_JOIN ||
                                  JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
                        if (!visited[build_idx]) {
                            visited[build_idx] = 1;
                        }
                    }
                }
                build_idx = next[build_idx];
            }

            if constexpr (JoinOpType == TJoinOp::LEFT_OUTER_JOIN ||
                          JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
                // `(!matched_cnt || probe_idxs[matched_cnt - 1] != probe_idx)` means not match one build side
                probe_visited |= (matched_cnt && probe_idxs[matched_cnt - 1] == probe_idx);
                if (!build_idx) {
                    if (!probe_visited) {
                        probe_idxs[matched_cnt] = probe_idx;
                        build_idxs[matched_cnt] = 0;
                        matched_cnt++;
                    }
                    probe_visited = false;
                }
            }
            probe_idx++;
        };

        if (build_idx) {
            do_the_probe();
        }

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            build_idx = build_idx_map[probe_idx];
            do_the_probe();
        }

        probe_idx -= (build_idx != 0);
        return std::tuple {probe_idx, build_idx, matched_cnt};
    }

    const Key* __restrict build_keys;
    std::vector<uint8_t> visited;

    uint32_t bucket_size = 1;
    int max_batch_size = 4064;

    std::vector<uint32_t> first = {0};
    std::vector<uint32_t> next = {0};

    // use in iter hash map
    mutable uint32_t iter_idx = 1;
    vectorized::Arena* pool;
    bool _has_null_key = false;
    bool _empty_build_side = true;
};
} // namespace doris