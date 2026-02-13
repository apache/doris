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

#include <algorithm>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

namespace doris::vectorized {

/// Manages the read order of predicate columns in lazy-read mode.
///
/// During the first EXPLORATION_ROUNDS batches, it tries random column orders
/// and records which order yields the lowest "round cost" (i.e., fewest rows
/// that survive after each column's filter). After exploration, it locks in
/// the best order found and uses it for all subsequent batches.
class ColumnReadOrderCtx {
public:
    /// @param col_indices  Indices into the predicate_columns arrays (0-based).
    /// @param col_cost_map Index -> estimated per-row decode cost (e.g., type_length).
    /// @param total_cost   Sum of all column costs (initial upper bound for round cost).
    ColumnReadOrderCtx(std::vector<size_t> col_indices,
                       std::unordered_map<size_t, size_t> col_cost_map, size_t total_cost)
            : _best_order(std::move(col_indices)),
              _col_cost_map(std::move(col_cost_map)),
              _min_round_cost(total_cost) {}

    /// Returns the column read order for the current batch.
    /// During exploration, returns a random permutation; afterwards, the best order.
    const std::vector<size_t>& get_column_read_order() {
        if (_exploration_remaining > 0) {
            _trying_order = _best_order;
            std::shuffle(_trying_order.begin(), _trying_order.end(),
                         std::mt19937(std::random_device {}()));
            return _trying_order;
        }
        return _best_order;
    }

    /// Called after each batch to record cost metrics.
    /// @param round_cost      Accumulated cost for this batch (weighted by rows decoded).
    /// @param first_selectivity  Fraction of rows surviving after the first column's filter.
    void update(size_t round_cost, double first_selectivity) {
        if (_exploration_remaining > 0) {
            if (round_cost < _min_round_cost ||
                (round_cost == _min_round_cost && first_selectivity > 0 &&
                 first_selectivity < _best_first_selectivity)) {
                _best_order = _trying_order;
                _min_round_cost = round_cost;
                _best_first_selectivity = first_selectivity;
            }
            _trying_order.clear();
            _exploration_remaining--;
        }
    }

    size_t get_column_cost(size_t col_index) const {
        auto it = _col_cost_map.find(col_index);
        return it != _col_cost_map.end() ? it->second : 0;
    }

    bool in_exploration() const { return _exploration_remaining > 0; }

private:
    static constexpr int EXPLORATION_ROUNDS = 10;

    std::vector<size_t> _best_order;
    std::vector<size_t> _trying_order;
    std::unordered_map<size_t, size_t> _col_cost_map; // col_index -> per-row cost
    size_t _min_round_cost;
    double _best_first_selectivity = 1.0;
    int _exploration_remaining = EXPLORATION_ROUNDS;
};

} // namespace doris::vectorized
