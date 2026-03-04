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

// This file is copied from
// https://github.com/bytedance/bolt/blob/b68480b45bbf8086add7bd1e64ad0ee6557198dd/bolt/exec/HybridSorter.h
// and modified by Doris
//
// Original Copyright:
// Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
// Licensed under the Apache License, Version 2.0

#pragma once
#include <pdqsort.h>

#include <gfx/timsort.hpp>
#include <iterator>

#include "common/compiler_util.h"
#include "common/logging.h"
namespace doris::vectorized {

// HybridSorter samples during runtime and selects an appropriate sort algorithm.
// It compares PdqSort and TimSort performance dynamically and chooses the most efficient one
// based on the characteristics of the input data.
class HybridSorter {
public:
    // Sorting algorithm options: Auto (auto-select), TimSort, or PdqSort
    enum class SortAlgo { Auto, TimSort, PdqSort };

    HybridSorter() : _enable_use_hybrid_sort(false) {};

    HybridSorter(bool enable_use_hybrid_sort) : _enable_use_hybrid_sort(enable_use_hybrid_sort) {};

    // Main sorting function: sorts elements in the range [first, last) using the comparator cmp.
    // Automatically selects between PdqSort and TimSort based on runtime profiling.
    template <std::random_access_iterator Iter, typename Compare>
    void sort(Iter first, Iter last, Compare cmp) {
        // If hybrid sorting is disabled, directly use PdqSort
        if (!_enable_use_hybrid_sort) {
            pdqsort(first, last, cmp);
            return;
        }

        auto rec_cnt = std::distance(first, last);

        // Periodically reset and re-probe to adapt to changing data patterns
        if (UNLIKELY(block_cnt % PROBE_AGAIN_BATCH_NUMBER == 0)) {
            // Prepare to probe: reset metrics and algorithm selection
            reset();
        }

        // If algo has been determined, or very small batch, no need to probe
        if (algo == SortAlgo::PdqSort ||
            (rec_cnt < SMALL_BATCH_THRESHOLD && algo == SortAlgo::Auto)) {
            pdqsort(first, last, cmp);
        } else if (algo == SortAlgo::TimSort) {
            gfx::timsort(first, last, cmp);
        } else {
            // At auto-probe stage: alternately test PdqSort and TimSort
            // to collect performance metrics for algorithm selection
            size_t cmp_cnt = 0;
            // Wrapper comparator that counts comparison operations
            auto cmpWithCnt = [&cmp_cnt, &cmp](auto lhs, auto rhs) -> bool {
                ++cmp_cnt;
                return static_cast<bool>(std::invoke(cmp, lhs, rhs));
            };

            if (is_pdq_sort_turn()) {
                pdqsort(first, last, cmpWithCnt);
            } else {
                gfx::timsort(first, last, cmpWithCnt);
            }

            update_metrics(cmp_cnt, std::distance(first, last));

            // If a probe round is finished, each algorithm has been run
            // NUM_PROBE_PER_ROUND times.
            // Now, check if the performance difference is significant
            if (probe_rounds_end()) {
                check_if_significant();
            }
        }

        ++block_cnt;
    }

    // Returns the currently selected sorting algorithm
    SortAlgo get_sort_algo() const noexcept { return algo; }

private:
    // Checks if the performance difference between PdqSort and TimSort is significant.
    // If one algorithm is significantly better (by significantThreshold_), it is selected.
    // Otherwise, the threshold is decayed to make selection easier in subsequent rounds.
    void check_if_significant() {
        // Calculate efficiency: higher ratio means fewer comparisons per record (better)
        // efficiency = record_cnt / compare_cnt
        auto pdqEfficiency = pdq_metric.compare_cnt > 0
                                     ? ((double)pdq_metric.intput_cnt) / pdq_metric.compare_cnt
                                     : -1.0;
        auto timEfficiency = tim_metric.compare_cnt > 0
                                     ? ((double)tim_metric.intput_cnt) / tim_metric.compare_cnt
                                     : -1.0;

        if (pdqEfficiency > timEfficiency * significantThreshold_) {
            algo = SortAlgo::PdqSort;
            VLOG_ROW << "PdqSort is chosen: pdqEfficiency / timEfficiency = "
                     << pdqEfficiency / timEfficiency << std::endl;
            return;
        }
        if (timEfficiency > pdqEfficiency * significantThreshold_) {
            algo = SortAlgo::TimSort;
            VLOG_ROW << "TimSort is chosen: timEfficiency / pdqEfficiency = "
                     << timEfficiency / pdqEfficiency << std::endl;
            return;
        }

        // Decay the threshold to make algorithm selection easier over time
        significantThreshold_ *= DECAY_FACTOR;

        // If after multiple rounds the difference is still not significant enough,
        // stop probing and simply choose the algorithm with better efficiency
        if (significantThreshold_ <= 1.0) {
            algo = pdqEfficiency >= timEfficiency ? SortAlgo::PdqSort : SortAlgo::TimSort;
        }
    }

    // Updates performance metrics for the algorithm that was just used.
    // Records the number of comparisons and records processed.
    void update_metrics(size_t cmp_cnt, size_t record_cnt) {
        if (is_pdq_sort_turn()) {
            pdq_metric.compare_cnt += cmp_cnt;
            pdq_metric.intput_cnt += record_cnt;
        } else {
            tim_metric.compare_cnt += cmp_cnt;
            tim_metric.intput_cnt += record_cnt;
        }
    }

    // Resets the sorter state to begin a new probing cycle.
    // Clears all metrics and resets the algorithm to Auto mode.
    void reset() {
        algo = SortAlgo::Auto;
        significantThreshold_ = SIGNIFICANT_THRESHOLD;
        pdq_metric.compare_cnt = 0;
        pdq_metric.intput_cnt = 0;
        tim_metric.compare_cnt = 0;
        tim_metric.intput_cnt = 0;
    }

    // Returns true if it's PdqSort's turn in the alternating probe pattern
    inline bool is_pdq_sort_turn() const noexcept { return block_cnt % 2 == 0; }

    // Returns true if a complete probe round has finished
    // (both algorithms have been tested NUM_PROBE_PER_ROUND times)
    inline bool probe_rounds_end() const noexcept {
        return (block_cnt + 1) % (NUM_PROBE_PER_ROUND * 2) == 0;
    }

private:
    // Configuration constants
    constexpr static size_t NUM_PROBE_PER_ROUND =
            4; // Number of times to test each algorithm per round
    constexpr static size_t PROBE_AGAIN_BATCH_NUMBER =
            NUM_PROBE_PER_ROUND * 128;          // Batches before re-probing
    constexpr static double DECAY_FACTOR = 0.9; // Threshold decay rate per round
    constexpr static double SIGNIFICANT_THRESHOLD =
            1.4; // Initial threshold for significant difference (40%)
    constexpr static size_t SMALL_BATCH_THRESHOLD =
            128; // Skip probing for batches smaller than this

    // Currently selected algorithm (Auto, TimSort, or PdqSort)
    SortAlgo algo {SortAlgo::Auto};

    // Stateful statistics info:
    double significantThreshold_ {
            SIGNIFICANT_THRESHOLD}; // Current threshold for algorithm selection

    // Metrics structure for tracking algorithm performance
    struct CompareMetric {
        size_t compare_cnt {0}; // Total number of comparisons performed
        size_t intput_cnt {0};  // Total number of records processed
    };
    CompareMetric tim_metric; // Performance metrics for TimSort
    CompareMetric pdq_metric; // Performance metrics for PdqSort

    size_t block_cnt {0}; // Counter for sorting batches processed

    const bool _enable_use_hybrid_sort; // Flag to enable/disable hybrid sorting
};

} // namespace doris::vectorized