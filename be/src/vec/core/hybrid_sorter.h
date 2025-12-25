/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <pdqsort.h>

#include <iterator>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "timsort.hpp"
namespace doris::vectorized {

enum class SortAlgo { kAuto, kTimSort, kPdqSort };

/// A hybrid sort algorithm with statefull statistics.
/// It will select pdqsort or timsort at runtime according to the statistics.
class HybridSorter {
public:
    HybridSorter() = default;

    // if a specified algorithm is set by Planner, do not probe
    explicit HybridSorter(SortAlgo algo) : algo_(algo), enableProbe_(algo == SortAlgo::kAuto) {}

    template <std::random_access_iterator Iter, typename Compare>
    void sort(Iter first, Iter last, Compare cmp) {
        auto recCnt = std::distance(first, last);

        if (UNLIKELY(batchCnt_ % PROBE_AGAIN_BATCH_NUMBER == 0)) {
            // prepare to probe
            reset();
        }

        // If algo has been determined, or very small batch, no need to probe
        if (algo_ == SortAlgo::kPdqSort ||
            (recCnt < SMALL_BATCH_THRESHOLD && algo_ == SortAlgo::kAuto)) {
            pdqsort(first, last, cmp);
        } else if (algo_ == SortAlgo::kTimSort) {
            gfx::timsort(first, last, cmp);
        } else {
            // At auto-probe stage
            size_t cmpCnt = 0;
            auto cmpWithCnt = [&cmpCnt, &cmp](auto lhs, auto rhs) -> bool {
                ++cmpCnt;
                return static_cast<bool>(std::invoke(cmp, lhs, rhs));
            };

            if (isPdqSortTurn()) {
                pdqsort(first, last, cmpWithCnt);
            } else {
                gfx::timsort(first, last, cmpWithCnt);
            }

            updateMetrics(cmpCnt, std::distance(first, last));

            // If a probe round is finished, each algorithm has been run
            // NUM_PROBE_PER_ROUND times.
            // Now, check if the performance difference is significant
            if (probeRoundsEnd()) {
                checkIfSignificant();
            }
        }

        ++batchCnt_;
    }

    SortAlgo getSortAlgo() const noexcept { return algo_; }

private:
    void checkIfSignificant() {
        //  efficiency = recordCnt / compareCnt
        auto pdqEfficiency = pdqMetric.compareCnt > 0
                                     ? ((double)pdqMetric.recordCnt) / pdqMetric.compareCnt
                                     : -1.0;
        auto timEfficiency = timMetric.compareCnt > 0
                                     ? ((double)timMetric.recordCnt) / timMetric.compareCnt
                                     : -1.0;

        if (pdqEfficiency > timEfficiency * significantThreshold_) {
            algo_ = SortAlgo::kPdqSort;
            VLOG_ROW << "PdqSort is chosen: pdqEfficiency / timEfficiency = "
                     << pdqEfficiency / timEfficiency << std::endl;
            return;
        }
        if (timEfficiency > pdqEfficiency * significantThreshold_) {
            algo_ = SortAlgo::kTimSort;
            VLOG_ROW << "TimSort is chosen: timEfficiency / pdqEfficiency = "
                     << timEfficiency / pdqEfficiency << std::endl;
            return;
        }

        // decay the threshold
        significantThreshold_ *= DECAY_FACTOR;

        // if rounds later, the differece is still not so significant,
        // stop probing, just choose the better one
        if (significantThreshold_ <= 1.0) {
            algo_ = pdqEfficiency >= timEfficiency ? SortAlgo::kPdqSort : SortAlgo::kTimSort;
        }
    }

    void updateMetrics(size_t cmpCnt, size_t recordCnt) {
        if (isPdqSortTurn()) {
            pdqMetric.compareCnt += cmpCnt;
            pdqMetric.recordCnt += recordCnt;
        } else {
            timMetric.compareCnt += cmpCnt;
            timMetric.recordCnt += recordCnt;
        }
    }

    void reset() {
        if (enableProbe_) {
            algo_ = SortAlgo::kAuto;
        }
        significantThreshold_ = SIGNIFICANT_THRESHOLD;
        pdqMetric.compareCnt = 0;
        pdqMetric.recordCnt = 0;
        timMetric.compareCnt = 0;
        timMetric.recordCnt = 0;
    }

    inline bool isPdqSortTurn() const noexcept { return batchCnt_ % 2 == 0; }

    inline bool probeRoundsEnd() const noexcept {
        return (batchCnt_ + 1) % (NUM_PROBE_PER_ROUND * 2) == 0;
    }

private:
    // Magic numbers
    constexpr static size_t NUM_PROBE_PER_ROUND = 4;
    constexpr static size_t PROBE_AGAIN_BATCH_NUMBER = NUM_PROBE_PER_ROUND * 128;
    constexpr static double DECAY_FACTOR = 0.9;
    constexpr static double SIGNIFICANT_THRESHOLD = 1.4;
    constexpr static size_t SMALL_BATCH_THRESHOLD = 128;

    SortAlgo algo_ {SortAlgo::kAuto};
    bool enableProbe_ {true};

    // Stateful stastics info:
    double significantThreshold_ {SIGNIFICANT_THRESHOLD};

    struct CompareMetric {
        size_t compareCnt {0};
        size_t recordCnt {0};
    };
    CompareMetric timMetric;
    CompareMetric pdqMetric;

    size_t batchCnt_ {0};
};

} // namespace doris::vectorized