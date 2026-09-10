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

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace doris::segment_v2::gram {

// Bounds on a solved density, in permille. They exist so a pathological sample cannot produce
// an index that holds nothing or one that holds a gram at every position; they are not a
// tuning range, and a solve landing on either edge means the sample was degenerate.
inline constexpr uint16_t kMinSolvedDensityPermille = 5;
inline constexpr uint16_t kMaxSolvedDensityPermille = 500;

// Accumulates the evidence a density is solved from, one row at a time.
//
// The promise a sparse gram index makes is "a literal of at least `min_literal_len` bytes can
// be found". A window of that many bytes is indexable exactly when it holds a boundary with
// room for a whole gram after it, and a boundary is decided by the identity of a byte pair --
// so the smallest pair hash in a window is the density at which that window starts being
// indexable. The sparsest density keeping the promise for a share q of windows is therefore
// the q-quantile of those minima, and a quantile needs no sample kept: the hashes are 16 bits,
// so a histogram over their whole range answers it exactly in one prefix scan.
//
// That matters on the write path, which is where this runs. Observing a row is linear in its
// length with a monotonic deque, the histogram is a fixed 256 KB whatever the sample size, and
// solving is one scan of 65,536 counters. Nothing is retained per row and nothing is sorted.
class DensitySolver {
public:
    // `max_gram_len` is the scheme's max_gram: a boundary yields a gram only if a whole one
    // fits after it, so the last max_gram_len-1 positions of a window cannot carry coverage.
    // Counting them made an earlier version optimistic -- it asked for 95% of 12-byte literals
    // on URL paths and delivered 86%.
    DensitySolver(size_t min_literal_len, size_t max_gram_len);

    // Feeds one column value. Only ASCII runs contribute, since they are the only bytes the
    // extractor indexes.
    void observe(std::string_view value);

    // Windows seen so far. Zero means nothing of the promised length was present and solve()
    // has no evidence to work from.
    uint64_t observed_windows() const { return _windows; }

    // The sparsest density keeping the promise for `coverage_permille` of the observed
    // windows, clamped to the bounds above. With no evidence it returns the ceiling rather
    // than inventing a rate.
    uint16_t solve(uint32_t coverage_permille) const;

    // Fixed regardless of how much has been observed.
    static constexpr size_t heap_bytes() { return kHashValues * sizeof(uint32_t); }

private:
    static constexpr size_t kHashValues = 1U << 16U;

    size_t _min_literal_len;
    size_t _max_gram_len;
    uint64_t _windows = 0;
    // Counts of per-window minimum hashes, indexed by the hash itself.
    std::vector<uint32_t> _histogram;
    // Scratch reused across rows so observing a row allocates nothing.
    mutable std::vector<uint32_t> _mono;
};

// One-shot convenience over DensitySolver, for tests and offline analysis.
uint16_t solve_density_permille(const std::vector<std::string>& sample_rows, size_t min_literal_len,
                                size_t max_gram_len, uint32_t coverage_permille);

} // namespace doris::segment_v2::gram
