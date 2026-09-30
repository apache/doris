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

#include "storage/index/inverted/gram/gram_density.h"

#include <algorithm>

#include "storage/index/inverted/gram/gram_extractor.h"

namespace doris::segment_v2::gram {

DensitySolver::DensitySolver(size_t min_literal_len, size_t max_gram_len, bool lower_case)
        : _min_literal_len(min_literal_len),
          _max_gram_len(max_gram_len),
          _lower_case(lower_case),
          _histogram(kHashValues, 0) {}

namespace gram_density_detail {
// The bytes the extractor cuts grams from: ASCII, and never NUL -- a candidate gram that
// contains one is dropped, so a literal window that contains one can never be found.
inline bool indexable_byte(unsigned char c) {
    return c < 0x80 && c != 0;
}
} // namespace gram_density_detail

void DensitySolver::observe(std::string_view value) {
    if (_min_literal_len < _max_gram_len || _max_gram_len < 2) {
        return;
    }
    // Boundaries with a whole gram's room after them inside a window: a boundary at k yields
    // [k, k+max_gram_len) at worst, so k may not exceed len - max_gram_len. That leaves
    // `per_window` candidate positions in a window of the promised length.
    const size_t per_window = _min_literal_len - _max_gram_len + 1;
    // The extractor folds ASCII case before it hashes when the scheme says so; the pair
    // hashes that decide a boundary have to be computed over the same bytes here.
    const auto byte_at = [&](std::string_view run, size_t k) -> uint8_t {
        auto c = static_cast<unsigned char>(run[k]);
        if (_lower_case && c >= 'A' && c <= 'Z') {
            c = static_cast<unsigned char>(c - 'A' + 'a');
        }
        return c;
    };

    size_t i = 0;
    const size_t n = value.size();
    while (i < n) {
        // Only runs of indexable bytes: a window spanning anything else could never produce
        // a gram, so it carries no evidence either way.
        if (!gram_density_detail::indexable_byte(static_cast<unsigned char>(value[i]))) {
            ++i;
            continue;
        }
        size_t j = i;
        while (j < n && gram_density_detail::indexable_byte(static_cast<unsigned char>(value[j]))) {
            ++j;
        }
        const std::string_view run = value.substr(i, j - i);
        i = j;
        if (run.size() < _min_literal_len) {
            continue;
        }
        // Sliding minimum over the run's pair hashes, one window per start position. The
        // monotonic queue keeps this linear in the run rather than quadratic in the window,
        // which is what makes it affordable on the write path.
        //
        // A window starting at s spans [s, s + min_literal_len) and its candidate boundary
        // positions are [s, s + per_window); the last real window therefore ends the scan at
        // pair run.size() - max_gram_len, and exactly run.size() - min_literal_len + 1 windows
        // are counted. Running the queue over the remaining pairs would count max_gram_len - 2
        // more windows that extend past the run, which biased the quantile toward the tail.
        const size_t last_pair = run.size() - _max_gram_len;
        _mono.clear();
        size_t head = 0;
        for (size_t p = 0; p <= last_pair; ++p) {
            const uint16_t h = boundary_hash16(byte_at(run, p), byte_at(run, p + 1));
            while (_mono.size() > head && boundary_hash16(byte_at(run, _mono.back()),
                                                          byte_at(run, _mono.back() + 1)) >= h) {
                _mono.pop_back();
            }
            _mono.push_back(static_cast<uint32_t>(p));
            if (p + 1 < per_window) {
                continue;
            }
            const size_t window_start = p + 1 - per_window;
            while (_mono[head] < window_start) {
                ++head;
            }
            const uint16_t min_hash =
                    boundary_hash16(byte_at(run, _mono[head]), byte_at(run, _mono[head] + 1));
            ++_histogram[min_hash];
            ++_windows;
        }
    }
}

DensitySolver::Solution DensitySolver::solve_detailed(uint32_t coverage_permille) const {
    Solution out;
    if (_windows == 0 || coverage_permille == 0) {
        // No evidence, or nothing asked for. Stay where a configured default would have been
        // rather than inventing a rate from nothing.
        out.density_permille = kMaxSolvedDensityPermille;
        out.required_permille = kMaxSolvedDensityPermille;
        return out;
    }
    // A window is covered exactly when its minimum is below the threshold, so the prefix sum
    // of the histogram IS the coverage curve: walk it until the requested share is reached and
    // the threshold that achieves it -- and nothing larger -- is one past that hash.
    const uint64_t need = (static_cast<uint64_t>(coverage_permille) * _windows + 999) / 1000;
    uint64_t seen = 0;
    uint32_t threshold = kHashValues;
    for (size_t h = 0; h < kHashValues; ++h) {
        seen += _histogram[h];
        if (seen >= need) {
            // `is_boundary` compares strictly, so the threshold has to clear this hash.
            threshold = static_cast<uint32_t>(h) + 1;
            break;
        }
    }
    // GramExtractor derives its threshold as density_permille * 65536 / 1000 with integer
    // division; round up or the resolved density can land a notch below what was solved for.
    out.required_permille =
            static_cast<uint32_t>((static_cast<uint64_t>(threshold) * 1000 + 65535) / 65536);
    const uint32_t clamped = std::clamp<uint32_t>(out.required_permille, kMinSolvedDensityPermille,
                                                  kMaxSolvedDensityPermille);
    out.density_permille = static_cast<uint16_t>(clamped);
    out.clamped = clamped != out.required_permille;
    // The share the clamped rate really keeps: the windows whose minimum sits below the
    // threshold GramExtractor will derive from it.
    const size_t clamped_threshold = std::min<size_t>(
            static_cast<size_t>(static_cast<uint64_t>(clamped) * 65536 / 1000), kHashValues);
    uint64_t covered = 0;
    for (size_t h = 0; h < clamped_threshold; ++h) {
        covered += _histogram[h];
    }
    out.achieved_coverage_permille = static_cast<uint32_t>(covered * 1000 / _windows);
    return out;
}

uint16_t DensitySolver::solve(uint32_t coverage_permille) const {
    return solve_detailed(coverage_permille).density_permille;
}

uint16_t solve_density_permille(const std::vector<std::string>& sample_rows, size_t min_literal_len,
                                size_t max_gram_len, uint32_t coverage_permille) {
    DensitySolver solver(min_literal_len, max_gram_len);
    for (const std::string& row : sample_rows) {
        solver.observe(row);
    }
    return solver.solve(coverage_permille);
}

} // namespace doris::segment_v2::gram
