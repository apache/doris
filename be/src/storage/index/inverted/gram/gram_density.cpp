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

DensitySolver::DensitySolver(size_t min_literal_len, size_t max_gram_len)
        : _min_literal_len(min_literal_len),
          _max_gram_len(max_gram_len),
          _histogram(kHashValues, 0) {}

void DensitySolver::observe(std::string_view value) {
    if (_min_literal_len < _max_gram_len || _max_gram_len < 2) {
        return;
    }
    // Boundaries with a whole gram's room after them inside a window: a boundary at k yields
    // [k, k+max_gram_len) at worst, so k may not exceed len - max_gram_len. That leaves
    // `per_window` candidate positions in a window of the promised length.
    const size_t per_window = _min_literal_len - _max_gram_len + 1;

    size_t i = 0;
    const size_t n = value.size();
    while (i < n) {
        // Only ASCII runs: they are the only bytes the extractor indexes, so a window
        // spanning anything else could never produce a gram.
        if (static_cast<unsigned char>(value[i]) >= 0x80) {
            ++i;
            continue;
        }
        size_t j = i;
        while (j < n && static_cast<unsigned char>(value[j]) < 0x80) {
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
        const size_t pairs = run.size() - 1;
        _mono.clear();
        size_t head = 0;
        for (size_t p = 0; p < pairs; ++p) {
            const uint16_t h =
                    boundary_hash16(static_cast<uint8_t>(run[p]), static_cast<uint8_t>(run[p + 1]));
            while (_mono.size() > head &&
                   boundary_hash16(static_cast<uint8_t>(run[_mono.back()]),
                                   static_cast<uint8_t>(run[_mono.back() + 1])) >= h) {
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
            const uint16_t min_hash = boundary_hash16(static_cast<uint8_t>(run[_mono[head]]),
                                                      static_cast<uint8_t>(run[_mono[head] + 1]));
            ++_histogram[min_hash];
            ++_windows;
        }
    }
}

uint16_t DensitySolver::solve(uint32_t coverage_permille) const {
    if (_windows == 0 || coverage_permille == 0) {
        // No evidence, or nothing asked for. Stay where a configured default would have been
        // rather than inventing a rate from nothing.
        return kMaxSolvedDensityPermille;
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
    uint32_t permille =
            static_cast<uint32_t>((static_cast<uint64_t>(threshold) * 1000 + 65535) / 65536);
    permille = std::clamp<uint32_t>(permille, kMinSolvedDensityPermille, kMaxSolvedDensityPermille);
    return static_cast<uint16_t>(permille);
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
