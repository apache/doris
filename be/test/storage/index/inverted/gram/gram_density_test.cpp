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
//
// Density is solved from the column, not chosen for it.
//
// The knob was a configured number, and every value picked for it was wrong somewhere: 0.25
// costs more to write and store than the data needs, 0.10 silently stops indexing a 21-byte
// literal. Both failures come from the same mistake -- a boundary rate is being set without
// looking at the bytes it will be applied to, and the same rate lands differently on
// different data. Measured at a nominal 0.25, the realised grams per byte were 0.215 on log
// text, 0.288 on URL paths and 0.183 on agent traces: a 1.6x spread from one number.
//
// What the index actually promises is "a literal of at least L bytes can be found". Given
// that promise, the rate is not a choice. A window of L bytes is indexable exactly when it
// contains a boundary, a boundary is decided by the identity of a byte pair, so the smallest
// hash among a window's pairs decides the rate at which that window becomes indexable. Take
// those minima over the column's own text and the sparsest rate keeping the promise for a
// share q of windows is the q-quantile of them. One pass, exact, and it comes out different
// on different data because the data is different.

#include "storage/index/inverted/gram/gram_density.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/gram/gram_extractor.h"
#include "storage/index/inverted/gram/gram_scheme.h"

namespace doris::segment_v2::gram {
namespace {

GramScheme scheme_at(uint16_t density_permille) {
    GramScheme s;
    s.mode = GramMode::SPARSE;
    s.min_len = 3;
    s.max_len = 4;
    s.density_permille = density_permille;
    return s;
}

// The share of L-byte windows of `rows` that contain at least one boundary at this density --
// i.e. the share of literals of that length the index would be able to serve.
double covered_share(const std::vector<std::string>& rows, size_t len, uint16_t density) {
    GramExtractor extractor(scheme_at(density));
    size_t total = 0;
    size_t covered = 0;
    for (const std::string& row : rows) {
        if (row.size() < len) {
            continue;
        }
        for (size_t start = 0; start + len <= row.size(); ++start) {
            ++total;
            bool has = false;
            // Only positions with a whole gram's room left inside the window: a boundary
            // nearer the end than max_len emits nothing, so counting it would credit
            // coverage the extractor does not actually deliver.
            for (size_t i = start; i + 4 <= start + len; ++i) {
                if (extractor.is_boundary(static_cast<uint8_t>(row[i]),
                                          static_cast<uint8_t>(row[i + 1]))) {
                    has = true;
                    break;
                }
            }
            covered += has ? 1 : 0;
        }
    }
    return total == 0 ? 0.0 : static_cast<double>(covered) / static_cast<double>(total);
}

std::vector<std::string> log_like_rows() {
    std::vector<std::string> rows;
    for (int i = 0; i < 400; ++i) {
        rows.push_back("rpc error: code = Unavailable desc = transport closing conn " +
                       std::to_string(i * 7919));
    }
    return rows;
}

// Low-entropy text: a handful of path components repeated. The same byte pairs recur, so a
// window's pairs are far from independent and a rate solved on log text is wrong here.
std::vector<std::string> url_like_rows() {
    static const char* parts[] = {"/images/", "/french/", "/english/", "/history/", "/docs/"};
    std::vector<std::string> rows;
    for (int i = 0; i < 400; ++i) {
        rows.push_back(std::string("GET ") + parts[i % 5] + parts[(i / 5) % 5] + "index.html");
    }
    return rows;
}

} // namespace

// The promise is kept: at the solved density, at least the requested share of windows of the
// promised length really do contain a boundary.
TEST(GramDensityTest, SolvedDensityKeepsThePromise) {
    for (const auto& rows : {log_like_rows(), url_like_rows()}) {
        for (const size_t len : {8U, 12U, 16U}) {
            for (const uint32_t q : {900U, 950U, 990U}) {
                const uint16_t density = solve_density_permille(rows, len, 4, q);
                const double share = covered_share(rows, len, density);
                EXPECT_GE(share, q / 1000.0 - 0.02)
                        << "len=" << len << " q=" << q << " solved density=" << density;
            }
        }
    }
}

// And it is the SPARSEST rate that keeps it: one notch lower stops keeping it. This is what
// makes the answer a solution rather than a safe over-estimate -- an index any larger than
// this is paying for coverage it was not asked for.
TEST(GramDensityTest, SolvedDensityIsNotLargerThanItNeedsToBe) {
    const std::vector<std::string> rows = log_like_rows();
    const uint32_t q = 950;
    const size_t len = 12;
    const uint16_t density = solve_density_permille(rows, len, 4, q);
    ASSERT_GT(density, 1U) << "the fixture must not solve to the floor for this to mean anything";
    const double at = covered_share(rows, len, density);
    const double below = covered_share(rows, len, static_cast<uint16_t>(density / 2));
    EXPECT_GE(at, q / 1000.0 - 0.02);
    EXPECT_LT(below, at) << "halving the solved density must cost coverage, or the solved "
                            "value was not the sparsest one that works";
}

// The whole point: the same promise resolves to different rates on different columns.
TEST(GramDensityTest, DifferentDataSolvesToDifferentDensity) {
    const uint16_t on_logs = solve_density_permille(log_like_rows(), 12, 4, 950);
    const uint16_t on_urls = solve_density_permille(url_like_rows(), 12, 4, 950);
    EXPECT_NE(on_logs, on_urls)
            << "a rate solved from the data cannot come out identical on data this different "
               "(logs="
            << on_logs << ", urls=" << on_urls << ")";
}

// A longer promise is cheaper to keep: more pairs in the window, so a sparser rate suffices.
TEST(GramDensityTest, LongerPromisedLiteralsAllowASparserIndex) {
    const std::vector<std::string> rows = log_like_rows();
    const uint16_t at8 = solve_density_permille(rows, 8, 4, 950);
    const uint16_t at24 = solve_density_permille(rows, 24, 4, 950);
    EXPECT_LT(at24, at8) << "at8=" << at8 << " at24=" << at24;
}

// Degenerate inputs must not produce a density that indexes nothing or everything.
TEST(GramDensityTest, DegenerateInputFallsBackWithinBounds) {
    EXPECT_GE(solve_density_permille({}, 12, 4, 950), kMinSolvedDensityPermille);
    EXPECT_LE(solve_density_permille({}, 12, 4, 950), kMaxSolvedDensityPermille);
    // Rows shorter than the promised length carry no window of that length at all.
    EXPECT_GE(solve_density_permille({"ab", "cd"}, 12, 4, 950), kMinSolvedDensityPermille);
    // A demand for total coverage cannot exceed the ceiling.
    EXPECT_LE(solve_density_permille(log_like_rows(), 12, 4, 1000), kMaxSolvedDensityPermille);
}

} // namespace doris::segment_v2::gram
