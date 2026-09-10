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
// What the solver actually answers on real columns, and what it costs.
//
// A rule that adapts is only worth the name if it lands somewhere different on data that is
// different, and if what it lands on is defensible. This prints, for each corpus and each
// promise, the solved density, the grams per byte it produces, and the share of literals of
// the promised length it really covers -- so the claim can be checked rather than believed.
//
// Corpus-driven and skipped without one:
//   GRAM_DENSITY_CORPUS=/path/to/lines.txt  GRAM_DENSITY_ROWS=200000

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <vector>

#include "storage/index/inverted/gram/gram_density.h"
#include "storage/index/inverted/gram/gram_extractor.h"
#include "storage/index/inverted/gram/gram_scheme.h"

namespace doris::segment_v2::gram {
namespace {

std::vector<std::string> read_rows(const char* path, size_t rows) {
    std::vector<std::string> out;
    std::ifstream in(path);
    std::string line;
    while (out.size() < rows && std::getline(in, line)) {
        if (!line.empty()) {
            out.push_back(line);
        }
    }
    return out;
}

GramScheme scheme_at(uint16_t density) {
    GramScheme s;
    s.mode = GramMode::SPARSE;
    s.min_len = 3;
    s.max_len = 4;
    s.density_permille = density;
    return s;
}

struct Outcome {
    double grams_per_byte = 0.0;
    double covered = 0.0;
};

Outcome evaluate(const std::vector<std::string>& rows, uint16_t density, size_t len) {
    GramExtractor extractor(scheme_at(density));
    std::vector<std::string_view> grams;
    size_t total_grams = 0;
    size_t total_bytes = 0;
    size_t windows = 0;
    size_t covered = 0;
    for (const std::string& row : rows) {
        extractor.extract(row, &grams);
        total_grams += grams.size();
        total_bytes += row.size();
        // Coverage is measured the way a query experiences it: take the row's own substrings
        // of the promised length and ask whether the extractor produces anything for them.
        for (size_t start = 0; start + len <= row.size(); start += len) {
            ++windows;
            extractor.extract(row.substr(start, len), &grams);
            covered += grams.empty() ? 0 : 1;
        }
    }
    return {total_bytes == 0 ? 0.0 : static_cast<double>(total_grams) / total_bytes,
            windows == 0 ? 0.0 : static_cast<double>(covered) / windows};
}

} // namespace

TEST(GramDensityCorpusTest, ReportsWhatEachPromiseSolvesTo) {
    const char* path = std::getenv("GRAM_DENSITY_CORPUS");
    if (path == nullptr) {
        GTEST_SKIP() << "set GRAM_DENSITY_CORPUS to a line-per-record corpus";
    }
    const char* rows_env = std::getenv("GRAM_DENSITY_ROWS");
    const size_t want = rows_env != nullptr ? std::strtoul(rows_env, nullptr, 10) : 200000;
    const std::vector<std::string> rows = read_rows(path, want);
    ASSERT_FALSE(rows.empty()) << path;

    size_t bytes = 0;
    for (const std::string& r : rows) {
        bytes += r.size();
    }
    printf("\ncorpus: %s, %zu rows, %.2f MB, %.1f B/row\n", path, rows.size(), bytes / 1e6,
           static_cast<double>(bytes) / rows.size());
    printf("%10s %8s %10s %12s %11s   %s\n", "promise L", "q", "solved", "grams/byte", "covered",
           "vs fixed 0.25");
    for (const size_t len : {8U, 12U, 16U, 24U}) {
        for (const uint32_t q : {950U, 990U}) {
            const uint16_t solved = solve_density_permille(rows, len, 4, q);
            const Outcome got = evaluate(rows, solved, len);
            const Outcome fixed = evaluate(rows, 250, len);
            printf("%10zu %8.3f %10.3f %12.3f %10.1f%%   grams/byte %.3f, covered %.1f%%\n", len,
                   q / 1000.0, solved / 1000.0, got.grams_per_byte, 100.0 * got.covered,
                   fixed.grams_per_byte, 100.0 * fixed.covered);
            EXPECT_GE(solved, kMinSolvedDensityPermille);
            EXPECT_LE(solved, kMaxSolvedDensityPermille);
        }
    }
}

} // namespace doris::segment_v2::gram
