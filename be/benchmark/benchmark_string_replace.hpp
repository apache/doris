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

// ============================================================
// Benchmark: String Replace — old (per-row std::string) vs new (columnar)
//
// Measures the performance of string replace when needle and
// replacement are both constants. The "new" path uses a two-level
// search strategy: memchr (glibc AVX512) as a per-row first-byte
// pre-filter for fast no-match short-circuiting, then a prebuilt
// ASCIICaseSensitiveStringSearcher (SSE4.1) for full needle matching.
// Writes output directly to ColumnString chars/offsets, matching
// _replace_const_pattern() in FunctionReplace.
// ============================================================

#include <benchmark/benchmark.h>

#include <cstring>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "core/column/column_string.h"
#include "exec/common/string_searcher.h"

namespace doris {

// ---- Old implementation (current Doris: per-row std::string find/replace) ----
static std::string replace_old(std::string str, std::string_view old_str,
                               std::string_view new_str) {
    if (old_str.empty()) {
        return str;
    }
    std::string::size_type pos = 0;
    std::string::size_type old_len = old_str.size();
    std::string::size_type new_len = new_str.size();
    while ((pos = str.find(old_str, pos)) != std::string::npos) {
        str.replace(pos, old_len, new_str);
        pos += new_len;
    }
    return str;
}

static void replace_old_column(const ColumnString& src, const std::string& needle,
                               const std::string& replacement, ColumnString& dst) {
    size_t rows = src.size();
    for (size_t i = 0; i < rows; ++i) {
        StringRef ref = src.get_data_at(i);
        std::string result = replace_old(ref.to_string(), needle, replacement);
        dst.insert_data(result.data(), result.size());
    }
}

// ---- New implementation (columnar: memchr pre-filter + SSE4.1 searcher) ----
// Matches the fast path in FunctionReplace::_replace_const_pattern().
// Two-level search strategy:
//  1. memchr (glibc AVX512) for needle's first byte per row.
//     If absent -> guaranteed no match -> single bulk memcpy, no SSE4.1 overhead.
//  2. ASCIICaseSensitiveStringSearcher (SSE4.1, built once) for full needle scan
//     only on rows where the first byte was found.
// Writes output directly to ColumnString chars/offsets — no per-row std::string.
static void replace_new_column(const ColumnString& src, const std::string& needle,
                               const std::string& replacement, ColumnString& dst) {
    auto& dst_chars = dst.get_chars();
    auto& dst_offsets = dst.get_offsets();
    size_t rows = src.size();

    dst_chars.reserve(src.get_chars().size());
    dst_offsets.resize(rows);

    if (needle.empty()) {
        dst_chars.insert(src.get_chars().begin(), src.get_chars().end());
        memcpy(dst_offsets.data(), src.get_offsets().data(), rows * sizeof(dst_offsets[0]));
        return;
    }

    // Build SSE4.1 searcher once — first+second byte masks precomputed here.
    ASCIICaseSensitiveStringSearcher searcher(needle.data(), needle.size());
    const size_t needle_size = needle.size();
    const size_t replacement_size = replacement.size();
    const char* replacement_data = replacement.data();
    const auto needle_first = static_cast<unsigned char>(needle[0]);

    for (size_t i = 0; i < rows; ++i) {
        StringRef row = src.get_data_at(i);
        const char* const row_end = row.data + row.size;

        // Level-1: memchr for needle's first byte (glibc AVX512).
        // First byte absent -> no match possible -> bulk-copy entire row.
        if (memchr(row.data, needle_first, row.size) == nullptr) {
            size_t old_size = dst_chars.size();
            dst_chars.resize(old_size + row.size);
            memcpy(&dst_chars[old_size], row.data, row.size);
            dst_offsets[i] = static_cast<ColumnString::Offset>(dst_chars.size());
            continue;
        }

        // Level-2: SSE4.1 searcher for full needle matching.
        const char* pos = row.data;
        while (pos < row_end) {
            const char* match = searcher.search(pos, row_end);
            size_t prefix_len = static_cast<size_t>(match - pos);
            if (prefix_len > 0) {
                size_t old_size = dst_chars.size();
                dst_chars.resize(old_size + prefix_len);
                memcpy(&dst_chars[old_size], pos, prefix_len);
            }
            if (match == row_end) {
                break;
            }
            if (replacement_size > 0) {
                size_t old_size = dst_chars.size();
                dst_chars.resize(old_size + replacement_size);
                memcpy(&dst_chars[old_size], replacement_data, replacement_size);
            }
            pos = match + needle_size;
        }
        dst_offsets[i] = static_cast<ColumnString::Offset>(dst_chars.size());
    }
}

// ---- Helper: build a ColumnString with random data containing the needle ----
static ColumnString::MutablePtr build_test_column(size_t num_rows, size_t avg_len,
                                                  const std::string& needle, double hit_rate,
                                                  unsigned seed = 42) {
    auto col = ColumnString::create();
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> char_dist('a', 'z');
    std::uniform_real_distribution<double> hit_dist(0.0, 1.0);
    std::uniform_int_distribution<size_t> len_dist(avg_len / 2, avg_len * 3 / 2);

    for (size_t r = 0; r < num_rows; ++r) {
        size_t len = len_dist(rng);
        std::string s;
        s.reserve(len + needle.size() * 3);
        size_t written = 0;
        while (written < len) {
            if (!needle.empty() && hit_dist(rng) < hit_rate && written + needle.size() <= len) {
                s += needle;
                written += needle.size();
            } else {
                s += static_cast<char>(char_dist(rng));
                ++written;
            }
        }
        col->insert_data(s.data(), s.size());
    }
    return col;
}

// -------- Benchmarks --------

// Small strings, high hit rate, many rows
static void BM_Replace_Old_SmallStr(benchmark::State& state) {
    std::string needle = "abc";
    std::string replacement = "XY";
    auto src = build_test_column(10000, 50, needle, 0.1);
    for (auto _ : state) {
        auto dst = ColumnString::create();
        replace_old_column(*src, needle, replacement, *dst);
        benchmark::DoNotOptimize(dst);
    }
}

static void BM_Replace_New_SmallStr(benchmark::State& state) {
    std::string needle = "abc";
    std::string replacement = "XY";
    auto src = build_test_column(10000, 50, needle, 0.1);
    for (auto _ : state) {
        auto dst = ColumnString::create();
        replace_new_column(*src, needle, replacement, *dst);
        benchmark::DoNotOptimize(dst);
    }
}

// Medium strings, moderate hit rate
static void BM_Replace_Old_MedStr(benchmark::State& state) {
    std::string needle = "hello";
    std::string replacement = "world!";
    auto src = build_test_column(5000, 200, needle, 0.05);
    for (auto _ : state) {
        auto dst = ColumnString::create();
        replace_old_column(*src, needle, replacement, *dst);
        benchmark::DoNotOptimize(dst);
    }
}

static void BM_Replace_New_MedStr(benchmark::State& state) {
    std::string needle = "hello";
    std::string replacement = "world!";
    auto src = build_test_column(5000, 200, needle, 0.05);
    for (auto _ : state) {
        auto dst = ColumnString::create();
        replace_new_column(*src, needle, replacement, *dst);
        benchmark::DoNotOptimize(dst);
    }
}

// Large strings, low hit rate
static void BM_Replace_Old_LargeStr(benchmark::State& state) {
    std::string needle = "pattern";
    std::string replacement = "REPLACED";
    auto src = build_test_column(1000, 1000, needle, 0.02);
    for (auto _ : state) {
        auto dst = ColumnString::create();
        replace_old_column(*src, needle, replacement, *dst);
        benchmark::DoNotOptimize(dst);
    }
}

static void BM_Replace_New_LargeStr(benchmark::State& state) {
    std::string needle = "pattern";
    std::string replacement = "REPLACED";
    auto src = build_test_column(1000, 1000, needle, 0.02);
    for (auto _ : state) {
        auto dst = ColumnString::create();
        replace_new_column(*src, needle, replacement, *dst);
        benchmark::DoNotOptimize(dst);
    }
}

// No matches (needle not present) — measures search overhead
static void BM_Replace_Old_NoMatch(benchmark::State& state) {
    std::string needle = "ZZZZZZ";
    std::string replacement = "X";
    auto src = build_test_column(10000, 100, "abc", 0.0); // no ZZZZZZ in data
    for (auto _ : state) {
        auto dst = ColumnString::create();
        replace_old_column(*src, needle, replacement, *dst);
        benchmark::DoNotOptimize(dst);
    }
}

static void BM_Replace_New_NoMatch(benchmark::State& state) {
    std::string needle = "ZZZZZZ";
    std::string replacement = "X";
    auto src = build_test_column(10000, 100, "abc", 0.0);
    for (auto _ : state) {
        auto dst = ColumnString::create();
        replace_new_column(*src, needle, replacement, *dst);
        benchmark::DoNotOptimize(dst);
    }
}

BENCHMARK(BM_Replace_Old_SmallStr);
BENCHMARK(BM_Replace_New_SmallStr);
BENCHMARK(BM_Replace_Old_MedStr);
BENCHMARK(BM_Replace_New_MedStr);
BENCHMARK(BM_Replace_Old_LargeStr);
BENCHMARK(BM_Replace_New_LargeStr);
BENCHMARK(BM_Replace_Old_NoMatch);
BENCHMARK(BM_Replace_New_NoMatch);

} // namespace doris
