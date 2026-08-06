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

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "core/string_ref.h"
#include "util/simd/vstring_function.h"

namespace doris {

namespace {

using BenchmarkSymbolId = int32_t;

std::string make_utf8_string(size_t chars, size_t seed, bool transpose_adjacent) {
    static const std::vector<std::string> symbols = {"数", "据", "库", "查", "询", "表",
                                                     "列", "你", "好", "测", "试", "节",
                                                     "点", "分", "区", "行"};

    std::string result;
    result.reserve(chars * 3);
    for (size_t i = 0; i < chars; ++i) {
        size_t symbol_index = i;
        if (transpose_adjacent && chars > 2) {
            if (i == 1) {
                symbol_index = 2;
            } else if (i == 2) {
                symbol_index = 1;
            }
        }
        result += symbols[(symbol_index * 7 + seed) % symbols.size()];
    }
    return result;
}

void append_utf8_symbols(std::string_view str, const std::vector<size_t>& offsets,
                         std::vector<BenchmarkSymbolId>& symbols,
                         std::unordered_map<std::string_view, BenchmarkSymbolId>& symbol_ids,
                         BenchmarkSymbolId& next_symbol) {
    symbols.reserve(symbols.size() + offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        const size_t offset = offsets[i];
        const size_t next_offset = i + 1 < offsets.size() ? offsets[i + 1] : str.size();
        const std::string_view symbol(str.data() + offset, next_offset - offset);
        auto [it, inserted] = symbol_ids.emplace(symbol, next_symbol);
        if (inserted) {
            ++next_symbol;
        }
        symbols.push_back(it->second);
    }
}

int32_t by_symbols(const std::vector<BenchmarkSymbolId>& left,
                   const std::vector<BenchmarkSymbolId>& right, size_t alphabet_size) {
    const size_t m = left.size();
    const size_t n = right.size();
    if (m == 0) {
        return static_cast<int32_t>(n);
    }
    if (n == 0) {
        return static_cast<int32_t>(m);
    }

    const int32_t max_distance = static_cast<int32_t>(m + n);
    std::vector<int32_t> last_row(alphabet_size, 0);
    std::vector<int32_t> dist((m + 2) * (n + 2), 0);
    auto at = [&](size_t i, size_t j) -> int32_t& { return dist[i * (n + 2) + j]; };

    at(0, 0) = max_distance;
    for (size_t i = 0; i <= m; ++i) {
        at(i + 1, 0) = max_distance;
        at(i + 1, 1) = static_cast<int32_t>(i);
    }
    for (size_t j = 0; j <= n; ++j) {
        at(0, j + 1) = max_distance;
        at(1, j + 1) = static_cast<int32_t>(j);
    }

    for (size_t i = 1; i <= m; ++i) {
        int32_t last_match_col = 0;
        for (size_t j = 1; j <= n; ++j) {
            const int32_t last_match_row = last_row[right[j - 1]];
            const int32_t transposition_col = last_match_col;
            int32_t cost = 1;
            if (left[i - 1] == right[j - 1]) {
                cost = 0;
                last_match_col = static_cast<int32_t>(j);
            }

            const int32_t replace_cost = at(i, j) + cost;
            const int32_t insert_cost = at(i + 1, j) + 1;
            const int32_t delete_cost = at(i, j + 1) + 1;
            const int32_t transpose_cost = at(last_match_row, transposition_col) +
                                           static_cast<int32_t>(i - last_match_row - 1) + 1 +
                                           static_cast<int32_t>(j - transposition_col - 1);
            at(i + 1, j + 1) = std::min(std::min(replace_cost, insert_cost),
                                        std::min(delete_cost, transpose_cost));
        }
        last_row[left[i - 1]] = static_cast<int32_t>(i);
    }

    return at(m + 1, n + 1);
}

int32_t damerau_utf8_symbol_mapped(std::string_view left, const std::vector<size_t>& left_offsets,
                                   std::string_view right,
                                   const std::vector<size_t>& right_offsets) {
    std::vector<BenchmarkSymbolId> left_symbols;
    std::vector<BenchmarkSymbolId> right_symbols;
    std::unordered_map<std::string_view, BenchmarkSymbolId> symbol_ids;
    symbol_ids.reserve(left_offsets.size() + right_offsets.size());
    BenchmarkSymbolId next_symbol = 0;
    append_utf8_symbols(left, left_offsets, left_symbols, symbol_ids, next_symbol);
    append_utf8_symbols(right, right_offsets, right_symbols, symbol_ids, next_symbol);
    return by_symbols(left_symbols, right_symbols, next_symbol);
}

std::string_view utf8_symbol_at(std::string_view str, const std::vector<size_t>& offsets,
                                size_t index) {
    const size_t offset = offsets[index];
    const size_t next_offset = index + 1 < offsets.size() ? offsets[index + 1] : str.size();
    return {str.data() + offset, next_offset - offset};
}

int32_t damerau_utf8_direct_compare(std::string_view left, const std::vector<size_t>& left_offsets,
                                    std::string_view right,
                                    const std::vector<size_t>& right_offsets) {
    const size_t m = left_offsets.size();
    const size_t n = right_offsets.size();
    if (m == 0) {
        return static_cast<int32_t>(n);
    }
    if (n == 0) {
        return static_cast<int32_t>(m);
    }

    const int32_t max_distance = static_cast<int32_t>(m + n);
    std::unordered_map<std::string_view, int32_t> last_row;
    last_row.reserve(m + n);
    std::vector<int32_t> dist((m + 2) * (n + 2), 0);
    auto at = [&](size_t i, size_t j) -> int32_t& { return dist[i * (n + 2) + j]; };

    at(0, 0) = max_distance;
    for (size_t i = 0; i <= m; ++i) {
        at(i + 1, 0) = max_distance;
        at(i + 1, 1) = static_cast<int32_t>(i);
    }
    for (size_t j = 0; j <= n; ++j) {
        at(0, j + 1) = max_distance;
        at(1, j + 1) = static_cast<int32_t>(j);
    }

    for (size_t i = 1; i <= m; ++i) {
        int32_t last_match_col = 0;
        const std::string_view left_symbol = utf8_symbol_at(left, left_offsets, i - 1);
        for (size_t j = 1; j <= n; ++j) {
            const std::string_view right_symbol = utf8_symbol_at(right, right_offsets, j - 1);
            auto it = last_row.find(right_symbol);
            const int32_t last_match_row = it == last_row.end() ? 0 : it->second;
            const int32_t transposition_col = last_match_col;
            int32_t cost = 1;
            if (left_symbol == right_symbol) {
                cost = 0;
                last_match_col = static_cast<int32_t>(j);
            }

            const int32_t replace_cost = at(i, j) + cost;
            const int32_t insert_cost = at(i + 1, j) + 1;
            const int32_t delete_cost = at(i, j + 1) + 1;
            const int32_t transpose_cost = at(last_match_row, transposition_col) +
                                           static_cast<int32_t>(i - last_match_row - 1) + 1 +
                                           static_cast<int32_t>(j - transposition_col - 1);
            at(i + 1, j + 1) = std::min(std::min(replace_cost, insert_cost),
                                        std::min(delete_cost, transpose_cost));
        }
        last_row[left_symbol] = static_cast<int32_t>(i);
    }

    return at(m + 1, n + 1);
}

void get_utf8_offsets(const std::string& str, std::vector<size_t>& offsets) {
    const StringRef ref(str.data(), str.size());
    simd::VStringFunctions::get_utf8_char_offsets(ref, offsets);
}

void BM_DamerauLevenshtein_UTF8(benchmark::State& state, bool use_symbol_mapped, size_t chars) {
    const std::string left = make_utf8_string(chars, 0, false);
    const std::string right = make_utf8_string(chars, 0, true);

    std::vector<size_t> left_offsets;
    std::vector<size_t> right_offsets;
    get_utf8_offsets(left, left_offsets);
    get_utf8_offsets(right, right_offsets);

    for (auto _ : state) {
        auto distance =
                use_symbol_mapped
                        ? damerau_utf8_symbol_mapped(left, left_offsets, right, right_offsets)
                        : damerau_utf8_direct_compare(left, left_offsets, right, right_offsets);
        benchmark::DoNotOptimize(distance);
    }
}

} // namespace

BENCHMARK_CAPTURE(BM_DamerauLevenshtein_UTF8, SymbolMapped_16, true, 16)
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_DamerauLevenshtein_UTF8, DirectCompare_16, false, 16)
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_DamerauLevenshtein_UTF8, SymbolMapped_64, true, 64)
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_DamerauLevenshtein_UTF8, DirectCompare_64, false, 64)
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_DamerauLevenshtein_UTF8, SymbolMapped_128, true, 128)
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_DamerauLevenshtein_UTF8, DirectCompare_128, false, 128)
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_DamerauLevenshtein_UTF8, SymbolMapped_256, true, 256)
        ->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_DamerauLevenshtein_UTF8, DirectCompare_256, false, 256)
        ->Unit(benchmark::kMicrosecond);

} // namespace doris
