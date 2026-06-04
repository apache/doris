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

// Benchmark count_substrings(str, constant_pattern).
//
// Naive mirrors the current FunctionCountSubString path: scan every byte offset
// and compare the pattern with memcmp_small_allow_overflow15. StringSearch and
// Searcher reuse a prebuilt constant-pattern searcher and count non-overlapping
// matches.

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_const.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/memcmp_small.h"
#include "core/string_ref.h"
#include "exec/common/string_searcher.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "util/string_search.hpp"

namespace doris {
namespace {

struct CountSubstringsCase {
    std::string name;
    size_t rows;
    size_t row_size;
    size_t match_interval;
    size_t matches_per_matched_row;
    size_t false_first_byte_interval;
    std::string pattern;
};

void fill_count_base_row(std::string& row, size_t row_idx, size_t row_size) {
    static constexpr std::string_view chars =
            "bcdefghijklmnopqrstuvwxyz0123456789BCDEFGHIJKLMNOPQRSTUVWXYZ";

    row.clear();
    for (size_t pos = 0; pos < row_size; ++pos) {
        row.push_back(chars[(row_idx + pos) % chars.size()]);
    }
}

ColumnString::MutablePtr build_count_column(const CountSubstringsCase& test_case) {
    auto column = ColumnString::create();
    std::string row;
    row.reserve(test_case.row_size);

    for (size_t row_idx = 0; row_idx < test_case.rows; ++row_idx) {
        fill_count_base_row(row, row_idx, test_case.row_size);

        if (test_case.match_interval != 0 && row_idx % test_case.match_interval == 0 &&
            test_case.pattern.size() <= row.size()) {
            const size_t stride = row.size() / (test_case.matches_per_matched_row + 1);
            for (size_t match_idx = 0; match_idx < test_case.matches_per_matched_row; ++match_idx) {
                const size_t pos =
                        std::min(stride * (match_idx + 1), row.size() - test_case.pattern.size());
                row.replace(pos, test_case.pattern.size(), test_case.pattern);
            }
        } else if (test_case.false_first_byte_interval != 0 &&
                   row_idx % test_case.false_first_byte_interval == 0 && !row.empty()) {
            row[row.size() / 2] = test_case.pattern[0];
        }

        column->insert_data(row.data(), row.size());
    }

    return column;
}

int count_naive(StringRef str_ref, StringRef pattern_ref) {
    if (str_ref.size == 0 || pattern_ref.size == 0) {
        return 0;
    }

    int count = 0;
    for (size_t str_pos = 0; str_pos <= str_ref.size;) {
        size_t pos = str_pos;
        while (pos < str_ref.size &&
               memcmp_small_allow_overflow15(reinterpret_cast<const uint8_t*>(str_ref.data + pos),
                                             reinterpret_cast<const uint8_t*>(pattern_ref.data),
                                             pattern_ref.size)) {
            ++pos;
        }
        const size_t skipped = pos - str_pos;
        if (skipped == str_ref.size - str_pos) {
            break;
        }
        ++count;
        str_pos += skipped + pattern_ref.size;
    }
    return count;
}

int count_with_string_search(StringRef str_ref, StringRef pattern_ref, const StringSearch& search) {
    if (str_ref.size == 0 || pattern_ref.size == 0) {
        return 0;
    }

    int count = 0;
    const char* pos = str_ref.data;
    const char* const end = str_ref.data + str_ref.size;
    while (pos < end) {
        const char* match = search.search(pos, static_cast<size_t>(end - pos));
        if (match == end) {
            break;
        }
        ++count;
        pos = match + pattern_ref.size;
    }
    return count;
}

int count_with_searcher(StringRef str_ref, StringRef pattern_ref,
                        const ASCIICaseSensitiveStringSearcher& searcher) {
    if (str_ref.size == 0 || pattern_ref.size == 0) {
        return 0;
    }

    int count = 0;
    const char* pos = str_ref.data;
    const char* const end = str_ref.data + str_ref.size;
    while (pos < end) {
        const char* match = searcher.search(pos, end);
        if (match == end) {
            break;
        }
        ++count;
        pos = match + pattern_ref.size;
    }
    return count;
}

template <typename CountFunc>
int count_column(const ColumnString& src, CountFunc&& count_func) {
    int total = 0;
    for (size_t row_idx = 0; row_idx < src.size(); ++row_idx) {
        total += count_func(src.get_data_at(row_idx));
    }
    return total;
}

ColumnPtr build_const_pattern_column(const std::string& pattern, size_t rows) {
    auto nested = ColumnString::create();
    nested->insert_data(pattern.data(), pattern.size());
    return ColumnConst::create(std::move(nested), rows);
}

Block build_count_substrings_block(ColumnPtr src, ColumnPtr pattern) {
    auto string_type = std::make_shared<DataTypeString>();
    auto result_type = std::make_shared<DataTypeInt32>();
    Block block;
    block.insert(ColumnWithTypeAndName {std::move(src), string_type, "src"});
    block.insert(ColumnWithTypeAndName {std::move(pattern), string_type, "pattern"});
    block.insert(ColumnWithTypeAndName {nullptr, result_type, "result"});
    return block;
}

FunctionBasePtr get_count_substrings_function(const Block& block) {
    auto result_type = std::make_shared<DataTypeInt32>();
    auto function = SimpleFunctionFactory::instance().get_function(
            "count_substrings",
            ColumnsWithTypeAndName {block.get_by_position(0), block.get_by_position(1)},
            result_type);
    if (function == nullptr) {
        throw std::runtime_error("count_substrings benchmark cannot get function");
    }
    return function;
}

int sum_result_column(const ColumnPtr& result) {
    auto full_column = result->convert_to_full_column_if_const();
    const auto& result_column = assert_cast<const ColumnInt32&>(*full_column);
    int total = 0;
    for (const auto value : result_column.get_data()) {
        total += value;
    }
    return total;
}

int execute_count_substrings(FunctionBasePtr function, Block& block, size_t rows) {
    FunctionContext* context = nullptr;
    auto status = function->execute(context, block, {0, 1}, 2, rows);
    if (!status.ok()) {
        throw std::runtime_error("count_substrings benchmark execute failed: " +
                                 status.to_string());
    }
    return sum_result_column(block.get_by_position(2).column);
}

void verify_same_result(const CountSubstringsCase& test_case, const ColumnString& src) {
    const StringRef pattern_ref(test_case.pattern.data(), test_case.pattern.size());
    StringSearch search(&pattern_ref);
    ASCIICaseSensitiveStringSearcher searcher(pattern_ref.data, pattern_ref.size);

    const int naive =
            count_column(src, [&](StringRef row) { return count_naive(row, pattern_ref); });
    const int string_search = count_column(
            src, [&](StringRef row) { return count_with_string_search(row, pattern_ref, search); });
    const int direct_searcher = count_column(
            src, [&](StringRef row) { return count_with_searcher(row, pattern_ref, searcher); });
    auto block =
            build_count_substrings_block(src.clone_resized(src.size()),
                                         build_const_pattern_column(test_case.pattern, src.size()));
    const int function_result =
            execute_count_substrings(get_count_substrings_function(block), block, src.size());

    if (naive != string_search || naive != direct_searcher || naive != function_result) {
        throw std::runtime_error("count_substrings benchmark result mismatch: " + test_case.name);
    }
}

void set_counters(benchmark::State& state, const CountSubstringsCase& test_case) {
    const double bytes = static_cast<double>(test_case.rows * test_case.row_size);
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * bytes));
    state.counters["rows"] = benchmark::Counter(static_cast<double>(test_case.rows));
    state.counters["row_size"] = benchmark::Counter(static_cast<double>(test_case.row_size));
}

void BM_CountSubstrings_Naive(benchmark::State& state, const CountSubstringsCase& test_case) {
    auto src = build_count_column(test_case);
    verify_same_result(test_case, *src);
    const StringRef pattern_ref(test_case.pattern.data(), test_case.pattern.size());

    for (auto _ : state) {
        int total =
                count_column(*src, [&](StringRef row) { return count_naive(row, pattern_ref); });
        benchmark::DoNotOptimize(total);
        benchmark::ClobberMemory();
    }

    set_counters(state, test_case);
}

void BM_CountSubstrings_StringSearch(benchmark::State& state,
                                     const CountSubstringsCase& test_case) {
    auto src = build_count_column(test_case);
    verify_same_result(test_case, *src);
    const StringRef pattern_ref(test_case.pattern.data(), test_case.pattern.size());

    for (auto _ : state) {
        StringSearch search(&pattern_ref);
        int total = count_column(*src, [&](StringRef row) {
            return count_with_string_search(row, pattern_ref, search);
        });
        benchmark::DoNotOptimize(total);
        benchmark::ClobberMemory();
    }

    set_counters(state, test_case);
}

void BM_CountSubstrings_Searcher(benchmark::State& state, const CountSubstringsCase& test_case) {
    auto src = build_count_column(test_case);
    verify_same_result(test_case, *src);
    const StringRef pattern_ref(test_case.pattern.data(), test_case.pattern.size());

    for (auto _ : state) {
        ASCIICaseSensitiveStringSearcher searcher(pattern_ref.data, pattern_ref.size);
        int total = count_column(*src, [&](StringRef row) {
            return count_with_searcher(row, pattern_ref, searcher);
        });
        benchmark::DoNotOptimize(total);
        benchmark::ClobberMemory();
    }

    set_counters(state, test_case);
}

void BM_CountSubstrings_FunctionConstPattern(benchmark::State& state,
                                             const CountSubstringsCase& test_case) {
    auto src = build_count_column(test_case);
    verify_same_result(test_case, *src);
    auto block = build_count_substrings_block(
            std::move(src), build_const_pattern_column(test_case.pattern, test_case.rows));
    auto function = get_count_substrings_function(block);

    for (auto _ : state) {
        int total = execute_count_substrings(function, block, test_case.rows);
        benchmark::DoNotOptimize(total);
        benchmark::ClobberMemory();
    }

    set_counters(state, test_case);
}

const CountSubstringsCase SMALL_FREQUENT {"SmallFrequent", 16384, 64, 2, 4, 0, "abc"};
const CountSubstringsCase MEDIUM_RARE {"MediumRare", 8192, 256, 64, 1, 0, "abcXYZ12"};
const CountSubstringsCase LONG_NO_FIRST_BYTE {"LongNoFirstByte", 512, 32768, 0, 0, 0, "aaaaaaaa"};
const CountSubstringsCase LONG_FALSE_FIRST_BYTE {
        "LongFalseFirstByte", 512, 32768, 0, 0, 1, "abcXYZ12"};
const CountSubstringsCase LONG_RARE {"LongRare", 512, 32768, 32, 1, 0, "abcXYZ12abcXYZ12"};
const CountSubstringsCase LONG_FREQUENT {"LongFrequent", 512, 32768, 1, 64, 0, "abcXYZ12"};
const CountSubstringsCase LONG_NEEDLE {
        "LongNeedle", 512, 32768, 32, 1, 0, "abcXYZ12abcXYZ12abcXYZ12abcXYZ12"};

#define REGISTER_COUNT_SUBSTRINGS_BENCHMARKS(name, test_case)                                    \
    BENCHMARK_CAPTURE(BM_CountSubstrings_Naive, name, test_case)->Unit(benchmark::kMicrosecond); \
    BENCHMARK_CAPTURE(BM_CountSubstrings_StringSearch, name, test_case)                          \
            ->Unit(benchmark::kMicrosecond);                                                     \
    BENCHMARK_CAPTURE(BM_CountSubstrings_Searcher, name, test_case)                              \
            ->Unit(benchmark::kMicrosecond);                                                     \
    BENCHMARK_CAPTURE(BM_CountSubstrings_FunctionConstPattern, name, test_case)                  \
            ->Unit(benchmark::kMicrosecond)

REGISTER_COUNT_SUBSTRINGS_BENCHMARKS(SmallFrequent, SMALL_FREQUENT);
REGISTER_COUNT_SUBSTRINGS_BENCHMARKS(MediumRare, MEDIUM_RARE);
REGISTER_COUNT_SUBSTRINGS_BENCHMARKS(LongNoFirstByte, LONG_NO_FIRST_BYTE);
REGISTER_COUNT_SUBSTRINGS_BENCHMARKS(LongFalseFirstByte, LONG_FALSE_FIRST_BYTE);
REGISTER_COUNT_SUBSTRINGS_BENCHMARKS(LongRare, LONG_RARE);
REGISTER_COUNT_SUBSTRINGS_BENCHMARKS(LongFrequent, LONG_FREQUENT);
REGISTER_COUNT_SUBSTRINGS_BENCHMARKS(LongNeedle, LONG_NEEDLE);

#undef REGISTER_COUNT_SUBSTRINGS_BENCHMARKS

} // namespace
} // namespace doris
