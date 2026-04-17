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

#include <benchmark/benchmark.h>

#include <bitset>
#include <random>
#include <string>
#include <unordered_set>

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

#include "core/column/column_string.h"
#include "exprs/function/url/find_symbols.h"
#include "util/simd/vstring_function.h"

namespace doris {

// ==================== Old implementations (current Doris) ====================

struct TrimInOld {
    // Old ASCII path: std::bitset<128> + scalar loop
    static void trim_in_ascii_old(const ColumnString::Chars& str_data,
                                  const ColumnString::Offsets& str_offsets,
                                  const StringRef& remove_str, ColumnString::Chars& res_data,
                                  ColumnString::Offsets& res_offsets) {
        const size_t offset_size = str_offsets.size();
        res_offsets.resize(offset_size);
        res_data.reserve(str_data.size());

        std::bitset<128> char_lookup;
        const char* remove_begin = remove_str.data;
        const char* remove_end = remove_str.data + remove_str.size;

        while (remove_begin < remove_end) {
            char_lookup.set(static_cast<unsigned char>(*remove_begin));
            remove_begin += 1;
        }

        for (size_t i = 0; i < offset_size; ++i) {
            const char* str_begin =
                    reinterpret_cast<const char*>(str_data.data() + str_offsets[i - 1]);
            const char* str_end = reinterpret_cast<const char*>(str_data.data() + str_offsets[i]);
            const char* left_trim_pos = str_begin;
            const char* right_trim_pos = str_end;

            // ltrim
            while (left_trim_pos < str_end) {
                if (!char_lookup.test(static_cast<unsigned char>(*left_trim_pos))) {
                    break;
                }
                ++left_trim_pos;
            }

            // rtrim
            while (right_trim_pos > left_trim_pos) {
                --right_trim_pos;
                if (!char_lookup.test(static_cast<unsigned char>(*right_trim_pos))) {
                    ++right_trim_pos;
                    break;
                }
            }

            res_data.insert_assume_reserved(left_trim_pos, right_trim_pos);
            res_offsets[i] = (ColumnString::Offset)res_data.size();
        }
    }

    // Old UTF-8 path: unordered_set<string_view> + hash lookup
    static void trim_in_utf8_old(const ColumnString::Chars& str_data,
                                 const ColumnString::Offsets& str_offsets,
                                 const StringRef& remove_str, ColumnString::Chars& res_data,
                                 ColumnString::Offsets& res_offsets) {
        const size_t offset_size = str_offsets.size();
        res_offsets.resize(offset_size);
        res_data.reserve(str_data.size());

        std::unordered_set<std::string_view> char_lookup;
        const char* remove_begin = remove_str.data;
        const char* remove_end = remove_str.data + remove_str.size;

        while (remove_begin < remove_end) {
            size_t byte_len, char_len;
            std::tie(byte_len, char_len) = simd::VStringFunctions::iterate_utf8_with_limit_length(
                    remove_begin, remove_end, 1);
            char_lookup.insert(std::string_view(remove_begin, byte_len));
            remove_begin += byte_len;
        }

        for (size_t i = 0; i < offset_size; ++i) {
            const char* str_begin =
                    reinterpret_cast<const char*>(str_data.data() + str_offsets[i - 1]);
            const char* str_end = reinterpret_cast<const char*>(str_data.data() + str_offsets[i]);
            const char* left_trim_pos = str_begin;
            const char* right_trim_pos = str_end;

            // ltrim
            while (left_trim_pos < str_end) {
                size_t byte_len, char_len;
                std::tie(byte_len, char_len) =
                        simd::VStringFunctions::iterate_utf8_with_limit_length(left_trim_pos,
                                                                               str_end, 1);
                if (char_lookup.find(std::string_view(left_trim_pos, byte_len)) ==
                    char_lookup.end()) {
                    break;
                }
                left_trim_pos += byte_len;
            }

            // rtrim
            while (right_trim_pos > left_trim_pos) {
                const char* prev_char_pos = right_trim_pos;
                do {
                    --prev_char_pos;
                } while ((*prev_char_pos & 0xC0) == 0x80);
                size_t byte_len = right_trim_pos - prev_char_pos;
                if (char_lookup.find(std::string_view(prev_char_pos, byte_len)) ==
                    char_lookup.end()) {
                    break;
                }
                right_trim_pos = prev_char_pos;
            }

            res_data.insert_assume_reserved(left_trim_pos, right_trim_pos);
            res_offsets[i] = (ColumnString::Offset)res_data.size();
        }
    }
};

// TrimInNew — mirrors the optimized hot path from function_string.cpp
// Inlined here because TrimInUtil is defined inside a .cpp and not visible to benchmarks.
// Only covers the <=16 ASCII chars and <=32 UTF-8 codepoints paths (benchmark scenarios
// do not exercise the fallback paths for larger trim sets).
struct TrimInNew {
    static void trim_in_ascii_new(const ColumnString::Chars& str_data,
                                  const ColumnString::Offsets& str_offsets,
                                  const StringRef& remove_str, ColumnString::Chars& res_data,
                                  ColumnString::Offsets& res_offsets) {
        const size_t offset_size = str_offsets.size();
        res_offsets.resize(offset_size);
        res_data.reserve(str_data.size());

        SearchSymbols symbols(std::string(remove_str.data, remove_str.size));

        bool char_lookup[256] = {};
        for (size_t j = 0; j < remove_str.size; ++j) {
            char_lookup[static_cast<unsigned char>(remove_str.data[j])] = true;
        }

        for (size_t i = 0; i < offset_size; ++i) {
            const char* str_begin =
                    reinterpret_cast<const char*>(str_data.data() + str_offsets[i - 1]);
            const char* str_end = reinterpret_cast<const char*>(str_data.data() + str_offsets[i]);
            const char* left_trim_pos = str_begin;
            const char* right_trim_pos = str_end;

            // ltrim
            if (left_trim_pos < str_end &&
                char_lookup[static_cast<unsigned char>(*left_trim_pos)]) {
                left_trim_pos = find_first_not_symbols(
                        std::string_view(str_begin, str_end - str_begin), symbols);
            }

            // rtrim
            if (right_trim_pos > left_trim_pos &&
                char_lookup[static_cast<unsigned char>(*(right_trim_pos - 1))]) {
                const char* pos =
                        find_last_not_symbols_or_null(left_trim_pos, right_trim_pos, symbols);
                right_trim_pos = pos ? pos + 1 : left_trim_pos;
            }

            res_data.insert_assume_reserved(left_trim_pos, right_trim_pos);
            res_offsets[i] = (ColumnString::Offset)res_data.size();
        }
    }

    static void trim_in_utf8_new(const ColumnString::Chars& str_data,
                                 const ColumnString::Offsets& str_offsets,
                                 const StringRef& remove_str, ColumnString::Chars& res_data,
                                 ColumnString::Offsets& res_offsets) {
        const size_t offset_size = str_offsets.size();
        res_offsets.resize(offset_size);
        res_data.reserve(str_data.size());

        static constexpr size_t MAX_TRIM_CHARS = 32;
        struct Utf8Char {
            char data[6];
            uint8_t len;
        };
        Utf8Char trim_chars[MAX_TRIM_CHARS];
        size_t num_trim_chars = 0;

        const char* remove_begin = remove_str.data;
        const char* remove_end = remove_str.data + remove_str.size;

        while (remove_begin < remove_end && num_trim_chars < MAX_TRIM_CHARS) {
            uint8_t byte_len = get_utf8_byte_length(static_cast<uint8_t>(*remove_begin));
            if (remove_begin + byte_len > remove_end) break;
            trim_chars[num_trim_chars].len = byte_len;
            memcpy(trim_chars[num_trim_chars].data, remove_begin, byte_len);
            ++num_trim_chars;
            remove_begin += byte_len;
        }

        auto is_trim_char = [&](const char* pos, size_t byte_len) -> bool {
            for (size_t c = 0; c < num_trim_chars; ++c) {
                if (trim_chars[c].len == byte_len &&
                    memcmp(trim_chars[c].data, pos, byte_len) == 0) {
                    return true;
                }
            }
            return false;
        };

        for (size_t i = 0; i < offset_size; ++i) {
            const char* str_begin =
                    reinterpret_cast<const char*>(str_data.data() + str_offsets[i - 1]);
            const char* str_end = reinterpret_cast<const char*>(str_data.data() + str_offsets[i]);
            const char* left_trim_pos = str_begin;
            const char* right_trim_pos = str_end;

            // ltrim
            while (left_trim_pos < str_end) {
                uint8_t byte_len = get_utf8_byte_length(static_cast<uint8_t>(*left_trim_pos));
                if (left_trim_pos + byte_len > str_end) break;
                if (!is_trim_char(left_trim_pos, byte_len)) break;
                left_trim_pos += byte_len;
            }

            // rtrim
            while (right_trim_pos > left_trim_pos) {
                const char* prev_char_pos = right_trim_pos;
                do {
                    --prev_char_pos;
                } while (prev_char_pos > left_trim_pos && (*prev_char_pos & 0xC0) == 0x80);
                size_t byte_len = right_trim_pos - prev_char_pos;
                if (!is_trim_char(prev_char_pos, byte_len)) break;
                right_trim_pos = prev_char_pos;
            }

            res_data.insert_assume_reserved(left_trim_pos, right_trim_pos);
            res_offsets[i] = (ColumnString::Offset)res_data.size();
        }
    }
};

// ==================== Benchmark helpers ====================

static void prepare_ascii_column(ColumnString& col, size_t num_rows, size_t str_len,
                                 const std::string& trim_chars_str) {
    std::mt19937 gen(42);
    std::uniform_int_distribution<int> char_dist(32, 126); // printable ASCII
    std::uniform_int_distribution<int> trim_count_dist(0, 10);

    for (size_t i = 0; i < num_rows; ++i) {
        std::string s;
        // Add random trim chars at front
        int front_count = trim_count_dist(gen);
        for (int j = 0; j < front_count; ++j) {
            s += trim_chars_str[gen() % trim_chars_str.size()];
        }
        // Add random non-trim content
        for (size_t j = 0; j < str_len; ++j) {
            char c;
            do {
                c = static_cast<char>(char_dist(gen));
            } while (trim_chars_str.find(c) != std::string::npos);
            s += c;
        }
        // Add random trim chars at back
        int back_count = trim_count_dist(gen);
        for (int j = 0; j < back_count; ++j) {
            s += trim_chars_str[gen() % trim_chars_str.size()];
        }
        col.insert_data(s.data(), s.size());
    }
}

static void prepare_utf8_column(ColumnString& col, size_t num_rows) {
    // Use a mix of ASCII and multi-byte UTF-8 characters
    // trim chars: "你好" (6 bytes, 2 chars)
    std::mt19937 gen(42);

    // some multi-byte chars for content
    const char* content_chars[] = {"a", "b", "c", "世", "界", "测", "试"};
    const size_t num_content = 7;
    // trim chars
    const char* trim_parts[] = {"你", "好"};
    const size_t num_trim = 2;

    std::uniform_int_distribution<int> trim_count_dist(0, 5);
    std::uniform_int_distribution<int> content_dist(0, num_content - 1);
    std::uniform_int_distribution<int> trim_dist(0, num_trim - 1);

    for (size_t i = 0; i < num_rows; ++i) {
        std::string s;
        int front = trim_count_dist(gen);
        for (int j = 0; j < front; ++j) {
            s += trim_parts[trim_dist(gen)];
        }
        // 10 content chars
        for (int j = 0; j < 10; ++j) {
            s += content_chars[content_dist(gen)];
        }
        int back = trim_count_dist(gen);
        for (int j = 0; j < back; ++j) {
            s += trim_parts[trim_dist(gen)];
        }
        col.insert_data(s.data(), s.size());
    }
}

// ==================== Benchmark functions ====================

static void BM_TrimInAscii_Old(benchmark::State& state) {
    size_t num_rows = state.range(0);
    std::string trim_chars = " \t\n\r";
    auto col = ColumnString::create();
    prepare_ascii_column(*col, num_rows, 20, trim_chars);

    StringRef remove_str(trim_chars.data(), trim_chars.size());

    for (auto _ : state) {
        // Match production overhead: is_ascii check on both remove_str and column data
        bool all_ascii = simd::VStringFunctions::is_ascii(remove_str) &&
                         simd::VStringFunctions::is_ascii(
                                 StringRef(reinterpret_cast<const char*>(col->get_chars().data()),
                                           col->get_chars().size()));
        benchmark::DoNotOptimize(all_ascii);
        auto res = ColumnString::create();
        TrimInOld::trim_in_ascii_old(col->get_chars(), col->get_offsets(), remove_str,
                                     res->get_chars(), res->get_offsets());
        benchmark::DoNotOptimize(res);
    }

    state.SetItemsProcessed(state.iterations() * num_rows);
}

static void BM_TrimInAscii_New(benchmark::State& state) {
    size_t num_rows = state.range(0);
    std::string trim_chars = " \t\n\r";
    auto col = ColumnString::create();
    prepare_ascii_column(*col, num_rows, 20, trim_chars);

    StringRef remove_str(trim_chars.data(), trim_chars.size());

    for (auto _ : state) {
        // Match production overhead: is_ascii check on both remove_str and column data
        bool all_ascii = simd::VStringFunctions::is_ascii(remove_str) &&
                         simd::VStringFunctions::is_ascii(
                                 StringRef(reinterpret_cast<const char*>(col->get_chars().data()),
                                           col->get_chars().size()));
        benchmark::DoNotOptimize(all_ascii);
        auto res = ColumnString::create();
        TrimInNew::trim_in_ascii_new(col->get_chars(), col->get_offsets(), remove_str,
                                     res->get_chars(), res->get_offsets());
        benchmark::DoNotOptimize(res);
    }

    state.SetItemsProcessed(state.iterations() * num_rows);
}

static void BM_TrimInUtf8_Old(benchmark::State& state) {
    size_t num_rows = state.range(0);
    auto col = ColumnString::create();
    prepare_utf8_column(*col, num_rows);

    std::string trim_chars = "你好";
    StringRef remove_str(trim_chars.data(), trim_chars.size());

    for (auto _ : state) {
        // Match production overhead: is_ascii check
        bool all_ascii = simd::VStringFunctions::is_ascii(remove_str) &&
                         simd::VStringFunctions::is_ascii(
                                 StringRef(reinterpret_cast<const char*>(col->get_chars().data()),
                                           col->get_chars().size()));
        benchmark::DoNotOptimize(all_ascii);
        auto res = ColumnString::create();
        TrimInOld::trim_in_utf8_old(col->get_chars(), col->get_offsets(), remove_str,
                                    res->get_chars(), res->get_offsets());
        benchmark::DoNotOptimize(res);
    }

    state.SetItemsProcessed(state.iterations() * num_rows);
}

static void BM_TrimInUtf8_New(benchmark::State& state) {
    size_t num_rows = state.range(0);
    auto col = ColumnString::create();
    prepare_utf8_column(*col, num_rows);

    std::string trim_chars = "你好";
    StringRef remove_str(trim_chars.data(), trim_chars.size());

    for (auto _ : state) {
        // Match production overhead: is_ascii check
        bool all_ascii = simd::VStringFunctions::is_ascii(remove_str) &&
                         simd::VStringFunctions::is_ascii(
                                 StringRef(reinterpret_cast<const char*>(col->get_chars().data()),
                                           col->get_chars().size()));
        benchmark::DoNotOptimize(all_ascii);
        auto res = ColumnString::create();
        TrimInNew::trim_in_utf8_new(col->get_chars(), col->get_offsets(), remove_str,
                                    res->get_chars(), res->get_offsets());
        benchmark::DoNotOptimize(res);
    }

    state.SetItemsProcessed(state.iterations() * num_rows);
}

// ---- ASCII: many trim chars (8 chars) ----
static void BM_TrimInAsciiManyChars_Old(benchmark::State& state) {
    size_t num_rows = state.range(0);
    std::string trim_chars = " \t\n\r.,-;";
    auto col = ColumnString::create();
    prepare_ascii_column(*col, num_rows, 20, trim_chars);

    StringRef remove_str(trim_chars.data(), trim_chars.size());

    for (auto _ : state) {
        // Match production overhead: is_ascii check
        bool all_ascii = simd::VStringFunctions::is_ascii(remove_str) &&
                         simd::VStringFunctions::is_ascii(
                                 StringRef(reinterpret_cast<const char*>(col->get_chars().data()),
                                           col->get_chars().size()));
        benchmark::DoNotOptimize(all_ascii);
        auto res = ColumnString::create();
        TrimInOld::trim_in_ascii_old(col->get_chars(), col->get_offsets(), remove_str,
                                     res->get_chars(), res->get_offsets());
        benchmark::DoNotOptimize(res);
    }

    state.SetItemsProcessed(state.iterations() * num_rows);
}

static void BM_TrimInAsciiManyChars_New(benchmark::State& state) {
    size_t num_rows = state.range(0);
    std::string trim_chars = " \t\n\r.,-;";
    auto col = ColumnString::create();
    prepare_ascii_column(*col, num_rows, 20, trim_chars);

    StringRef remove_str(trim_chars.data(), trim_chars.size());

    for (auto _ : state) {
        // Match production overhead: is_ascii check
        bool all_ascii = simd::VStringFunctions::is_ascii(remove_str) &&
                         simd::VStringFunctions::is_ascii(
                                 StringRef(reinterpret_cast<const char*>(col->get_chars().data()),
                                           col->get_chars().size()));
        benchmark::DoNotOptimize(all_ascii);
        auto res = ColumnString::create();
        TrimInNew::trim_in_ascii_new(col->get_chars(), col->get_offsets(), remove_str,
                                     res->get_chars(), res->get_offsets());
        benchmark::DoNotOptimize(res);
    }

    state.SetItemsProcessed(state.iterations() * num_rows);
}

// ---- No-match scenario (nothing to trim) ----
static void BM_TrimInAsciiNoMatch_Old(benchmark::State& state) {
    size_t num_rows = state.range(0);
    std::string trim_chars = "xyz"; // chars unlikely in data
    auto col = ColumnString::create();
    // prepare data without any trim chars
    for (size_t i = 0; i < num_rows; ++i) {
        std::string s(30, 'a');
        col->insert_data(s.data(), s.size());
    }

    StringRef remove_str(trim_chars.data(), trim_chars.size());

    for (auto _ : state) {
        // Match production overhead: is_ascii check
        bool all_ascii = simd::VStringFunctions::is_ascii(remove_str) &&
                         simd::VStringFunctions::is_ascii(
                                 StringRef(reinterpret_cast<const char*>(col->get_chars().data()),
                                           col->get_chars().size()));
        benchmark::DoNotOptimize(all_ascii);
        auto res = ColumnString::create();
        TrimInOld::trim_in_ascii_old(col->get_chars(), col->get_offsets(), remove_str,
                                     res->get_chars(), res->get_offsets());
        benchmark::DoNotOptimize(res);
    }

    state.SetItemsProcessed(state.iterations() * num_rows);
}

static void BM_TrimInAsciiNoMatch_New(benchmark::State& state) {
    size_t num_rows = state.range(0);
    std::string trim_chars = "xyz";
    auto col = ColumnString::create();
    for (size_t i = 0; i < num_rows; ++i) {
        std::string s(30, 'a');
        col->insert_data(s.data(), s.size());
    }

    StringRef remove_str(trim_chars.data(), trim_chars.size());

    for (auto _ : state) {
        // Match production overhead: is_ascii check
        bool all_ascii = simd::VStringFunctions::is_ascii(remove_str) &&
                         simd::VStringFunctions::is_ascii(
                                 StringRef(reinterpret_cast<const char*>(col->get_chars().data()),
                                           col->get_chars().size()));
        benchmark::DoNotOptimize(all_ascii);
        auto res = ColumnString::create();
        TrimInNew::trim_in_ascii_new(col->get_chars(), col->get_offsets(), remove_str,
                                     res->get_chars(), res->get_offsets());
        benchmark::DoNotOptimize(res);
    }

    state.SetItemsProcessed(state.iterations() * num_rows);
}

} // namespace doris

BENCHMARK(doris::BM_TrimInAscii_Old)
        ->Unit(benchmark::kMicrosecond)
        ->Arg(1024)
        ->Arg(4096)
        ->Arg(65536)
        ->Repetitions(3)
        ->DisplayAggregatesOnly();

BENCHMARK(doris::BM_TrimInAscii_New)
        ->Unit(benchmark::kMicrosecond)
        ->Arg(1024)
        ->Arg(4096)
        ->Arg(65536)
        ->Repetitions(3)
        ->DisplayAggregatesOnly();

BENCHMARK(doris::BM_TrimInAsciiManyChars_Old)
        ->Unit(benchmark::kMicrosecond)
        ->Arg(1024)
        ->Arg(4096)
        ->Arg(65536)
        ->Repetitions(3)
        ->DisplayAggregatesOnly();

BENCHMARK(doris::BM_TrimInAsciiManyChars_New)
        ->Unit(benchmark::kMicrosecond)
        ->Arg(1024)
        ->Arg(4096)
        ->Arg(65536)
        ->Repetitions(3)
        ->DisplayAggregatesOnly();

BENCHMARK(doris::BM_TrimInAsciiNoMatch_Old)
        ->Unit(benchmark::kMicrosecond)
        ->Arg(1024)
        ->Arg(4096)
        ->Arg(65536)
        ->Repetitions(3)
        ->DisplayAggregatesOnly();

BENCHMARK(doris::BM_TrimInAsciiNoMatch_New)
        ->Unit(benchmark::kMicrosecond)
        ->Arg(1024)
        ->Arg(4096)
        ->Arg(65536)
        ->Repetitions(3)
        ->DisplayAggregatesOnly();

BENCHMARK(doris::BM_TrimInUtf8_Old)
        ->Unit(benchmark::kMicrosecond)
        ->Arg(1024)
        ->Arg(4096)
        ->Arg(65536)
        ->Repetitions(3)
        ->DisplayAggregatesOnly();

BENCHMARK(doris::BM_TrimInUtf8_New)
        ->Unit(benchmark::kMicrosecond)
        ->Arg(1024)
        ->Arg(4096)
        ->Arg(65536)
        ->Repetitions(3)
        ->DisplayAggregatesOnly();
