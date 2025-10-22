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

#include <random>
#include <vector>

#include "vec/functions/function_string.cpp"
#include "vec/functions/string_hex_util.h"

namespace doris::vectorized {

// old logic for to_base64
struct OldToBase64Impl {
    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnString::Chars& dst_data, ColumnString::Offsets& dst_offsets) {
        auto rows_count = offsets.size();
        dst_offsets.resize(rows_count);
        std::array<char, string_hex::MAX_STACK_CIPHER_LEN> stack_buf;
        std::vector<char> heap_buf;
        for (int i = 0; i < rows_count; ++i) {
            const auto* source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            size_t srclen = offsets[i] - offsets[i - 1];

            if (srclen == 0) {
                StringOP::push_empty_string(i, dst_data, dst_offsets);
                continue;
            }

            auto cipher_len = 4 * ((srclen + 2) / 3);
            char* dst = nullptr;
            if (cipher_len <= stack_buf.size()) {
                dst = stack_buf.data();
            } else {
                heap_buf.resize(cipher_len);
                dst = heap_buf.data();
            }

            auto outlen = base64_encode((const unsigned char*)source, srclen, (unsigned char*)dst);

            StringOP::push_value_string(std::string_view(dst, outlen), i, dst_data, dst_offsets);
        }
        return Status::OK();
    }
};

// old logic for from_base64
struct OldFromBase64Impl {
    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnString::Chars& dst_data, ColumnString::Offsets& dst_offsets,
                         NullMap& null_map) {
        auto rows_count = offsets.size();
        dst_offsets.resize(rows_count);
        std::array<char, string_hex::MAX_STACK_CIPHER_LEN> stack_buf;
        std::vector<char> heap_buf;
        for (int i = 0; i < rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, dst_data, dst_offsets, null_map);
                continue;
            }

            const auto* source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            ColumnString::Offset srclen = offsets[i] - offsets[i - 1];

            if (srclen == 0) {
                StringOP::push_empty_string(i, dst_data, dst_offsets);
                continue;
            }

            auto cipher_len = srclen / 4 * 3;
            char* dst = nullptr;
            if (cipher_len <= stack_buf.size()) {
                dst = stack_buf.data();
            } else {
                heap_buf.resize(cipher_len);
                dst = heap_buf.data();
            }
            auto outlen = base64_decode(source, srclen, dst);

            if (outlen < 0) {
                StringOP::push_null_string(i, dst_data, dst_offsets, null_map);
            } else {
                StringOP::push_value_string(std::string_view(dst, outlen), i, dst_data,
                                            dst_offsets);
            }
        }

        return Status::OK();
    }
};

// old logic for unhex
struct OldUnHexImpl {
    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnString::Chars& dst_data, ColumnString::Offsets& dst_offsets) {
        auto rows_count = offsets.size();
        dst_offsets.resize(rows_count);
        std::array<char, string_hex::MAX_STACK_CIPHER_LEN> stack_buf;
        std::vector<char> heap_buf;
        for (int i = 0; i < rows_count; ++i) {
            const auto* source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            ColumnString::Offset srclen = offsets[i] - offsets[i - 1];

            if (srclen == 0) {
                StringOP::push_empty_string(i, dst_data, dst_offsets);
                continue;
            }

            auto cipher_len = srclen / 2;
            char* dst = nullptr;
            if (cipher_len <= stack_buf.size()) {
                dst = stack_buf.data();
            } else {
                heap_buf.resize(cipher_len);
                dst = heap_buf.data();
            }

            int outlen = string_hex::hex_decode(source, srclen, dst);
            StringOP::push_value_string(std::string_view(dst, outlen), i, dst_data, dst_offsets);
        }

        return Status::OK();
    }

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnString::Chars& dst_data, ColumnString::Offsets& dst_offsets,
                         ColumnUInt8::Container* null_map_data) {
        auto rows_count = offsets.size();
        dst_offsets.resize(rows_count);
        std::array<char, string_hex::MAX_STACK_CIPHER_LEN> stack_buf;
        std::vector<char> heap_buf;
        for (int i = 0; i < rows_count; ++i) {
            const auto* source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            ColumnString::Offset srclen = offsets[i] - offsets[i - 1];

            if (srclen == 0) {
                StringOP::push_null_string(i, dst_data, dst_offsets, *null_map_data);
                continue;
            }

            auto cipher_len = srclen / 2;
            char* dst = nullptr;
            if (cipher_len <= stack_buf.size()) {
                dst = stack_buf.data();
            } else {
                heap_buf.resize(cipher_len);
                dst = heap_buf.data();
            }

            int outlen = string_hex::hex_decode(source, srclen, dst);
            if (outlen == 0) {
                StringOP::push_null_string(i, dst_data, dst_offsets, *null_map_data);
                continue;
            }

            StringOP::push_value_string(std::string_view(dst, outlen), i, dst_data, dst_offsets);
        }

        return Status::OK();
    }
};

static void generate_test_data(ColumnString::Chars& data, ColumnString::Offsets& offsets,
                               size_t num_rows, size_t str_len, unsigned char max_char) {
    const std::string base64_chars =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz"
            "0123456789+/";
    std::mt19937 rng(12345);
    std::uniform_int_distribution<unsigned char> dist(0, max_char);

    offsets.resize(num_rows);
    data.clear();
    data.reserve(num_rows * str_len);

    size_t offset = 0;
    for (size_t i = 0; i < num_rows; ++i) {
        for (size_t j = 0; j < str_len; ++j) {
            data.push_back(static_cast<char>(base64_chars[dist(rng)]));
        }
        offset += str_len;
        offsets[i] = cast_set<uint32_t>(offset);
    }
}

static void BM_ToBase64Impl_Old(benchmark::State& state) {
    size_t rows = state.range(0);
    size_t len = state.range(1);
    ColumnString::Chars data;
    ColumnString::Offsets offsets;
    generate_test_data(data, offsets, rows, len, 63);

    ColumnString::Chars dst_data;
    ColumnString::Offsets dst_offsets;

    for (auto _ : state) {
        dst_data.clear();
        dst_offsets.clear();
        benchmark::DoNotOptimize(OldToBase64Impl::vector(data, offsets, dst_data, dst_offsets));
    }
}

static void BM_ToBase64Impl_New(benchmark::State& state) {
    size_t rows = state.range(0);
    size_t len = state.range(1);
    ColumnString::Chars data;
    ColumnString::Offsets offsets;
    generate_test_data(data, offsets, rows, len, 63);

    ColumnString::Chars dst_data;
    ColumnString::Offsets dst_offsets;

    for (auto _ : state) {
        dst_data.clear();
        dst_offsets.clear();
        benchmark::DoNotOptimize(ToBase64Impl::vector(data, offsets, dst_data, dst_offsets));
    }
}

// 10, 100000 is a big data test case for testing memory allocation on the heap
BENCHMARK(BM_ToBase64Impl_Old)
        ->Args({1000, 256})
        ->Args({100, 65536})
        ->Args({10, 100000})
        ->Unit(benchmark::kNanosecond);
BENCHMARK(BM_ToBase64Impl_New)
        ->Args({1000, 256})
        ->Args({100, 65536})
        ->Args({10, 100000})
        ->Unit(benchmark::kNanosecond);

static void BM_FromBase64Impl_Old(benchmark::State& state) {
    size_t rows = state.range(0);
    size_t len = state.range(1);
    ColumnString::Chars data;
    ColumnString::Offsets offsets;
    auto null_map = ColumnUInt8::create(rows, 0);
    generate_test_data(data, offsets, rows, len, 63);

    ColumnString::Chars dst_data;
    ColumnString::Offsets dst_offsets;

    for (auto _ : state) {
        dst_data.clear();
        dst_offsets.clear();
        benchmark::DoNotOptimize(OldFromBase64Impl::vector(data, offsets, dst_data, dst_offsets,
                                                           null_map->get_data()));
    }
}

static void BM_FromBase64Impl_New(benchmark::State& state) {
    size_t rows = state.range(0);
    size_t len = state.range(1);
    ColumnString::Chars data;
    ColumnString::Offsets offsets;
    auto null_map = ColumnUInt8::create(rows, 0);
    generate_test_data(data, offsets, rows, len, 63);

    ColumnString::Chars dst_data;
    ColumnString::Offsets dst_offsets;

    for (auto _ : state) {
        dst_data.clear();
        dst_offsets.clear();
        benchmark::DoNotOptimize(
                FromBase64Impl::vector(data, offsets, dst_data, dst_offsets, null_map->get_data()));
    }
}

// 10, 100000 is a big data test case for testing memory allocation on the heap
BENCHMARK(BM_FromBase64Impl_Old)
        ->Args({1000, 256})
        ->Args({100, 65536})
        ->Args({10, 100000})
        ->Unit(benchmark::kNanosecond);
BENCHMARK(BM_FromBase64Impl_New)
        ->Args({1000, 256})
        ->Args({100, 65536})
        ->Args({10, 100000})
        ->Unit(benchmark::kNanosecond);

static void BM_UnhexImpl_Old(benchmark::State& state) {
    size_t rows = state.range(0);
    size_t len = state.range(1);
    ColumnString::Chars data;
    ColumnString::Offsets offsets;
    generate_test_data(data, offsets, rows, len, 16);

    ColumnString::Chars dst_data;
    ColumnString::Offsets dst_offsets;

    for (auto _ : state) {
        dst_data.clear();
        dst_offsets.clear();
        benchmark::DoNotOptimize(OldUnHexImpl::vector(data, offsets, dst_data, dst_offsets));
    }
}

static void BM_UnhexImpl_New(benchmark::State& state) {
    size_t rows = state.range(0);
    size_t len = state.range(1);
    ColumnString::Chars data;
    ColumnString::Offsets offsets;
    generate_test_data(data, offsets, rows, len, 16);

    ColumnString::Chars dst_data;
    ColumnString::Offsets dst_offsets;

    for (auto _ : state) {
        dst_data.clear();
        dst_offsets.clear();
        benchmark::DoNotOptimize(
                UnHexImpl<UnHexImplEmpty>::vector(data, offsets, dst_data, dst_offsets));
    }
}

// 100, 100000 is a big data test case for testing memory allocation on the heap
BENCHMARK(BM_UnhexImpl_Old)
        ->Args({1000, 256})
        ->Args({100, 65536})
        ->Args({100, 100000})
        ->Unit(benchmark::kNanosecond);
BENCHMARK(BM_UnhexImpl_New)
        ->Args({1000, 256})
        ->Args({100, 65536})
        ->Args({100, 100000})
        ->Unit(benchmark::kNanosecond);

static void BM_UnhexNullImpl_Old(benchmark::State& state) {
    size_t rows = state.range(0);
    size_t len = state.range(1);
    ColumnString::Chars data;
    ColumnString::Offsets offsets;
    auto null_map = ColumnUInt8::create(rows, 0);
    generate_test_data(data, offsets, rows, len, 16);

    ColumnString::Chars dst_data;
    ColumnString::Offsets dst_offsets;

    for (auto _ : state) {
        dst_data.clear();
        dst_offsets.clear();
        benchmark::DoNotOptimize(
                OldUnHexImpl::vector(data, offsets, dst_data, dst_offsets, &null_map->get_data()));
    }
}

static void BM_UnhexNullImpl_New(benchmark::State& state) {
    size_t rows = state.range(0);
    size_t len = state.range(1);
    ColumnString::Chars data;
    ColumnString::Offsets offsets;
    auto null_map = ColumnUInt8::create(rows, 0);
    generate_test_data(data, offsets, rows, len, 16);

    ColumnString::Chars dst_data;
    ColumnString::Offsets dst_offsets;

    for (auto _ : state) {
        dst_data.clear();
        dst_offsets.clear();
        benchmark::DoNotOptimize(UnHexImpl<UnHexImplNull>::vector(
                data, offsets, dst_data, dst_offsets, &null_map->get_data()));
    }
}

// 100, 100000 is a big data test case for testing memory allocation on the heap
BENCHMARK(BM_UnhexNullImpl_Old)
        ->Args({1000, 256})
        ->Args({100, 65536})
        ->Args({100, 100000})
        ->Unit(benchmark::kNanosecond);
BENCHMARK(BM_UnhexNullImpl_New)
        ->Args({1000, 256})
        ->Args({100, 65536})
        ->Args({100, 100000})
        ->Unit(benchmark::kNanosecond);

} // namespace doris::vectorized