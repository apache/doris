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

#include "exprs/function/function_string.cpp"
#include "exprs/function/string_hex_util.h"

namespace doris {

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

struct OldRepeatImpl {
    static Status vector_vector(const ColumnString::Chars& data,
                                const ColumnString::Offsets& offsets,
                                const ColumnInt32::Container& repeats,
                                ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets,
                                ColumnUInt8::Container& null_map) {
        const size_t input_row_size = offsets.size();

        fmt::memory_buffer buffer;
        res_offsets.resize(input_row_size);
        null_map.resize_fill(input_row_size, 0);
        for (size_t i = 0; i < input_row_size; ++i) {
            buffer.clear();
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            const size_t size = offsets[i] - offsets[i - 1];
            const int repeat = repeats[i];
            if (repeat <= 0) {
                StringOP::push_empty_string(i, res_data, res_offsets);
                continue;
            }

            ColumnString::check_chars_length(static_cast<size_t>(repeat) * size + res_data.size(),
                                             0);
            for (int j = 0; j < repeat; ++j) {
                buffer.append(raw_str, raw_str + size);
            }
            StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i, res_data,
                                        res_offsets);
        }
        return Status::OK();
    }

    static Status vector_const(const ColumnString::Chars& data,
                               const ColumnString::Offsets& offsets, int repeat,
                               ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets,
                               ColumnUInt8::Container& null_map) {
        const size_t input_row_size = offsets.size();

        fmt::memory_buffer buffer;
        res_offsets.resize(input_row_size);
        null_map.resize_fill(input_row_size, 0);
        for (size_t i = 0; i < input_row_size; ++i) {
            buffer.clear();
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            const size_t size = offsets[i] - offsets[i - 1];
            ColumnString::check_chars_length(static_cast<size_t>(repeat) * size + res_data.size(),
                                             0);
            for (int j = 0; j < repeat; ++j) {
                buffer.append(raw_str, raw_str + size);
            }
            StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i, res_data,
                                        res_offsets);
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

    uint32_t offset = 0;
    for (size_t i = 0; i < num_rows; ++i) {
        for (size_t j = 0; j < str_len; ++j) {
            data.push_back(static_cast<char>(base64_chars[dist(rng)]));
        }
        offset += str_len;
        offsets[i] = offset;
    }
}

static ColumnString::MutablePtr generate_test_string_column(size_t num_rows, size_t str_len,
                                                            unsigned char max_char) {
    auto column = ColumnString::create();
    generate_test_data(column->get_chars(), column->get_offsets(), num_rows, str_len, max_char);
    return column;
}

static void generate_repeat_times(ColumnInt32::Container& repeats, size_t num_rows,
                                  int32_t max_repeat) {
    repeats.resize(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        repeats[i] = max_repeat == 0 ? 0 : static_cast<int32_t>(i % max_repeat) + 1;
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
        auto status = OldToBase64Impl::vector(data, offsets, dst_data, dst_offsets);
        benchmark::DoNotOptimize(status);
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
        auto status = ToBase64Impl::vector(data, offsets, dst_data, dst_offsets);
        benchmark::DoNotOptimize(status);
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
        auto status = OldFromBase64Impl::vector(data, offsets, dst_data, dst_offsets,
                                                null_map->get_data());
        benchmark::DoNotOptimize(status);
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
        auto status =
                FromBase64Impl::vector(data, offsets, dst_data, dst_offsets, null_map->get_data());
        benchmark::DoNotOptimize(status);
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
        auto status = OldUnHexImpl::vector(data, offsets, dst_data, dst_offsets);
        benchmark::DoNotOptimize(status);
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
        auto status = UnHexImpl<UnHexImplEmpty>::vector(data, offsets, dst_data, dst_offsets);
        benchmark::DoNotOptimize(status);
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
        auto status =
                OldUnHexImpl::vector(data, offsets, dst_data, dst_offsets, &null_map->get_data());
        benchmark::DoNotOptimize(status);
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
        auto status = UnHexImpl<UnHexImplNull>::vector(data, offsets, dst_data, dst_offsets,
                                                       &null_map->get_data());
        benchmark::DoNotOptimize(status);
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

static void BM_RepeatVectorImpl_Old(benchmark::State& state) {
    const size_t rows = state.range(0);
    const size_t len = state.range(1);
    const auto max_repeat = static_cast<int32_t>(state.range(2));
    ColumnString::Chars data;
    ColumnString::Offsets offsets;
    ColumnInt32::Container repeats;
    ColumnString::Chars dst_data;
    ColumnString::Offsets dst_offsets;
    ColumnUInt8::Container null_map;

    generate_test_data(data, offsets, rows, len, 63);
    generate_repeat_times(repeats, rows, max_repeat);

    for (auto _ : state) {
        dst_data.clear();
        dst_offsets.clear();
        null_map.clear();
        auto status = OldRepeatImpl::vector_vector(data, offsets, repeats, dst_data, dst_offsets,
                                                   null_map);
        benchmark::DoNotOptimize(status);
    }
}

static void BM_RepeatVectorImpl_New(benchmark::State& state) {
    const size_t rows = state.range(0);
    const size_t len = state.range(1);
    const auto max_repeat = static_cast<int32_t>(state.range(2));
    ColumnInt32::Container repeats;
    ColumnString::Chars dst_data;
    ColumnString::Offsets dst_offsets;
    ColumnUInt8::Container null_map;
    FunctionStringRepeat function;
    auto source_column = generate_test_string_column(rows, len, 63);

    generate_repeat_times(repeats, rows, max_repeat);

    for (auto _ : state) {
        dst_data.clear();
        dst_offsets.clear();
        null_map.clear();
        auto status =
                function.vector_vector(*source_column, repeats, dst_data, dst_offsets, null_map);
        benchmark::DoNotOptimize(status);
    }
}

BENCHMARK(BM_RepeatVectorImpl_Old)
        ->Args({4096, 16, 8})
        ->Args({4096, 16, 64})
        ->Args({1024, 128, 16})
        ->Args({4096, 0, 64})
        ->Unit(benchmark::kNanosecond);
BENCHMARK(BM_RepeatVectorImpl_New)
        ->Args({4096, 16, 8})
        ->Args({4096, 16, 64})
        ->Args({1024, 128, 16})
        ->Args({4096, 0, 64})
        ->Unit(benchmark::kNanosecond);

static void BM_RepeatConstImpl_Old(benchmark::State& state) {
    const size_t rows = state.range(0);
    const size_t len = state.range(1);
    const auto repeat = static_cast<int32_t>(state.range(2));
    ColumnString::Chars data;
    ColumnString::Offsets offsets;
    ColumnString::Chars dst_data;
    ColumnString::Offsets dst_offsets;
    ColumnUInt8::Container null_map;

    generate_test_data(data, offsets, rows, len, 63);

    for (auto _ : state) {
        dst_data.clear();
        dst_offsets.clear();
        null_map.clear();
        auto status =
                OldRepeatImpl::vector_const(data, offsets, repeat, dst_data, dst_offsets, null_map);
        benchmark::DoNotOptimize(status);
    }
}

static void BM_RepeatConstImpl_New(benchmark::State& state) {
    const size_t rows = state.range(0);
    const size_t len = state.range(1);
    const auto repeat = static_cast<int32_t>(state.range(2));
    ColumnString::Chars dst_data;
    ColumnString::Offsets dst_offsets;
    ColumnUInt8::Container null_map;
    FunctionStringRepeat function;
    auto source_column = generate_test_string_column(rows, len, 63);

    for (auto _ : state) {
        dst_data.clear();
        dst_offsets.clear();
        null_map.clear();
        static_cast<void>(
                function.vector_const(*source_column, repeat, dst_data, dst_offsets, null_map));
        benchmark::ClobberMemory();
    }
}

BENCHMARK(BM_RepeatConstImpl_Old)
        ->Args({4096, 16, 8})
        ->Args({4096, 16, 64})
        ->Args({1024, 128, 16})
        ->Args({4096, 0, 64})
        ->Unit(benchmark::kNanosecond);
BENCHMARK(BM_RepeatConstImpl_New)
        ->Args({4096, 16, 8})
        ->Args({4096, 16, 64})
        ->Args({1024, 128, 16})
        ->Args({4096, 0, 64})
        ->Unit(benchmark::kNanosecond);

} // namespace doris