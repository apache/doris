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

#include <vector>

#include "vec/core/wide_integer.h"

namespace doris {
template <typename T>
inline void bit_pack_32(const T* input, uint8_t in_num, int bit_width, uint8_t* output) {
    wide::UInt256 s = 0;
    uint8_t output_mask = 255;
    int tail_count = in_num & 31;
    int full_batch_size = (in_num >> 5) << 5;
    int output_size = bit_width << 2;

    for (int i = 0; i < full_batch_size; i += 32) {
        s |= static_cast<wide::UInt256>(input[i + 35]);
        s |= static_cast<wide::UInt256>(input[i + 34]) << (1 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 33]) << (2 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 32]) << (3 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 31]) << (4 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 30]) << (5 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 29]) << (6 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 28]) << (7 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 27]) << (8 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 26]) << (9 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 25]) << (10 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 24]) << (11 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 23]) << (12 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 22]) << (13 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 21]) << (14 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 20]) << (15 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 19]) << (16 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 18]) << (17 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 17]) << (18 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 16]) << (19 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 15]) << (20 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 14]) << (21 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 13]) << (22 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 12]) << (23 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 11]) << (24 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 10]) << (25 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 9]) << (26 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 8]) << (27 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 7]) << (28 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 6]) << (29 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 5]) << (30 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 4]) << (31 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 3]) << (32 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 2]) << (33 * bit_width);
        s |= static_cast<wide::UInt256>(input[i + 1]) << (34 * bit_width);
        s |= static_cast<wide::UInt256>(input[i]) << (35 * bit_width);

        for (int j = 0; j < output_size; j++) {
            output[j] = (s >> ((output_size - j - 1) << 3)) & output_mask;
        }
        output += output_size;
        s = 0;
    }

    // remainder
    int byte = tail_count * bit_width;
    int bytes = (byte + 7) >> 3;

    for (int i = 0; i < tail_count; i++) {
        s |= (static_cast<__int128_t>(input[i + full_batch_size]))
             << ((tail_count - i - 1) * bit_width);
    }

    s <<= (bytes << 3) - byte;

    for (int i = 0; i < bytes; i++) {
        output[i] = (s >> ((bytes - i - 1) << 3)) & output_mask;
    }
}

template <typename T, typename U>
inline void bit_pack_16(const T* input, uint8_t in_num, int bit_width, uint8_t* output) {
    U s = 0;
    uint8_t output_mask = 255;
    int tail_count = in_num & 15;
    int full_batch_size = (in_num >> 4) << 4;
    int output_size = bit_width << 1;

    for (int i = 0; i < full_batch_size; i += 16) {
        s |= static_cast<U>(input[i + 15]);
        s |= static_cast<U>(input[i + 14]) << (bit_width);
        s |= static_cast<U>(input[i + 13]) << (2 * bit_width);
        s |= static_cast<U>(input[i + 12]) << (3 * bit_width);
        s |= static_cast<U>(input[i + 11]) << (4 * bit_width);
        s |= static_cast<U>(input[i + 10]) << (5 * bit_width);
        s |= static_cast<U>(input[i + 9]) << (6 * bit_width);
        s |= static_cast<U>(input[i + 8]) << (7 * bit_width);
        s |= static_cast<U>(input[i + 7]) << (8 * bit_width);
        s |= (static_cast<U>(input[i + 6])) << (9 * bit_width);
        s |= (static_cast<U>(input[i + 5])) << (10 * bit_width);
        s |= (static_cast<U>(input[i + 4])) << (11 * bit_width);
        s |= (static_cast<U>(input[i + 3])) << (12 * bit_width);
        s |= (static_cast<U>(input[i + 2])) << (13 * bit_width);
        s |= (static_cast<U>(input[i + 1])) << (14 * bit_width);
        s |= (static_cast<U>(input[i])) << (15 * bit_width);

        for (int j = 0; j < output_size; j++) {
            output[j] = (s >> ((output_size - j - 1) << 3)) & output_mask;
        }
        output += output_size;
        s = 0;
    }

    // remainder
    int byte = tail_count * bit_width;
    int bytes = (byte + 7) >> 3;

    for (int i = 0; i < tail_count; i++) {
        s |= (static_cast<__int128_t>(input[i + full_batch_size]))
             << ((tail_count - i - 1) * bit_width);
    }

    s <<= (bytes << 3) - byte;

    for (int i = 0; i < bytes; i++) {
        output[i] = (s >> ((bytes - i - 1) << 3)) & output_mask;
    }
}

template <typename T, typename U>
inline void bit_pack_8(const T* input, uint8_t in_num, int bit_width, uint8_t* output) {
    U s = 0;
    uint8_t output_mask = 255;
    int tail_count = in_num & 7;
    int full_batch_size = (in_num >> 3) << 3;

    for (int i = 0; i < full_batch_size; i += 8) {
        s |= static_cast<U>(input[i + 7]);
        s |= (static_cast<U>(input[i + 6])) << bit_width;
        s |= (static_cast<U>(input[i + 5])) << (2 * bit_width);
        s |= (static_cast<U>(input[i + 4])) << (3 * bit_width);
        s |= (static_cast<U>(input[i + 3])) << (4 * bit_width);
        s |= (static_cast<U>(input[i + 2])) << (5 * bit_width);
        s |= (static_cast<U>(input[i + 1])) << (6 * bit_width);
        s |= (static_cast<U>(input[i])) << (7 * bit_width);

        for (int j = 0; j < bit_width; j++) {
            output[j] = (s >> ((bit_width - j - 1) << 3)) & output_mask;
        }
        output += bit_width;
        s = 0;
    }

    // remainder
    int byte = tail_count * bit_width;
    int bytes = (byte + 7) >> 3;

    for (int i = 0; i < tail_count; i++) {
        s |= (static_cast<U>(input[i + full_batch_size])) << ((tail_count - i - 1) * bit_width);
    }

    s <<= (bytes << 3) - byte;

    for (int i = 0; i < bytes; i++) {
        output[i] = (s >> ((bytes - i - 1) << 3)) & output_mask;
    }
}

template <typename T, typename U>
inline void bit_pack_4(const T* input, uint8_t in_num, int bit_width, uint8_t* output) {
    U s = 0;
    uint8_t output_mask = 255;
    int tail_count = in_num & 3;
    int full_batch_size = (in_num >> 2) << 2;
    int output_size = 0; // How many outputs can be processed at a time
    int bit_width_remainder =
            (bit_width << 2) & 7; // How many bits will be left after processing 4 numbers at a time
    int extra_bit = 0;            // Extra bits after each process

    for (int i = 0; i < full_batch_size; i += 4) {
        s <<= bit_width;
        s |= (static_cast<U>(input[i]));
        s <<= bit_width;
        s |= (static_cast<U>(input[i + 1]));
        s <<= bit_width;
        s |= (static_cast<U>(input[i + 2]));
        s <<= bit_width;
        s |= (static_cast<U>(input[i + 3]));

        output_size = (bit_width * 4 + extra_bit) >> 3;
        extra_bit = (extra_bit + bit_width_remainder) & 7;
        for (int j = 0; j < output_size; j++) {
            output[j] = (s >> (((output_size - j - 1) << 3) + extra_bit)) & output_mask;
        }
        output += output_size;
        s &= (1 << extra_bit) - 1;
    }

    // remainder
    int byte = tail_count * bit_width;
    if (extra_bit != 0) byte += extra_bit;
    int bytes = (byte + 7) >> 3;

    for (int i = 0; i < tail_count; i++) {
        s <<= bit_width;
        s |= (input[i + full_batch_size]);
    }

    s <<= (bytes << 3) - byte;

    for (int i = 0; i < bytes; i++) {
        output[i] = (s >> (((bytes - i - 1) << 3))) & output_mask;
    }
}

template <typename T, typename U>
inline void bit_pack_2(const T* input, uint8_t in_num, int bit_width, uint8_t* output) {
    U s = 0;
    uint8_t output_mask = 255;
    int tail_count = in_num & 1;
    int full_batch_size = (in_num >> 1) << 1;
    int output_size = 0; // How many outputs can be processed at a time
    int bit_width_remainder =
            (bit_width << 1) & 7; // How many bits will be left after processing 4 numbers at a time
    int extra_bit = 0;            // Extra bits after each process

    for (int i = 0; i < full_batch_size; i += 2) {
        s <<= bit_width;
        s |= (static_cast<U>(input[i]));
        s <<= bit_width;
        s |= (static_cast<U>(input[i + 1]));

        output_size = (bit_width + bit_width + extra_bit) >> 3;
        extra_bit = (extra_bit + bit_width_remainder) & 7;
        for (int j = 0; j < output_size; j++) {
            output[j] = (s >> (((output_size - j - 1) << 3) + extra_bit)) & output_mask;
        }
        output += output_size;
        s &= (1 << extra_bit) - 1;
    }

    // remainder
    int byte = tail_count * bit_width;
    if (extra_bit != 0) byte += extra_bit;
    int bytes = (byte + 7) >> 3;

    for (int i = 0; i < tail_count; i++) {
        s <<= bit_width;
        s |= (input[i + full_batch_size]);
    }

    s <<= (bytes << 3) - byte;

    for (int i = 0; i < bytes; i++) {
        output[i] = (s >> (((bytes - i - 1) << 3))) & output_mask;
    }
}

template <typename T>
void bit_pack_1(const T* input, uint8_t in_num, int bit_width, uint8_t* output) {
    int output_mask = 255;
    int need_bit = 0; // still need

    for (int i = 0; i < in_num; i++) {
        T x = input[i];
        int width = bit_width;
        if (need_bit) {
            *output |= x >> (width - need_bit);
            output++;
            width -= need_bit;
        }
        int num = width >> 3;      // How many outputs can be processed at a time
        int remainder = width & 7; // How many bits are left to store

        for (int j = 0; j < num; j++) {
            *output = (x >> (((num - j - 1) << 3) + remainder)) & output_mask;
            output++;
        }
        if (remainder) {
            *output = (x & ((1 << remainder) - 1)) << (8 - remainder);
            need_bit = 8 - remainder;
        } else {
            need_bit = 0;
        }
    }
}

void get_testdata(__int128_t* test_data, int n, int w) {
    __int128_t in_mask = ((__int128_t(1)) << w) - 1;
    for (int i = 0; i < n; i++) {
        test_data[i] = (i & in_mask);
    }
}

static void BM_BitPack_w8_8_int64(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;
    std::vector<__int128_t> test_data(n);
    get_testdata(test_data.data(), n, w);
    int size = (n * w + 7) / 8;
    std::vector<uint8_t> output(size);

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());
        benchmark::DoNotOptimize(output.data());
        bit_pack_8<__int128_t, int64_t>(test_data.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}
BENCHMARK(BM_BitPack_w8_8_int64)->DenseRange(1, 8)->Unit(benchmark::kNanosecond);

static void BM_BitPack_w8_16_int128(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;
    std::vector<__int128_t> test_data(n);
    get_testdata(test_data.data(), n, w);
    int size = (n * w + 7) / 8;
    std::vector<uint8_t> output(size);

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());
        benchmark::DoNotOptimize(output.data());
        bit_pack_16<__int128_t, __int128_t>(test_data.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}
BENCHMARK(BM_BitPack_w8_16_int128)->DenseRange(1, 8)->Unit(benchmark::kNanosecond);

static void BM_BitPack_w8_32_int256(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;
    std::vector<__int128_t> test_data(n);
    get_testdata(test_data.data(), n, w);
    int size = (n * w + 7) / 8;
    std::vector<uint8_t> output(size);

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());
        benchmark::DoNotOptimize(output.data());
        bit_pack_32(test_data.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}
BENCHMARK(BM_BitPack_w8_32_int256)->DenseRange(1, 8)->Unit(benchmark::kNanosecond);

static void BM_BitPack_w16_4_int64(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;
    std::vector<__int128_t> test_data(n);
    get_testdata(test_data.data(), n, w);
    int size = (n * w + 7) / 8;
    std::vector<uint8_t> output(size);

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());
        benchmark::DoNotOptimize(output.data());
        bit_pack_16<__int128_t, int64_t>(test_data.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}
BENCHMARK(BM_BitPack_w16_4_int64)->DenseRange(9, 16)->Unit(benchmark::kNanosecond);

static void BM_BitPack_w16_8_int128(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;
    std::vector<__int128_t> test_data(n);
    get_testdata(test_data.data(), n, w);
    int size = (n * w + 7) / 8;
    std::vector<uint8_t> output(size);

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());
        benchmark::DoNotOptimize(output.data());
        bit_pack_8<__int128_t, __int128_t>(test_data.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}
BENCHMARK(BM_BitPack_w16_8_int128)->DenseRange(9, 16)->Unit(benchmark::kNanosecond);

static void BM_BitPack_w16_16_int256(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;
    std::vector<__int128_t> test_data(n);
    get_testdata(test_data.data(), n, w);
    int size = (n * w + 7) / 8;
    std::vector<uint8_t> output(size);

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());
        benchmark::DoNotOptimize(output.data());
        bit_pack_16<__int128_t, wide::UInt256>(test_data.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}
BENCHMARK(BM_BitPack_w16_16_int256)->DenseRange(9, 16)->Unit(benchmark::kNanosecond);

static void BM_BitPack_w32_2_int128(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;
    std::vector<__int128_t> test_data(n);
    get_testdata(test_data.data(), n, w);
    int size = (n * w + 7) / 8;
    std::vector<uint8_t> output(size);

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());
        benchmark::DoNotOptimize(output.data());
        bit_pack_2<__int128_t, __int128_t>(test_data.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}
BENCHMARK(BM_BitPack_w32_2_int128)->DenseRange(17, 32)->Unit(benchmark::kNanosecond);

static void BM_BitPack_w32_4_int128(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;
    std::vector<__int128_t> test_data(n);
    get_testdata(test_data.data(), n, w);
    int size = (n * w + 7) / 8;
    std::vector<uint8_t> output(size);

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());
        benchmark::DoNotOptimize(output.data());
        bit_pack_4<__int128_t, __int128_t>(test_data.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}
BENCHMARK(BM_BitPack_w32_4_int128)->DenseRange(17, 32)->Unit(benchmark::kNanosecond);

static void BM_BitPack_w32_8_int256(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;
    std::vector<__int128_t> test_data(n);
    get_testdata(test_data.data(), n, w);
    int size = (n * w + 7) / 8;
    std::vector<uint8_t> output(size);

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());
        benchmark::DoNotOptimize(output.data());
        bit_pack_8<__int128_t, wide::UInt256>(test_data.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}
BENCHMARK(BM_BitPack_w32_8_int256)->DenseRange(17, 32)->Unit(benchmark::kNanosecond);

static void BM_BitPack_w64_4_int256(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;
    std::vector<__int128_t> test_data(n);
    get_testdata(test_data.data(), n, w);
    int size = (n * w + 7) / 8;
    std::vector<uint8_t> output(size);

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());
        benchmark::DoNotOptimize(output.data());
        bit_pack_4<__int128_t, wide::UInt256>(test_data.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}
BENCHMARK(BM_BitPack_w64_4_int256)->DenseRange(33, 64)->Unit(benchmark::kNanosecond);

static void BM_BitPack_1(benchmark::State& state) {
    int w = state.range(0);
    int n = 255;
    std::vector<__int128_t> test_data(n);
    get_testdata(test_data.data(), n, w);
    int size = (n * w + 7) / 8;
    std::vector<uint8_t> output(size);

    for (auto _ : state) {
        benchmark::DoNotOptimize(test_data.data());
        benchmark::DoNotOptimize(output.data());
        bit_pack_1<__int128_t>(test_data.data(), n, w, output.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(int64_t(state.iterations()) * size);
}
BENCHMARK(BM_BitPack_1)->DenseRange(1, 32)->Unit(benchmark::kNanosecond);
} // namespace doris