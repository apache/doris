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

#include "util/simd/parquet_kernels.h"

#include <array>
#include <cmath>
#include <cstring>
#include <type_traits>

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#include <immintrin.h>
#define DORIS_PARQUET_X86_SIMD
#endif

namespace doris::parquet_simd {
namespace {

bool has_avx2() {
#ifdef DORIS_PARQUET_X86_SIMD
    return __builtin_cpu_supports("avx2");
#else
    return false;
#endif
}

void byte_stream_split_decode_scalar(const uint8_t* src, size_t width, size_t offset,
                                     size_t num_values, size_t stride, uint8_t* dest) {
    for (size_t row = 0; row < num_values; ++row) {
        for (size_t byte = 0; byte < width; ++byte) {
            dest[row * width + byte] = src[byte * stride + offset + row];
        }
    }
}

#ifdef DORIS_PARQUET_X86_SIMD
template <size_t WIDTH>
__attribute__((target("avx2"))) void byte_stream_split_decode_avx2(const uint8_t* src,
                                                                   size_t offset, size_t num_values,
                                                                   size_t stride, uint8_t* dest) {
    static_assert(WIDTH == 4 || WIDTH == 8);
    constexpr size_t STEPS = WIDTH == 8 ? 3 : 2;
    constexpr size_t LANES = sizeof(__m256i);
    const size_t blocks = num_values / LANES;
    __m256i stage[STEPS + 1][WIDTH];
    __m256i result[WIDTH];

    for (size_t block = 0; block < blocks; ++block) {
        for (size_t stream = 0; stream < WIDTH; ++stream) {
            stage[0][stream] = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(
                    src + stream * stride + offset + block * LANES));
        }
        for (size_t step = 0; step < STEPS; ++step) {
            // AVX2 unpack instructions operate independently in each 128-bit half. Keep the
            // byte-transpose hierarchy lane-local, then stitch the halves only after the last
            // unpack so every output vector contains contiguous decoded rows.
            for (size_t pair = 0; pair < WIDTH / 2; ++pair) {
                stage[step + 1][pair * 2] =
                        _mm256_unpacklo_epi8(stage[step][pair], stage[step][WIDTH / 2 + pair]);
                stage[step + 1][pair * 2 + 1] =
                        _mm256_unpackhi_epi8(stage[step][pair], stage[step][WIDTH / 2 + pair]);
            }
        }
        for (size_t pair = 0; pair < WIDTH / 2; ++pair) {
            result[pair] = _mm256_permute2x128_si256(stage[STEPS][pair * 2],
                                                     stage[STEPS][pair * 2 + 1], 0x20);
            result[WIDTH / 2 + pair] = _mm256_permute2x128_si256(stage[STEPS][pair * 2],
                                                                 stage[STEPS][pair * 2 + 1], 0x31);
        }
        for (size_t lane = 0; lane < WIDTH; ++lane) {
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + (block * WIDTH + lane) * LANES),
                                result[lane]);
        }
    }
    const size_t processed = blocks * LANES;
    byte_stream_split_decode_scalar(src, WIDTH, offset + processed, num_values - processed, stride,
                                    dest + processed * WIDTH);
}

__attribute__((target("avx2"))) void delta_decode_int32_avx2(int32_t* values, size_t count,
                                                             int32_t min_delta,
                                                             int32_t* last_value) {
    const __m256i min_delta_vec = _mm256_set1_epi32(min_delta);
    const size_t vector_count = count / 8 * 8;
    for (size_t row = 0; row < vector_count; row += 8) {
        __m256i value = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(values + row));
        value = _mm256_add_epi32(value, min_delta_vec);
        value = _mm256_add_epi32(value, _mm256_slli_si256(value, 4));
        value = _mm256_add_epi32(value, _mm256_slli_si256(value, 8));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(values + row), value);
    }
    __m128i carry = _mm_set1_epi32(*last_value);
    for (size_t row = 0; row < vector_count; row += 4) {
        __m128i value = _mm_loadu_si128(reinterpret_cast<const __m128i*>(values + row));
        value = _mm_add_epi32(value, carry);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(values + row), value);
        carry = _mm_shuffle_epi32(value, _MM_SHUFFLE(3, 3, 3, 3));
    }
    if (vector_count != 0) {
        *last_value = _mm_cvtsi128_si32(carry);
    }
    using Unsigned = uint32_t;
    for (size_t row = vector_count; row < count; ++row) {
        values[row] = static_cast<int32_t>(static_cast<Unsigned>(values[row]) +
                                           static_cast<Unsigned>(min_delta) +
                                           static_cast<Unsigned>(*last_value));
        *last_value = values[row];
    }
}

__attribute__((target("avx2"))) void delta_decode_int64_avx2(int64_t* values, size_t count,
                                                             int64_t min_delta,
                                                             int64_t* last_value) {
    const __m256i min_delta_vec = _mm256_set1_epi64x(min_delta);
    const size_t vector_count = count / 4 * 4;
    for (size_t row = 0; row < vector_count; row += 4) {
        __m256i value = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(values + row));
        value = _mm256_add_epi64(value, min_delta_vec);
        value = _mm256_add_epi64(value, _mm256_slli_si256(value, 8));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(values + row), value);
    }
    __m128i carry = _mm_set1_epi64x(*last_value);
    for (size_t row = 0; row < vector_count; row += 2) {
        __m128i value = _mm_loadu_si128(reinterpret_cast<const __m128i*>(values + row));
        value = _mm_add_epi64(value, carry);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(values + row), value);
        carry = _mm_unpackhi_epi64(value, value);
    }
    if (vector_count != 0) {
        *last_value = _mm_cvtsi128_si64(carry);
    }
    using Unsigned = uint64_t;
    for (size_t row = vector_count; row < count; ++row) {
        values[row] = static_cast<int64_t>(static_cast<Unsigned>(values[row]) +
                                           static_cast<Unsigned>(min_delta) +
                                           static_cast<Unsigned>(*last_value));
        *last_value = values[row];
    }
}

__attribute__((target("avx2"))) void dictionary_gather_avx2(const uint8_t* dictionary,
                                                            const uint32_t* indices, size_t count,
                                                            size_t value_width, uint8_t* dest) {
    size_t row = 0;
    if (value_width == 4) {
        for (; row + 8 <= count; row += 8) {
            const __m256i ids = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(indices + row));
            const __m256i gathered =
                    _mm256_i32gather_epi32(reinterpret_cast<const int*>(dictionary), ids, 4);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + row * 4), gathered);
        }
    } else {
        for (; row + 4 <= count; row += 4) {
            const __m128i ids = _mm_loadu_si128(reinterpret_cast<const __m128i*>(indices + row));
            const __m256i gathered =
                    _mm256_i32gather_epi64(reinterpret_cast<const long long*>(dictionary), ids, 8);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + row * 8), gathered);
        }
    }
    for (; row < count; ++row) {
        memcpy(dest + row * value_width, dictionary + indices[row] * value_width, value_width);
    }
}

template <size_t LANES>
constexpr auto make_expand_permute_lut() {
    std::array<std::array<int32_t, 8>, 1U << LANES> lut {};
    for (size_t mask = 0; mask < lut.size(); ++mask) {
        int32_t source = 0;
        for (size_t lane = 0; lane < LANES; ++lane) {
            const int32_t value = (mask & (1U << lane)) != 0 ? source++ : 0;
            if constexpr (LANES == 8) {
                lut[mask][lane] = value;
            } else {
                lut[mask][lane * 2] = value * 2;
                lut[mask][lane * 2 + 1] = value * 2 + 1;
            }
        }
    }
    return lut;
}

constexpr auto EXPAND_PERMUTE_32 = make_expand_permute_lut<8>();
constexpr auto EXPAND_PERMUTE_64 = make_expand_permute_lut<4>();

__attribute__((target("avx2"))) void expand_nullable_avx2(uint8_t* bytes, size_t compact_count,
                                                          const uint8_t* nulls, size_t output_count,
                                                          size_t value_width) {
    size_t source = compact_count;
    size_t output = output_count;
    if (value_width == 4) {
        auto* values = reinterpret_cast<int32_t*>(bytes);
        while (output >= 8) {
            const size_t start = output - 8;
            uint32_t valid_mask = 0;
            for (size_t lane = 0; lane < 8; ++lane) {
                valid_mask |= static_cast<uint32_t>(nulls[start + lane] == 0) << lane;
            }
            const size_t valid = std::popcount(valid_mask);
            source -= valid;
            const __m256i load_mask = _mm256_cmpgt_epi32(_mm256_set1_epi32(static_cast<int>(valid)),
                                                         _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7));
            const __m256i compact = _mm256_maskload_epi32(values + source, load_mask);
            const __m256i permute = _mm256_loadu_si256(
                    reinterpret_cast<const __m256i*>(EXPAND_PERMUTE_32[valid_mask].data()));
            __m256i expanded = _mm256_permutevar8x32_epi32(compact, permute);
            const __m256i valid_lanes = _mm256_setr_epi32(
                    -(valid_mask & 1U), -((valid_mask >> 1) & 1U), -((valid_mask >> 2) & 1U),
                    -((valid_mask >> 3) & 1U), -((valid_mask >> 4) & 1U), -((valid_mask >> 5) & 1U),
                    -((valid_mask >> 6) & 1U), -((valid_mask >> 7) & 1U));
            expanded = _mm256_and_si256(expanded, valid_lanes);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(values + start), expanded);
            output = start;
        }
    } else {
        auto* values = reinterpret_cast<int64_t*>(bytes);
        while (output >= 4) {
            const size_t start = output - 4;
            uint32_t valid_mask = 0;
            for (size_t lane = 0; lane < 4; ++lane) {
                valid_mask |= static_cast<uint32_t>(nulls[start + lane] == 0) << lane;
            }
            const size_t valid = std::popcount(valid_mask);
            source -= valid;
            const __m256i load_mask =
                    _mm256_setr_epi64x(valid > 0 ? -1LL : 0, valid > 1 ? -1LL : 0,
                                       valid > 2 ? -1LL : 0, valid > 3 ? -1LL : 0);
            const __m256i compact = _mm256_maskload_epi64(
                    reinterpret_cast<const long long*>(values + source), load_mask);
            const __m256i permute = _mm256_loadu_si256(
                    reinterpret_cast<const __m256i*>(EXPAND_PERMUTE_64[valid_mask].data()));
            __m256i expanded = _mm256_permutevar8x32_epi32(compact, permute);
            const __m256i valid_lanes = _mm256_setr_epi64x(
                    (valid_mask & 1U) != 0 ? -1LL : 0, (valid_mask & 2U) != 0 ? -1LL : 0,
                    (valid_mask & 4U) != 0 ? -1LL : 0, (valid_mask & 8U) != 0 ? -1LL : 0);
            expanded = _mm256_and_si256(expanded, valid_lanes);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(values + start), expanded);
            output = start;
        }
    }
    while (output > 0) {
        --output;
        if (nulls[output] != 0) {
            memset(bytes + output * value_width, 0, value_width);
        } else {
            --source;
            memmove(bytes + output * value_width, bytes + source * value_width, value_width);
        }
    }
}

template <typename Vec>
Vec combine_comparison(Vec equal, Vec greater, Vec less, RawComparisonOp op) {
    const Vec all = [] {
        if constexpr (std::is_same_v<Vec, __m256i>) {
            return _mm256_set1_epi32(-1);
        } else if constexpr (std::is_same_v<Vec, __m256>) {
            return _mm256_castsi256_ps(_mm256_set1_epi32(-1));
        } else {
            return _mm256_castsi256_pd(_mm256_set1_epi32(-1));
        }
    }();
    const auto bit_or = [](Vec lhs, Vec rhs) {
        if constexpr (std::is_same_v<Vec, __m256i>) {
            return _mm256_or_si256(lhs, rhs);
        } else if constexpr (std::is_same_v<Vec, __m256>) {
            return _mm256_or_ps(lhs, rhs);
        } else {
            return _mm256_or_pd(lhs, rhs);
        }
    };
    const auto bit_xor = [](Vec lhs, Vec rhs) {
        if constexpr (std::is_same_v<Vec, __m256i>) {
            return _mm256_xor_si256(lhs, rhs);
        } else if constexpr (std::is_same_v<Vec, __m256>) {
            return _mm256_xor_ps(lhs, rhs);
        } else {
            return _mm256_xor_pd(lhs, rhs);
        }
    };
    switch (op) {
    case RawComparisonOp::EQ:
        return equal;
    case RawComparisonOp::NE:
        return bit_xor(equal, all);
    case RawComparisonOp::LT:
        return less;
    case RawComparisonOp::LE:
        return bit_or(less, equal);
    case RawComparisonOp::GT:
        return greater;
    case RawComparisonOp::GE:
        return bit_or(greater, equal);
    }
    __builtin_unreachable();
}

__attribute__((target("avx2"))) void raw_compare_int32_avx2(const uint8_t* bytes, size_t count,
                                                            int32_t literal, RawComparisonOp op,
                                                            uint8_t* matches) {
    size_t row = 0;
    const __m256i rhs = _mm256_set1_epi32(literal);
    for (; row + 8 <= count; row += 8) {
        const __m256i lhs = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(bytes + row * 4));
        const __m256i equal = _mm256_cmpeq_epi32(lhs, rhs);
        const __m256i greater = _mm256_cmpgt_epi32(lhs, rhs);
        const __m256i less = _mm256_cmpgt_epi32(rhs, lhs);
        const uint32_t mask = static_cast<uint32_t>(_mm256_movemask_ps(
                _mm256_castsi256_ps(combine_comparison(equal, greater, less, op))));
        for (size_t lane = 0; lane < 8; ++lane) {
            matches[row + lane] &= static_cast<uint8_t>((mask >> lane) & 1U);
        }
    }
    for (; row < count; ++row) {
        int32_t value;
        memcpy(&value, bytes + row * 4, sizeof(value));
        bool keep = false;
        switch (op) {
        case RawComparisonOp::EQ:
            keep = value == literal;
            break;
        case RawComparisonOp::NE:
            keep = value != literal;
            break;
        case RawComparisonOp::LT:
            keep = value < literal;
            break;
        case RawComparisonOp::LE:
            keep = value <= literal;
            break;
        case RawComparisonOp::GT:
            keep = value > literal;
            break;
        case RawComparisonOp::GE:
            keep = value >= literal;
            break;
        }
        matches[row] &= static_cast<uint8_t>(keep);
    }
}

__attribute__((target("avx2"))) void raw_compare_int64_avx2(const uint8_t* bytes, size_t count,
                                                            int64_t literal, RawComparisonOp op,
                                                            uint8_t* matches) {
    size_t row = 0;
    const __m256i rhs = _mm256_set1_epi64x(literal);
    for (; row + 4 <= count; row += 4) {
        const __m256i lhs = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(bytes + row * 8));
        const __m256i equal = _mm256_cmpeq_epi64(lhs, rhs);
        const __m256i greater = _mm256_cmpgt_epi64(lhs, rhs);
        const __m256i less = _mm256_cmpgt_epi64(rhs, lhs);
        const uint32_t mask = static_cast<uint32_t>(_mm256_movemask_pd(
                _mm256_castsi256_pd(combine_comparison(equal, greater, less, op))));
        for (size_t lane = 0; lane < 4; ++lane) {
            matches[row + lane] &= static_cast<uint8_t>((mask >> lane) & 1U);
        }
    }
    for (; row < count; ++row) {
        int64_t value;
        memcpy(&value, bytes + row * 8, sizeof(value));
        bool keep = false;
        switch (op) {
        case RawComparisonOp::EQ:
            keep = value == literal;
            break;
        case RawComparisonOp::NE:
            keep = value != literal;
            break;
        case RawComparisonOp::LT:
            keep = value < literal;
            break;
        case RawComparisonOp::LE:
            keep = value <= literal;
            break;
        case RawComparisonOp::GT:
            keep = value > literal;
            break;
        case RawComparisonOp::GE:
            keep = value >= literal;
            break;
        }
        matches[row] &= static_cast<uint8_t>(keep);
    }
}

template <typename T>
bool scalar_compare(T lhs, T rhs, RawComparisonOp op) {
    const auto equal = [](T left, T right) {
        if constexpr (std::is_floating_point_v<T>) {
            return (std::isnan(left) && std::isnan(right)) || left == right;
        }
        return left == right;
    };
    const auto greater = [](T left, T right) {
        if constexpr (std::is_floating_point_v<T>) {
            // Match Doris Compare: NaN is equal to NaN and greater than every finite value.
            if (std::isnan(right)) {
                return false;
            }
            if (std::isnan(left)) {
                return true;
            }
        }
        return left > right;
    };
    switch (op) {
    case RawComparisonOp::EQ:
        return equal(lhs, rhs);
    case RawComparisonOp::NE:
        return !equal(lhs, rhs);
    case RawComparisonOp::LT:
        return greater(rhs, lhs);
    case RawComparisonOp::LE:
        return !greater(lhs, rhs);
    case RawComparisonOp::GT:
        return greater(lhs, rhs);
    case RawComparisonOp::GE:
        return !greater(rhs, lhs);
    }
    __builtin_unreachable();
}

__attribute__((target("avx2"))) void raw_compare_float_avx2(const uint8_t* bytes, size_t count,
                                                            float literal, RawComparisonOp op,
                                                            uint8_t* matches) {
    size_t row = 0;
    const __m256 rhs = _mm256_set1_ps(literal);
    for (; row + 8 <= count; row += 8) {
        const __m256 lhs = _mm256_loadu_ps(reinterpret_cast<const float*>(bytes + row * 4));
        const __m256 lhs_nan = _mm256_cmp_ps(lhs, lhs, _CMP_UNORD_Q);
        const __m256 rhs_nan = _mm256_cmp_ps(rhs, rhs, _CMP_UNORD_Q);
        const __m256 both_nan = _mm256_and_ps(lhs_nan, rhs_nan);
        const __m256 equal = _mm256_or_ps(_mm256_cmp_ps(lhs, rhs, _CMP_EQ_OQ), both_nan);
        const __m256 greater = _mm256_or_ps(_mm256_cmp_ps(lhs, rhs, _CMP_GT_OQ),
                                            _mm256_andnot_ps(rhs_nan, lhs_nan));
        const __m256 less = _mm256_or_ps(_mm256_cmp_ps(lhs, rhs, _CMP_LT_OQ),
                                         _mm256_andnot_ps(lhs_nan, rhs_nan));
        const uint32_t mask = static_cast<uint32_t>(
                _mm256_movemask_ps(combine_comparison(equal, greater, less, op)));
        for (size_t lane = 0; lane < 8; ++lane) {
            matches[row + lane] &= static_cast<uint8_t>((mask >> lane) & 1U);
        }
    }
    for (; row < count; ++row) {
        float value;
        memcpy(&value, bytes + row * 4, sizeof(value));
        matches[row] &= static_cast<uint8_t>(scalar_compare(value, literal, op));
    }
}

__attribute__((target("avx2"))) void raw_compare_double_avx2(const uint8_t* bytes, size_t count,
                                                             double literal, RawComparisonOp op,
                                                             uint8_t* matches) {
    size_t row = 0;
    const __m256d rhs = _mm256_set1_pd(literal);
    for (; row + 4 <= count; row += 4) {
        const __m256d lhs = _mm256_loadu_pd(reinterpret_cast<const double*>(bytes + row * 8));
        const __m256d lhs_nan = _mm256_cmp_pd(lhs, lhs, _CMP_UNORD_Q);
        const __m256d rhs_nan = _mm256_cmp_pd(rhs, rhs, _CMP_UNORD_Q);
        const __m256d both_nan = _mm256_and_pd(lhs_nan, rhs_nan);
        const __m256d equal = _mm256_or_pd(_mm256_cmp_pd(lhs, rhs, _CMP_EQ_OQ), both_nan);
        const __m256d greater = _mm256_or_pd(_mm256_cmp_pd(lhs, rhs, _CMP_GT_OQ),
                                             _mm256_andnot_pd(rhs_nan, lhs_nan));
        const __m256d less = _mm256_or_pd(_mm256_cmp_pd(lhs, rhs, _CMP_LT_OQ),
                                          _mm256_andnot_pd(lhs_nan, rhs_nan));
        const uint32_t mask = static_cast<uint32_t>(
                _mm256_movemask_pd(combine_comparison(equal, greater, less, op)));
        for (size_t lane = 0; lane < 4; ++lane) {
            matches[row + lane] &= static_cast<uint8_t>((mask >> lane) & 1U);
        }
    }
    for (; row < count; ++row) {
        double value;
        memcpy(&value, bytes + row * 8, sizeof(value));
        matches[row] &= static_cast<uint8_t>(scalar_compare(value, literal, op));
    }
}
#endif

template <typename T>
void delta_decode_scalar(T* values, size_t count, T min_delta, T* last_value) {
    using Unsigned = std::make_unsigned_t<T>;
    for (size_t row = 0; row < count; ++row) {
        values[row] = static_cast<T>(static_cast<Unsigned>(values[row]) +
                                     static_cast<Unsigned>(min_delta) +
                                     static_cast<Unsigned>(*last_value));
        *last_value = values[row];
    }
}

template <typename T>
void raw_compare_scalar(const uint8_t* bytes, size_t count, T literal, RawComparisonOp op,
                        uint8_t* matches) {
    for (size_t row = 0; row < count; ++row) {
        if (matches[row] == 0) {
            continue;
        }
        T value;
        memcpy(&value, bytes + row * sizeof(T), sizeof(T));
        matches[row] = static_cast<uint8_t>(scalar_compare(value, literal, op));
    }
}

} // namespace

void byte_stream_split_decode(const uint8_t* src, size_t width, size_t offset, size_t num_values,
                              size_t stride, uint8_t* dest) {
#ifdef DORIS_PARQUET_X86_SIMD
    if (has_avx2() && num_values >= 32 && (width == 4 || width == 8)) {
        if (width == 4) {
            byte_stream_split_decode_avx2<4>(src, offset, num_values, stride, dest);
        } else {
            byte_stream_split_decode_avx2<8>(src, offset, num_values, stride, dest);
        }
        return;
    }
#endif
    byte_stream_split_decode_scalar(src, width, offset, num_values, stride, dest);
}

void delta_decode(int32_t* values, size_t count, int32_t min_delta, int32_t* last_value) {
#ifdef DORIS_PARQUET_X86_SIMD
    if (has_avx2() && count >= 8) {
        delta_decode_int32_avx2(values, count, min_delta, last_value);
        return;
    }
#endif
    delta_decode_scalar(values, count, min_delta, last_value);
}

void delta_decode(int64_t* values, size_t count, int64_t min_delta, int64_t* last_value) {
#ifdef DORIS_PARQUET_X86_SIMD
    if (has_avx2() && count >= 4) {
        delta_decode_int64_avx2(values, count, min_delta, last_value);
        return;
    }
#endif
    delta_decode_scalar(values, count, min_delta, last_value);
}

void dictionary_gather(const uint8_t* dictionary, const uint32_t* indices, size_t count,
                       size_t value_width, uint8_t* dest) {
#ifdef DORIS_PARQUET_X86_SIMD
    if (has_avx2() && ((value_width == 4 && count >= 8) || (value_width == 8 && count >= 4))) {
        dictionary_gather_avx2(dictionary, indices, count, value_width, dest);
        return;
    }
#endif
    for (size_t row = 0; row < count; ++row) {
        memcpy(dest + row * value_width, dictionary + indices[row] * value_width, value_width);
    }
}

void expand_nullable_values(uint8_t* values, size_t compact_count, const uint8_t* nulls,
                            size_t output_count, size_t value_width) {
#ifdef DORIS_PARQUET_X86_SIMD
    if (has_avx2() &&
        ((value_width == 4 && output_count >= 8) || (value_width == 8 && output_count >= 4))) {
        // Backward expansion is required because the compact input and expanded output alias.
        // Each SIMD block loads all of its source lanes before overwriting the wider destination.
        expand_nullable_avx2(values, compact_count, nulls, output_count, value_width);
        return;
    }
#endif
    size_t source = compact_count;
    for (size_t output = output_count; output > 0;) {
        --output;
        if (nulls[output] != 0) {
            memset(values + output * value_width, 0, value_width);
        } else {
            --source;
            memmove(values + output * value_width, values + source * value_width, value_width);
        }
    }
}

void raw_compare(const uint8_t* values, size_t count, int32_t literal, RawComparisonOp op,
                 uint8_t* matches) {
#ifdef DORIS_PARQUET_X86_SIMD
    if (has_avx2() && count >= 8) {
        raw_compare_int32_avx2(values, count, literal, op, matches);
        return;
    }
#endif
    raw_compare_scalar(values, count, literal, op, matches);
}

void raw_compare(const uint8_t* values, size_t count, int64_t literal, RawComparisonOp op,
                 uint8_t* matches) {
#ifdef DORIS_PARQUET_X86_SIMD
    if (has_avx2() && count >= 4) {
        raw_compare_int64_avx2(values, count, literal, op, matches);
        return;
    }
#endif
    raw_compare_scalar(values, count, literal, op, matches);
}

void raw_compare(const uint8_t* values, size_t count, float literal, RawComparisonOp op,
                 uint8_t* matches) {
#ifdef DORIS_PARQUET_X86_SIMD
    if (has_avx2() && count >= 8) {
        // Doris orders NaN above every finite value and considers NaN equal to NaN. The SIMD
        // masks deliberately reconstruct that total order instead of using ordered FP compares.
        raw_compare_float_avx2(values, count, literal, op, matches);
        return;
    }
#endif
    raw_compare_scalar(values, count, literal, op, matches);
}

void raw_compare(const uint8_t* values, size_t count, double literal, RawComparisonOp op,
                 uint8_t* matches) {
#ifdef DORIS_PARQUET_X86_SIMD
    if (has_avx2() && count >= 4) {
        raw_compare_double_avx2(values, count, literal, op, matches);
        return;
    }
#endif
    raw_compare_scalar(values, count, literal, op, matches);
}

} // namespace doris::parquet_simd
