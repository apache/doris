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

#include "exprs/function/fmod_fast.h"

#include <string.h>

#include <cmath>

#include "common/compiler_util.h"

namespace doris::fmod_fast {
namespace {

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#define DORIS_HAS_X87_FMOD_FAST 1

ALWAYS_INLINE inline double fmod_x87_fprem(double a, double b) {
    double r;
    asm volatile(
            "fldl %[b]\n\t"
            "fldl %[a]\n\t"
            "1:\n\t"
            "fprem\n\t"
            "fnstsw %%ax\n\t"
            "testb $4, %%ah\n\t"
            "jne 1b\n\t"
            "fstp %%st(1)\n\t"
            "fstpl %[r]\n\t"
            : [r] "=m"(r)
            : [a] "m"(a), [b] "m"(b)
            : "ax", "cc", "st");
    return r;
}
#else
#define DORIS_HAS_X87_FMOD_FAST 0
#endif

ALWAYS_INLINE inline double fmod_double(double a, double b) {
#if DORIS_HAS_X87_FMOD_FAST
    if (b != 0.0 && std::isfinite(a) && std::isfinite(b)) {
        double abs_a = std::fabs(a);
        double abs_b = std::fabs(b);
        if (abs_a < abs_b) {
            return a;
        }
        if (abs_a == abs_b) {
            return std::copysign(0.0, a);
        }
        return fmod_x87_fprem(a, b);
    }
#endif
    return std::fmod(a, b);
}

ALWAYS_INLINE inline float fmod_float(float a, float b) {
    return static_cast<float>(fmod_double(static_cast<double>(a), static_cast<double>(b)));
}

ALWAYS_INLINE inline double fmod_value(double a, double b) {
    return fmod_double(a, b);
}

ALWAYS_INLINE inline float fmod_value(float a, float b) {
    return fmod_float(a, b);
}

template <typename T>
ALWAYS_INLINE inline void vector_vector_impl(const T* lhs, const T* rhs, T* result,
                                             uint8_t* null_map, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        uint8_t is_null = rhs[i] == T(0);
        null_map[i] = is_null;
        T adjusted_rhs = rhs[i] + static_cast<T>(is_null);
        result[i] = fmod_value(lhs[i], adjusted_rhs);
    }
}

template <typename T>
ALWAYS_INLINE inline void vector_constant_impl(const T* lhs, T rhs, T* result, uint8_t* null_map,
                                               size_t size) {
    uint8_t is_null = rhs == T(0);
    memset(null_map, is_null, size);
    if (is_null) {
        return;
    }

    for (size_t i = 0; i < size; ++i) {
        result[i] = fmod_value(lhs[i], rhs);
    }
}

template <typename T>
ALWAYS_INLINE inline void constant_vector_impl(T lhs, const T* rhs, T* result, uint8_t* null_map,
                                               size_t size) {
    for (size_t i = 0; i < size; ++i) {
        uint8_t is_null = rhs[i] == T(0);
        null_map[i] = is_null;
        T adjusted_rhs = rhs[i] + static_cast<T>(is_null);
        result[i] = fmod_value(lhs, adjusted_rhs);
    }
}

} // namespace

bool is_x87_fast_path_enabled() {
    return DORIS_HAS_X87_FMOD_FAST;
}

double scalar(double a, double b) {
    return fmod_double(a, b);
}

float scalar(float a, float b) {
    return fmod_float(a, b);
}

void vector_vector(const double* lhs, const double* rhs, double* result, uint8_t* null_map,
                   size_t size) {
    vector_vector_impl(lhs, rhs, result, null_map, size);
}

void vector_vector(const float* lhs, const float* rhs, float* result, uint8_t* null_map,
                   size_t size) {
    vector_vector_impl(lhs, rhs, result, null_map, size);
}

void vector_constant(const double* lhs, double rhs, double* result, uint8_t* null_map,
                     size_t size) {
    vector_constant_impl(lhs, rhs, result, null_map, size);
}

void vector_constant(const float* lhs, float rhs, float* result, uint8_t* null_map, size_t size) {
    vector_constant_impl(lhs, rhs, result, null_map, size);
}

void constant_vector(double lhs, const double* rhs, double* result, uint8_t* null_map,
                     size_t size) {
    constant_vector_impl(lhs, rhs, result, null_map, size);
}

void constant_vector(float lhs, const float* rhs, float* result, uint8_t* null_map, size_t size) {
    constant_vector_impl(lhs, rhs, result, null_map, size);
}

} // namespace doris::fmod_fast
