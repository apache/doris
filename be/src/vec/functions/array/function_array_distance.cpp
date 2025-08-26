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

#include "vec/functions/array/function_array_distance.h"

#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

#if defined(__x86_64__) && (defined(__clang_major__) && (__clang_major__ > 10))
#define PRAGMA_IMPRECISE_FUNCTION_BEGIN _Pragma("float_control(precise, off, push)")
#define PRAGMA_IMPRECISE_FUNCTION_END _Pragma("float_control(pop)")

#elif defined(__GNUC__)
#define PRAGMA_IMPRECISE_FUNCTION_BEGIN \
    _Pragma("GCC push_options")         \
            _Pragma("GCC optimize (\"unroll-loops,associative-math,no-signed-zeros\")")
#define PRAGMA_IMPRECISE_FUNCTION_END _Pragma("GCC pop_options")
#else
#define PRAGMA_IMPRECISE_FUNCTION_BEGIN
#define PRAGMA_IMPRECISE_FUNCTION_END
#endif

PRAGMA_IMPRECISE_FUNCTION_BEGIN
float L1Distance::distance(const float* x, const float* y, size_t d) {
    size_t i;
    float res = 0;
    for (i = 0; i < d; i++) {
        res += fabs(x[i] - y[i]);
    }
    return res;
}

float L2Distance::distance(const float* x, const float* y, size_t d) {
    size_t i;
    float res = 0;
    for (i = 0; i < d; i++) {
        const float tmp = x[i] - y[i];
        res += tmp * tmp;
    }
    return std::sqrt(res);
}

float CosineDistance::distance(const float* x, const float* y, size_t d) {
    float dot_prod = 0;
    float squared_x = 0;
    float squared_y = 0;
    for (size_t i = 0; i < d; ++i) {
        dot_prod += x[i] * y[i];
        squared_x += x[i] * x[i];
        squared_y += y[i] * y[i];
    }
    // division by zero check
    if (squared_x == 0 || squared_y == 0) [[unlikely]] {
        return 2.F;
    }
    return 1 - dot_prod / sqrt(squared_x * squared_y);
}

float InnerProduct::distance(const float* x, const float* y, size_t d) {
    float res = 0.F;
    for (size_t i = 0; i != d; ++i) {
        res += x[i] * y[i];
    }
    return res;
}
PRAGMA_IMPRECISE_FUNCTION_END

void register_function_array_distance(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayDistance<L1Distance>>();
    factory.register_function<FunctionArrayDistance<L2Distance>>();
    factory.register_function<FunctionArrayDistance<CosineDistance>>();
    factory.register_function<FunctionArrayDistance<InnerProduct>>();
}

} // namespace doris::vectorized
