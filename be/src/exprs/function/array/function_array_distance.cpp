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

#include "exprs/function/array/function_array_distance.h"

#include <algorithm>

#include "exprs/function/simple_function_factory.h"

namespace doris {

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
float CosineDistance::distance(const float* x, const float* y, size_t d) {
    if (d == 0) {
        return 2.0f;
    }

    DCHECK(x != nullptr && y != nullptr);

    float dot_prod = 0;
    float squared_x = 0;
    float squared_y = 0;
    for (size_t i = 0; i < d; ++i) {
        dot_prod += x[i] * y[i];
        squared_x += x[i] * x[i];
        squared_y += y[i] * y[i];
    }

    if (squared_x == 0 || squared_y == 0) {
        return 2.0f;
    }

    // Accumulate the norm in double and take a single square root. Computing
    // (double)squared_x * (double)squared_y cannot overflow for finite float inputs,
    // whereas the float expression sqrt(squared_x * squared_y) overflows to +inf for
    // large-magnitude vectors and would silently yield a distance of 1.0.
    const double norm = std::sqrt(static_cast<double>(squared_x) * static_cast<double>(squared_y));
    // Clamp the cosine to [-1, 1] before mapping to a distance. Floating-point rounding
    // can push the ratio slightly outside [-1, 1] (e.g. 1.0000001 for identical vectors),
    // which would otherwise produce a tiny negative distance.
    const float cosine = std::clamp(static_cast<float>(dot_prod / norm), -1.0f, 1.0f);
    return 1.0f - cosine;
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
float CosineSimilarity::distance(const float* x, const float* y, size_t d) {
    if (d == 0) {
        return 0.0f;
    }

    DCHECK(x != nullptr && y != nullptr);

    float dot_prod = 0;
    float squared_x = 0;
    float squared_y = 0;
    for (size_t i = 0; i < d; ++i) {
        dot_prod += x[i] * y[i];
        squared_x += x[i] * x[i];
        squared_y += y[i] * y[i];
    }

    if (squared_x == 0 || squared_y == 0) {
        return 0.0f;
    }

    // See CosineDistance::distance: the double-precision norm avoids float overflow,
    // and clamping keeps the result within the mathematically valid [-1, 1] range.
    const double norm = std::sqrt(static_cast<double>(squared_x) * static_cast<double>(squared_y));
    return std::clamp(static_cast<float>(dot_prod / norm), -1.0f, 1.0f);
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

void register_function_array_distance(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayDistance<L1Distance>>();
    factory.register_function<FunctionArrayDistance<L2Distance>>();
    factory.register_function<FunctionArrayDistance<CosineDistance>>();
    factory.register_function<FunctionArrayDistance<CosineSimilarity>>();
    factory.register_function<FunctionArrayDistance<InnerProduct>>();
    factory.register_function<FunctionArrayDistance<L2DistanceApproximate>>();
    factory.register_function<FunctionArrayDistance<InnerProductApproximate>>();
}

} // namespace doris
