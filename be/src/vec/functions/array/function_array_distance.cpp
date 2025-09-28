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

FAISS_PRAGMA_IMPRECISE_FUNCTION_BEGIN
float CosineDistance::distance(const float* x, const float* y, size_t d) {
    float dot_prod = 0;
    float squared_x = 0;
    float squared_y = 0;
    for (size_t i = 0; i < d; ++i) {
        dot_prod += x[i] * y[i];
        squared_x += x[i] * x[i];
        squared_y += y[i] * y[i];
    }
    if (squared_x == 0 or squared_y == 0) {
        return 2.0f;
    }
    return 1 - dot_prod / sqrt(squared_x * squared_y);
}
FAISS_PRAGMA_IMPRECISE_FUNCTION_END

void register_function_array_distance(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayDistance<L1Distance>>();
    factory.register_function<FunctionArrayDistance<L2Distance>>();
    factory.register_function<FunctionArrayDistance<CosineDistance>>();
    factory.register_function<FunctionArrayDistance<InnerProduct>>();
    factory.register_function<FunctionArrayDistance<L2DistanceApproximate>>();
    factory.register_function<FunctionArrayDistance<InnerProductApproximate>>();
}

} // namespace doris::vectorized
