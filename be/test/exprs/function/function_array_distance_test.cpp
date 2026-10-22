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

#include <gtest/gtest.h>

#include "core/data_type/data_type_number.h"
#include "function_test_util.h"

namespace doris {

TEST(ArrayDistanceFunctionsTest, ApproximateCosineScalarFallbackPreservesZeroSemantics) {
    const InputTypeSet types {TYPE_ARRAY, TYPE_FLOAT, TYPE_ARRAY, TYPE_FLOAT};
    const TestArray x {Float32(1), Float32(0)};
    const TestArray y {Float32(0), Float32(1)};
    const TestArray zero {Float32(0), Float32(0)};
    const DataSet distances {{{x, x}, Float32(0)}, {{x, y}, Float32(1)}, {{x, zero}, Float32(2)}};
    const DataSet similarities {
            {{x, x}, Float32(1)}, {{x, y}, Float32(0)}, {{x, zero}, Float32(0)}};
    EXPECT_TRUE((check_function<DataTypeFloat32, false>("cosine_distance_approximate", types,
                                                        distances))
                        .ok());
    EXPECT_TRUE((check_function<DataTypeFloat32, false>("cosine_similarity_approximate", types,
                                                        similarities))
                        .ok());
}

TEST(ArrayDistanceFunctionsTest, HammingCountsDifferentBitsAndSupportsEmptyArrays) {
    const InputTypeSet types {TYPE_ARRAY, TYPE_BIGINT, TYPE_ARRAY, TYPE_BIGINT};
    const TestArray x {Int64(0), Int64(255), Int64(170)};
    const TestArray y {Int64(255), Int64(0), Int64(85)};
    const TestArray empty;
    const DataSet rows {{{x, y}, Float32(24)}, {{x, x}, Float32(0)}, {{empty, empty}, Float32(0)}};
    for (const std::string name : {"hamming_distance", "hamming_distance_approximate"}) {
        EXPECT_TRUE((check_function<DataTypeFloat32, false>(name, types, rows)).ok());
    }
}

TEST(ArrayDistanceFunctionsTest, HammingRejectsOutOfRangeBeforeByteConversion) {
    const int64_t zero[] = {0};
    for (const int64_t value : {-1, 256, 65536}) {
        EXPECT_THROW(HammingDistance::distance(&value, zero, 1), Exception);
        EXPECT_THROW(HammingDistance::distance(zero, &value, 1), Exception);
    }
    const int64_t high[] = {255};
    EXPECT_EQ(8, HammingDistance::distance(zero, high, 1));
}

} // namespace doris
