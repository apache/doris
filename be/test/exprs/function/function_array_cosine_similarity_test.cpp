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

#include <gtest/gtest.h>

#include <cmath>
#include <string>

#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"

namespace doris {

TEST(function_cosine_similarity_test, cosine_similarity) {
    std::string func_name = "cosine_similarity";
    TestArray empty_arr;

    // cosine_similarity(Array<Float>, Array<Float>) - identical vectors
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};

        TestArray vec1 = {Float32(1.0), Float32(2.0), Float32(3.0)};
        TestArray vec2 = {Float32(1.0), Float32(2.0), Float32(3.0)};
        DataSet data_set = {{{vec1, vec2}, Float32(1.0)}};

        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }

    // cosine_similarity(Array<Float>, Array<Float>) - orthogonal vectors
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};

        TestArray vec1 = {Float32(1.0), Float32(0.0)};
        TestArray vec2 = {Float32(0.0), Float32(1.0)};
        DataSet data_set = {{{vec1, vec2}, Float32(0.0)}};

        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }

    // cosine_similarity(Array<Float>, Array<Float>) - opposite vectors
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};

        TestArray vec1 = {Float32(1.0), Float32(2.0), Float32(3.0)};
        TestArray vec2 = {Float32(-1.0), Float32(-2.0), Float32(-3.0)};
        DataSet data_set = {{{vec1, vec2}, Float32(-1.0)}};

        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }

    // cosine_similarity(Array<Float>, Array<Float>) - zero vector handling
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};

        TestArray vec1 = {Float32(0.0), Float32(0.0), Float32(0.0)};
        TestArray vec2 = {Float32(1.0), Float32(2.0), Float32(3.0)};
        TestArray vec3 = {Float32(0.0), Float32(0.0)};
        DataSet data_set = {{{vec1, vec2}, Float32(0.0)},
                            {{vec2, vec1}, Float32(0.0)},
                            {{vec3, vec3}, Float32(0.0)}};

        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }

    // cosine_similarity(Array<Float>, Array<Float>) - empty arrays
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};

        DataSet data_set = {{{empty_arr, empty_arr}, Float32(0.0)}};

        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }

    // cosine_similarity(Array<Float>, Array<Float>) - known value test
    // cos_sim([1,2,3], [3,5,7]) = 34 / sqrt(14*83) ≈ 0.9974149
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};

        TestArray vec1 = {Float32(1.0), Float32(2.0), Float32(3.0)};
        TestArray vec2 = {Float32(3.0), Float32(5.0), Float32(7.0)};
        // Expected: 34 / sqrt(14 * 83) = 34 / sqrt(1162) ≈ 0.9974149.
        // Mirror the production formula exactly (double-precision norm) so the
        // exact float comparison in check_function matches bit-for-bit.
        float expected = static_cast<float>(34.0 / std::sqrt(14.0 * 83.0));
        DataSet data_set = {{{vec1, vec2}, Float32(expected)}};

        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }

    // cosine_similarity(Array<Float>, Array<Float>) - 2D vectors
    // cos_sim([3,4], [4,3]) = 24 / sqrt(25*25) = 24/25 = 0.96
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};

        TestArray vec1 = {Float32(3.0), Float32(4.0)};
        TestArray vec2 = {Float32(4.0), Float32(3.0)};
        DataSet data_set = {{{vec1, vec2}, Float32(0.96)}};

        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }

    // cosine_similarity(Array<Float>, Array<Float>) - single element
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};

        TestArray vec1 = {Float32(5.0)};
        TestArray vec2 = {Float32(10.0)};
        DataSet data_set = {{{vec1, vec2}, Float32(1.0)}};

        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }

    // cosine_similarity(Array<Float>, Array<Float>) - negative values
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};

        TestArray vec1 = {Float32(-1.0), Float32(-2.0)};
        TestArray vec2 = {Float32(1.0), Float32(2.0)};
        DataSet data_set = {{{vec1, vec2}, Float32(-1.0)}};

        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }

    // cosine_similarity(Array<Float>, Array<Float>) - mixed values
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};

        TestArray vec1 = {Float32(1.0), Float32(-1.0), Float32(1.0)};
        TestArray vec2 = {Float32(-1.0), Float32(1.0), Float32(-1.0)};
        DataSet data_set = {{{vec1, vec2}, Float32(-1.0)}};

        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }
}

TEST(function_cosine_distance_test, cosine_distance) {
    std::string func_name = "cosine_distance";
    TestArray empty_arr;
    InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};

    // identical vectors -> distance 0.0 (and crucially never a negative distance)
    {
        TestArray vec1 = {Float32(1.0), Float32(2.0), Float32(3.0)};
        TestArray vec2 = {Float32(1.0), Float32(2.0), Float32(3.0)};
        DataSet data_set = {{{vec1, vec2}, Float32(0.0)}};
        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }

    // orthogonal vectors -> distance 1.0
    {
        TestArray vec1 = {Float32(1.0), Float32(0.0)};
        TestArray vec2 = {Float32(0.0), Float32(1.0)};
        DataSet data_set = {{{vec1, vec2}, Float32(1.0)}};
        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }

    // opposite vectors -> distance 2.0
    {
        TestArray vec1 = {Float32(1.0), Float32(2.0), Float32(3.0)};
        TestArray vec2 = {Float32(-1.0), Float32(-2.0), Float32(-3.0)};
        DataSet data_set = {{{vec1, vec2}, Float32(2.0)}};
        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }

    // zero vector and empty array keep the legacy fallback distance of 2.0
    {
        TestArray zero_vec = {Float32(0.0), Float32(0.0), Float32(0.0)};
        TestArray vec = {Float32(1.0), Float32(2.0), Float32(3.0)};
        DataSet data_set = {{{zero_vec, vec}, Float32(2.0)},
                            {{empty_arr, empty_arr}, Float32(2.0)}};
        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }

    // known value: 1 - 34 / sqrt(14 * 83). Mirror the production formula exactly.
    {
        TestArray vec1 = {Float32(1.0), Float32(2.0), Float32(3.0)};
        TestArray vec2 = {Float32(3.0), Float32(5.0), Float32(7.0)};
        float expected = 1.0f - static_cast<float>(34.0 / std::sqrt(14.0 * 83.0));
        DataSet data_set = {{{vec1, vec2}, Float32(expected)}};
        static_cast<void>(check_function<DataTypeFloat32, false>(func_name, input_types, data_set));
    }
}

// Regression tests for the numerical-stability fixes: large-magnitude vectors must
// not overflow the norm (legacy sqrt(squared_x * squared_y) produced +inf and a
// wrong result), and the cosine must stay within [-1, 1].
TEST(function_cosine_numerical_stability_test, large_magnitude_no_overflow) {
    InputTypeSet input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};

    // squared_x = squared_y = 2e38 (within FLT_MAX), but squared_x * squared_y = 4e76
    // overflows float. The double-precision norm keeps parallel vectors at cos = 1.0.
    TestArray big1 = {Float32(1e19), Float32(1e19)};
    TestArray big2 = {Float32(1e19), Float32(1e19)};

    {
        DataSet data_set = {{{big1, big2}, Float32(1.0)}};
        static_cast<void>(
                check_function<DataTypeFloat32, false>("cosine_similarity", input_types, data_set));
    }
    {
        DataSet data_set = {{{big1, big2}, Float32(0.0)}};
        static_cast<void>(
                check_function<DataTypeFloat32, false>("cosine_distance", input_types, data_set));
    }
}

} // namespace doris
