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

#include "function_test_util.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

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
        // Expected: 34 / sqrt(14 * 83) = 34 / sqrt(1162) ≈ 0.9974149
        float expected = 34.0f / std::sqrt(14.0f * 83.0f);
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

} // namespace doris::vectorized
