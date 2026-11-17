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

#include "core/data_type/data_type_number.h"
#include "exprs/function/function_test_util.h"

namespace doris {

class FunctionArrayDistanceNullTest : public testing::TestWithParam<const char*> {};

INSTANTIATE_TEST_SUITE_P(AllDistances, FunctionArrayDistanceNullTest,
                         testing::Values("l1_distance", "l2_distance", "inner_product",
                                         "cosine_distance", "cosine_similarity",
                                         "l2_distance_approximate", "inner_product_approximate"));

TEST_P(FunctionArrayDistanceNullTest, ValidVectorsProduceNullableDistances) {
    const TestArray vector = {Float32(3), Float32(4)};
    const std::string name = GetParam();
    const Float32 expected = name == "inner_product" || name == "inner_product_approximate"
                                     ? Float32(25)
                             : name == "cosine_similarity" ? Float32(1)
                                                           : Float32(0);
    const DataSet rows = {{{vector, vector}, expected}};
    for (bool constants : {false, true}) {
        InputTypeSet types = {Notnull {PrimitiveType::TYPE_ARRAY}, PrimitiveType::TYPE_FLOAT,
                              Notnull {PrimitiveType::TYPE_ARRAY}, PrimitiveType::TYPE_FLOAT};
        if (constants) {
            types[0] = ConstedNotnull {PrimitiveType::TYPE_ARRAY};
            types[2] = ConstedNotnull {PrimitiveType::TYPE_ARRAY};
        }
        ASSERT_TRUE((check_function<DataTypeFloat32, true>(GetParam(), types, rows)).ok());
    }
}

TEST_P(FunctionArrayDistanceNullTest, MissingDataOnEitherSideProducesNull) {
    const InputTypeSet types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};
    const TestArray empty;
    const TestArray vector = {Float32(3), Float32(4)};
    const DataSet rows = {{{empty, vector}, Null()},  {{vector, empty}, Null()},
                          {{empty, empty}, Null()},   {{Null(), vector}, Null()},
                          {{vector, Null()}, Null()}, {{Null(), Null()}, Null()},
                          {{Null(), empty}, Null()},  {{empty, Null()}, Null()}};
    ASSERT_TRUE((check_function<DataTypeFloat32, true>(GetParam(), types, rows)).ok());
}

TEST_P(FunctionArrayDistanceNullTest, ValidConstantQueryWithMissingData) {
    const TestArray vector = {Float32(3), Float32(4)};
    for (int constant_arg : {0, 1}) {
        InputTypeSet types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                              PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};
        types[constant_arg * 2] = Consted {PrimitiveType::TYPE_ARRAY};
        const DataSet rows =
                constant_arg == 0
                        ? DataSet {{{vector, TestArray {}}, Null()}, {{vector, Null()}, Null()}}
                        : DataSet {{{TestArray {}, vector}, Null()}, {{Null(), vector}, Null()}};
        ASSERT_TRUE((check_function<DataTypeFloat32, true>(GetParam(), types, rows)).ok());
    }
}

TEST_P(FunctionArrayDistanceNullTest, MissingLiteralsProduceNull) {
    const TestArray empty;
    const TestArray vector = {Float32(3), Float32(4)};
    const DataSet rows = {{{empty, vector}, Null()},  {{vector, empty}, Null()},
                          {{empty, empty}, Null()},   {{Null(), vector}, Null()},
                          {{vector, Null()}, Null()}, {{Null(), Null()}, Null()},
                          {{Null(), empty}, Null()},  {{empty, Null()}, Null()}};
    for (bool first_constant : {false, true}) {
        for (bool second_constant : {false, true}) {
            InputTypeSet types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                  PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};
            if (first_constant) {
                types[0] = Consted {PrimitiveType::TYPE_ARRAY};
            }
            if (second_constant) {
                types[2] = Consted {PrimitiveType::TYPE_ARRAY};
            }
            for (const auto& row : rows) {
                ASSERT_TRUE((check_function<DataTypeFloat32, true>(GetParam(), types, {row})).ok());
            }
        }
    }
}

TEST_P(FunctionArrayDistanceNullTest, NullElementsAndDimensionMismatchRemainErrors) {
    for (bool constant_query : {false, true}) {
        InputTypeSet types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                              PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};
        if (constant_query) {
            types[2] = Consted {PrimitiveType::TYPE_ARRAY};
        }
        const TestArray vector = {Float32(3), Float32(4)};
        const TestArray with_null = {Float32(3), Null()};
        for (bool first_has_null : {false, true}) {
            const DataSet rows = first_has_null ? DataSet {{{with_null, vector}, Null()}}
                                                : DataSet {{{vector, with_null}, Null()}};
            auto status =
                    check_function<DataTypeFloat32, true>(GetParam(), types, rows, -1, -1, true);
            EXPECT_FALSE(status.ok());
            EXPECT_NE(status.to_string().find("cannot have null"), std::string::npos);
        }
        const DataSet rows = {{{TestArray {Float32(3)}, vector}, Null()}};
        auto status = check_function<DataTypeFloat32, true>(GetParam(), types, rows, -1, -1, true);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("different input element sizes"), std::string::npos);
    }
}

TEST(FunctionArrayApproximateDistanceTest, ConstantQueryProducesNullableDistances) {
    const InputTypeSet types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                Consted {PrimitiveType::TYPE_ARRAY}, PrimitiveType::TYPE_FLOAT};
    const TestArray a = {Float32(0), Float32(0)};
    const TestArray b = {Float32(3), Float32(4)};
    const DataSet l2 = {{{a, b}, Float32(5)}, {{b, b}, Float32(0)}};
    ASSERT_TRUE((check_function<DataTypeFloat32, true>("l2_distance_approximate", types, l2)).ok());
    const DataSet ip = {{{a, b}, Float32(0)}, {{b, b}, Float32(25)}};
    ASSERT_TRUE(
            (check_function<DataTypeFloat32, true>("inner_product_approximate", types, ip)).ok());
}

} // namespace doris
