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

TEST(FunctionArrayApproximateDistanceTest, EmptyDataArraysProduceNull) {
    const InputTypeSet types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};
    const TestArray empty;
    const TestArray a = {Float32(0), Float32(0)};
    const TestArray b = {Float32(3), Float32(4)};
    const DataSet l2 = {{{empty, b}, Null()}, {{a, b}, Float32(5)}};
    ASSERT_TRUE((check_function<DataTypeFloat32, true>("l2_distance_approximate", types, l2)).ok());
    const DataSet ip = {{{empty, b}, Null()}, {{b, b}, Float32(25)}};
    ASSERT_TRUE(
            (check_function<DataTypeFloat32, true>("inner_product_approximate", types, ip)).ok());
}

TEST(FunctionArrayApproximateDistanceTest, EmptyQueryArraysAreRejected) {
    const TestArray empty;
    const TestArray vector = {Float32(1), Float32(2)};
    for (const auto* name : {"l2_distance_approximate", "inner_product_approximate"}) {
        for (bool constant_query : {false, true}) {
            InputTypeSet types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                  PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};
            if (constant_query) {
                types[2] = Consted {PrimitiveType::TYPE_ARRAY};
            }
            // A missing data vector must not mask an invalid query vector.
            for (const auto& data : {vector, empty}) {
                const DataSet rows = {{{data, empty}, Null()}};
                auto status =
                        check_function<DataTypeFloat32, true>(name, types, rows, -1, -1, true);
                EXPECT_FALSE(status.ok());
                EXPECT_NE(status.to_string().find("query vector cannot be empty"),
                          std::string::npos);
            }
        }
    }
}

TEST(FunctionArrayApproximateDistanceTest, ConstantQueryCanBeTheFirstArgument) {
    const InputTypeSet types = {Consted {PrimitiveType::TYPE_ARRAY}, PrimitiveType::TYPE_FLOAT,
                                PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};
    const TestArray query = {Float32(3), Float32(4)};
    const DataSet rows = {{{query, TestArray {}}, Null()}, {{query, query}, Float32(0)}};
    ASSERT_TRUE(
            (check_function<DataTypeFloat32, true>("l2_distance_approximate", types, rows)).ok());
    const DataSet invalid = {{{TestArray {}, query}, Null()}};
    auto status = check_function<DataTypeFloat32, true>("l2_distance_approximate", types, invalid,
                                                        -1, -1, true);
    EXPECT_FALSE(status.ok());
}

TEST(FunctionArrayApproximateDistanceTest, NonemptyDimensionMismatchRemainsAnError) {
    const InputTypeSet types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};
    const TestArray a = {Float32(1)};
    const TestArray b = {Float32(1), Float32(2)};
    const DataSet rows = {{{a, b}, Null()}};
    for (const auto* name : {"l2_distance_approximate", "inner_product_approximate"}) {
        auto status = check_function<DataTypeFloat32, true>(name, types, rows, -1, -1, true);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("different input element sizes"), std::string::npos);
    }
}

TEST(FunctionArrayApproximateDistanceTest, ConstantQueryAndOldFeResultLayout) {
    const InputTypeSet types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                Consted {PrimitiveType::TYPE_ARRAY}, PrimitiveType::TYPE_FLOAT};
    const TestArray a = {Float32(0), Float32(0)};
    const TestArray b = {Float32(3), Float32(4)};
    const DataSet rows = {{{a, b}, Float32(5)}, {{b, b}, Float32(0)}};
    ASSERT_TRUE(
            (check_function<DataTypeFloat32, false>("l2_distance_approximate", types, rows)).ok());
    const DataSet empty = {{{TestArray {}, b}, Null()}, {{a, b}, Float32(5)}};
    ASSERT_TRUE(
            (check_function<DataTypeFloat32, true>("l2_distance_approximate", types, empty)).ok());
}

TEST(FunctionArrayApproximateDistanceTest, ExactDistanceKeepsItsEmptyArrayBehavior) {
    const InputTypeSet types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT,
                                PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_FLOAT};
    const TestArray empty;
    const DataSet rows = {{{empty, empty}, Float32(0)}};
    ASSERT_TRUE((check_function<DataTypeFloat32, false>("l2_distance", types, rows)).ok());
}

} // namespace doris
