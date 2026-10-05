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

#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <random>
#include <string>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "exprs/function/fmod_fast.h"
#include "exprs/function/function_test_util.h"
#include "testutil/any_type.h"

namespace doris {
namespace {

uint64_t bits(double v) {
    return std::bit_cast<uint64_t>(v);
}

uint32_t bits(float v) {
    return std::bit_cast<uint32_t>(v);
}

void expect_same_double(double actual, double expected, double lhs, double rhs) {
    if (std::isnan(expected)) {
        ASSERT_TRUE(std::isnan(actual)) << "lhs=" << lhs << " rhs=" << rhs;
    } else {
        ASSERT_EQ(bits(expected), bits(actual)) << "lhs=" << lhs << " rhs=" << rhs
                                                << " expected=" << expected << " actual=" << actual;
    }
}

void expect_same_float(float actual, float expected, float lhs, float rhs) {
    if (std::isnan(expected)) {
        ASSERT_TRUE(std::isnan(actual)) << "lhs=" << lhs << " rhs=" << rhs;
    } else {
        ASSERT_EQ(bits(expected), bits(actual)) << "lhs=" << lhs << " rhs=" << rhs
                                                << " expected=" << expected << " actual=" << actual;
    }
}

double reference_fmod(double lhs, double rhs) {
    return std::fmod(lhs, rhs);
}

float reference_fmod(float lhs, float rhs) {
    return static_cast<float>(std::fmod(static_cast<double>(lhs), static_cast<double>(rhs)));
}

template <typename T>
std::vector<T> interesting_values();

template <>
std::vector<double> interesting_values<double>() {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const double inf = std::numeric_limits<double>::infinity();
    return {0.0,
            -0.0,
            1.0,
            -1.0,
            2.0,
            -2.0,
            2.5,
            -2.5,
            1000.1575,
            -1000.1575,
            44'728'676'500.0,
            -44'728'676'500.0,
            std::numeric_limits<double>::min(),
            -std::numeric_limits<double>::min(),
            std::numeric_limits<double>::denorm_min(),
            -std::numeric_limits<double>::denorm_min(),
            std::numeric_limits<double>::max(),
            -std::numeric_limits<double>::max(),
            inf,
            -inf,
            nan};
}

template <>
std::vector<float> interesting_values<float>() {
    const float nan = std::numeric_limits<float>::quiet_NaN();
    const float inf = std::numeric_limits<float>::infinity();
    return {0.0F,
            -0.0F,
            1.0F,
            -1.0F,
            2.0F,
            -2.0F,
            2.5F,
            -2.5F,
            1000.1575F,
            -1000.1575F,
            1.0e10F,
            -1.0e10F,
            std::numeric_limits<float>::min(),
            -std::numeric_limits<float>::min(),
            std::numeric_limits<float>::denorm_min(),
            -std::numeric_limits<float>::denorm_min(),
            std::numeric_limits<float>::max(),
            -std::numeric_limits<float>::max(),
            inf,
            -inf,
            nan};
}

template <typename T>
void check_scalar_pair(T lhs, T rhs);

template <>
void check_scalar_pair<double>(double lhs, double rhs) {
    expect_same_double(fmod_fast::scalar(lhs, rhs), reference_fmod(lhs, rhs), lhs, rhs);
}

template <>
void check_scalar_pair<float>(float lhs, float rhs) {
    expect_same_float(fmod_fast::scalar(lhs, rhs), reference_fmod(lhs, rhs), lhs, rhs);
}

template <typename T>
void check_scalar_corner_cases() {
    const auto values = interesting_values<T>();
    for (T lhs : values) {
        for (T rhs : values) {
            check_scalar_pair(lhs, rhs);
        }
    }
}

template <typename T>
void check_actual_load_distribution() {
    constexpr double db_scales[] = {1234.4500, 1876.2222, 8945.7353, 5612.6245, 4646.7853,
                                    6523.5285, 1000.1575, 6555.5678, 2587.8535, 3754.2575};
    for (double scale : db_scales) {
        for (int64_t row = 1; row <= 5'000'000; row += 9973) {
            T db = static_cast<T>(static_cast<double>(row) * scale);
            T in_one = static_cast<T>(static_cast<double>(row) * 2e-7);
            T in_ten = static_cast<T>(static_cast<double>(row) * 2e-6);
            check_scalar_pair(db, db);
            check_scalar_pair(in_one, db);
            check_scalar_pair(db, in_one);
            check_scalar_pair(db, in_ten);
        }
    }
}

template <typename T>
void check_random_finite_distribution() {
    std::mt19937_64 rng(0x9e3779b97f4a7c15ULL);
    std::uniform_real_distribution<double> large(-4.5e10, 4.5e10);
    std::uniform_real_distribution<double> small(-10.0, 10.0);
    std::uniform_real_distribution<double> tiny(-1e-200, 1e-200);
    for (int i = 0; i < 20000; ++i) {
        T lhs = static_cast<T>(large(rng));
        T rhs = static_cast<T>(small(rng));
        if (rhs == T(0)) {
            rhs = static_cast<T>(0.125);
        }
        check_scalar_pair(lhs, rhs);
        check_scalar_pair(static_cast<T>(small(rng)), lhs == T(0) ? static_cast<T>(1) : lhs);
        check_scalar_pair(static_cast<T>(tiny(rng)), rhs);
    }
}

template <typename T>
void fill_batch_inputs(std::vector<T>* lhs, std::vector<T>* rhs) {
    const auto values = interesting_values<T>();
    for (size_t i = 0; i < values.size(); ++i) {
        for (size_t j = 0; j < values.size(); ++j) {
            lhs->push_back(values[i]);
            rhs->push_back(values[j]);
        }
    }

    constexpr double db_scales[] = {1234.4500, 1876.2222, 8945.7353, 5612.6245, 4646.7853,
                                    6523.5285, 1000.1575, 6555.5678, 2587.8535, 3754.2575};
    for (double scale : db_scales) {
        for (int64_t row = 1; row <= 5'000'000; row += 1543) {
            T db = static_cast<T>(static_cast<double>(row) * scale);
            T in_one = static_cast<T>(static_cast<double>(row) * 2e-7);
            T in_ten = static_cast<T>(static_cast<double>(row) * 2e-6);
            lhs->push_back(db);
            rhs->push_back(db);
            lhs->push_back(static_cast<T>(in_one));
            rhs->push_back(db);
            lhs->push_back(db);
            rhs->push_back(in_one);
            lhs->push_back(db);
            rhs->push_back(in_ten);
        }
    }
}

template <typename T>
void check_batch_vector_vector();

template <>
void check_batch_vector_vector<double>() {
    std::vector<double> lhs;
    std::vector<double> rhs;
    fill_batch_inputs(&lhs, &rhs);
    std::vector<double> result(lhs.size());
    std::vector<uint8_t> null_map(lhs.size());

    fmod_fast::vector_vector(lhs.data(), rhs.data(), result.data(), null_map.data(), lhs.size());
    for (size_t i = 0; i < lhs.size(); ++i) {
        uint8_t expected_null = rhs[i] == 0.0;
        ASSERT_EQ(expected_null, null_map[i]) << i;
        double adjusted_rhs = rhs[i] + static_cast<double>(expected_null);
        expect_same_double(result[i], reference_fmod(lhs[i], adjusted_rhs), lhs[i], adjusted_rhs);
    }
}

template <>
void check_batch_vector_vector<float>() {
    std::vector<float> lhs;
    std::vector<float> rhs;
    fill_batch_inputs(&lhs, &rhs);
    std::vector<float> result(lhs.size());
    std::vector<uint8_t> null_map(lhs.size());

    fmod_fast::vector_vector(lhs.data(), rhs.data(), result.data(), null_map.data(), lhs.size());
    for (size_t i = 0; i < lhs.size(); ++i) {
        uint8_t expected_null = rhs[i] == 0.0F;
        ASSERT_EQ(expected_null, null_map[i]) << i;
        float adjusted_rhs = rhs[i] + static_cast<float>(expected_null);
        expect_same_float(result[i], reference_fmod(lhs[i], adjusted_rhs), lhs[i], adjusted_rhs);
    }
}

template <typename T>
void check_batch_const_shapes();

template <>
void check_batch_const_shapes<double>() {
    std::vector<double> lhs;
    std::vector<double> rhs;
    fill_batch_inputs(&lhs, &rhs);
    std::vector<double> result(lhs.size(), -777.0);
    std::vector<uint8_t> null_map(lhs.size());

    fmod_fast::vector_constant(lhs.data(), 0.0, result.data(), null_map.data(), lhs.size());
    for (size_t i = 0; i < lhs.size(); ++i) {
        ASSERT_EQ(1, null_map[i]) << i;
        ASSERT_EQ(bits(-777.0), bits(result[i])) << i;
    }

    fmod_fast::vector_constant(lhs.data(), 0.125, result.data(), null_map.data(), lhs.size());
    for (size_t i = 0; i < lhs.size(); ++i) {
        ASSERT_EQ(0, null_map[i]) << i;
        expect_same_double(result[i], reference_fmod(lhs[i], 0.125), lhs[i], 0.125);
    }

    fmod_fast::constant_vector(12345.678, rhs.data(), result.data(), null_map.data(), rhs.size());
    for (size_t i = 0; i < rhs.size(); ++i) {
        uint8_t expected_null = rhs[i] == 0.0;
        ASSERT_EQ(expected_null, null_map[i]) << i;
        double adjusted_rhs = rhs[i] + static_cast<double>(expected_null);
        expect_same_double(result[i], reference_fmod(12345.678, adjusted_rhs), 12345.678,
                           adjusted_rhs);
    }
}

template <>
void check_batch_const_shapes<float>() {
    std::vector<float> lhs;
    std::vector<float> rhs;
    fill_batch_inputs(&lhs, &rhs);
    std::vector<float> result(lhs.size(), -777.0F);
    std::vector<uint8_t> null_map(lhs.size());

    fmod_fast::vector_constant(lhs.data(), 0.0F, result.data(), null_map.data(), lhs.size());
    for (size_t i = 0; i < lhs.size(); ++i) {
        ASSERT_EQ(1, null_map[i]) << i;
        ASSERT_EQ(bits(-777.0F), bits(result[i])) << i;
    }

    fmod_fast::vector_constant(lhs.data(), 0.125F, result.data(), null_map.data(), lhs.size());
    for (size_t i = 0; i < lhs.size(); ++i) {
        ASSERT_EQ(0, null_map[i]) << i;
        expect_same_float(result[i], reference_fmod(lhs[i], 0.125F), lhs[i], 0.125F);
    }

    fmod_fast::constant_vector(12345.678F, rhs.data(), result.data(), null_map.data(), rhs.size());
    for (size_t i = 0; i < rhs.size(); ++i) {
        uint8_t expected_null = rhs[i] == 0.0F;
        ASSERT_EQ(expected_null, null_map[i]) << i;
        float adjusted_rhs = rhs[i] + static_cast<float>(expected_null);
        expect_same_float(result[i], reference_fmod(12345.678F, adjusted_rhs), 12345.678F,
                          adjusted_rhs);
    }
}

} // namespace

TEST(FunctionFmodFastTest, ScalarCornerCasesMatchStdFmod) {
    check_scalar_corner_cases<double>();
    check_scalar_corner_cases<float>();
}

TEST(FunctionFmodFastTest, ActualLoadDistributionMatchesStdFmod) {
    check_actual_load_distribution<double>();
    check_actual_load_distribution<float>();
}

TEST(FunctionFmodFastTest, RandomFiniteDistributionMatchesStdFmod) {
    check_random_finite_distribution<double>();
    check_random_finite_distribution<float>();
}

TEST(FunctionFmodFastTest, BatchVectorVectorMatchesStdFmod) {
    check_batch_vector_vector<double>();
    check_batch_vector_vector<float>();
}

TEST(FunctionFmodFastTest, BatchConstShapesMatchStdFmod) {
    check_batch_const_shapes<double>();
    check_batch_const_shapes<float>();
}

TEST(FunctionFmodFastTest, DorisFunctionNullSemanticsStayUnchanged) {
    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE};
    DataSet data_set = {
            {{5.5, 2.0}, reference_fmod(5.5, 2.0)},
            {{-5.5, 2.0}, reference_fmod(-5.5, 2.0)},
            {{5.5, -2.0}, reference_fmod(5.5, -2.0)},
            {{1.0, 0.0}, Null()},
            {{0.0, 0.0}, Null()},
            {{44'728'676'500.0, 0.9999998}, reference_fmod(44'728'676'500.0, 0.9999998)}};
    static_cast<void>(check_function<DataTypeFloat64, true>("fmod", input_types, data_set));
    static_cast<void>(check_function<DataTypeFloat64, true>("mod", input_types, data_set));
}

} // namespace doris
