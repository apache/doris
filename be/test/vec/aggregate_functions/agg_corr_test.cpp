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

#include <random>

#include "agg_function_test.h"
#include "vec/aggregate_functions/aggregate_function_corr.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

struct AggregateFunctionCorrTest : public AggregateFunctiontest {
    using Corr = CorrMoment<TYPE_DOUBLE>;
    using CorrWelford = CorrMomentWelford<TYPE_DOUBLE>;

    std::vector<double> random_data(size_t n) {
        std::vector<double> data(n);
        std::random_device rd;                             // obtain a random number from hardware
        std::mt19937 gen(rd());                            // seed the generator
        std::uniform_real_distribution<> dis(0.0, 100000); // define the range

        for (size_t i = 0; i < n; ++i) {
            data[i] = dis(gen);
        }
        return data;
    }

    std::vector<double> random_data_big(size_t n) {
        std::vector<double> data(n);
        std::random_device rd;  // obtain a random number from hardware
        std::mt19937 gen(rd()); // seed the generator
        std::uniform_real_distribution<> dis(100000000000000000000.0,
                                             1000000000000000000000.0); // define the range

        for (size_t i = 0; i < n; ++i) {
            data[i] = dis(gen);
        }
        return data;
    }

    void try_test_corr() {
        auto func_small = [&]() -> std::tuple<Corr, CorrWelford> {
            Corr corr;
            CorrWelford corr_welford;
            const int size = 10000;
            auto data1 = random_data(size);
            auto data2 = random_data(size);

            for (int i = 0; i < size; ++i) {
                corr.add(data1[i], data2[i]);
                corr_welford.add(data1[i], data2[i]);
            }

            double corr_result = corr.get();
            double corr_welford_result = corr_welford.get();
            std::cout << std::fixed << std::setprecision(16) << "corr_result: " << corr_result
                      << std::endl;
            std::cout << std::fixed << std::setprecision(16)
                      << "corr_welford_result: " << corr_welford_result << std::endl;
            EXPECT_NEAR(corr_result, corr_welford_result, 1e-6);
            return std::tuple {corr, corr_welford};
        };

        auto func_big = [&]() {
            Corr corr;
            CorrWelford corr_welford;
            const int size = 20000;
            auto data1 = random_data_big(size);
            auto data2 = random_data_big(size);

            for (int i = 0; i < size; ++i) {
                corr.add(data1[i], data2[i]);
                corr_welford.add(data1[i], data2[i]);
            }

            double corr_result = corr.get();
            double corr_welford_result = corr_welford.get();
            std::cout << std::fixed << std::setprecision(16) << "corr_result: " << corr_result
                      << std::endl;
            std::cout << std::fixed << std::setprecision(16)
                      << "corr_welford_result: " << corr_welford_result << std::endl;
            EXPECT_NEAR(corr_result, corr_welford_result, 1e-6);
            return std::tuple {corr, corr_welford};
        };

        auto [corr_big, corr_welford_big] = func_big();

        for (int i = 0; i < 5; i++) {
            auto [corr_small, corr_welford_small] = func_small();
            corr_big.merge(corr_small);
            corr_welford_big.merge(corr_welford_small);
        }

        double corr_result = corr_big.get();
        double corr_welford_result = corr_welford_big.get();
        std::cout << std::fixed << std::setprecision(16) << "corr_result: " << corr_result
                  << std::endl;
        std::cout << std::fixed << std::setprecision(16)
                  << "corr_welford_result: " << corr_welford_result << std::endl;
        EXPECT_NEAR(corr_result, corr_welford_result, 1e-6);
    }
};

TEST_F(AggregateFunctionCorrTest, test) {
    try_test_corr();
    try_test_corr();
    try_test_corr();
    try_test_corr();
    try_test_corr();
}

TEST_F(AggregateFunctionCorrTest, test_corr) {
    create_agg("corr", false,
               {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()});

    execute(Block({ColumnHelper::create_column_with_name<DataTypeFloat64>({1, 2, 3, 4, 5}),
                   ColumnHelper::create_column_with_name<DataTypeFloat64>({1, 2, 3, 4, 5})}),
            ColumnHelper::create_column_with_name<DataTypeFloat64>({1}));
}

TEST_F(AggregateFunctionCorrTest, test_corr_welford) {
    create_agg("corr_welford", false,
               {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()});

    execute(Block({ColumnHelper::create_column_with_name<DataTypeFloat64>({1, 2, 3, 4, 5}),
                   ColumnHelper::create_column_with_name<DataTypeFloat64>({1, 2, 3, 4, 5})}),
            ColumnHelper::create_column_with_name<DataTypeFloat64>({1}));
}

} // namespace doris::vectorized
