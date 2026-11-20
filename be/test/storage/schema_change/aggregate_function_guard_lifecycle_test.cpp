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

#include <memory>
#include <vector>

#include "common/exception.h"
#include "core/data_type/data_type_string.h"
#include "exprs/aggregate/aggregate_function_min_max.h"

namespace doris {

namespace {

template <bool FailCreate>
struct CountedMinState : AggregateFunctionMinData<SingleValueDataString> {
    static inline int created = 0;
    static inline int destroyed = 0;

    CountedMinState() {
        if constexpr (FailCreate) {
            throw Exception(ErrorCode::MEM_ALLOC_FAILED, "injected state creation failure");
        }
        ++created;
    }
    ~CountedMinState() { ++destroyed; }
};

} // namespace

TEST(SchemaChangeAggregateGuardTest, LaterCreateFailureReleasesCompletedPrefix) {
    using First = CountedMinState<false>;
    using Second = CountedMinState<true>;
    First::created = First::destroyed = 0;
    Second::created = Second::destroyed = 0;

    std::vector<AggregateFunctionPtr> functions;
    std::vector<std::unique_ptr<AggregateFunctionGuard>> states;
    DataTypes args {std::make_shared<DataTypeString>()};
    functions.push_back(std::make_shared<AggregateFunctionsSingleValue<First>>(args));
    states.push_back(std::make_unique<AggregateFunctionGuard>(functions.back().get()));
    functions.push_back(std::make_shared<AggregateFunctionsSingleValue<Second>>(args));
    EXPECT_THROW(states.push_back(std::make_unique<AggregateFunctionGuard>(functions.back().get())),
                 Exception);
    EXPECT_EQ(First::created, 1);
    EXPECT_EQ(First::destroyed, 0);
    EXPECT_EQ(Second::created, 0);
    EXPECT_EQ(Second::destroyed, 0);

    states.clear();
    EXPECT_EQ(First::destroyed, 1);
    EXPECT_EQ(Second::destroyed, 0);
}

} // namespace doris
