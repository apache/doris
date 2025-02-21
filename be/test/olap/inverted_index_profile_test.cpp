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

#include "olap/inverted_index_profile.h"

#include <gtest/gtest.h>

#include <memory>

#include "olap/inverted_index_stats.h"

namespace doris {

TEST(InvertedIndexProfileReporterTest, UpdateTest) {
    auto runtime_profile = std::make_unique<RuntimeProfile>("test_profile");

    InvertedIndexStatistics statistics;
    statistics.stats.push_back({"test_column1", 101, 201});
    statistics.stats.push_back({"test_column2", 102, 202});

    InvertedIndexProfileReporter reporter;
    reporter.update(runtime_profile.get(), &statistics);

    ASSERT_EQ(runtime_profile->get_counter("fr_test_column1")->value(), 101);
    ASSERT_EQ(runtime_profile->get_counter("ft_test_column1")->value(), 201);
    ASSERT_EQ(runtime_profile->get_counter("fr_test_column2")->value(), 102);
    ASSERT_EQ(runtime_profile->get_counter("ft_test_column2")->value(), 202);
}

} // namespace doris