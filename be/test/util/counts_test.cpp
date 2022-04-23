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

#include "util/counts.h"

#include <gtest/gtest.h>

#include "testutil/test_util.h"

namespace doris {

class TCountsTest : public testing::Test {};

TEST_F(TCountsTest, TotalTest) {
    Counts counts;
    // 1 1 1 2 5 7 7 9 9 19
    // >>> import numpy as np
    // >>> a = np.array([1,1,1,2,5,7,7,9,9,19])
    // >>> p = np.percentile(a, 20)
    counts.increment(1, 3);
    counts.increment(5, 1);
    counts.increment(2, 1);
    counts.increment(9, 1);
    counts.increment(9, 1);
    counts.increment(19, 1);
    counts.increment(7, 2);

    doris_udf::DoubleVal result = counts.terminate(0.2);
    EXPECT_EQ(1, result.val);
    uint8_t* writer = new uint8_t[counts.serialized_size()];
    uint8_t* type_reader = writer;
    counts.serialize(writer);

    Counts other;
    other.unserialize(type_reader);
    doris_udf::DoubleVal result1 = other.terminate(0.2);
    EXPECT_EQ(result.val, result1.val);

    Counts other1;
    other1.increment(1, 1);
    other1.increment(100, 3);
    other1.increment(50, 3);
    other1.increment(10, 1);
    other1.increment(99, 2);

    counts.merge(&other1);
    // 1 1 1 1 2 5 7 7 9 9 10 19 50 50 50 99 99 100 100 100
    EXPECT_EQ(counts.terminate(0.3).val, 6.4);
    delete[] writer;
}

} // namespace doris
