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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <cstdint>

#include "gtest/gtest_pred_impl.h"

namespace doris {

class TCountsTest : public testing::Test {};

TEST_F(TCountsTest, TotalTest) {
    Counts<int64_t> counts;
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

    double result = counts.terminate(0.2);
    EXPECT_EQ(1, result);

    auto cs = vectorized::ColumnString::create();
    vectorized::BufferWritable bw(*cs);
    counts.serialize(bw);
    bw.commit();

    Counts<int64_t> other;
    StringRef res(cs->get_chars().data(), cs->get_chars().size());
    vectorized::BufferReadable br(res);
    other.unserialize(br);
    double result1 = other.terminate(0.2);
    EXPECT_EQ(result, result1);

    Counts<int64_t> other1;
    other1.increment(1, 1);
    other1.increment(100, 3);
    other1.increment(50, 3);
    other1.increment(10, 1);
    other1.increment(99, 2);

    // deserialize other1
    cs->clear();
    other1.serialize(bw);
    bw.commit();
    Counts<int64_t> other1_deserialized;
    vectorized::BufferReadable br1(res);
    other1_deserialized.unserialize(br1);

    Counts<int64_t> merge_res;
    merge_res.merge(&other);
    merge_res.merge(&other1_deserialized);
    // 1 1 1 1 2 5 7 7 9 9 10 19 50 50 50 99 99 100 100 100
    EXPECT_EQ(merge_res.terminate(0.3), 6.4);
}

} // namespace doris
