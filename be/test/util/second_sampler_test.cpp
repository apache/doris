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

#include "util/second_sampler.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"

namespace doris {

TEST(SecondSamplerTest, testsampler) {
    {
        SecondSampler<int64_t> sampler(7, 3);
        EXPECT_FALSE(sampler.is_init_succ());

        int64_t ret = 0;
        Status stat = sampler.get_window_value(ret, 7);
        EXPECT_FALSE(stat.ok());
        stat = sampler.get_full_window_value(ret);
        EXPECT_FALSE(stat.ok());
    }

    {
        // arr_size=4, [0,0,0,0], cur_idx=0
        SecondSampler<int64_t> sampler(8, 2);
        EXPECT_TRUE(sampler.is_init_succ());

        int64_t ret = 0;
        Status stat = sampler.get_window_value(ret, 9);
        EXPECT_FALSE(stat.ok());
        stat = sampler.get_window_value(ret, 8);
        EXPECT_TRUE(stat.ok());
        EXPECT_TRUE(ret == 0);

        int64_t val1 = 10;
        // [0,10,0,0], cur_idx=1
        sampler.update_sample(val1);
        int64_t val2 = 15;
        // [0,10,5,0], cur_idx=2
        sampler.update_sample(val2);
        int64_t ret2 = 0;
        stat = sampler.get_window_value(ret2, 8);
        EXPECT_TRUE(stat.ok());
        EXPECT_TRUE(ret2 == val2);

        int64_t val3 = 17;
        // [0,10,5,2], cur_idx=3
        sampler.update_sample(val3);
        int64_t val4 = 17;
        // [0,10,5,2], cur_idx=0
        sampler.update_sample(val4);
        int64_t ret3 = 0;
        stat = sampler.get_window_value(ret3, 8);
        EXPECT_TRUE(stat.ok());
        EXPECT_TRUE(ret3 == val4);

        int64_t val5 = 20;
        // [0,3,5,2], cur_idx=1
        sampler.update_sample(val5);
        int64_t ret4 = 0;
        stat = sampler.get_window_value(ret4, 8);
        EXPECT_TRUE(stat.ok());
        EXPECT_TRUE(ret4 == 10);

        int64_t ret5 = 0;
        stat = sampler.get_window_value(ret5, 4);
        EXPECT_TRUE(stat.ok());
        EXPECT_TRUE(ret5 == 3);

        int64_t ret6 = 0;
        stat = sampler.get_window_value(ret6, 5);
        EXPECT_TRUE(stat.ok());
        EXPECT_TRUE(ret6 == 5);
    }
}

}; // namespace doris