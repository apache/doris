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

#include "olap/rowset/unique_rowset_id_generator.h"

#include <gtest/gtest.h>

#include <iostream>

#include "testutil/test_util.h"
#include "util/pretty_printer.h"
#include "util/runtime_profile.h"
#include "util/threadpool.h"

namespace doris {
class UniqueRowsetIdGeneratorTest : public testing::Test {};

TEST_F(UniqueRowsetIdGeneratorTest, RowsetIdFormatTest) {
    {
        int64_t hi = 1; // version
        hi <<= 56;
        RowsetId rowset_id;
        rowset_id.init(123);
        EXPECT_EQ(rowset_id.version, 1);
        EXPECT_EQ(rowset_id.hi, 123 + hi);
        EXPECT_EQ(rowset_id.mi, 0);
        EXPECT_EQ(rowset_id.lo, 0);
        EXPECT_EQ(std::string("123"), rowset_id.to_string());
    }
    {
        int64_t hi = 1; // version
        hi <<= 56;
        RowsetId rowset_id;
        rowset_id.init("123");
        EXPECT_EQ(rowset_id.version, 1);
        EXPECT_EQ(rowset_id.hi, 123 + hi);
        EXPECT_EQ(rowset_id.mi, 0);
        EXPECT_EQ(rowset_id.lo, 0);
        EXPECT_EQ(std::string("123"), rowset_id.to_string());
    }

    {
        int64_t hi = 2; // version
        hi <<= 56;
        const std::string rowset_id_v2("0200000000000003c04f58d989cab2f2efd45faa20449189");
        RowsetId rowset_id;
        rowset_id.init(rowset_id_v2);
        EXPECT_EQ(rowset_id.version, 2);
        EXPECT_EQ(rowset_id.hi, 3 + hi);
        EXPECT_EQ(std::string(rowset_id_v2), rowset_id.to_string());
    }
}

TEST_F(UniqueRowsetIdGeneratorTest, GenerateIdTest) {
    UniqueId backend_uid = UniqueId::gen_uid();
    UniqueRowsetIdGenerator id_generator(backend_uid);
    int64_t hi = 2; // version
    hi <<= 56;

    RowsetId rowset_id = id_generator.next_id(); // hi == 1
    EXPECT_EQ(rowset_id.hi, hi + 1);
    EXPECT_EQ(rowset_id.version, 2);
    rowset_id = id_generator.next_id(); // hi == 2
    EXPECT_EQ(rowset_id.hi, hi + 2);
    EXPECT_EQ(rowset_id.version, 2);
    EXPECT_EQ(backend_uid.lo, rowset_id.lo);
    EXPECT_EQ(backend_uid.hi, rowset_id.mi);
    EXPECT_NE(rowset_id.hi, 0);
    bool in_use = id_generator.id_in_use(rowset_id);
    EXPECT_TRUE(in_use);
    id_generator.release_id(rowset_id);
    in_use = id_generator.id_in_use(rowset_id);
    EXPECT_FALSE(in_use);

    int64_t high = rowset_id.hi + 1;
    rowset_id = id_generator.next_id(); // hi == 3
    EXPECT_EQ(rowset_id.hi, high);
    in_use = id_generator.id_in_use(rowset_id);
    EXPECT_TRUE(in_use);

    std::string rowset_mid_str = rowset_id.to_string().substr(16, 16);
    std::string backend_mid_str = backend_uid.to_string().substr(0, 16);
    EXPECT_EQ(rowset_mid_str, backend_mid_str);
}

TEST_F(UniqueRowsetIdGeneratorTest, GenerateIdBenchmark) {
    const int kNumThreads = 8;
    const int kIdPerThread = LOOP_LESS_OR_MORE(1000, 1000000);

    UniqueId backend_uid = UniqueId::gen_uid();
    UniqueRowsetIdGenerator id_generator(backend_uid);
    std::unique_ptr<ThreadPool> pool;
    Status s = ThreadPoolBuilder("GenerateIdBenchmark")
                       .set_min_threads(kNumThreads)
                       .set_max_threads(kNumThreads)
                       .build(&pool);
    EXPECT_TRUE(s.ok()) << s.to_string();

    int64_t cost_ns = 0;
    {
        SCOPED_RAW_TIMER(&cost_ns);
        for (int i = 0; i < kNumThreads; i++) {
            EXPECT_TRUE(pool->submit_func([&id_generator, kIdPerThread]() {
                                for (int i = 0; i < kIdPerThread; ++i) {
                                    id_generator.next_id();
                                }
                            }).ok());
        }
        pool->wait();
    }

    int64_t hi = 2; // version
    hi <<= 56;
    RowsetId last_id = id_generator.next_id();
    EXPECT_EQ(last_id.hi, hi + kNumThreads * kIdPerThread + 1);
}

} // namespace doris
