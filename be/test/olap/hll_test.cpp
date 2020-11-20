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

#include "olap/hll.h"

#include <gtest/gtest.h>

#include "util/hash_util.hpp"
#include "util/slice.h"

namespace doris {

class TestHll : public testing::Test {
public:
    virtual ~TestHll() {}
};

static uint64_t hash(uint64_t value) {
    return HashUtil::murmur_hash64A(&value, 8, 0);
}
//  keep logic same with java version in fe when you change hll_test.cpp,see HllTest.java
TEST_F(TestHll, Normal) {
    uint8_t buf[HLL_REGISTERS_COUNT + 1];

    // empty
    {
        Slice str((char*)buf, 0);
        ASSERT_FALSE(HyperLogLog::is_valid(str));
    }
    // check unknown type
    {
        buf[0] = 60;
        Slice str((char*)buf, 1);
        ASSERT_FALSE(HyperLogLog::is_valid(str));
    }

    // empty
    {
        HyperLogLog empty_hll;
        int len = empty_hll.serialize(buf);
        ASSERT_EQ(1, len);
        HyperLogLog test_hll(Slice((char*)buf, len));
        ASSERT_EQ(0, test_hll.estimate_cardinality());

        // check serialize
        {
            Slice str((char*)buf, len);
            ASSERT_TRUE(HyperLogLog::is_valid(str));
        }
        {
            Slice str((char*)buf, len + 1);
            ASSERT_FALSE(HyperLogLog::is_valid(str));
        }
    }
    // explicit [0. 100)
    HyperLogLog explicit_hll;
    {
        for (int i = 0; i < 100; ++i) {
            explicit_hll.update(hash(i));
        }
        int len = explicit_hll.serialize(buf);
        ASSERT_EQ(1 + 1 + 100 * 8, len);

        // check serialize
        {
            Slice str((char*)buf, len);
            ASSERT_TRUE(HyperLogLog::is_valid(str));
        }
        {
            Slice str((char*)buf, 1);
            ASSERT_FALSE(HyperLogLog::is_valid(str));
        }

        HyperLogLog test_hll(Slice((char*)buf, len));
        test_hll.update(hash(0));
        {
            HyperLogLog other_hll;
            for (int i = 0; i < 100; ++i) {
                other_hll.update(hash(i));
            }
            test_hll.merge(other_hll);
        }
        ASSERT_EQ(100, test_hll.estimate_cardinality());
    }
    // sparse [1024, 2048)
    HyperLogLog sparse_hll;
    {
        for (int i = 0; i < 1024; ++i) {
            sparse_hll.update(hash(i + 1024));
        }
        int len = sparse_hll.serialize(buf);
        ASSERT_TRUE(len < HLL_REGISTERS_COUNT + 1);

        // check serialize
        {
            Slice str((char*)buf, len);
            ASSERT_TRUE(HyperLogLog::is_valid(str));
        }
        {
            Slice str((char*)buf, 1 + 3);
            ASSERT_FALSE(HyperLogLog::is_valid(str));
        }

        HyperLogLog test_hll(Slice((char*)buf, len));
        test_hll.update(hash(1024));
        {
            HyperLogLog other_hll;
            for (int i = 0; i < 1024; ++i) {
                other_hll.update(hash(i + 1024));
            }
            test_hll.merge(other_hll);
        }
        auto cardinality = test_hll.estimate_cardinality();
        ASSERT_EQ(sparse_hll.estimate_cardinality(), cardinality);
        // 2% error rate
        ASSERT_TRUE(cardinality > 1000 && cardinality < 1045);
    }
    // full [64 * 1024, 128 * 1024)
    HyperLogLog full_hll;
    {
        for (int i = 0; i < 64 * 1024; ++i) {
            full_hll.update(hash(64 * 1024 + i));
        }
        int len = full_hll.serialize(buf);
        ASSERT_EQ(HLL_REGISTERS_COUNT + 1, len);

        // check serialize
        {
            Slice str((char*)buf, len);
            ASSERT_TRUE(HyperLogLog::is_valid(str));
        }
        {
            Slice str((char*)buf, len + 1);
            ASSERT_FALSE(HyperLogLog::is_valid(str));
        }

        HyperLogLog test_hll(Slice((char*)buf, len));
        auto cardinality = test_hll.estimate_cardinality();
        ASSERT_EQ(full_hll.estimate_cardinality(), cardinality);
        // 2% error rate
        ASSERT_TRUE(cardinality > 62 * 1024 && cardinality < 66 * 1024);
    }
    // merge explicit to empty_hll
    {
        HyperLogLog new_explicit_hll;
        new_explicit_hll.merge(explicit_hll);
        ASSERT_EQ(100, new_explicit_hll.estimate_cardinality());

        // merge another explicit
        {
            HyperLogLog other_hll;
            for (int i = 100; i < 200; ++i) {
                other_hll.update(hash(i));
            }
            // this is converted to full
            other_hll.merge(new_explicit_hll);
            ASSERT_TRUE(other_hll.estimate_cardinality() > 190);
        }
        // merge full
        {
            new_explicit_hll.merge(full_hll);
            ASSERT_TRUE(new_explicit_hll.estimate_cardinality() > full_hll.estimate_cardinality());
        }
    }
    // merge sparse to empty_hll
    {
        HyperLogLog new_sparse_hll;
        new_sparse_hll.merge(sparse_hll);
        ASSERT_EQ(sparse_hll.estimate_cardinality(), new_sparse_hll.estimate_cardinality());

        // merge explicit
        new_sparse_hll.merge(explicit_hll);
        ASSERT_TRUE(new_sparse_hll.estimate_cardinality() > sparse_hll.estimate_cardinality());

        // merge full
        new_sparse_hll.merge(full_hll);
        ASSERT_TRUE(new_sparse_hll.estimate_cardinality() > full_hll.estimate_cardinality());
    }
}

TEST_F(TestHll, InvalidPtr) {
    {
        HyperLogLog hll(Slice((char*)nullptr, 0));
        ASSERT_EQ(0, hll.estimate_cardinality());
    }
    {
        uint8_t buf[64] = {60};
        HyperLogLog hll(Slice(buf, 1));
        ASSERT_EQ(0, hll.estimate_cardinality());
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
