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
#include "util/arena.h"
#include "util/random.h"

namespace doris {

class ArenaTest : public testing::Test { };

TEST_F(ArenaTest, Empty) {
    Arena arena;
}

TEST_F(ArenaTest, Simple) {
    std::vector<std::pair<size_t, char*> > allocated;
    Arena arena;
    const int N = 100000;
    size_t bytes = 0;
    Random rnd(301);
    for (int i = 0; i < N; i++) {
        size_t s;
        if (i % (N / 10) == 0) {
            s = i;
        } else {
            s = rnd.OneIn(4000) ? rnd.Uniform(6000) :
                (rnd.OneIn(10) ? rnd.Uniform(100) : rnd.Uniform(20));
        }
        if (s == 0) {
            // Our arena disallows size 0 allocations.
            s = 1;
        }
        char* r;
        if (rnd.OneIn(10)) {
            r = arena.AllocateAligned(s);
        } else {
            r = arena.Allocate(s);
        }

        for (size_t b = 0; b < s; b++) {
            // Fill the "i"th allocation with a known bit pattern
            r[b] = i % 256;
        }
        bytes += s;
        allocated.push_back(std::make_pair(s, r));
        ASSERT_GE(arena.MemoryUsage(), bytes);
        if (i > N / 10) {
            ASSERT_LE(arena.MemoryUsage(), bytes * 1.10);
        }
    }
    for (size_t i = 0; i < allocated.size(); i++) {
        size_t num_bytes = allocated[i].first;
        const char* p = allocated[i].second;
        for (size_t b = 0; b < num_bytes; b++) {
            // Check the "i"th allocation for the known bit pattern
            ASSERT_EQ(int(p[b]) & 0xff, i % 256);
        }
    }
}

}  // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
