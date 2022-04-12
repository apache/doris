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

#include "util/sort_heap.h"

#include <algorithm>
#include <queue>
#include <random>

#include "gtest/gtest.h"

namespace doris {

struct int_comparator {
    bool operator()(int l, int r) { return l < r; }
};

class SortHeapTest : public testing::Test {
public:
    SortHeapTest() = default;
    ~SortHeapTest() = default;

private:
    std::default_random_engine _re;
    int_comparator cp;
};

TEST_F(SortHeapTest, IntBasicTest) {
    std::priority_queue<int, std::vector<int>, int_comparator> pq(cp);
    doris::SortingHeap<int, std::vector<int>, int_comparator> sh(cp);
    // test default result
    const int test_case_1 = 10;
    for (size_t i = 0; i < test_case_1; ++i) {
        int res = _re();
        pq.push(res);
        sh.push(res);
    }
    EXPECT_EQ(pq.size(), sh.size());
    for (size_t i = 0; i < test_case_1; ++i) {
        EXPECT_EQ(sh.top(), pq.top());
        pq.pop();
        sh.remove_top();
    }
}

TEST_F(SortHeapTest, IntReplaceTest) {
    std::priority_queue<int, std::vector<int>, int_comparator> pq(cp);
    doris::SortingHeap<int, std::vector<int>, int_comparator> sh(cp);
    // test replace
    const int test_case_2 = 10;
    for (size_t i = 0; i < test_case_2; ++i) {
        int res = _re();
        pq.push(res);
        sh.push(res);
    }

    for (size_t i = 0; i < 2 * test_case_2; ++i) {
        int res = _re();
        EXPECT_EQ(sh.top(), pq.top());
        if (res < sh.top()) {
            sh.replace_top(res);
            pq.pop();
            pq.push(res);
        }
    }

    EXPECT_EQ(sh.size(), pq.size());
    int container_size = sh.size();
    for (size_t i = 0; i < container_size; ++i) {
        EXPECT_EQ(sh.top(), pq.top());
        pq.pop();
        sh.remove_top();
    }
}

} // namespace doris
