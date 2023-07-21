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
//
// This file is copied from
// https://github.com/apache/kudu/blob/master/src/kudu/util/interval_tree-test.cc
// and modified by Doris

#include "util/interval_tree.h"

#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <tuple> // IWYU pragma: keep
#include <utility>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "gutil/stringprintf.h"
#include "gutil/strings/substitute.h"
#include "testutil/test_util.h"
#include "util/interval_tree-inl.h"

using std::pair;
using std::string;
using std::vector;
using strings::Substitute;

namespace doris {

// Test harness.
class TestIntervalTree : public testing::Test {};

// Simple interval class for integer intervals.
struct IntInterval {
    IntInterval(int left, int right, int id = -1) : left(left), right(right), id(id) {}

    // std::nullopt means infinity.
    // [left,  right] is closed interval.
    // [lower, upper) is half-open interval, so the upper is exclusive.
    bool Intersects(const std::optional<int>& lower, const std::optional<int>& upper) const {
        if (lower == std::nullopt && upper == std::nullopt) {
            //         [left, right]
            //            |     |
            // [-OO,                      +OO)
        } else if (lower == std::nullopt) {
            //         [left, right]
            //            |
            // [-OO,    upper)
            if (*upper <= this->left) return false;
        } else if (upper == std::nullopt) {
            //         [left, right]                          /
            //                     \                          /
            //                      [lower, +OO)              /
            if (*lower > this->right) return false;
        } else {
            //         [left, right]                          /
            //                     \                          /
            //                      [lower, upper)            /
            if (*lower > this->right) return false;
            //         [left, right]                          /
            //            |                                   /
            // [lower,  upper)                                /
            if (*upper <= this->left) return false;
        }
        return true;
    }

    string ToString() const { return strings::Substitute("[$0, $1]($2) ", left, right, id); }

    int left, right, id;
};

// A wrapper around an int which can be compared with IntTraits::compare()
// but also can keep a counter of how many times it has been compared. Used
// for TestBigO below.
struct CountingQueryPoint {
    explicit CountingQueryPoint(int v) : val(v), count(new int(0)) {}

    int val;
    std::shared_ptr<int> count;
};

// Traits definition for intervals made up of ints on either end.
struct IntTraits {
    typedef int point_type;
    typedef IntInterval interval_type;
    static point_type get_left(const IntInterval& x) { return x.left; }
    static point_type get_right(const IntInterval& x) { return x.right; }
    static int compare(int a, int b) {
        if (a < b) return -1;
        if (a > b) return 1;
        return 0;
    }

    static int compare(const CountingQueryPoint& q, int b) {
        (*q.count)++;
        return compare(q.val, b);
    }
    static int compare(int a, const CountingQueryPoint& b) { return -compare(b, a); }

    static int compare(const std::optional<int>& a, const int b, const EndpointIfNone& type) {
        if (a == std::nullopt) {
            return ((POSITIVE_INFINITY == type) ? 1 : -1);
        }

        return compare(*a, b);
    }

    static int compare(const int a, const std::optional<int>& b, const EndpointIfNone& type) {
        return -compare(b, a, type);
    }
};

// Compare intervals in an arbitrary but consistent way - this is only
// used for verifying that the two algorithms come up with the same results.
// It's not necessary to define this to use an interval tree.
static bool CompareIntervals(const IntInterval& a, const IntInterval& b) {
    return std::make_tuple(a.left, a.right, a.id) < std::make_tuple(b.left, b.right, b.id);
}

// Stringify a list of int intervals, for easy test error reporting.
static string Stringify(const vector<IntInterval>& intervals) {
    string ret;
    bool first = true;
    for (const IntInterval& interval : intervals) {
        if (!first) {
            ret.append(",");
        }
        ret.append(interval.ToString());
    }
    return ret;
}

// Find any intervals in 'intervals' which contain 'query_point' by brute force.
static void FindContainingBruteForce(const vector<IntInterval>& intervals, int query_point,
                                     vector<IntInterval>* results) {
    for (const IntInterval& i : intervals) {
        if (query_point >= i.left && query_point <= i.right) {
            results->push_back(i);
        }
    }
}

// Find any intervals in 'intervals' which intersect 'query_interval' by brute force.
static void FindIntersectingBruteForce(const vector<IntInterval>& intervals,
                                       const std::optional<int>& lower,
                                       const std::optional<int>& upper,
                                       vector<IntInterval>* results) {
    for (const IntInterval& i : intervals) {
        if (i.Intersects(lower, upper)) {
            results->push_back(i);
        }
    }
}

// Verify that IntervalTree::FindContainingPoint yields the same results as the naive
// brute-force O(n) algorithm.
static void VerifyFindContainingPoint(const vector<IntInterval>& all_intervals,
                                      const IntervalTree<IntTraits>& tree, int query_point) {
    vector<IntInterval> results;
    tree.FindContainingPoint(query_point, &results);
    std::sort(results.begin(), results.end(), CompareIntervals);

    vector<IntInterval> brute_force;
    FindContainingBruteForce(all_intervals, query_point, &brute_force);
    std::sort(brute_force.begin(), brute_force.end(), CompareIntervals);

    SCOPED_TRACE(Stringify(all_intervals) + StringPrintf(" {q=%d}", query_point));
    EXPECT_EQ(Stringify(brute_force), Stringify(results));
}

// Verify that IntervalTree::FindIntersectingInterval yields the same results as the naive
// brute-force O(n) algorithm.
static void VerifyFindIntersectingInterval(const vector<IntInterval>& all_intervals,
                                           const IntervalTree<IntTraits>& tree,
                                           const IntInterval& query_interval) {
    const auto& Process = [&](const std::optional<int>& lower, const std::optional<int>& upper) {
        vector<IntInterval> results;
        tree.FindIntersectingInterval(lower, upper, &results);
        std::sort(results.begin(), results.end(), CompareIntervals);

        vector<IntInterval> brute_force;
        FindIntersectingBruteForce(all_intervals, lower, upper, &brute_force);
        std::sort(brute_force.begin(), brute_force.end(), CompareIntervals);
        EXPECT_EQ(Stringify(brute_force), Stringify(results));
    };

    {
        // [lower, upper)
        std::optional<int> lower = query_interval.left;
        std::optional<int> upper = query_interval.right;
        SCOPED_TRACE(Stringify(all_intervals) + StringPrintf(" {q=[%d, %d)}", *lower, *upper));
        Process(lower, upper);
    }

    {
        // [-OO, upper)
        std::optional<int> lower = std::nullopt;
        std::optional<int> upper = query_interval.right;
        SCOPED_TRACE(Stringify(all_intervals) + StringPrintf(" {q=[-OO, %d)}", *upper));
        Process(lower, upper);
    }

    {
        // [lower, +OO)
        std::optional<int> lower = query_interval.left;
        std::optional<int> upper = std::nullopt;
        SCOPED_TRACE(Stringify(all_intervals) + StringPrintf(" {q=[%d, +OO)}", *lower));
        Process(lower, upper);
    }

    {
        // [-OO, +OO)
        std::optional<int> lower = query_interval.left;
        std::optional<int> upper = std::nullopt;
        SCOPED_TRACE(Stringify(all_intervals) + StringPrintf(" {q=[-OO, +OO)}"));
        Process(lower, upper);
    }
}

static vector<IntInterval> CreateRandomIntervals(int n = 100) {
    vector<IntInterval> intervals;
    for (int i = 0; i < n; i++) {
        int l = rand_rng_int(0, 100);    // NOLINT(runtime/threadsafe_fn)
        int r = l + rand_rng_int(0, 20); // NOLINT(runtime/threadsafe_fn)
        intervals.emplace_back(l, r, i);
    }
    return intervals;
}

TEST_F(TestIntervalTree, TestBasic) {
    vector<IntInterval> intervals;
    intervals.emplace_back(1, 2, 1);
    intervals.emplace_back(3, 4, 2);
    intervals.emplace_back(1, 4, 3);
    IntervalTree<IntTraits> t(intervals);

    for (int i = 0; i <= 5; i++) {
        VerifyFindContainingPoint(intervals, t, i);

        for (int j = i; j <= 5; j++) {
            VerifyFindIntersectingInterval(intervals, t, IntInterval(i, j, 0));
        }
    }
}

TEST_F(TestIntervalTree, TestRandomized) {
    // Generate 100 random intervals spanning 0-200 and build an interval tree from them.
    vector<IntInterval> intervals = CreateRandomIntervals();
    IntervalTree<IntTraits> t(intervals);

    // Test that we get the correct result on every possible query.
    for (int i = -1; i < 201; i++) {
        VerifyFindContainingPoint(intervals, t, i);
    }

    // Test that we get the correct result for random intervals
    for (int i = 0; i < 100; i++) {
        int l = rand_rng_int(0, 100);     // NOLINT(runtime/threadsafe_fn)
        int r = rand_rng_int(l, l + 100); // NOLINT(runtime/threadsafe_fn)
        VerifyFindIntersectingInterval(intervals, t, IntInterval(l, r));
    }
}

TEST_F(TestIntervalTree, TestEmpty) {
    vector<IntInterval> empty;
    IntervalTree<IntTraits> t(empty);

    VerifyFindContainingPoint(empty, t, 1);
    VerifyFindIntersectingInterval(empty, t, IntInterval(1, 2, 0));
}

TEST_F(TestIntervalTree, TestBigO) {
#ifndef NDEBUG
    LOG(WARNING) << "big-O results are not valid if DCHECK is enabled";
    return;
#endif
    LOG(INFO) << "num_int\tnum_q\tresults\tsimple\tbatch";
    for (int num_intervals = 1; num_intervals < 2000; num_intervals *= 2) {
        vector<IntInterval> intervals = CreateRandomIntervals(num_intervals);
        IntervalTree<IntTraits> t(intervals);
        for (int num_queries = 1; num_queries < 2000; num_queries *= 2) {
            vector<CountingQueryPoint> queries;
            for (int i = 0; i < num_queries; i++) {
                queries.emplace_back(rand_rng_int(0, 100));
            }
            std::sort(queries.begin(), queries.end(),
                      [](const CountingQueryPoint& a, const CountingQueryPoint& b) {
                          return a.val < b.val;
                      });

            // Test using batch algorithm.
            int num_results_batch = 0;
            t.ForEachIntervalContainingPoints(
                    queries, [&](CountingQueryPoint query_point, const IntInterval& interval) {
                        num_results_batch++;
                    });
            int num_comparisons_batch = 0;
            for (const auto& q : queries) {
                num_comparisons_batch += *q.count;
                *q.count = 0;
            }

            // Test using one-by-one queries.
            int num_results_simple = 0;
            for (auto& q : queries) {
                vector<IntInterval> tmp_intervals;
                t.FindContainingPoint(q, &tmp_intervals);
                num_results_simple += tmp_intervals.size();
            }
            int num_comparisons_simple = 0;
            for (const auto& q : queries) {
                num_comparisons_simple += *q.count;
            }
            ASSERT_EQ(num_results_simple, num_results_batch);

            LOG(INFO) << num_intervals << "\t" << num_queries << "\t" << num_results_simple << "\t"
                      << num_comparisons_simple << "\t" << num_comparisons_batch;
        }
    }
}

TEST_F(TestIntervalTree, TestMultiQuery) {
    const int kNumQueries = 1;
    vector<IntInterval> intervals = CreateRandomIntervals(10);
    IntervalTree<IntTraits> t(intervals);

    // Generate random queries.
    vector<int> queries;
    for (int i = 0; i < kNumQueries; i++) {
        queries.push_back(rand_rng_int(0, 100));
    }
    std::sort(queries.begin(), queries.end());

    vector<pair<string, int>> results_simple;
    for (int q : queries) {
        vector<IntInterval> tmp_intervals;
        t.FindContainingPoint(q, &tmp_intervals);
        for (const auto& interval : tmp_intervals) {
            results_simple.emplace_back(interval.ToString(), q);
        }
    }

    vector<pair<string, int>> results_batch;
    t.ForEachIntervalContainingPoints(queries, [&](int query_point, const IntInterval& interval) {
        results_batch.emplace_back(interval.ToString(), query_point);
    });

    // Check the property that, when the batch query points are in sorted order,
    // the results are grouped by interval, and within each interval, sorted by
    // query point. Each interval may have at most two groups.
    std::optional<pair<string, int>> prev = std::nullopt;
    std::map<string, int> intervals_seen;
    for (int i = 0; i < results_batch.size(); i++) {
        const auto& cur = results_batch[i];
        // If it's another query point hitting the same interval,
        // make sure the query points are returned in order.
        if (prev && prev->first == cur.first) {
            EXPECT_GE(cur.second, prev->second) << prev->first;
        } else {
            // It's the start of a new interval's data. Make sure that we don't
            // see the same interval twice.
            EXPECT_LE(++intervals_seen[cur.first], 2)
                    << "Saw more than two groups for interval " << cur.first;
        }
        prev = cur;
    }

    std::sort(results_simple.begin(), results_simple.end());
    std::sort(results_batch.begin(), results_batch.end());
    ASSERT_EQ(results_simple, results_batch);
}

} // namespace doris
