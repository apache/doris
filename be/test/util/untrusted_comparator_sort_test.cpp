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

#include "util/untrusted_comparator_sort.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <random>
#include <stdexcept>
#include <vector>

#include "core/custom_allocator.h"

namespace doris {

namespace {

// Upper bound on comparator calls promised by sort_with_untrusted_comparator.
size_t max_comparator_calls(size_t n) {
    return n < 2 ? 0 : n * static_cast<size_t>(std::ceil(std::log2(static_cast<double>(n))));
}

std::vector<size_t> identity_permutation(size_t n) {
    std::vector<size_t> data(n);
    for (size_t i = 0; i < n; ++i) {
        data[i] = i;
    }
    return data;
}

bool is_permutation_of_identity(const std::vector<size_t>& data) {
    std::vector<bool> seen(data.size(), false);
    for (size_t v : data) {
        if (v >= data.size() || seen[v]) {
            return false;
        }
        seen[v] = true;
    }
    return true;
}

// Runs the sort on 0..n-1 (shuffled by `shuffle`) with an arbitrary comparator and checks the
// guarantees that hold for any comparator: bounded number of calls, every argument in range,
// and the output being a permutation of the input.
template <typename Less>
std::vector<size_t> sort_and_check_invariants(size_t n, bool shuffle, Less less) {
    auto data = identity_permutation(n);
    if (shuffle) {
        std::mt19937 rng(n);
        std::shuffle(data.begin(), data.end(), rng);
    }

    size_t calls = 0;
    bool argument_out_of_range = false;
    auto checked_less = [&](size_t a, size_t b) {
        ++calls;
        argument_out_of_range |= a >= n || b >= n;
        return less(a, b);
    };

    DorisVector<size_t> scratch;
    sort_with_untrusted_comparator(data.data(), data.data() + data.size(), scratch, checked_less);

    EXPECT_FALSE(argument_out_of_range) << "n=" << n;
    EXPECT_LE(calls, max_comparator_calls(n)) << "n=" << n;
    EXPECT_TRUE(is_permutation_of_identity(data)) << "n=" << n;
    return data;
}

const std::vector<size_t> kSizes = {0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 33, 100, 1000, 1025};

} // namespace

TEST(UntrustedComparatorSortTest, ConsistentComparatorSorts) {
    for (size_t n : kSizes) {
        auto data = sort_and_check_invariants(n, true, [](size_t a, size_t b) { return a < b; });
        EXPECT_EQ(data, identity_permutation(n)) << "n=" << n;

        data = sort_and_check_invariants(n, true, [](size_t a, size_t b) { return a > b; });
        auto expected = identity_permutation(n);
        std::reverse(expected.begin(), expected.end());
        EXPECT_EQ(data, expected) << "n=" << n;
    }
}

TEST(UntrustedComparatorSortTest, ConsistentComparatorIsStable) {
    // Sort by key only; equal keys must keep the order in which they appear in the input.
    for (size_t n : kSizes) {
        auto key = [](size_t v) { return v % 7; };
        auto data = sort_and_check_invariants(n, true,
                                              [&](size_t a, size_t b) { return key(a) < key(b); });

        std::vector<size_t> input = identity_permutation(n);
        std::mt19937 rng(n);
        std::shuffle(input.begin(), input.end(), rng);
        std::stable_sort(input.begin(), input.end(),
                         [&](size_t a, size_t b) { return key(a) < key(b); });
        EXPECT_EQ(data, input) << "n=" << n;
    }
}

TEST(UntrustedComparatorSortTest, SortedInputCostsLinearComparisons) {
    for (size_t n : kSizes) {
        size_t calls = 0;
        auto data = identity_permutation(n);
        DorisVector<size_t> scratch;
        sort_with_untrusted_comparator(data.data(), data.data() + n, scratch,
                                       [&](size_t a, size_t b) {
                                           ++calls;
                                           return a < b;
                                       });
        EXPECT_EQ(data, identity_permutation(n)) << "n=" << n;
        EXPECT_LE(calls, n) << "n=" << n;
    }
}

TEST(UntrustedComparatorSortTest, AlwaysLessComparator) {
    for (size_t n : kSizes) {
        sort_and_check_invariants(n, true, [](size_t, size_t) { return true; });
    }
}

TEST(UntrustedComparatorSortTest, NeverLessComparatorKeepsInputOrder) {
    for (size_t n : kSizes) {
        auto data = sort_and_check_invariants(n, false, [](size_t, size_t) { return false; });
        EXPECT_EQ(data, identity_permutation(n)) << "n=" << n;
    }
}

TEST(UntrustedComparatorSortTest, PartiallyReflexiveComparator) {
    // Every pair of values >= 50 compares as "less" in both directions, which is the shape of
    // the SQL comparator `(x, y) -> IF(x > 100 AND y > 100, -1, ...)`.
    for (size_t n : kSizes) {
        sort_and_check_invariants(n, true, [](size_t a, size_t b) {
            if (a >= 50 && b >= 50) {
                return true;
            }
            return a < b;
        });
    }
}

TEST(UntrustedComparatorSortTest, RandomComparator) {
    std::mt19937 rng(42);
    for (size_t n : kSizes) {
        sort_and_check_invariants(n, true, [&](size_t, size_t) { return (rng() & 1) == 1; });
    }
}

TEST(UntrustedComparatorSortTest, ScratchIsReusedAcrossCalls) {
    DorisVector<size_t> scratch;
    for (size_t n : std::vector<size_t> {1000, 3, 0, 17, 1025}) {
        auto data = identity_permutation(n);
        std::mt19937 rng(n);
        std::shuffle(data.begin(), data.end(), rng);
        sort_with_untrusted_comparator(data.data(), data.data() + n, scratch,
                                       [](size_t a, size_t b) { return a < b; });
        EXPECT_EQ(data, identity_permutation(n)) << "n=" << n;
        EXPECT_GE(scratch.capacity(), n);
    }
}

TEST(UntrustedComparatorSortTest, ComparatorExceptionPropagates) {
    auto data = identity_permutation(100);
    std::reverse(data.begin(), data.end());
    DorisVector<size_t> scratch;
    size_t calls = 0;
    EXPECT_THROW(sort_with_untrusted_comparator(data.data(), data.data() + data.size(), scratch,
                                                [&](size_t a, size_t b) {
                                                    if (++calls == 50) {
                                                        throw std::runtime_error("lambda failed");
                                                    }
                                                    return a < b;
                                                }),
                 std::runtime_error);
    EXPECT_EQ(calls, size_t {50});
}

} // namespace doris
