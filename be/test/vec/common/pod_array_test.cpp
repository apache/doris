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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/tests/gtest_pod_array.cpp
// and modified by Doris

#include "vec/common/pod_array.h"

#include <gtest/gtest.h>

#include "vec/common/allocator_fwd.h"

namespace doris {

TEST(PODArrayTest, PODArrayBasicMove) {
    static constexpr size_t initial_bytes = 32;
    using Array = vectorized::PODArray<uint64_t, initial_bytes,
                                       AllocatorWithStackMemory<Allocator<false>, initial_bytes>>;

    {
        Array arr;
        Array arr2;
        arr2 = std::move(arr);
    }

    {
        Array arr;

        arr.push_back(1);
        arr.push_back(2);
        arr.push_back(3);

        Array arr2;

        arr2 = std::move(arr);

        ASSERT_EQ(arr2.size(), 3);
        ASSERT_EQ(arr2[0], 1);
        ASSERT_EQ(arr2[1], 2);
        ASSERT_EQ(arr2[2], 3);

        arr = std::move(arr2);

        ASSERT_EQ(arr.size(), 3);
        ASSERT_EQ(arr[0], 1);
        ASSERT_EQ(arr[1], 2);
        ASSERT_EQ(arr[2], 3);
    }

    {
        Array arr;

        arr.push_back(1);
        arr.push_back(2);
        arr.push_back(3);
        arr.push_back(4);
        arr.push_back(5);

        Array arr2;

        arr2 = std::move(arr);

        ASSERT_EQ(arr2.size(), 5);
        ASSERT_EQ(arr2[0], 1);
        ASSERT_EQ(arr2[1], 2);
        ASSERT_EQ(arr2[2], 3);
        ASSERT_EQ(arr2[3], 4);
        ASSERT_EQ(arr2[4], 5);

        arr = std::move(arr2);

        ASSERT_EQ(arr.size(), 5);
        ASSERT_EQ(arr[0], 1);
        ASSERT_EQ(arr[1], 2);
        ASSERT_EQ(arr[2], 3);
        ASSERT_EQ(arr[3], 4);
        ASSERT_EQ(arr[4], 5);
    }

    {
        Array arr;

        arr.push_back(1);
        arr.push_back(2);
        arr.push_back(3);

        Array arr2;

        arr2.push_back(4);
        arr2.push_back(5);
        arr2.push_back(6);
        arr2.push_back(7);

        arr2 = std::move(arr);

        ASSERT_EQ(arr2.size(), 3);
        ASSERT_EQ(arr2[0], 1);
        ASSERT_EQ(arr2[1], 2);
        ASSERT_EQ(arr2[2], 3);
    }

    {
        Array arr;

        arr.push_back(1);
        arr.push_back(2);
        arr.push_back(3);

        Array arr2;

        arr2.push_back(4);
        arr2.push_back(5);
        arr2.push_back(6);
        arr2.push_back(7);
        arr2.push_back(8);

        arr = std::move(arr2);

        ASSERT_EQ(arr.size(), 5);
        ASSERT_EQ(arr[0], 4);
        ASSERT_EQ(arr[1], 5);
        ASSERT_EQ(arr[2], 6);
        ASSERT_EQ(arr[3], 7);
        ASSERT_EQ(arr[4], 8);
    }
}

TEST(PODArrayTest, PODArrayBasicSwap) {
    static constexpr size_t initial_bytes = 32;
    using Array = vectorized::PODArray<uint64_t, initial_bytes,
                                       AllocatorWithStackMemory<Allocator<false>, initial_bytes>>;

    {
        Array arr;
        Array arr2;
        arr.swap(arr2);
        arr2.swap(arr);
    }

    {
        Array arr;

        Array arr2;

        arr2.push_back(1);
        arr2.push_back(2);
        arr2.push_back(3);

        arr.swap(arr2);

        ASSERT_TRUE(arr.size() == 3);
        ASSERT_TRUE(arr[0] == 1);
        ASSERT_TRUE(arr[1] == 2);
        ASSERT_TRUE(arr[2] == 3);

        ASSERT_TRUE(arr2.empty());

        arr.swap(arr2);

        ASSERT_TRUE(arr.empty());

        ASSERT_TRUE(arr2.size() == 3);
        ASSERT_TRUE(arr2[0] == 1);
        ASSERT_TRUE(arr2[1] == 2);
        ASSERT_TRUE(arr2[2] == 3);
    }

    {
        Array arr;

        Array arr2;

        arr2.push_back(1);
        arr2.push_back(2);
        arr2.push_back(3);
        arr2.push_back(4);
        arr2.push_back(5);

        arr.swap(arr2);

        ASSERT_TRUE(arr.size() == 5);
        ASSERT_TRUE(arr[0] == 1);
        ASSERT_TRUE(arr[1] == 2);
        ASSERT_TRUE(arr[2] == 3);
        ASSERT_TRUE(arr[3] == 4);
        ASSERT_TRUE(arr[4] == 5);

        ASSERT_TRUE(arr2.empty());

        arr.swap(arr2);

        ASSERT_TRUE(arr.empty());

        ASSERT_TRUE(arr2.size() == 5);
        ASSERT_TRUE(arr2[0] == 1);
        ASSERT_TRUE(arr2[1] == 2);
        ASSERT_TRUE(arr2[2] == 3);
        ASSERT_TRUE(arr2[3] == 4);
        ASSERT_TRUE(arr2[4] == 5);
    }

    {
        Array arr;

        arr.push_back(1);
        arr.push_back(2);
        arr.push_back(3);

        Array arr2;

        arr2.push_back(4);
        arr2.push_back(5);
        arr2.push_back(6);

        arr.swap(arr2);

        ASSERT_TRUE(arr.size() == 3);
        ASSERT_TRUE(arr[0] == 4);
        ASSERT_TRUE(arr[1] == 5);
        ASSERT_TRUE(arr[2] == 6);

        ASSERT_TRUE(arr2.size() == 3);
        ASSERT_TRUE(arr2[0] == 1);
        ASSERT_TRUE(arr2[1] == 2);
        ASSERT_TRUE(arr2[2] == 3);

        arr.swap(arr2);

        ASSERT_TRUE(arr.size() == 3);
        ASSERT_TRUE(arr[0] == 1);
        ASSERT_TRUE(arr[1] == 2);
        ASSERT_TRUE(arr[2] == 3);

        ASSERT_TRUE(arr2.size() == 3);
        ASSERT_TRUE(arr2[0] == 4);
        ASSERT_TRUE(arr2[1] == 5);
        ASSERT_TRUE(arr2[2] == 6);
    }

    {
        Array arr;

        arr.push_back(1);
        arr.push_back(2);

        Array arr2;

        arr2.push_back(3);
        arr2.push_back(4);
        arr2.push_back(5);

        arr.swap(arr2);

        ASSERT_TRUE(arr.size() == 3);
        ASSERT_TRUE(arr[0] == 3);
        ASSERT_TRUE(arr[1] == 4);
        ASSERT_TRUE(arr[2] == 5);

        ASSERT_TRUE(arr2.size() == 2);
        ASSERT_TRUE(arr2[0] == 1);
        ASSERT_TRUE(arr2[1] == 2);

        arr.swap(arr2);

        ASSERT_TRUE(arr.size() == 2);
        ASSERT_TRUE(arr[0] == 1);
        ASSERT_TRUE(arr[1] == 2);

        ASSERT_TRUE(arr2.size() == 3);
        ASSERT_TRUE(arr2[0] == 3);
        ASSERT_TRUE(arr2[1] == 4);
        ASSERT_TRUE(arr2[2] == 5);
    }

    {
        Array arr;

        arr.push_back(1);
        arr.push_back(2);
        arr.push_back(3);

        Array arr2;

        arr2.push_back(4);
        arr2.push_back(5);
        arr2.push_back(6);
        arr2.push_back(7);
        arr2.push_back(8);

        arr.swap(arr2);

        ASSERT_TRUE(arr.size() == 5);
        ASSERT_TRUE(arr[0] == 4);
        ASSERT_TRUE(arr[1] == 5);
        ASSERT_TRUE(arr[2] == 6);
        ASSERT_TRUE(arr[3] == 7);
        ASSERT_TRUE(arr[4] == 8);

        ASSERT_TRUE(arr2.size() == 3);
        ASSERT_TRUE(arr2[0] == 1);
        ASSERT_TRUE(arr2[1] == 2);
        ASSERT_TRUE(arr2[2] == 3);

        arr.swap(arr2);

        ASSERT_TRUE(arr.size() == 3);
        ASSERT_TRUE(arr[0] == 1);
        ASSERT_TRUE(arr[1] == 2);
        ASSERT_TRUE(arr[2] == 3);

        ASSERT_TRUE(arr2.size() == 5);
        ASSERT_TRUE(arr2[0] == 4);
        ASSERT_TRUE(arr2[1] == 5);
        ASSERT_TRUE(arr2[2] == 6);
        ASSERT_TRUE(arr2[3] == 7);
        ASSERT_TRUE(arr2[4] == 8);
    }

    {
        Array arr;

        arr.push_back(1);
        arr.push_back(2);
        arr.push_back(3);
        arr.push_back(4);
        arr.push_back(5);

        Array arr2;

        arr2.push_back(6);
        arr2.push_back(7);
        arr2.push_back(8);
        arr2.push_back(9);
        arr2.push_back(10);

        arr.swap(arr2);

        ASSERT_TRUE(arr.size() == 5);
        ASSERT_TRUE(arr[0] == 6);
        ASSERT_TRUE(arr[1] == 7);
        ASSERT_TRUE(arr[2] == 8);
        ASSERT_TRUE(arr[3] == 9);
        ASSERT_TRUE(arr[4] == 10);

        ASSERT_TRUE(arr2.size() == 5);
        ASSERT_TRUE(arr2[0] == 1);
        ASSERT_TRUE(arr2[1] == 2);
        ASSERT_TRUE(arr2[2] == 3);
        ASSERT_TRUE(arr2[3] == 4);
        ASSERT_TRUE(arr2[4] == 5);

        arr.swap(arr2);

        ASSERT_TRUE(arr.size() == 5);
        ASSERT_TRUE(arr[0] == 1);
        ASSERT_TRUE(arr[1] == 2);
        ASSERT_TRUE(arr[2] == 3);
        ASSERT_TRUE(arr[3] == 4);
        ASSERT_TRUE(arr[4] == 5);

        ASSERT_TRUE(arr2.size() == 5);
        ASSERT_TRUE(arr2[0] == 6);
        ASSERT_TRUE(arr2[1] == 7);
        ASSERT_TRUE(arr2[2] == 8);
        ASSERT_TRUE(arr2[3] == 9);
        ASSERT_TRUE(arr2[4] == 10);
    }
}

TEST(PODArrayTest, PODArrayBasicSwapMoveConstructor) {
    static constexpr size_t initial_bytes = 32;
    using Array = vectorized::PODArray<uint64_t, initial_bytes,
                                       AllocatorWithStackMemory<Allocator<false>, initial_bytes>>;

    {
        Array arr;
        Array arr2 {std::move(arr)};
    }

    {
        Array arr;

        arr.push_back(1);
        arr.push_back(2);
        arr.push_back(3);

        Array arr2 {std::move(arr)};

        ASSERT_TRUE(arr.empty()); // NOLINT

        ASSERT_TRUE(arr2.size() == 3);
        ASSERT_TRUE(arr2[0] == 1);
        ASSERT_TRUE(arr2[1] == 2);
        ASSERT_TRUE(arr2[2] == 3);
    }

    {
        Array arr;

        arr.push_back(1);
        arr.push_back(2);
        arr.push_back(3);
        arr.push_back(4);
        arr.push_back(5);

        Array arr2 {std::move(arr)};

        ASSERT_TRUE(arr.empty()); // NOLINT

        ASSERT_TRUE(arr2.size() == 5);
        ASSERT_TRUE(arr2[0] == 1);
        ASSERT_TRUE(arr2[1] == 2);
        ASSERT_TRUE(arr2[2] == 3);
        ASSERT_TRUE(arr2[3] == 4);
        ASSERT_TRUE(arr2[4] == 5);
    }
}

TEST(PODArrayTest, PODArrayInsert) {
    {
        std::string str = "test_string_abacaba";
        vectorized::PODArray<char> chars;
        chars.insert(chars.end(), str.begin(), str.end());
        EXPECT_EQ(str, std::string(chars.data(), chars.size()));

        std::string insert_in_the_middle = "insert_in_the_middle";
        auto pos = str.size() / 2;
        str.insert(str.begin() + pos, insert_in_the_middle.begin(), insert_in_the_middle.end());
        chars.insert(chars.begin() + pos, insert_in_the_middle.begin(), insert_in_the_middle.end());
        EXPECT_EQ(str, std::string(chars.data(), chars.size()));

        std::string insert_with_resize;
        insert_with_resize.reserve(chars.capacity() * 2);
        char cur_char = 'a';
        while (insert_with_resize.size() < insert_with_resize.capacity()) {
            insert_with_resize += cur_char;
            if (cur_char == 'z') {
                cur_char = 'a';
            } else {
                ++cur_char;
            }
        }
        str.insert(str.begin(), insert_with_resize.begin(), insert_with_resize.end());
        chars.insert(chars.begin(), insert_with_resize.begin(), insert_with_resize.end());
        EXPECT_EQ(str, std::string(chars.data(), chars.size()));
    }
    {
        vectorized::PODArray<uint64_t> values;
        vectorized::PODArray<uint64_t> values_to_insert;

        for (size_t i = 0; i < 120; ++i) {
            values.emplace_back(i);
        }

        values.insert(values.begin() + 1, values_to_insert.begin(), values_to_insert.end());
        ASSERT_EQ(values.size(), 120);

        values_to_insert.emplace_back(0);
        values_to_insert.emplace_back(1);

        values.insert(values.begin() + 1, values_to_insert.begin(), values_to_insert.end());
        ASSERT_EQ(values.size(), 122);

        values_to_insert.clear();
        for (size_t i = 0; i < 240; ++i) {
            values_to_insert.emplace_back(i);
        }

        values.insert(values.begin() + 1, values_to_insert.begin(), values_to_insert.end());
        ASSERT_EQ(values.size(), 362);
    }
}

// TEST(PODArrayTest, PODArrayInsertFromItself)
// {
//     {
//         vectorized::PaddedPODArray<UInt64> array { 1 };

//         for (size_t i = 0; i < 3; ++i)
//             array.insertFromItself(array.begin(), array.end());

//         vectorized::PaddedPODArray<UInt64> expected {1,1,1,1,1,1,1,1};
//         ASSERT_EQ(array,expected);
//     }
// }

TEST(PODArrayTest, PODArrayAssign) {
    {
        vectorized::PaddedPODArray<uint64_t> array;
        array.push_back(1);
        array.push_back(2);

        array.assign({1, 2, 3});

        ASSERT_EQ(array.size(), 3);
        ASSERT_EQ(array, vectorized::PaddedPODArray<uint64_t>({1, 2, 3}));
    }
    {
        vectorized::PaddedPODArray<uint64_t> array;
        array.push_back(1);
        array.push_back(2);

        array.assign({});

        ASSERT_TRUE(array.empty());
    }
    {
        vectorized::PaddedPODArray<uint64_t> array;
        array.assign({});

        ASSERT_TRUE(array.empty());
    }
}

TEST(PODArrayTest, PODNoOverallocation) {
    /// Check that PaddedPODArray allocates for smaller number of elements than the power of two due to padding.
    /// NOTE: It's Ok to change these numbers if you will modify initial size or padding.

    vectorized::PaddedPODArray<char> chars;
    std::vector<size_t> capacities;

    size_t prev_capacity = 0;
    for (size_t i = 0; i < 1000000; ++i) {
        chars.emplace_back();
        if (chars.capacity() != prev_capacity) {
            prev_capacity = chars.capacity();
            capacities.emplace_back(prev_capacity);
        }
    }

    EXPECT_EQ(capacities, (std::vector<size_t> {4064, 8160, 16352, 32736, 65504, 131040, 262112,
                                                524256, 1048544}));
}

template <size_t size>
struct ItemWithSize {
    char v[size] {};
};

TEST(PODArrayTest, PODInsertElementSizeNotMultipleOfLeftPadding) {
    using ItemWith24Size = ItemWithSize<24>;
    vectorized::PaddedPODArray<ItemWith24Size> arr1_initially_empty;

    size_t items_to_insert_size = 120000;

    for (size_t test = 0; test < items_to_insert_size; ++test) {
        arr1_initially_empty.emplace_back();
    }

    EXPECT_EQ(arr1_initially_empty.size(), items_to_insert_size);

    vectorized::PaddedPODArray<ItemWith24Size> arr2_initially_nonempty;

    for (size_t test = 0; test < items_to_insert_size; ++test) {
        arr2_initially_nonempty.emplace_back();
    }

    EXPECT_EQ(arr1_initially_empty.size(), items_to_insert_size);
}

TEST(PODArrayTest, PODErase) {
    {
        vectorized::PaddedPODArray<uint64_t> items {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        vectorized::PaddedPODArray<uint64_t> expected;
        expected = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        items.erase(items.begin(), items.begin());
        EXPECT_EQ(items, expected);

        items.erase(items.end(), items.end());
        EXPECT_EQ(items, expected);
    }
    {
        vectorized::PaddedPODArray<uint64_t> actual {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        vectorized::PaddedPODArray<uint64_t> expected;

        expected = {0, 1, 4, 5, 6, 7, 8, 9};
        actual.erase(actual.begin() + 2, actual.begin() + 4);
        EXPECT_EQ(actual, expected);

        expected = {0, 1, 4};
        actual.erase(actual.begin() + 3, actual.end());
        EXPECT_EQ(actual, expected);

        expected = {};
        actual.erase(actual.begin(), actual.end());
        EXPECT_EQ(actual, expected);

        for (size_t i = 0; i < 10; ++i) {
            actual.emplace_back(static_cast<uint64_t>(i));
        }

        expected = {0, 1, 4, 5, 6, 7, 8, 9};
        actual.erase(actual.begin() + 2, actual.begin() + 4);
        EXPECT_EQ(actual, expected);

        expected = {0, 1, 4};
        actual.erase(actual.begin() + 3, actual.end());
        EXPECT_EQ(actual, expected);

        expected = {};
        actual.erase(actual.begin(), actual.end());
        EXPECT_EQ(actual, expected);
    }
    {
        vectorized::PaddedPODArray<uint64_t> actual {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        vectorized::PaddedPODArray<uint64_t> expected;

        expected = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        actual.erase(actual.begin());
        EXPECT_EQ(actual, expected);
    }
}

} // end namespace doris
