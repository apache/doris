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

#include "runtime/primitive_type.h"
#include "vec/common/allocator_fwd.h"
#include "vec/core/field.h"

namespace doris {

TEST(PODArrayTest, PODArrayBasicMove) {
    static constexpr size_t initial_bytes = 32;
    using Array = vectorized::PODArray<
            uint64_t, initial_bytes,
            AllocatorWithStackMemory<Allocator<false, false, false, DefaultMemoryAllocator, false>,
                                     initial_bytes>>;

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
    using Array = vectorized::PODArray<
            uint64_t, initial_bytes,
            AllocatorWithStackMemory<Allocator<false, false, false, DefaultMemoryAllocator, false>,
                                     initial_bytes>>;

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
    using Array = vectorized::PODArray<
            uint64_t, initial_bytes,
            AllocatorWithStackMemory<Allocator<false, false, false, DefaultMemoryAllocator, false>,
                                     initial_bytes>>;

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

TEST(PODArrayTest, PaddedPODArrayTrackingMemory) {
    auto t = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL, "UT");
    //  PaddedPODArray
    {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(t);
        EXPECT_EQ(t->consumption(), 0);
        size_t PRE_GROWTH_SIZE = vectorized::TrackingGrowthMinSize;

        vectorized::PaddedPODArray<uint64_t, 4096> array;
        EXPECT_EQ(t->consumption(), 0);
        int64_t mem_consumption = t->consumption();
        size_t c_res_mem = 0;

        auto tracking_size = [&array, &c_res_mem](size_t c_end_new) {
            return (((c_end_new - c_res_mem) >> 16) << 16) + (array.allocated_bytes() >> 3);
        };

        auto check_memory_growth = [&t, &mem_consumption, &c_res_mem](size_t mem_growth) {
            EXPECT_EQ(t->consumption(), mem_consumption + mem_growth)
                    << ", current: " << mem_consumption << ", c_res_mem: " << c_res_mem
                    << ", mem_growth: " << mem_growth << ", expected: " << t->consumption() << ", "
                    << get_stack_trace();
            std::cout << "PODArrayTest check_memory, current: " << mem_consumption
                      << ", c_res_mem: " << c_res_mem << ", mem_growth: " << mem_growth
                      << ", expected: " << t->consumption() << std::endl;
            mem_consumption += mem_growth;
            c_res_mem += mem_growth;
        };

        array.push_back(1);
        EXPECT_EQ(array.size(), 1);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        // c_end_new: 8, c_res_mem: 0, c_end_of_storage: 4064, res_mem_growth: 4064
        check_memory_growth(array.allocated_bytes());

        array.push_back(2);
        EXPECT_EQ(array.size(), 2);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(0);

        array.resize_fill(PRE_GROWTH_SIZE / sizeof(uint64_t));
        EXPECT_EQ(array.size(), PRE_GROWTH_SIZE / sizeof(uint64_t));
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        // c_end_new: 262144, c_res_mem: 4064, c_end_of_storage: 524256, res_mem_growth: 262144
        check_memory_growth(tracking_size(PRE_GROWTH_SIZE));

        array.push_back(3);
        EXPECT_EQ(array.size(), (PRE_GROWTH_SIZE / sizeof(uint64_t)) + 1);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(0);

        array.assign({1, 2, 3});
        EXPECT_EQ(array.size(), 3);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(0);

        array.resize((PRE_GROWTH_SIZE / sizeof(uint64_t)) * 3);
        EXPECT_EQ(array.size(), (PRE_GROWTH_SIZE / sizeof(uint64_t)) * 3);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        //  c_end_new: 786432, c_res_mem: 266208, c_end_of_storage: 1048544, res_mem_growth: 589824
        check_memory_growth(tracking_size(PRE_GROWTH_SIZE * 3));

        array.push_back_without_reserve(11);
        EXPECT_EQ(array.size(), (PRE_GROWTH_SIZE / sizeof(uint64_t)) * 3 + 1);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(0);

        array.resize_fill(PRE_GROWTH_SIZE / sizeof(uint64_t) * 6, 2);
        EXPECT_EQ(array.size(), PRE_GROWTH_SIZE / sizeof(uint64_t) * 6);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        // c_end_new: 1572864, c_res_mem: 856032, c_end_of_storage: 2097120, res_mem_growth: 917504
        check_memory_growth(tracking_size(PRE_GROWTH_SIZE * 6));

        array.push_back_without_reserve(22);
        EXPECT_EQ(array.size(), (PRE_GROWTH_SIZE / sizeof(uint64_t)) * 6 + 1);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(0);

        array.emplace_back(33);
        EXPECT_EQ(array.size(), (PRE_GROWTH_SIZE / sizeof(uint64_t)) * 6 + 2);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(0);

        array.pop_back();
        EXPECT_EQ(array.size(), (PRE_GROWTH_SIZE / sizeof(uint64_t)) * 6 + 1);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(0);

        for (int i = 0; i < (PRE_GROWTH_SIZE / sizeof(uint64_t)) * 2; i++) {
            array.push_back(2);
            array.pop_back();
            array.pop_back();
        }
        EXPECT_EQ(array.size(), (PRE_GROWTH_SIZE / sizeof(uint64_t)) * 4 + 1);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(0);

        array.resize_fill(PRE_GROWTH_SIZE / sizeof(uint64_t) * 7, 3);
        EXPECT_EQ(array.size(), PRE_GROWTH_SIZE / sizeof(uint64_t) * 7);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        // c_end_new: 1835008, c_res_mem: 1773536, c_end_of_storage: 2097120, res_mem_growth: 262144,
        check_memory_growth(tracking_size(PRE_GROWTH_SIZE * 7));

        {
            vectorized::PaddedPODArray<uint64_t, 32> array2;
            array2.push_back(3);
            doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
            // c_end_new: 8, c_res_mem: 0, c_end_of_storage: 8, res_mem_growth: 8
            EXPECT_EQ(t->consumption(),
                      mem_consumption + array2.allocated_bytes()); // 40 = array2.allocated_bytes()
            array2.resize_fill(PRE_GROWTH_SIZE / sizeof(uint64_t), 3);
            doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
            // c_end_new: 262144, c_res_mem: 8, c_end_of_storage: 524256, res_mem_growth: 262144
            EXPECT_EQ(t->consumption(),
                      mem_consumption + 40 +
                              PRE_GROWTH_SIZE); // 40 + PRE_GROWTH_SIZE = array2.c_res_mem
            array.insert(array2.begin(), array2.end());
            doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
            // c_end_new: 2097152, c_res_mem: 2035680, c_end_of_storage: 4194272, res_mem_growth: 524288
            EXPECT_EQ(t->consumption(),
                      mem_consumption + tracking_size(PRE_GROWTH_SIZE * 8) + 40 + PRE_GROWTH_SIZE);
        }
        EXPECT_EQ(array.size(), PRE_GROWTH_SIZE / sizeof(uint64_t) * 8);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(tracking_size(PRE_GROWTH_SIZE * 8)); // release array2

        {
            vectorized::PaddedPODArray<uint64_t, 32> array2;
            array2.resize_fill((PRE_GROWTH_SIZE / sizeof(uint64_t)) * 7, 3);
            // c_end_new: 1835008, c_res_mem: 0, c_end_of_storage: 2097120, res_mem_growth: 2097120
            array.insert_small_allow_read_write_overflow15(array2.begin(), array2.end());
            // c_end_new: 3932160, c_res_mem: 2559968, c_end_of_storage: 4194272, res_mem_growth: 1634304
        }
        EXPECT_EQ(array.size(), PRE_GROWTH_SIZE / sizeof(uint64_t) * 15);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(
                array.allocated_bytes() -
                mem_consumption); // array.capacity > tracking_size(PRE_GROWTH_SIZE * 15)

        {
            vectorized::PaddedPODArray<uint64_t, 32> array2;
            array2.push_back(3);
            array.insert(array2.begin(), array2.end());
        }
        EXPECT_EQ(array.size(), PRE_GROWTH_SIZE / sizeof(uint64_t) * 15 + 1);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(0);

        array.resize_fill(PRE_GROWTH_SIZE / sizeof(uint64_t) * 16, 4);
        EXPECT_EQ(array.size(), PRE_GROWTH_SIZE / sizeof(uint64_t) * 16);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        // c_end_new: 4194304, c_res_mem: 4194272, c_end_of_storage: 8388576, res_mem_growth: 1048576
        check_memory_growth(tracking_size(PRE_GROWTH_SIZE * 16));

        {
            vectorized::PaddedPODArray<uint64_t, 32> array2;
            array2.resize_fill(PRE_GROWTH_SIZE / sizeof(uint64_t), 3);
            array.insert(array.begin(), array2.begin(), array2.end());
        }
        EXPECT_EQ(array.size(), PRE_GROWTH_SIZE / sizeof(uint64_t) * 17);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(0);

        {
            vectorized::PaddedPODArray<uint64_t, 32> array2;
            array2.resize_fill((PRE_GROWTH_SIZE / sizeof(uint64_t) * 2), 3);
            array.insert_assume_reserved(array2.begin(), array2.end());
            array.insert_assume_reserved_and_allow_overflow(array2.begin(), array2.end());
        }
        EXPECT_EQ(array.size(), PRE_GROWTH_SIZE / sizeof(uint64_t) * 21);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(tracking_size(PRE_GROWTH_SIZE * 21));

        size_t n = 100;
        array.assign(n, (uint64_t)0);
        EXPECT_EQ(array.size(), 100);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(0);

        array.erase(array.begin() + 10, array.end());
        EXPECT_EQ(array.size(), 10);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        check_memory_growth(0);
    }
    EXPECT_EQ(t->consumption(), 0);
}

TEST(PODArrayTest, PODArrayTrackingMemory) {
    auto t = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL, "UT");

    // PODArray with AllocatorWithStackMemory
    {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(t);
        EXPECT_EQ(t->consumption(), 0);
        size_t PRE_GROWTH_SIZE = vectorized::TrackingGrowthMinSize;

        static constexpr size_t initial_bytes = 32;
        using Array =
                vectorized::PODArray<uint64_t, initial_bytes,
                                     AllocatorWithStackMemory<Allocator<
                                             false, false, false, DefaultMemoryAllocator, false>>>;
        Array array;

        array.push_back(1);
        EXPECT_EQ(array.size(), 1);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        // reset_resident_memory, c_end_new: 8, c_res_mem: 0, c_end_of_storage: 32, res_mem_growth: 32
        EXPECT_EQ(t->consumption(), array.allocated_bytes());

        array.resize_fill(PRE_GROWTH_SIZE / sizeof(uint64_t), 1);
        EXPECT_EQ(array.size(), PRE_GROWTH_SIZE / sizeof(uint64_t));
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        // reset_resident_memory, c_end_new: 262144, c_res_mem: 32, c_end_of_storage: 262144, res_mem_growth: 262112
        EXPECT_EQ(t->consumption(), PRE_GROWTH_SIZE);

        // test swap
        size_t new_capacity = 0;
        size_t new_size = 0;
        {
            Array array2;
            array2.resize_fill(PRE_GROWTH_SIZE / sizeof(uint64_t) * 2, 1);
            EXPECT_EQ(array2.size(), PRE_GROWTH_SIZE / sizeof(uint64_t) * 2);
            doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
            // reset_resident_memory, c_end_new: 524288, c_res_mem: 0, c_end_of_storage: 524288, res_mem_growth: 524288
            EXPECT_EQ(t->consumption(), PRE_GROWTH_SIZE * 3);
            new_capacity = array2.capacity();
            new_size = array2.size();

            array.swap(array2);
            EXPECT_EQ(array2.size(), PRE_GROWTH_SIZE / sizeof(uint64_t));
            EXPECT_EQ(array.size(), PRE_GROWTH_SIZE / sizeof(uint64_t) * 2);
            doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
            EXPECT_EQ(t->consumption(), PRE_GROWTH_SIZE * 3);
            EXPECT_EQ(array.capacity(), new_capacity);
            EXPECT_EQ(array.size(), new_size);
        }
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_EQ(t->consumption(), PRE_GROWTH_SIZE * 2);
    }
    EXPECT_EQ(t->consumption(), 0);

    // PODArray with Allocator
    {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(t);
        EXPECT_EQ(t->consumption(), 0);
        static constexpr size_t PRE_GROWTH_SIZE = (1ULL << 20); // 1M

        static constexpr size_t initial_bytes = 32;
        using Array =
                vectorized::PODArray<uint64_t, initial_bytes,
                                     Allocator<false, false, false, DefaultMemoryAllocator, false>>;
        Array array;

        array.push_back(1);
        EXPECT_EQ(array.size(), 1);
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_EQ(t->consumption(), array.allocated_bytes());

        array.resize_fill(PRE_GROWTH_SIZE / sizeof(uint64_t), 1);
        EXPECT_EQ(array.size(), PRE_GROWTH_SIZE / sizeof(uint64_t));
        doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_EQ(t->consumption(), PRE_GROWTH_SIZE);

        // test swap
        size_t new_capacity = 0;
        size_t new_size = 0;
        {
            Array array2;
            array2.resize_fill(PRE_GROWTH_SIZE / sizeof(uint64_t) * 2, 1);
            EXPECT_EQ(array2.size(), PRE_GROWTH_SIZE / sizeof(uint64_t) * 2);
            doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
            EXPECT_EQ(t->consumption(), PRE_GROWTH_SIZE * 3);
            new_capacity = array2.capacity();
            new_size = array2.size();

            array.swap(array2);
            EXPECT_EQ(array2.size(), PRE_GROWTH_SIZE / sizeof(uint64_t));
            EXPECT_EQ(array.size(), PRE_GROWTH_SIZE / sizeof(uint64_t) * 2);
            doris::thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
            EXPECT_EQ(t->consumption(), PRE_GROWTH_SIZE * 3);
            EXPECT_EQ(array.capacity(), new_capacity);
            EXPECT_EQ(array.size(), new_size);
        }
        EXPECT_EQ(t->consumption(), PRE_GROWTH_SIZE * 2);
    }
    EXPECT_EQ(t->consumption(), 0);
}

} // end namespace doris
