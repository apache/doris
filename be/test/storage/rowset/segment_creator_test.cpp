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

#include "storage/rowset/segment_creator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

#include "storage/rowset/beta_rowset_writer.h"

namespace doris {

class SegmentCreatorTest : public testing::Test {
protected:
    SegmentCreatorTest() : _segment_creator(_context, _segment_files, _index_files) {}

    RowsetWriterContext _context;
    SegmentFileCollection _segment_files;
    InvertedIndexFileCollection _index_files;
    SegmentCreator _segment_creator;
};

TEST_F(SegmentCreatorTest, AllocateWithoutLimit) {
    _segment_creator.set_segment_start_id(10);

    auto first_segment_id = _segment_creator.allocate_segment_id();
    ASSERT_TRUE(first_segment_id.has_value()) << first_segment_id.error();
    EXPECT_EQ(first_segment_id.value(), 10);

    auto second_segment_id = _segment_creator.allocate_segment_id();
    ASSERT_TRUE(second_segment_id.has_value()) << second_segment_id.error();
    EXPECT_EQ(second_segment_id.value(), 11);
}

TEST_F(SegmentCreatorTest, RejectAllocationOverLimit) {
    _segment_creator.set_segment_start_id(10, 2);

    auto first_segment_id = _segment_creator.allocate_segment_id();
    ASSERT_TRUE(first_segment_id.has_value()) << first_segment_id.error();
    EXPECT_EQ(first_segment_id.value(), 10);

    auto second_segment_id = _segment_creator.allocate_segment_id();
    ASSERT_TRUE(second_segment_id.has_value()) << second_segment_id.error();
    EXPECT_EQ(second_segment_id.value(), 11);

    auto exceeded_segment_id = _segment_creator.allocate_segment_id();
    ASSERT_FALSE(exceeded_segment_id.has_value());
    EXPECT_TRUE(exceeded_segment_id.error().is<ErrorCode::TOO_MANY_SEGMENTS>());
    EXPECT_EQ(_segment_creator.get_allocated_segment_id(), 12);
}

TEST_F(SegmentCreatorTest, RejectAllocationWithZeroLimit) {
    _segment_creator.set_segment_start_id(10, 0);

    auto segment_id = _segment_creator.allocate_segment_id();
    ASSERT_FALSE(segment_id.has_value());
    EXPECT_TRUE(segment_id.error().is<ErrorCode::TOO_MANY_SEGMENTS>());
    EXPECT_EQ(_segment_creator.get_allocated_segment_id(), 10);
}

TEST_F(SegmentCreatorTest, ConcurrentAllocationRespectsLimit) {
    constexpr int32_t kStartSegmentId = 100;
    constexpr int32_t kMaxSegmentNum = 64;
    constexpr int32_t kThreadNum = 8;
    constexpr int32_t kAllocationNumPerThread = 16;
    _segment_creator.set_segment_start_id(kStartSegmentId, kMaxSegmentNum);

    std::mutex allocated_ids_lock;
    std::vector<int32_t> allocated_ids;
    std::atomic<int32_t> exceeded_count = 0;
    std::atomic<int32_t> unexpected_error_count = 0;
    std::vector<std::thread> threads;
    threads.reserve(kThreadNum);
    for (int32_t thread_idx = 0; thread_idx < kThreadNum; ++thread_idx) {
        threads.emplace_back([&] {
            for (int32_t allocation_idx = 0; allocation_idx < kAllocationNumPerThread;
                 ++allocation_idx) {
                auto segment_id = _segment_creator.allocate_segment_id();
                if (segment_id.has_value()) {
                    std::lock_guard lock(allocated_ids_lock);
                    allocated_ids.push_back(segment_id.value());
                } else if (segment_id.error().is<ErrorCode::TOO_MANY_SEGMENTS>()) {
                    exceeded_count.fetch_add(1, std::memory_order_relaxed);
                } else {
                    unexpected_error_count.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(allocated_ids.size(), static_cast<size_t>(kMaxSegmentNum));
    std::sort(allocated_ids.begin(), allocated_ids.end());
    for (size_t index = 0; index < allocated_ids.size(); ++index) {
        EXPECT_EQ(allocated_ids[index], kStartSegmentId + static_cast<int32_t>(index));
    }
    EXPECT_EQ(exceeded_count.load(std::memory_order_relaxed),
              kThreadNum * kAllocationNumPerThread - kMaxSegmentNum);
    EXPECT_EQ(unexpected_error_count.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(_segment_creator.get_allocated_segment_id(), kStartSegmentId + kMaxSegmentNum);
}

} // namespace doris
