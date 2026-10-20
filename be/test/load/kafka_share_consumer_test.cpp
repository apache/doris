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

#include "load/routine_load/kafka_share_consumer.h"

#include <gtest/gtest.h>

#include <thread>
#include <tuple>
#include <vector>

#include "runtime/thread_context.h"

namespace doris {

class RecordingShareConsumer : public KafkaShareConsumer {
public:
    std::vector<std::tuple<int32_t, int64_t, bool>> acknowledgements;
    bool fail_record = false;
    bool fail_commit = false;
    int commits = 0;

protected:
    Status acknowledge_record(int32_t partition, int64_t offset, bool accept) override {
        if (fail_record) {
            return Status::InternalError("expired acquisition");
        }
        acknowledgements.emplace_back(partition, offset, accept);
        return Status::OK();
    }
    Status commit_acknowledgements() override {
        ++commits;
        return fail_commit ? Status::InternalError("lost acknowledgement response") : Status::OK();
    }
};

TEST(KafkaShareConsumerTest, CommitAcceptsOnlyWrittenRecords) {
    RecordingShareConsumer consumer;
    consumer.reserve_records(3);
    consumer._records = {{0, 101}, {0, 100}, {1, 50}};
    consumer._processed = 2;
    EXPECT_FALSE(consumer.reset().ok());
    ASSERT_TRUE(consumer.acknowledge_committed().ok());
    EXPECT_EQ(consumer.acknowledgements, (std::vector<std::tuple<int32_t, int64_t, bool>> {
                                                 {0, 101, true}, {0, 100, true}, {1, 50, false}}));
    EXPECT_EQ(consumer.commits, 1);
    EXPECT_TRUE(consumer.reset().ok());
}

TEST(KafkaShareConsumerTest, AbortReleasesEveryRecord) {
    RecordingShareConsumer consumer;
    consumer.reserve_records(2);
    consumer._records = {{0, 100}, {0, 101}};
    consumer._processed = 1;
    ASSERT_TRUE(consumer.cancel(nullptr).ok());
    EXPECT_EQ(consumer.acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 100, false}, {0, 101, false}}));
    EXPECT_TRUE(consumer.reset().ok());
}

TEST(KafkaShareConsumerTest, Completing101DoesNotAccept100) {
    RecordingShareConsumer consumer;
    consumer.reserve_records(2);
    consumer._records = {{0, 101}, {0, 100}};
    consumer._processed = 1;
    ASSERT_TRUE(consumer.acknowledge_committed().ok());
    EXPECT_EQ(consumer.acknowledgements,
              (std::vector<std::tuple<int32_t, int64_t, bool>> {{0, 101, true}, {0, 100, false}}));
}

TEST(KafkaShareConsumerTest, FailedAcknowledgementCannotBeReused) {
    RecordingShareConsumer consumer;
    consumer.reserve_records(1);
    consumer._records = {{0, 100}};
    consumer._processed = 1;
    consumer.fail_commit = true;
    EXPECT_FALSE(consumer.acknowledge_committed().ok());
    EXPECT_FALSE(consumer.reset().ok());
    EXPECT_TRUE(consumer._records.empty());
}

TEST(KafkaShareConsumerTest, ExpiredAcquisitionCannotBeReused) {
    RecordingShareConsumer consumer;
    consumer.reserve_records(1);
    consumer._records = {{0, 100}};
    consumer._processed = 1;
    consumer.fail_record = true;
    EXPECT_FALSE(consumer.acknowledge_committed().ok());
    EXPECT_EQ(consumer.commits, 0);
    EXPECT_FALSE(consumer.reset().ok());
}

TEST(KafkaShareConsumerTest, EmptyBatchDoesNotCommitAcknowledgements) {
    RecordingShareConsumer consumer;
    EXPECT_TRUE(consumer.acknowledge_committed().ok());
    EXPECT_EQ(consumer.commits, 0);
    EXPECT_TRUE(consumer.reset().ok());
}

TEST(KafkaShareConsumerTest, BufferMemoryFollowsConsumerAcrossThreads) {
    auto consumer = std::make_unique<RecordingShareConsumer>();
    auto tracker = consumer->_mem_tracker;
    std::thread worker([&] {
        consumer->reserve_records(1024);
        EXPECT_EQ(tracker->consumption(),
                  consumer->_records.capacity() * sizeof(std::pair<int32_t, int64_t>));
        consumer->_records.emplace_back(0, 100);
        consumer->_processed = 1;
        EXPECT_TRUE(consumer->acknowledge_committed().ok());
        EXPECT_TRUE(consumer->reset().ok());
        consumer->reserve_records(4096);
        EXPECT_EQ(tracker->consumption(),
                  consumer->_records.capacity() * sizeof(std::pair<int32_t, int64_t>));
    });
    worker.join();
    EXPECT_GT(tracker->consumption(), 0);
    auto caller_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "ShareTestCaller");
    std::thread cleanup([&] {
        SCOPED_ATTACH_TASK(caller_tracker);
        consumer->reserve_records(8192);
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker(),
                  caller_tracker.get());
        consumer.reset();
        EXPECT_EQ(thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker(),
                  caller_tracker.get());
    });
    cleanup.join();
    EXPECT_EQ(tracker->consumption(), 0);
    EXPECT_EQ(caller_tracker->consumption(), 0);
}

} // namespace doris
