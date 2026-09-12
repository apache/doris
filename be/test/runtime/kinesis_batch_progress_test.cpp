// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/Record.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "io/fs/kinesis_consumer_pipe.h"
#include "load/routine_load/data_consumer.h"
#include "load/routine_load/data_consumer_group.h"
#include "load/stream_load/stream_load_context.h"

namespace doris {

class RecordingKinesisPipe : public io::KinesisConsumerPipe {
public:
    Status append_json(const char* data, size_t size) override {
        appended.emplace_back(data, size);
        return Status::OK();
    }

    std::vector<std::string> appended;
};

class KinesisBatchProgressReproduction : public testing::Test {
protected:
    void verify_progress(int64_t row_budget, int64_t byte_budget, size_t expected_appended,
                         bool add_end_marker = false) {
        auto ctx = StreamLoadContext::create_shared(nullptr);
        ctx->load_type = TLoadType::ROUTINE_LOAD;
        ctx->load_src_type = TLoadSourceType::KINESIS;
        ctx->format = TFileFormatType::FORMAT_JSON;
        ctx->max_interval_s = 30;
        ctx->max_batch_rows = row_budget;
        ctx->max_batch_size = byte_budget;
        TKinesisLoadInfo info;
        info.__set_region("us-east-1");
        info.__set_stream("r1-unit-test");
        info.__set_shard_begin_sequence_number({{"shard-0", "0"}});
        ctx->kinesis_info = std::make_unique<KinesisLoadInfo>(info);

        auto consumer = std::make_shared<KinesisDataConsumer>(ctx);
        consumer->_init = true;
        KinesisDataConsumerGroup group(1);
        group.add_consumer(consumer);
        group._format = ctx->format;

        Aws::Kinesis::Model::GetRecordsResult response;
        for (int i = 1; i <= 5; ++i) {
            Aws::Kinesis::Model::Record record;
            record.SetSequenceNumber(std::to_string(i));
            std::string payload = "{\"id\":" + std::to_string(i) + "}";
            record.SetData(Aws::Utils::ByteBuffer(
                    reinterpret_cast<const unsigned char*>(payload.data()), payload.size()));
            response.AddRecords(std::move(record));
        }
        int64_t received = 0;
        int64_t queued = 0;
        // A valid interleaving: the producer queues a whole response before
        // the group drains it. Invoke the real producer processing function.
        ASSERT_TRUE(consumer->_process_records("shard-0", std::move(response), &group._queue,
                                               &received, &queued)
                            .ok());
        ASSERT_EQ(5, queued);
        if (add_end_marker) {
            KinesisQueueItem end_marker;
            end_marker.shard_id = "shard-0";
            end_marker.end_of_shard = true;
            ASSERT_TRUE(group._queue.blocking_put(end_marker));
        }
        // No additional input; the queue still drains its already queued records.
        group._queue.shutdown();

        auto pipe = std::make_shared<RecordingKinesisPipe>();
        Status consumer_status = Status::OK();
        ASSERT_TRUE(group._run_consume_loop(ctx, pipe, consumer_status).ok());
        ASSERT_EQ(expected_appended, pipe->appended.size());
        ASSERT_EQ(5 - expected_appended, group._queue.get_size());
        EXPECT_EQ(std::to_string(expected_appended),
                  ctx->kinesis_info->cmt_sequence_number.at("shard-0"));
        if (add_end_marker) {
            EXPECT_EQ(1, ctx->kinesis_info->closed_shard_ids.count("shard-0"));
        }
    }
};

TEST_F(KinesisBatchProgressReproduction, FullBatchControl) {
    verify_progress(5, 1024, 5);
}

TEST_F(KinesisBatchProgressReproduction, RowBudgetMustNotCommitQueuedRecords) {
    verify_progress(2, 1024, 2);
}

TEST_F(KinesisBatchProgressReproduction, ByteBudgetMustNotCommitQueuedRecords) {
    // Each JSON payload is eight bytes.
    verify_progress(5, 16, 2);
}

TEST_F(KinesisBatchProgressReproduction, ClosedMarkerFollowsAppendedRecords) {
    verify_progress(6, 1024, 5, true);
}

TEST_F(KinesisBatchProgressReproduction, QueueShutdownDuringPrefetchIsGraceful) {
    auto ctx = StreamLoadContext::create_shared(nullptr);
    TKinesisLoadInfo info;
    ctx->kinesis_info = std::make_unique<KinesisLoadInfo>(info);
    auto consumer = std::make_shared<KinesisDataConsumer>(ctx);
    KinesisDataConsumerGroup group(1);

    for (int i = 0; i < 500; ++i) {
        KinesisQueueItem item;
        item.shard_id = "shard-0";
        ASSERT_TRUE(group._queue.try_put(item));
    }

    Aws::Kinesis::Model::GetRecordsResult response;
    Aws::Kinesis::Model::Record record;
    record.SetSequenceNumber("1");
    const std::string payload = "{\"id\":1}";
    record.SetData(Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(payload.data()),
                                          payload.size()));
    response.AddRecords(std::move(record));

    Status producer_status = Status::OK();
    int64_t received = 0;
    int64_t queued = 0;
    std::thread producer([&] {
        producer_status = consumer->_process_records("shard-0", std::move(response), &group._queue,
                                                     &received, &queued);
    });
    while (group._queue.put_waiting_count_for_test() == 0) {
        std::this_thread::yield();
    }
    group._queue.shutdown();
    producer.join();

    ASSERT_TRUE(producer_status.ok());
    ASSERT_EQ(0, queued);
}

} // namespace doris
