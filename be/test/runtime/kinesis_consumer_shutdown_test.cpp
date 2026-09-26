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

#include <aws/core/Aws.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "io/fs/kinesis_consumer_pipe.h"
#include "load/routine_load/data_consumer.h"
#include "load/routine_load/data_consumer_group.h"
#include "load/routine_load/kinesis_conf.h"
#include "load/stream_load/stream_load_context.h"
#include "util/blocking_queue.hpp"

namespace doris {

class ShutdownKinesisClient : public Aws::Kinesis::KinesisClient {
public:
    MOCK_METHOD(Aws::Kinesis::Model::GetRecordsOutcome, GetRecords,
                (const Aws::Kinesis::Model::GetRecordsRequest&), (const, override));
};

class ShutdownRecordingPipe : public io::KinesisConsumerPipe {
public:
    Status append_json(const char* data, size_t size) override {
        rows.emplace_back(data, size);
        return Status::OK();
    }
    std::vector<std::string> rows;
};

class KinesisConsumerShutdownTest : public testing::Test {
protected:
    void SetUp() override {
        Aws::InitAPI(options);
        ctx = StreamLoadContext::create_shared(nullptr);
        ctx->load_type = TLoadType::ROUTINE_LOAD;
        ctx->load_src_type = TLoadSourceType::KINESIS;
        ctx->format = TFileFormatType::FORMAT_JSON;
        ctx->max_interval_s = 30;
        ctx->max_batch_rows = 1;
        ctx->max_batch_size = 1024 * 1024;
        TKinesisLoadInfo info;
        info.__set_region("us-east-1");
        info.__set_stream("shutdown-test");
        ctx->kinesis_info = std::make_unique<KinesisLoadInfo>(info);
        consumer = std::make_shared<KinesisDataConsumer>(ctx);
        consumer->_init = true;
        consumer->_kinesis_conf = std::make_unique<KinesisConf>();
        client = std::make_shared<ShutdownKinesisClient>();
        consumer->_kinesis_client = client;
        for (const auto& shard : {"A", "B", "C"}) {
            consumer->_consuming_shard_ids.insert(shard);
            consumer->_shard_iterators[shard] = std::string("iterator-") + shard;
        }
    }

    void TearDown() override {
        consumer.reset();
        client.reset();
        ctx.reset();
        Aws::ShutdownAPI(options);
    }

    static Aws::Kinesis::Model::GetRecordsResult page(int count) {
        Aws::Kinesis::Model::GetRecordsResult result;
        for (int i = 1; i <= count; ++i) {
            Aws::Kinesis::Model::Record record;
            record.SetSequenceNumber(std::to_string(i));
            const std::string payload = "{\"id\":" + std::to_string(i) + "}";
            record.SetData(Aws::Utils::ByteBuffer(
                    reinterpret_cast<const unsigned char*>(payload.data()), payload.size()));
            result.AddRecords(std::move(record));
        }
        result.SetNextShardIterator("next-iterator");
        result.SetMillisBehindLatest(100);
        return result;
    }

    void verify_batch_boundary(bool later_request_fails) {
        std::vector<std::string> requests;
        ON_CALL(*client, GetRecords(testing::_))
                .WillByDefault([&](const Aws::Kinesis::Model::GetRecordsRequest& request)
                                       -> Aws::Kinesis::Model::GetRecordsOutcome {
                    requests.push_back(request.GetShardIterator());
                    if (request.GetShardIterator() == "iterator-A") {
                        // The group queue holds 500 records and this batch consumes exactly one.
                        // Therefore A cannot finish enqueueing 502 records before queue shutdown.
                        return page(502);
                    }
                    if (later_request_fails) {
                        return Aws::Client::AWSError<Aws::Kinesis::KinesisErrors>(
                                Aws::Kinesis::KinesisErrors::RESOURCE_NOT_FOUND,
                                "ResourceNotFoundException", "unexpected request after shutdown",
                                false);
                    }
                    return page(0);
                });
        EXPECT_CALL(*client, GetRecords(testing::_)).Times(1);
        KinesisDataConsumerGroup group(1);
        group.add_consumer(consumer);
        auto pipe = std::make_shared<ShutdownRecordingPipe>();
        Status status = group.start_all(ctx, pipe);
        EXPECT_TRUE(status.ok()) << status;
        EXPECT_EQ((std::vector<std::string> {"iterator-A"}), requests);
        ASSERT_EQ(1, pipe->rows.size());
        EXPECT_EQ("{\"id\":1}", pipe->rows.front());
        ASSERT_EQ(1, ctx->kinesis_info->cmt_sequence_number.size());
        EXPECT_EQ("1", ctx->kinesis_info->cmt_sequence_number.at("A"));
        EXPECT_TRUE(ctx->kinesis_info->closed_shard_ids.empty());
        EXPECT_TRUE(ctx->kinesis_info->child_shard_parent_ids.empty());
    }

    Aws::SDKOptions options;
    std::shared_ptr<StreamLoadContext> ctx;
    std::shared_ptr<KinesisDataConsumer> consumer;
    std::shared_ptr<ShutdownKinesisClient> client;
};

TEST_F(KinesisConsumerShutdownTest, BatchLimitStopsBeforeRemainingShards) {
    verify_batch_boundary(false);
}

TEST_F(KinesisConsumerShutdownTest, UnrequestedSourceErrorCannotFailAppendedPrefix) {
    verify_batch_boundary(true);
}

TEST_F(KinesisConsumerShutdownTest, QueueShutdownStopsWithoutWaitingForCancellation) {
    BlockingQueue<KinesisQueueItem> queue(8);
    EXPECT_CALL(*client, GetRecords(testing::_))
            .Times(1)
            .WillOnce([&](const Aws::Kinesis::Model::GetRecordsRequest& request) {
                EXPECT_EQ("iterator-A", request.GetShardIterator());
                // Queue shutdown happens before the group's separate consumer.cancel() call.
                queue.shutdown();
                return Aws::Kinesis::Model::GetRecordsOutcome(page(1));
            });
    EXPECT_TRUE(consumer->group_consume(&queue, 1000).ok());
    EXPECT_FALSE(consumer->_cancelled);
    EXPECT_EQ(0, queue.get_size());
}

TEST_F(KinesisConsumerShutdownTest, CancellationAfterEmptyPageStopsBeforeNextShard) {
    BlockingQueue<KinesisQueueItem> queue(8);
    EXPECT_CALL(*client, GetRecords(testing::_))
            .Times(1)
            .WillOnce([&](const Aws::Kinesis::Model::GetRecordsRequest& request) {
                EXPECT_EQ("iterator-A", request.GetShardIterator());
                EXPECT_TRUE(consumer->cancel(ctx).ok());
                // No enqueue attempt: cancellation must also be checked between shard requests.
                return Aws::Kinesis::Model::GetRecordsOutcome(page(0));
            });
    EXPECT_TRUE(consumer->group_consume(&queue, 1000).ok());
    EXPECT_EQ(0, queue.get_size());
}

} // namespace doris
