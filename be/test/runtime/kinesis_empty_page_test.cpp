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

#include <aws/core/Aws.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "kinesis_fake_client.h"
#include "load/routine_load/data_consumer.h"
#include "load/routine_load/kinesis_conf.h"
#include "load/stream_load/stream_load_context.h"
#include "util/blocking_queue.hpp"

namespace doris {

class EmptyPageKinesisClient final : public Aws::Kinesis::KinesisClient {
public:
    explicit EmptyPageKinesisClient(int empty_page_count) : _empty_page_count(empty_page_count) {}

    Aws::Kinesis::Model::GetRecordsOutcome GetRecords(
            const Aws::Kinesis::Model::GetRecordsRequest& request) const override {
        ++get_records_calls;
        iterator_calls.emplace_back(request.GetShardIterator());
        Aws::Kinesis::Model::GetRecordsResult result;
        if (get_records_calls <= _empty_page_count) {
            result.SetMillisBehindLatest(100);
            result.SetNextShardIterator("iterator-after-empty-page");
            return result;
        }

        Aws::Kinesis::Model::Record record;
        record.SetSequenceNumber("1");
        const std::string payload = "{\"id\":1}";
        record.SetData(Aws::Utils::ByteBuffer(
                reinterpret_cast<const unsigned char*>(payload.data()), payload.size()));
        result.AddRecords(std::move(record));
        result.SetMillisBehindLatest(0);
        result.SetNextShardIterator("");
        return result;
    }

    mutable int get_records_calls = 0;
    mutable std::vector<std::string> iterator_calls;

private:
    const int _empty_page_count;
};

class KinesisEmptyPageReproduction : public testing::Test {
protected:
    void SetUp() override { Aws::InitAPI(options); }
    void TearDown() override { Aws::ShutdownAPI(options); }

    void expect_record_after_empty_pages(int empty_page_count) {
        auto ctx = StreamLoadContext::create_shared(nullptr);
        TKinesisLoadInfo info;
        info.__set_region("us-east-1");
        info.__set_stream("doris-latest-r2-unit-test");
        ctx->kinesis_info = std::make_unique<KinesisLoadInfo>(info);

        auto consumer = std::make_shared<KinesisDataConsumer>(ctx);
        consumer->_init = true;
        auto fake_client = std::make_shared<EmptyPageKinesisClient>(empty_page_count);
        consumer->_kinesis_client = fake_client;
        consumer->_kinesis_conf = std::make_unique<KinesisConf>();
        consumer->_shard_iterators.emplace("shard-0", "initial-iterator");
        consumer->_consuming_shard_ids.emplace("shard-0");

        BlockingQueue<KinesisQueueItem> queue(8);
        ASSERT_TRUE(consumer->group_consume(&queue, 1000).ok());
        ASSERT_EQ(empty_page_count + 1, fake_client->get_records_calls);
        ASSERT_EQ("initial-iterator", fake_client->iterator_calls.front());
        ASSERT_EQ("iterator-after-empty-page", fake_client->iterator_calls.at(1));

        KinesisQueueItem item;
        ASSERT_TRUE(queue.blocking_get(&item));
        ASSERT_FALSE(item.end_of_shard);
        ASSERT_EQ("1", item.record->GetSequenceNumber());
        queue.shutdown();
    }

    Aws::SDKOptions options;
};

TEST_F(KinesisEmptyPageReproduction, FollowsNextIteratorAfterEmptyPage) {
    expect_record_after_empty_pages(1);
}

TEST_F(KinesisEmptyPageReproduction, FollowsNextIteratorAfterMultipleEmptyPages) {
    expect_record_after_empty_pages(2);
}

TEST_F(KinesisEmptyPageReproduction, CaughtUpShardStopsBatchWithoutEof) {
    auto ctx = StreamLoadContext::create_shared(nullptr);
    TKinesisLoadInfo info;
    ctx->kinesis_info = std::make_unique<KinesisLoadInfo>(info);
    KinesisDataConsumer consumer(ctx);
    consumer._init = true;
    consumer._kinesis_conf = std::make_unique<KinesisConf>();
    auto client = std::make_shared<KinesisFakeClient>();
    client->set_records_pages("idle", {{.sequences = {},
                                        .next_iterator = "idle-next",
                                        .millis_behind_latest = 0,
                                        .child_parents = {}}});
    consumer._kinesis_client = client;
    ASSERT_TRUE(consumer.assign_shards({{"idle", "TRIM_HORIZON"}}, "test", ctx).ok());
    BlockingQueue<KinesisQueueItem> queue(8);
    ASSERT_TRUE(consumer.group_consume(&queue, 5000).ok());
    EXPECT_EQ(1, client->get_records_calls("idle"));
    EXPECT_EQ(0, queue.get_size());
    EXPECT_EQ("idle-next", consumer._shard_iterators.at("idle"));
    EXPECT_TRUE(consumer._consuming_shard_ids.empty());
    queue.shutdown();
}

TEST_F(KinesisEmptyPageReproduction, IdleShardDoesNotStopOtherShardWithBacklog) {
    auto ctx = StreamLoadContext::create_shared(nullptr);
    TKinesisLoadInfo info;
    ctx->kinesis_info = std::make_unique<KinesisLoadInfo>(info);
    KinesisDataConsumer consumer(ctx);
    consumer._init = true;
    consumer._kinesis_conf = std::make_unique<KinesisConf>();
    auto client = std::make_shared<KinesisFakeClient>();
    client->set_records_pages("idle", {{.sequences = {},
                                        .next_iterator = "idle-next",
                                        .millis_behind_latest = 0,
                                        .child_parents = {}}});
    client->set_records_pages("busy", {{.sequences = {},
                                        .next_iterator = "busy-next",
                                        .millis_behind_latest = 100,
                                        .child_parents = {}},
                                       {.sequences = {"101"},
                                        .next_iterator = "",
                                        .millis_behind_latest = 0,
                                        .child_parents = {}}});
    consumer._kinesis_client = client;
    ASSERT_TRUE(
            consumer.assign_shards({{"idle", "TRIM_HORIZON"}, {"busy", "100"}}, "test", ctx).ok());
    BlockingQueue<KinesisQueueItem> queue(8);
    ASSERT_TRUE(consumer.group_consume(&queue, 5000).ok());
    EXPECT_EQ(1, client->get_records_calls("idle"));
    EXPECT_EQ(2, client->get_records_calls("busy"));
    ASSERT_EQ(2, queue.get_size());
    KinesisQueueItem item;
    ASSERT_TRUE(queue.blocking_get(&item));
    EXPECT_FALSE(item.end_of_shard);
    EXPECT_EQ("101", item.record->GetSequenceNumber());
    ASSERT_TRUE(queue.blocking_get(&item));
    EXPECT_TRUE(item.end_of_shard);
    EXPECT_EQ("busy", item.shard_id);
    queue.shutdown();
}

} // namespace doris
