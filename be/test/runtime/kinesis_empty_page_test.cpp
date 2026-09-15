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

        BlockingQueue<std::shared_ptr<Aws::Kinesis::Model::Record>> queue(8);
        ASSERT_TRUE(consumer->group_consume(&queue, 1000).ok());
        ASSERT_EQ(empty_page_count + 1, fake_client->get_records_calls);
        ASSERT_EQ("initial-iterator", fake_client->iterator_calls.front());
        ASSERT_EQ("iterator-after-empty-page", fake_client->iterator_calls.at(1));

        std::shared_ptr<Aws::Kinesis::Model::Record> record;
        ASSERT_TRUE(queue.blocking_get(&record));
        ASSERT_EQ("1", record->GetSequenceNumber());
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

} // namespace doris
