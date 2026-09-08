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

#include <aws/kinesis/model/GetShardIteratorResult.h>

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "load/routine_load/data_consumer.h"
#include "runtime/exec_env.h"
#include "util/s3_util.h"

namespace doris {

class MockKinesisTailClient : public Aws::Kinesis::KinesisClient {
public:
    MOCK_METHOD(Aws::Kinesis::Model::GetShardIteratorOutcome, GetShardIterator,
                (const Aws::Kinesis::Model::GetShardIteratorRequest&), (const, override));
    MOCK_METHOD(Aws::Kinesis::Model::GetRecordsOutcome, GetRecords,
                (const Aws::Kinesis::Model::GetRecordsRequest&), (const, override));
};

class KinesisLatestSequenceTest : public testing::Test {
protected:
    static void SetUpTestSuite() { S3ClientFactory::instance(); }

    void SetUp() override {
        auto ctx = std::make_shared<StreamLoadContext>(ExecEnv::GetInstance());
        TKinesisLoadInfo info;
        info.__set_region("us-east-1");
        info.__set_stream("test-stream");
        ctx->kinesis_info = std::make_unique<KinesisLoadInfo>(info);
        consumer = std::make_unique<KinesisDataConsumer>(ctx);
        client = std::make_shared<MockKinesisTailClient>();
        consumer->_kinesis_client = client;
        consumer->_kinesis_conf = std::make_unique<KinesisConf>();
    }

    void expect_start() {
        EXPECT_CALL(*client, GetShardIterator(testing::_))
                .WillOnce([](const Aws::Kinesis::Model::GetShardIteratorRequest& request) {
                    EXPECT_EQ(request.GetShardIteratorType(),
                              Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);
                    Aws::Kinesis::Model::GetShardIteratorResult result;
                    result.SetShardIterator("i0");
                    return Aws::Kinesis::Model::GetShardIteratorOutcome(result);
                });
    }

    void expect_page(const std::string& iterator, std::vector<std::string> sequences,
                     const std::string& next, int64_t lag) {
        EXPECT_CALL(*client, GetRecords(testing::_))
                .WillOnce([iterator, sequences, next,
                           lag](const Aws::Kinesis::Model::GetRecordsRequest& request) {
                    EXPECT_EQ(request.GetShardIterator(), iterator);
                    EXPECT_EQ(request.GetLimit(), 10000);
                    Aws::Kinesis::Model::GetRecordsResult result;
                    for (const auto& sequence : sequences) {
                        Aws::Kinesis::Model::Record record;
                        record.SetSequenceNumber(sequence);
                        result.AddRecords(record);
                    }
                    result.SetNextShardIterator(next);
                    result.SetMillisBehindLatest(lag);
                    return Aws::Kinesis::Model::GetRecordsOutcome(result);
                });
    }

    std::shared_ptr<MockKinesisTailClient> client;
    std::unique_ptr<KinesisDataConsumer> consumer;
};

TEST_F(KinesisLatestSequenceTest, scans_all_pages_including_empty_intermediate_pages) {
    testing::InSequence ordered;
    expect_start();
    expect_page("i0", {"100", "101"}, "i1", 1000);
    expect_page("i1", {}, "i2", 500);
    expect_page("i2", {"150"}, "i3", 0);
    std::string sequence;
    EXPECT_TRUE(consumer->get_latest_sequence_number(
                                "shard-0", [] { return Status::OK(); }, &sequence)
                        .ok());
    EXPECT_EQ(sequence, "150");
}

TEST_F(KinesisLatestSequenceTest, empty_shard_uses_trim_horizon) {
    expect_start();
    expect_page("i0", {}, "i1", 0);
    std::string sequence;
    EXPECT_TRUE(consumer->get_latest_sequence_number(
                                "shard-0", [] { return Status::OK(); }, &sequence)
                        .ok());
    EXPECT_EQ(sequence, "TRIM_HORIZON");
}

TEST_F(KinesisLatestSequenceTest, empty_final_page_keeps_last_record_sequence) {
    testing::InSequence ordered;
    expect_start();
    expect_page("i0", {"100", "150"}, "i1", 100);
    expect_page("i1", {}, "i2", 0);
    std::string sequence;
    EXPECT_TRUE(consumer->get_latest_sequence_number(
                                "shard-0", [] { return Status::OK(); }, &sequence)
                        .ok());
    EXPECT_EQ(sequence, "150");
}

TEST_F(KinesisLatestSequenceTest, closed_shard_returns_last_sequence) {
    expect_start();
    expect_page("i0", {"100", "150"}, "", 10);
    std::string sequence;
    EXPECT_TRUE(consumer->get_latest_sequence_number(
                                "shard-0", [] { return Status::OK(); }, &sequence)
                        .ok());
    EXPECT_EQ(sequence, "150");
}

TEST_F(KinesisLatestSequenceTest, exhausted_budget_does_not_publish_position) {
    EXPECT_CALL(*client, GetShardIterator(testing::_)).Times(0);
    EXPECT_CALL(*client, GetRecords(testing::_)).Times(0);
    std::string sequence = "unchanged";
    EXPECT_FALSE(consumer->get_latest_sequence_number(
                                 "shard-0",
                                 [] { return Status::TimedOut("batch deadline exceeded"); },
                                 &sequence)
                         .ok());
    EXPECT_EQ(sequence, "unchanged");
}

TEST_F(KinesisLatestSequenceTest, failed_scan_does_not_publish_partial_position) {
    testing::InSequence ordered;
    expect_start();
    expect_page("i0", {"100"}, "i1", 1000);
    Aws::Client::AWSError<Aws::Kinesis::KinesisErrors> error(
            Aws::Kinesis::KinesisErrors::RESOURCE_NOT_FOUND, "ResourceNotFoundException",
            "shard unavailable", false);
    EXPECT_CALL(*client, GetRecords(testing::_))
            .WillOnce(testing::Return(Aws::Kinesis::Model::GetRecordsOutcome(error)));
    std::string sequence = "unchanged";
    EXPECT_FALSE(consumer->get_latest_sequence_number(
                                 "shard-0", [] { return Status::OK(); }, &sequence)
                         .ok());
    EXPECT_EQ(sequence, "unchanged");
}

TEST_F(KinesisLatestSequenceTest, cancellation_during_request_does_not_publish_position) {
    expect_start();
    bool cancelled = false;
    EXPECT_CALL(*client, GetRecords(testing::_))
            .WillOnce([&](const Aws::Kinesis::Model::GetRecordsRequest& request) {
                EXPECT_TRUE(request.GetContinueRequestHandler()(nullptr));
                cancelled = true;
                EXPECT_FALSE(request.GetContinueRequestHandler()(nullptr));
                Aws::Kinesis::Model::GetRecordsResult result;
                Aws::Kinesis::Model::Record record;
                record.SetSequenceNumber("150");
                result.AddRecords(record);
                result.SetMillisBehindLatest(0);
                result.SetNextShardIterator("i1");
                return Aws::Kinesis::Model::GetRecordsOutcome(result);
            });
    std::string sequence = "unchanged";
    EXPECT_FALSE(
            consumer->get_latest_sequence_number(
                            "shard-0",
                            [&] { return cancelled ? Status::Cancelled("paused") : Status::OK(); },
                            &sequence)
                    .ok());
    EXPECT_EQ(sequence, "unchanged");
}

} // namespace doris
