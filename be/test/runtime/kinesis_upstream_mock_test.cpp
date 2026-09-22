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

#include <memory>
#include <string>
#include <vector>

#include <aws/core/Aws.h>
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include "io/fs/kinesis_consumer_pipe.h"
#include "load/routine_load/data_consumer.h"
#include "load/routine_load/data_consumer_group.h"
#include "load/routine_load/kinesis_conf.h"
#include "kinesis_fake_client.h"
#include "load/stream_load/stream_load_context.h"

namespace doris {

class RecordingKinesisPipeForMock : public io::KinesisConsumerPipe {
public:
    Status append_json(const char* data, size_t size) override {
        rows.emplace_back(data, size);
        return Status::OK();
    }

    std::vector<std::string> rows;
};

class KinesisUpstreamMockTest : public testing::Test {
protected:
    void SetUp() override { Aws::InitAPI(_aws_options); }
    void TearDown() override { Aws::ShutdownAPI(_aws_options); }

    std::shared_ptr<StreamLoadContext> create_context() {
        auto ctx = StreamLoadContext::create_shared(nullptr);
        ctx->load_type = TLoadType::ROUTINE_LOAD;
        ctx->load_src_type = TLoadSourceType::KINESIS;
        ctx->format = TFileFormatType::FORMAT_JSON;
        ctx->max_interval_s = 30;
        ctx->max_batch_rows = 10;
        ctx->max_batch_size = 1024;
        TKinesisLoadInfo info;
        info.__set_region("us-east-1");
        info.__set_stream("mock-stream");
        ctx->kinesis_info = std::make_unique<KinesisLoadInfo>(info);
        return ctx;
    }

    Aws::SDKOptions _aws_options;
};

TEST_F(KinesisUpstreamMockTest, ListShardsPaginationPreservesLineageAndClosedState) {
    auto ctx = create_context();
    auto consumer = std::make_shared<KinesisDataConsumer>(ctx);
    consumer->_init = true;
    consumer->_kinesis_conf = std::make_unique<KinesisConf>();
    auto fake = std::make_shared<KinesisFakeClient>();
    fake->set_list_shards_pages({
            {KinesisFakeClient::Shard {"P", "", "", false}},
            {KinesisFakeClient::Shard {"C", "P", "", true}}});
    consumer->_kinesis_client = fake;

    std::vector<PShardInfo> shards;
    ASSERT_TRUE(consumer->get_shard_list(&shards).ok());
    ASSERT_EQ(2, shards.size());
    ASSERT_EQ(2, fake->list_shards_calls());
    ASSERT_EQ("P", shards[0].shard_id());
    ASSERT_EQ("C", shards[1].shard_id());
    ASSERT_EQ("P", shards[1].parent_shard_id());
    ASSERT_TRUE(shards[1].closed());
}

TEST_F(KinesisUpstreamMockTest, GetRecordsEofCarriesChildLineageToCommitAttachment) {
    auto ctx = create_context();
    auto consumer = std::make_shared<KinesisDataConsumer>(ctx);
    consumer->_init = true;
    consumer->_kinesis_conf = std::make_unique<KinesisConf>();
    consumer->_shard_iterators.emplace("P", "initial-P");
    consumer->_consuming_shard_ids.emplace("P");

    auto fake = std::make_shared<KinesisFakeClient>();
    fake->set_initial_iterator("initial-P", "P");
    fake->set_records_pages("P", {
            KinesisFakeClient::RecordsPage {{"1"}, "next-P", 100, {}},
            KinesisFakeClient::RecordsPage {{}, "", 0, {{"C", {"P"}}}}});
    consumer->_kinesis_client = fake;

    KinesisDataConsumerGroup group(1);
    group.add_consumer(consumer);
    group._format = ctx->format;
    auto pipe = std::make_shared<RecordingKinesisPipeForMock>();
    ASSERT_TRUE(group.start_all(ctx, pipe).ok());

    ASSERT_EQ(1, pipe->rows.size());
    ASSERT_EQ(1, ctx->kinesis_info->closed_shard_ids.count("P"));
    ASSERT_EQ(1, ctx->kinesis_info->child_shard_parent_ids.count("C"));
    ASSERT_EQ(1, ctx->kinesis_info->child_shard_parent_ids.at("C").count("P"));
    ASSERT_EQ(2, fake->get_records_calls("P"));
}

} // namespace doris
