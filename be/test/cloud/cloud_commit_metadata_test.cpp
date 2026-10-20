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

#include <gtest/gtest.h>

#include <memory>

#include "cloud/cloud_delta_writer.h"
#include "cloud/cloud_rowset_builder.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablets_channel.h"
#include "cloud/config.h"
#include "exec/sink/writer/vtablet_writer.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "storage/options.h"
#include "storage/tablet/tablet_meta.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

// Keep the real delta-writer commit and channel-reporting paths; replace only
// rowset I/O so these tests require neither an object store nor a meta service.
class CommitMetadataRowsetBuilder : public CloudRowsetBuilder {
public:
    CommitMetadataRowsetBuilder(CloudStorageEngine& engine, const WriteRequest& req)
            : CloudRowsetBuilder(engine, req, nullptr) {
        auto meta = std::make_shared<TabletMeta>();
        meta->_tablet_id = req.tablet_id;
        _tablet = std::make_shared<CloudTablet>(engine, meta);
    }

    Status init() override {
        if (change_skip_config_on_init) {
            config::skip_writing_empty_rowset_metadata = false;
        }
        return Status::OK();
    }
    Status build_rowset() override { return Status::OK(); }
    Status commit_rowset(const std::string&, int64_t) override {
        ++commits;
        return Status::OK();
    }
    Status submit_calc_delete_bitmap_task() override { return Status::OK(); }
    Status wait_calc_delete_bitmap() override { return Status::OK(); }
    Status set_txn_related_info() override { return Status::OK(); }
    void update_tablet_stats() override {}

    bool change_skip_config_on_init = false;
    int commits = 0;
};

} // namespace

class CloudCommitMetadataTest : public testing::Test {
protected:
    void SetUp() override {
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "CloudCommitMetadataTest");
        _attach_task = std::make_unique<AttachTask>(tracker);
        _engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
        WriteRequest req;
        req.tablet_id = 1000;
        req.partition_id = 100;
        req.binlog_tablet_id = 1001;
        auto writer = std::make_unique<CloudDeltaWriter>(*_engine, req, nullptr, UniqueId {});
        auto builder = std::make_unique<CommitMetadataRowsetBuilder>(*_engine, req);
        _builder = builder.get();
        writer->_rowset_builder = std::move(builder);
        _writer = writer.get();
        _channel = std::make_unique<CloudTabletsChannel>(
                *_engine, TabletsChannelKey {PUniqueId {}, 1}, UniqueId {}, false, nullptr);
        _channel->_partition_ids.insert(req.partition_id);
        _channel->_tablet_writers.emplace(req.tablet_id, std::move(writer));
        ++BaseTabletsChannel::_s_tablet_writer_count;
    }

    void TearDown() override {
        // The initialized-writer test exercises commit without creating a flush token.
        _writer->_is_init = false;
        _channel.reset();
        _engine.reset();
        _attach_task.reset();
        config::skip_writing_empty_rowset_metadata = _old_skip_empty;
    }

    const bool _old_skip_empty = config::skip_writing_empty_rowset_metadata;
    std::unique_ptr<AttachTask> _attach_task;
    std::unique_ptr<CloudStorageEngine> _engine;
    std::unique_ptr<CloudTabletsChannel> _channel;
    CloudDeltaWriter* _writer = nullptr;
    CommitMetadataRowsetBuilder* _builder = nullptr;
};

TEST_F(CloudCommitMetadataTest, SkippedEmptyRowsetReportsNoMetadata) {
    config::skip_writing_empty_rowset_metadata = true;
    // Changing the configuration after prepare must not change this writer's decision.
    _builder->change_skip_config_on_init = true;
    PTabletWriterAddBlockResult response;
    ASSERT_TRUE(_channel->_close_writers(&response).ok());
    EXPECT_EQ(_builder->commits, 0);
    ASSERT_EQ(response.tablet_vec_size(), 2);
    EXPECT_EQ(response.tablet_vec(0).tablet_id(), 1000);
    EXPECT_EQ(response.tablet_vec(1).tablet_id(), 1001);
    for (const auto& tablet : response.tablet_vec()) {
        EXPECT_TRUE(tablet.has_rowset_meta_written());
        EXPECT_FALSE(tablet.rowset_meta_written());
        EXPECT_EQ(tablet.received_rows(), 0);
    }
}

TEST_F(CloudCommitMetadataTest, PersistedEmptyRowsetReportsMetadata) {
    config::skip_writing_empty_rowset_metadata = false;
    PTabletWriterAddBlockResult response;
    ASSERT_TRUE(_channel->_close_writers(&response).ok());
    EXPECT_EQ(_builder->commits, 1);
    ASSERT_EQ(response.tablet_vec_size(), 2);
    for (const auto& tablet : response.tablet_vec()) {
        EXPECT_TRUE(tablet.has_rowset_meta_written());
        EXPECT_TRUE(tablet.rowset_meta_written());
        EXPECT_EQ(tablet.received_rows(), 0);
    }
}

TEST_F(CloudCommitMetadataTest, InitializedWriterDoesNotSkipZeroRowMetadata) {
    config::skip_writing_empty_rowset_metadata = true;
    // Empty-rowset skipping applies to uninitialized writers. A writer initialized
    // before rows were filtered still commits metadata, even with zero final rows.
    _writer->_is_init = true;
    ASSERT_TRUE(_writer->commit_rowset().ok());
    EXPECT_EQ(_builder->commits, 1);
    EXPECT_TRUE(_writer->rowset_meta_written());
}

TEST(VNodeChannelCommitMetadataTest, PreservesMetadataFlagPresence) {
    MockRuntimeState state;
    TDataSink sink;
    VTabletWriter parent(sink, {}, nullptr, nullptr);
    IndexChannel index(&parent, 1, nullptr);
    VNodeChannel node(&parent, &index, 10, false);
    node._state = &state;
    PTabletWriterAddBlockResult response;
    Status::OK().to_protobuf(response.mutable_status());
    for (int i = 0; i < 3; ++i) {
        auto* tablet = response.add_tablet_vec();
        tablet->set_tablet_id(1000 + i);
        tablet->set_schema_hash(0);
    }
    response.mutable_tablet_vec(0)->set_rowset_meta_written(true);
    response.mutable_tablet_vec(1)->set_rowset_meta_written(false);
    // The third result represents an old writer BE and must remain unknown in Thrift.
    WriteBlockCallbackContext context;
    context._is_last_rpc = true;
    node._add_block_success_callback(response, context);
    ASSERT_EQ(node._tablet_commit_infos.size(), 3);
    EXPECT_TRUE(node._tablet_commit_infos[0].__isset.rowset_meta_written);
    EXPECT_TRUE(node._tablet_commit_infos[0].rowset_meta_written);
    EXPECT_TRUE(node._tablet_commit_infos[1].__isset.rowset_meta_written);
    EXPECT_FALSE(node._tablet_commit_infos[1].rowset_meta_written);
    EXPECT_FALSE(node._tablet_commit_infos[2].__isset.rowset_meta_written);
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(node._tablet_commit_infos[i].tabletId, 1000 + i);
        EXPECT_EQ(node._tablet_commit_infos[i].backendId, 10);
    }
}

} // namespace doris
