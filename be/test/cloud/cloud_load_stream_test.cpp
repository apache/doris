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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "cloud/cloud_committed_rs_mgr.h"
#include "cloud/cloud_rowset_builder.h"
#include "cloud/cloud_rowset_writer.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_txn_delete_bitmap_cache.h"
#include "cloud/config.h"
#include "cpp/sync_point.h"
#include "load/channel/load_stream_mgr.h"
#include "load/channel/load_stream_writer.h"
#include "load/delta_writer/delta_writer_v2.h"
#include "runtime/exec_env.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/tablet/tablet_meta.h"
#include "util/defer_op.h"
#include "util/time.h"
#include "util/work_thread_pool.hpp"

namespace doris {
namespace {

class EmptyLoadRowsetBuilder : public CloudRowsetBuilder {
public:
    using CloudRowsetBuilder::CloudRowsetBuilder;

    Status init() override {
        skip_metadata_at_init = _skip_writing_rowset_metadata;
        return Status::InternalError("stop before preparing rowset metadata");
    }

    Status commit_rowset(const std::string&, int64_t) override {
        ++commit_calls;
        return Status::InternalError("injected commit failure");
    }

    bool skip_metadata_at_init = false;
    int commit_calls = 0;
};

class ClosingLoadRowsetBuilder : public BaseRowsetBuilder {
public:
    ClosingLoadRowsetBuilder(CloudStorageEngine& engine, const WriteRequest& req,
                             std::atomic<int>& prepared, std::function<Status()> commit)
            : BaseRowsetBuilder(req, nullptr), _prepared(prepared), _commit(std::move(commit)) {
        _rowset_writer = std::make_shared<CloudRowsetWriter>(engine);
        _rowset_writer->_rowset_meta = std::make_shared<RowsetMeta>();
    }

    Status init() override { return Status::OK(); }
    Status build_rowset() override {
        ++_prepared;
        return Status::OK();
    }
    Status submit_calc_delete_bitmap_task() override { return Status::OK(); }
    Status wait_calc_delete_bitmap() override { return Status::OK(); }
    Status commit_txn() override { return _commit(); }

private:
    std::atomic<int>& _prepared;
    std::function<Status()> _commit;
};

} // namespace

class CloudLoadStreamTest : public testing::Test {
protected:
    void SetUp() override {
        _old_cloud_unique_id = config::cloud_unique_id;
        _old_skip_empty = config::skip_writing_empty_rowset_metadata;
        _old_make_visible = config::enable_cloud_make_rs_visible_on_be;
        config::cloud_unique_id = "cloud_load_stream_test";
        config::enable_cloud_make_rs_visible_on_be = true;
        _old_engine = std::move(ExecEnv::GetInstance()->_storage_engine);
        auto engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        _engine->_committed_rs_mgr = std::make_unique<CloudCommittedRSMgr>();
        _engine->_txn_delete_bitmap_cache = std::make_unique<CloudTxnDeleteBitmapCache>(1024);
        ASSERT_TRUE(_engine->_txn_delete_bitmap_cache->init().ok());
    }

    void TearDown() override {
        ExecEnv::GetInstance()->set_storage_engine(std::move(_old_engine));
        config::cloud_unique_id = _old_cloud_unique_id;
        config::skip_writing_empty_rowset_metadata = _old_skip_empty;
        config::enable_cloud_make_rs_visible_on_be = _old_make_visible;
    }

    CloudTabletSPtr create_tablet(KeysType keys_type, bool mow) {
        TabletMetaPB meta;
        meta.set_tablet_id(10001);
        meta.set_table_id(10002);
        meta.set_enable_unique_key_merge_on_write(mow);
        meta.mutable_schema()->set_keys_type(keys_type);
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->init_from_pb(meta);
        auto tablet = std::make_shared<CloudTablet>(*_engine, tablet_meta);
        std::unique_lock lock(tablet->get_header_lock());
        tablet->reset_approximate_stats(0, 0, 0, 0);
        return tablet;
    }

    CloudStorageEngine* _engine = nullptr;
    std::unique_ptr<BaseStorageEngine> _old_engine;
    std::string _old_cloud_unique_id;
    bool _old_skip_empty = false;
    bool _old_make_visible = false;
};

TEST_F(CloudLoadStreamTest, SinkMowSnapshotSkipsEmptyRowsets) {
    const bool old_check = config::enable_merge_on_write_correctness_check;
    config::enable_merge_on_write_correctness_check = true;
    Defer restore_check([&] { config::enable_merge_on_write_correctness_check = old_check; });
    auto tablet = create_tablet(UNIQUE_KEYS, true);
    auto* sp = SyncPoint::get_instance();
    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [&](auto&& args) {
        *try_any_cast<TabletMetaSharedPtr*>(args[1]) = tablet->tablet_meta();
        try_any_cast_ret<Status>(args)->second = true;
    });
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [](auto&& args) { try_any_cast_ret<Status>(args)->second = true; });
    sp->enable_processing();
    Defer cleanup([&] {
        sp->disable_processing();
        sp->clear_all_call_backs();
    });

    WriteRequest req;
    req.tablet_id = tablet->tablet_id();
    req.txn_id = 123;
    DeltaWriterV2 writer(&req, {}, nullptr);
    PCloudLoadMowSnapshot snapshot;
    snapshot.set_version(7);
    snapshot.mutable_delete_bitmap();
    RowsetIdUnorderedSet expected_ids;
    for (int64_t i = 1; i <= 3; ++i) {
        RowsetId id;
        id.init(2, 0, req.tablet_id, i);
        expected_ids.insert(id);
        auto* meta = snapshot.add_rowsets();
        meta->set_rowset_id_v2(id.to_string());
        meta->set_tablet_id(req.tablet_id);
        meta->set_rowset_type(BETA_ROWSET);
        meta->set_num_segments(0);
        tablet->tablet_schema()->to_schema_pb(meta->mutable_tablet_schema());
    }
    // Cover absent, empty, and valid resources on zero-segment rowsets.
    snapshot.mutable_rowsets(1)->set_resource_id("");
    snapshot.mutable_rowsets(2)->set_resource_id("test_resource");
    RowsetWriterContext context;
    ASSERT_TRUE(writer._init_mow_context_from_snapshot(context, snapshot).ok());
    ASSERT_NE(context.mow_context, nullptr);
    EXPECT_EQ(*context.mow_context->rowset_ids, expected_ids);
    EXPECT_TRUE(context.mow_context->rowset_ptrs.empty());
    EXPECT_EQ(context.mow_context->max_version, snapshot.version());
    EXPECT_EQ(context.mow_context->txn_id, req.txn_id);
    EXPECT_NE(context.mow_context->snapshot_delete_bitmap, nullptr);
    for (const auto& id : expected_ids) {
        EXPECT_TRUE(context.mow_context->delete_bitmap->contains(
                {id, DeleteBitmap::INVALID_SEGMENT_ID, DeleteBitmap::TEMP_VERSION_COMMON},
                DeleteBitmap::ROWSET_SENTINEL_MARK));
    }
    EXPECT_TRUE(context.mow_context->snapshot_delete_bitmap->delete_bitmap.empty());
    // The target must see the sentinels after the sink result is serialized and merged.
    PCloudLoadMowResult result;
    result.set_snapshot_version(snapshot.version());
    *result.mutable_delete_bitmap() = context.mow_context->delete_bitmap->to_pb();
    ASSERT_TRUE(CloudRowsetBuilder::validate_sink_mow_result(result, snapshot.version()).ok());
    auto target_bitmap = std::make_shared<DeleteBitmap>(req.tablet_id);
    target_bitmap->merge(DeleteBitmap::from_pb(result.delete_bitmap(), req.tablet_id));
    EXPECT_TRUE(tablet->check_delete_bitmap_correctness(target_bitmap, snapshot.version(),
                                                        req.txn_id, expected_ids)
                        .ok());

    // A mixed snapshot must still reconstruct its non-empty remote rowset.
    snapshot.mutable_rowsets(2)->set_num_segments(1);
    ASSERT_TRUE(writer._init_mow_context_from_snapshot(context, snapshot).ok());
    EXPECT_EQ(*context.mow_context->rowset_ids, expected_ids);
    ASSERT_EQ(context.mow_context->rowset_ptrs.size(), 1);
    EXPECT_EQ(context.mow_context->rowset_ptrs.front()->rowset_id().to_string(),
              snapshot.rowsets(2).rowset_id_v2());
    const auto non_empty_id = context.mow_context->rowset_ptrs.front()->rowset_id();
    for (const auto& id : expected_ids) {
        EXPECT_EQ(context.mow_context->delete_bitmap->contains(
                          {id, DeleteBitmap::INVALID_SEGMENT_ID, DeleteBitmap::TEMP_VERSION_COMMON},
                          DeleteBitmap::ROWSET_SENTINEL_MARK),
                  id != non_empty_id);
    }

    snapshot.mutable_rowsets(2)->clear_resource_id();
    auto st = writer._init_mow_context_from_snapshot(context, snapshot);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("has no storage resource"), std::string::npos);

    config::enable_merge_on_write_correctness_check = false;
    snapshot.mutable_rowsets(2)->set_num_segments(0);
    ASSERT_TRUE(writer._init_mow_context_from_snapshot(context, snapshot).ok());
    EXPECT_EQ(*context.mow_context->rowset_ids, expected_ids);
    EXPECT_TRUE(context.mow_context->delete_bitmap->delete_bitmap.empty());
}

TEST_F(CloudLoadStreamTest, SinkMowSnapshotDuringSchemaChange) {
    auto tablet = create_tablet(UNIQUE_KEYS, true);
    auto* sp = SyncPoint::get_instance();
    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [&](auto&& args) {
        *try_any_cast<TabletMetaSharedPtr*>(args[1]) = tablet->tablet_meta();
        try_any_cast_ret<Status>(args)->second = true;
    });
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                      [](auto&& args) { try_any_cast_ret<Status>(args)->second = true; });
    sp->enable_processing();
    Defer cleanup([&] {
        sp->disable_processing();
        sp->clear_all_call_backs();
    });

    auto make_rowset = [&](int64_t start, int64_t end) {
        auto meta = std::make_shared<RowsetMeta>();
        RowsetId id;
        id.init(2, 0, tablet->tablet_id(), end);
        meta->set_rowset_id(id);
        meta->set_rowset_type(BETA_ROWSET);
        meta->set_rowset_state(VISIBLE);
        meta->set_version({start, end});
        meta->set_resource_id("test_resource");
        meta->set_tablet_schema(tablet->tablet_schema());
        meta->set_num_segments(1);
        RowsetSharedPtr rowset;
        EXPECT_TRUE(RowsetFactory::create_rowset(nullptr, "", meta, &rowset).ok());
        return rowset;
    };
    auto incremental = make_rowset(4, 4);
    {
        std::unique_lock lock(tablet->get_header_lock());
        ASSERT_TRUE(tablet->set_tablet_state(TABLET_NOTREADY).ok());
        // Converted history [0-3] is not available yet.
        tablet->add_rowsets({incremental}, false, lock, false);
        tablet->tablet_meta()->delete_bitmap().add({incremental->rowset_id(), 0, 4}, 9);
    }
    WriteRequest req;
    req.tablet_id = tablet->tablet_id();
    auto make_builder = [&] {
        auto builder = std::make_unique<CloudRowsetBuilder>(*_engine, req, nullptr);
        builder->_tablet = tablet;
        builder->_rowset_writer = std::make_shared<CloudRowsetWriter>(*_engine);
        builder->_rowset_writer->_context.mow_context = std::make_shared<MowContext>(
                -1, req.txn_id, builder->_rowset_ids, std::vector<RowsetSharedPtr> {},
                std::make_shared<DeleteBitmap>(req.tablet_id));
        return builder;
    };
    auto builder = make_builder();
    PCloudLoadMowSnapshot snapshot;
    ASSERT_TRUE(builder->get_mow_snapshot_for_sink(&snapshot).ok());
    EXPECT_EQ(snapshot.version(), 4);
    EXPECT_EQ(snapshot.rowsets_size(), 0);
    EXPECT_TRUE(snapshot.has_delete_bitmap());
    EXPECT_EQ(snapshot.delete_bitmap().rowset_ids_size(), 0);
    EXPECT_TRUE(builder->_rowset_ids->empty());
    EXPECT_TRUE(builder->_rowset_writer->context().mow_context->rowset_ptrs.empty());

    {
        std::unique_lock lock(tablet->get_header_lock());
        tablet->add_rowsets({make_rowset(0, 1), make_rowset(2, 3)}, false, lock, false);
        ASSERT_TRUE(tablet->set_tablet_state(TABLET_RUNNING).ok());
    }
    PCloudLoadMowSnapshot same_load;
    ASSERT_TRUE(builder->get_mow_snapshot_for_sink(&same_load).ok());
    EXPECT_EQ(same_load.SerializeAsString(), snapshot.SerializeAsString());
    // A new load after conversion sees the completed historical version path.
    auto running_builder = make_builder();
    PCloudLoadMowSnapshot running_snapshot;
    ASSERT_TRUE(running_builder->get_mow_snapshot_for_sink(&running_snapshot).ok());
    EXPECT_EQ(running_snapshot.version(), 4);
    EXPECT_EQ(running_snapshot.rowsets_size(), 2);
    EXPECT_EQ(running_builder->_rowset_ids->size(), 2);
    auto bitmap = DeleteBitmap::from_pb(running_snapshot.delete_bitmap(), req.tablet_id);
    EXPECT_TRUE(bitmap.contains({incremental->rowset_id(), 0, 4}, 9));

    {
        std::unique_lock lock(tablet->get_header_lock());
        ASSERT_TRUE(tablet->set_tablet_state(TABLET_SHUTDOWN).ok());
    }
    auto shutdown_builder = make_builder();
    EXPECT_FALSE(shutdown_builder->get_mow_snapshot_for_sink(&snapshot).ok());
}

TEST_F(CloudLoadStreamTest, SetEmptyPolicyBeforePreparingMetadata) {
    for (bool skip_empty : {false, true}) {
        config::skip_writing_empty_rowset_metadata = skip_empty;
        for (bool is_empty : {false, true}) {
            WriteRequest req;
            LoadStreamWriter writer(&req, nullptr);
            auto builder = std::make_unique<EmptyLoadRowsetBuilder>(*_engine, req, nullptr);
            auto* observed_builder = builder.get();
            writer._rowset_builder = std::move(builder);
            auto st = writer.init(is_empty);
            ASSERT_FALSE(st.ok());
            EXPECT_NE(st.to_string().find("stop before preparing"), std::string::npos);
            EXPECT_EQ(observed_builder->skip_metadata_at_init, skip_empty && is_empty);
        }
    }
}

TEST_F(CloudLoadStreamTest, EmptyCommitSkipsRpcAndRegistersMarkers) {
    for (auto keys_type : {DUP_KEYS, AGG_KEYS, UNIQUE_KEYS}) {
        for (bool mow : {false, true}) {
            if (mow && keys_type != UNIQUE_KEYS) {
                continue;
            }
            WriteRequest req;
            req.tablet_id = 10001;
            req.txn_id = 100 + keys_type * 2 + mow;
            req.txn_expiration = UnixSeconds() + 3600;
            EmptyLoadRowsetBuilder builder(*_engine, req, nullptr);
            auto tablet = create_tablet(keys_type, mow);
            builder._tablet = tablet;
            builder._tablet_schema = tablet->tablet_schema();
            auto meta = std::make_shared<RowsetMeta>();
            meta->set_rowset_type(BETA_ROWSET);
            meta->set_rowset_state(PREPARED);
            meta->set_num_segments(0);
            ASSERT_TRUE(
                    RowsetFactory::create_rowset(builder._tablet_schema, "", meta, &builder._rowset)
                            .ok());
            builder.set_skip_writing_rowset_metadata(true);

            auto st = builder.commit_txn();
            ASSERT_TRUE(st.ok()) << st.to_string();
            EXPECT_EQ(builder.commit_calls, 0);
            EXPECT_TRUE(builder._is_committed);
            EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 1);
            if (mow) {
                EXPECT_TRUE(_engine->txn_delete_bitmap_cache().is_empty_rowset(req.txn_id,
                                                                               req.tablet_id));
            } else {
                auto marker =
                        _engine->committed_rs_mgr().get_committed_rowset(req.txn_id, req.tablet_id);
                ASSERT_TRUE(marker.has_value());
                EXPECT_EQ(marker->first, nullptr);
                EXPECT_GE(marker->second, req.txn_expiration);
            }
        }
    }
}

TEST_F(CloudLoadStreamTest, MetadataCommitFailureDoesNotRegisterMarker) {
    WriteRequest req;
    req.tablet_id = 10001;
    req.txn_id = 200;
    EmptyLoadRowsetBuilder builder(*_engine, req, nullptr);
    auto tablet = create_tablet(DUP_KEYS, false);
    builder._tablet = tablet;
    builder.set_skip_writing_rowset_metadata(false);

    auto st = builder.commit_txn();
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("injected commit failure"), std::string::npos);
    EXPECT_EQ(builder.commit_calls, 1);
    EXPECT_FALSE(builder._is_committed);
    EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 0);
    EXPECT_FALSE(_engine->committed_rs_mgr()
                         .get_committed_rowset(req.txn_id, req.tablet_id)
                         .has_value());
}

TEST_F(CloudLoadStreamTest, CloseTabletsConcurrentlyAndCollectAllResults) {
    constexpr int tablet_count = 25;
    FifoThreadPool pool(16, 64, "CloudLoadStreamCloseTest");
    LoadStreamMgr manager(1);
    manager.set_heavy_work_pool(&pool);
    RuntimeProfile profile("CloudLoadStreamCloseTest");
    PUniqueId load_id;
    IndexStream index(load_id, 1, 1, nullptr, &manager, &profile, UnixSeconds() + 3600, "", false);

    std::atomic<int> prepared {0};
    std::mutex mutex;
    std::condition_variable cv;
    int started = 0;
    int active = 0;
    int peak_active = 0;
    bool timed_out = false;
    for (int64_t tablet_id = 1; tablet_id <= tablet_count; ++tablet_id) {
        auto tablet = std::make_shared<TabletStream>(load_id, tablet_id, 1, &manager, &profile,
                                                     UnixSeconds() + 3600, "", false);
        WriteRequest req;
        req.tablet_id = tablet_id;
        auto writer = std::make_shared<LoadStreamWriter>(&req, nullptr);
        writer->_rowset_builder =
                std::make_unique<ClosingLoadRowsetBuilder>(*_engine, req, prepared, [&, tablet_id] {
                    EXPECT_EQ(prepared.load(), tablet_count);
                    std::unique_lock lock(mutex);
                    ++started;
                    ++active;
                    peak_active = std::max(peak_active, active);
                    cv.notify_all();
                    // Hold the first wave until ten tablets are closing concurrently.
                    // A serial implementation times out once instead of hanging the test.
                    if (!cv.wait_for(lock, std::chrono::seconds(5),
                                     [&] { return started >= 10 || timed_out; })) {
                        timed_out = true;
                        cv.notify_all();
                    }
                    --active;
                    return tablet_id % 2 == 0
                                   ? Status::OK()
                                   : Status::InternalError("injected tablet close failure");
                });
        writer->_rowset_writer = writer->_rowset_builder->rowset_writer();
        writer->_is_init = true;
        tablet->_load_stream_writer = std::move(writer);
        index._tablet_streams_map.emplace(tablet_id, std::move(tablet));
    }

    std::vector<int64_t> success_tablets;
    FailedTablets failed_tablets;
    index.close({}, &success_tablets, &failed_tablets);

    EXPECT_FALSE(timed_out);
    EXPECT_EQ(peak_active, 10);
    EXPECT_EQ(started, tablet_count);
    std::sort(success_tablets.begin(), success_tablets.end());
    std::vector<int64_t> expected_success;
    for (int64_t id = 2; id <= tablet_count; id += 2) {
        expected_success.push_back(id);
    }
    EXPECT_EQ(success_tablets, expected_success);
    std::vector<int64_t> failed_ids;
    for (const auto& [id, st] : failed_tablets) {
        failed_ids.push_back(id);
        EXPECT_NE(st.to_string().find("injected tablet close failure"), std::string::npos);
    }
    std::sort(failed_ids.begin(), failed_ids.end());
    std::vector<int64_t> expected_failed;
    for (int64_t id = 1; id <= tablet_count; id += 2) {
        expected_failed.push_back(id);
    }
    EXPECT_EQ(failed_ids, expected_failed);
}

} // namespace doris
