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

#include "cloud/cloud_internal_service.h"

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <unordered_map>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cpp/sync_point.h"
#include "gen_cpp/Status_types.h"
#include "storage/tablet/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {

namespace {

class TestClosure final : public google::protobuf::Closure {
public:
    explicit TestClosure(std::function<void()> callback) : _callback(std::move(callback)) {}

    void Run() override {
        if (_callback) {
            _callback();
        }
    }

private:
    std::function<void()> _callback;
};

} // namespace

class CloudInternalServiceTest : public testing::Test {
public:
    CloudInternalServiceTest() : _engine(CloudStorageEngine(EngineOptions())) {}

protected:
    static constexpr int64_t kSyncedTabletId = 90001;
    static constexpr int64_t kFailedTabletId = 90002;
    static constexpr int64_t kUncachedTabletId = 90003;

    void SetUp() override {
        auto sp = SyncPoint::get_instance();
        sp->clear_all_call_backs();
        sp->enable_processing();

        sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets",
                          [](auto&& args) { try_any_cast_ret<Status>(args)->second = true; });
        sp->set_call_back("CloudMetaMgr::get_tablet_meta", [this](auto&& args) {
            const auto tablet_id = try_any_cast<int64_t>(args[0]);
            auto* tablet_meta_ptr = try_any_cast<TabletMetaSharedPtr*>(args[1]);
            auto* ret = try_any_cast_ret<Status>(args);

            auto& call_count = _tablet_meta_call_count[tablet_id];
            ++call_count;
            if (tablet_id == kFailedTabletId && call_count >= 2) {
                ret->first = Status::InternalError<false>("injected sync_meta failure");
                ret->second = true;
                return;
            }

            *tablet_meta_ptr =
                    create_tablet_meta(tablet_id, call_count >= 2 ? "time_series" : "size_based");
            ret->second = true;
        });
    }

    void TearDown() override {
        auto sp = SyncPoint::get_instance();
        sp->disable_processing();
        sp->clear_all_call_backs();
    }

    TabletMetaSharedPtr create_tablet_meta(int64_t tablet_id, std::string compaction_policy) {
        TTabletSchema tablet_schema;
        TColumn column;
        column.__set_column_name("c1");
        column.__set_column_type(TColumnType());
        column.column_type.__set_type(TPrimitiveType::INT);
        column.__set_is_key(true);
        column.__set_aggregation_type(TAggregationType::NONE);
        column.__set_col_unique_id(0);
        tablet_schema.__set_columns({column});
        tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);

        std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id = {{0, 0}};
        auto tablet_meta = std::make_shared<TabletMeta>(
                1, 2, tablet_id, 15674, 4, 5, tablet_schema, 1, col_ordinal_to_unique_id,
                UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F);
        tablet_meta->set_compaction_policy(std::move(compaction_policy));
        return tablet_meta;
    }

    CloudStorageEngine _engine;
    std::unordered_map<int64_t, int> _tablet_meta_call_count;
};

TEST_F(CloudInternalServiceTest, TestSyncTabletMetaCountsSyncedSkippedAndFailedTablets) {
    auto synced_res = _engine.tablet_mgr().get_tablet(kSyncedTabletId);
    ASSERT_TRUE(synced_res.has_value()) << synced_res.error();
    auto failed_res = _engine.tablet_mgr().get_tablet(kFailedTabletId);
    ASSERT_TRUE(failed_res.has_value()) << failed_res.error();

    auto cached_synced_tablet = synced_res.value();
    auto cached_failed_tablet = failed_res.value();
    ASSERT_EQ("size_based", cached_synced_tablet->tablet_meta()->compaction_policy());
    ASSERT_EQ("size_based", cached_failed_tablet->tablet_meta()->compaction_policy());

    CloudInternalServiceImpl service(_engine, nullptr);
    PSyncTabletMetaRequest request;
    request.add_tablet_ids(kSyncedTabletId);
    request.add_tablet_ids(kUncachedTabletId);
    request.add_tablet_ids(kFailedTabletId);
    PSyncTabletMetaResponse response;

    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    TestClosure closure([&]() {
        std::lock_guard lock(mutex);
        done = true;
        cv.notify_all();
    });

    service.sync_tablet_meta(nullptr, &request, &response, &closure);

    std::unique_lock lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&] { return done; }));

    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(TStatusCode::OK, response.status().status_code()) << response.status().DebugString();
    EXPECT_EQ(1, response.synced_tablets());
    EXPECT_EQ(1, response.skipped_tablets());
    EXPECT_EQ(1, response.failed_tablets());
    EXPECT_EQ("time_series", cached_synced_tablet->tablet_meta()->compaction_policy());
    EXPECT_EQ("size_based", cached_failed_tablet->tablet_meta()->compaction_policy());
    EXPECT_EQ(nullptr, _engine.tablet_mgr().get_tablet_if_cached(kUncachedTabletId));
}

} // namespace doris
