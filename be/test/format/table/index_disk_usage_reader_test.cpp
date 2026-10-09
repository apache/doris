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

#include "format/table/index_disk_usage_reader.h"

#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cpp/sync_point.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet_meta.h"

namespace doris {

namespace {

std::unique_ptr<IndexDiskUsageReader> make_reader(RuntimeState* state) {
    TIndexDiskUsageMetadataParams params;
    params.__set_level("tablet");
    TMetaScanRange scan_range;
    scan_range.__set_index_disk_usage_params(params);
    return std::make_unique<IndexDiskUsageReader>(std::vector<SlotDescriptor*> {}, state, nullptr,
                                                  scan_range);
}

} // namespace

// The reader tags every index file read with its query, so the file cache and the remote scan
// cache write limiter treat index_disk_usage like any other scan of that query.
TEST(IndexDiskUsageReaderTest, InitReaderBuildsQueryIoContext) {
    RuntimeState state;
    auto reader = make_reader(&state);
    const Status st = reader->init_reader();
    ASSERT_TRUE(st.ok()) << st;
    const io::IOContext* io_ctx = reader->options().io_ctx;
    ASSERT_NE(io_ctx, nullptr);
    EXPECT_EQ(ReaderType::READER_QUERY, io_ctx->reader_type);
    EXPECT_EQ(&state.query_id(), io_ctx->query_id);
    EXPECT_TRUE(io_ctx->is_inverted_index);
    EXPECT_FALSE(io_ctx->inverted_index_snii_read_no_write_file_cache);
}

// The SNII cache write policy of the session applies to this scan as it does to a tablet read.
TEST(IndexDiskUsageReaderTest, InitReaderCopiesSniiCachePolicy) {
    RuntimeState state;
    TQueryOptions query_options;
    query_options.__set_inverted_index_snii_read_no_write_file_cache(true);
    state.set_query_options(query_options);
    auto reader = make_reader(&state);
    const Status st = reader->init_reader();
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_TRUE(reader->options().io_ctx->inverted_index_snii_read_no_write_file_cache);
}

// Reads of a TTL table are classified by the tablet TTL, so each tablet gets its own context.
TEST(IndexDiskUsageReaderTest, TabletIoContextCarriesTtl) {
    TUniqueId query_id;
    io::IOContext root;
    root.reader_type = ReaderType::READER_QUERY;
    root.query_id = &query_id;
    const io::IOContext tablet_ctx = IndexDiskUsageReader::tablet_io_context(root, 3600);
    EXPECT_EQ(3600, tablet_ctx.expiration_time);
    EXPECT_EQ(ReaderType::READER_QUERY, tablet_ctx.reader_type);
    EXPECT_EQ(&query_id, tablet_ctx.query_id);
    EXPECT_EQ(0, root.expiration_time);
}

// Drives IndexDiskUsageReader::capture_tablet_rowsets through a cloud tablet manager whose
// meta-service calls are replaced, so that the calls can be counted.
class IndexDiskUsageCaptureTest : public testing::Test {
protected:
    static constexpr int64_t kTabletId = 99001;

    void SetUp() override {
        ExecEnv::GetInstance()->set_storage_engine(
                std::make_unique<CloudStorageEngine>(EngineOptions {}));
        auto* sp = SyncPoint::get_instance();
        sp->clear_all_call_backs();
        sp->enable_processing();
        sp->set_call_back("CloudMetaMgr::get_tablet_meta", [](auto&& args) {
            *try_any_cast<TabletMetaSharedPtr*>(args[1]) = std::make_shared<TabletMeta>(
                    1, 2, kTabletId, 15674, 4, 5, TTabletSchema(), 6,
                    std::unordered_map<uint32_t, uint32_t> {{7, 8}}, UniqueId(9, 10),
                    TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F);
            try_any_cast_ret<Status>(args)->second = true;
        });
        sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [this](auto&& args) {
            ++_syncs;
            _on_sync(try_any_cast<CloudTablet*>(args[0]));
            try_any_cast_ret<Status>(args)->second = true;
        });
    }

    void TearDown() override {
        auto* sp = SyncPoint::get_instance();
        sp->disable_processing();
        sp->clear_all_call_backs();
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    static RowsetSharedPtr create_rowset(Version version) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_version(version);
        rs_meta->set_rowset_id(ExecEnv::GetInstance()->storage_engine().next_rowset_id());
        RowsetSharedPtr rowset;
        const Status st = RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset);
        EXPECT_TRUE(st.ok()) << st;
        return rowset;
    }

    static void add_rowsets(CloudTablet* tablet, std::vector<RowsetSharedPtr> rowsets,
                            bool version_overlap) {
        std::unique_lock wlock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), version_overlap, wlock, false);
    }

    // Makes the next meta-service sync deliver one rowset per version through `max_version`.
    void sync_delivers_versions(int64_t max_version) {
        _on_sync = [max_version](CloudTablet* tablet) {
            std::vector<RowsetSharedPtr> rowsets {create_rowset({0, 1})};
            for (int64_t version = 2; version <= max_version; ++version) {
                rowsets.push_back(create_rowset({version, version}));
            }
            add_rowsets(tablet, std::move(rowsets), false);
        };
    }

    // Makes the next meta-service sync deliver a compaction output that replaces cached rowsets.
    void sync_delivers_compaction(Version output) {
        _on_sync = [output](CloudTablet* tablet) {
            add_rowsets(tablet, {create_rowset(output)}, true);
        };
    }

    static std::vector<Version> versions_of(const std::vector<RowsetSharedPtr>& rowsets) {
        std::vector<Version> versions;
        versions.reserve(rowsets.size());
        for (const auto& rowset : rowsets) {
            versions.push_back(rowset->version());
        }
        return versions;
    }

    int _syncs = 0;
    std::function<void(CloudTablet*)> _on_sync = [](CloudTablet*) {};
};

// The lookup of a tablet that is not cached loads and synchronizes it, so the scan adds no second
// meta-service sync.
TEST_F(IndexDiskUsageCaptureTest, ColdLookupSyncsOnce) {
    sync_delivers_versions(3);

    auto captured = IndexDiskUsageReader::capture_tablet_rowsets(kTabletId, 3);
    ASSERT_TRUE(captured.has_value()) << captured.error();
    EXPECT_EQ(1, _syncs);
    EXPECT_EQ((std::vector<Version> {{0, 1}, {2, 2}, {3, 3}}), versions_of(captured->rowsets));
}

// A lookup can join a load that began before the scan version was committed, so a loaded tablet
// that lacks the scan version synchronizes up to it.
TEST_F(IndexDiskUsageCaptureTest, ColdLookupSyncsToTheScanVersionWhenTheLoadIsOlder) {
    _on_sync = [this](CloudTablet* tablet) {
        if (_syncs == 1) {
            add_rowsets(tablet,
                        {create_rowset({0, 1}), create_rowset({2, 2}), create_rowset({3, 3})},
                        false);
        } else {
            add_rowsets(tablet, {create_rowset({4, 4})}, false);
        }
    };

    auto captured = IndexDiskUsageReader::capture_tablet_rowsets(kTabletId, 4);
    ASSERT_TRUE(captured.has_value()) << captured.error();
    EXPECT_EQ(2, _syncs);
    EXPECT_EQ((std::vector<Version> {{0, 1}, {2, 2}, {3, 3}, {4, 4}}),
              versions_of(captured->rowsets));
}

// A compaction on another backend can replace the rowsets of a cached tablet without changing the
// visible version, so a cached tablet synchronizes again even when its cache is current.
TEST_F(IndexDiskUsageCaptureTest, CachedLookupSyncsCompactionAtCachedVersion) {
    sync_delivers_versions(3);
    ASSERT_TRUE(IndexDiskUsageReader::capture_tablet_rowsets(kTabletId, 3).has_value());
    ASSERT_EQ(1, _syncs);

    sync_delivers_compaction({2, 3});
    auto captured = IndexDiskUsageReader::capture_tablet_rowsets(kTabletId, 3);
    ASSERT_TRUE(captured.has_value()) << captured.error();
    EXPECT_EQ(2, _syncs);
    EXPECT_EQ((std::vector<Version> {{0, 1}, {2, 3}}), versions_of(captured->rowsets));
}

// A compaction that also covers a version newer than the scan version keeps the rowsets it
// replaced as stale rowsets, so the scan version can still be captured.
TEST_F(IndexDiskUsageCaptureTest, CompactionPastScanVersionKeepsScanVersionReadable) {
    sync_delivers_versions(3);
    ASSERT_TRUE(IndexDiskUsageReader::capture_tablet_rowsets(kTabletId, 3).has_value());

    sync_delivers_compaction({2, 4});
    auto captured = IndexDiskUsageReader::capture_tablet_rowsets(kTabletId, 3);
    ASSERT_TRUE(captured.has_value()) << captured.error();
    EXPECT_EQ(2, _syncs);
    EXPECT_NE(nullptr, captured->tablet->get_rowset_by_version(Version(2, 4)));
    EXPECT_EQ((std::vector<Version> {{0, 1}, {2, 2}, {3, 3}}), versions_of(captured->rowsets));
}

} // namespace doris
