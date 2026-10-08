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

#include <memory>
#include <unordered_map>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cpp/sync_point.h"
#include "io/io_common.h"
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

class IndexDiskUsageCaptureTest : public testing::Test {
protected:
    IndexDiskUsageCaptureTest() : _engine(EngineOptions {}) {}

    void SetUp() override {
        auto* sp = SyncPoint::get_instance();
        sp->clear_all_call_backs();
        sp->enable_processing();
    }

    void TearDown() override {
        auto* sp = SyncPoint::get_instance();
        sp->disable_processing();
        sp->clear_all_call_backs();
    }

    RowsetSharedPtr create_rowset(Version version) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_version(version);
        rs_meta->set_rowset_id(_engine.next_rowset_id());
        RowsetSharedPtr rowset;
        const Status st = RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset);
        EXPECT_TRUE(st.ok()) << st;
        return rowset;
    }

    // A cached cloud tablet with one rowset per version through `max_version`.
    CloudTabletSPtr create_tablet(int64_t max_version) {
        auto tablet_meta = std::make_shared<TabletMeta>(
                1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6,
                std::unordered_map<uint32_t, uint32_t> {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F);
        auto tablet = std::make_shared<CloudTablet>(_engine, std::move(tablet_meta));
        std::vector<RowsetSharedPtr> rowsets {create_rowset({0, 1})};
        for (int64_t version = 2; version <= max_version; ++version) {
            rowsets.push_back(create_rowset({version, version}));
        }
        std::unique_lock wlock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), false, wlock, false);
        return tablet;
    }

    // Answers each rowset sync as meta-service does once `compacted` replaced the rowsets it
    // covers.
    static void sync_returns(const CloudTabletSPtr& tablet, const RowsetSharedPtr& compacted,
                             int* syncs) {
        SyncPoint::get_instance()->set_call_back(
                "CloudMetaMgr::sync_tablet_rowsets", [tablet, compacted, syncs](auto&& outcome) {
                    ++*syncs;
                    {
                        std::unique_lock wlock(tablet->get_header_lock());
                        tablet->add_rowsets({compacted}, true, wlock, false);
                    }
                    auto* ret = try_any_cast_ret<Status>(outcome);
                    ret->first = Status::OK();
                    ret->second = true;
                });
    }

    static std::vector<Version> versions_of(const std::vector<RowsetSharedPtr>& rowsets) {
        std::vector<Version> versions;
        versions.reserve(rowsets.size());
        for (const auto& rowset : rowsets) {
            versions.push_back(rowset->version());
        }
        return versions;
    }

    CloudStorageEngine _engine;
};

// A compaction on another backend can replace the rowsets of a cached tablet without changing the
// visible version, so the scan synchronizes with meta-service even when the cache is current.
TEST_F(IndexDiskUsageCaptureTest, CaptureSyncsCompactionAtCachedVersion) {
    auto tablet = create_tablet(3);
    auto compacted = create_rowset({2, 3});
    int syncs = 0;
    sync_returns(tablet, compacted, &syncs);

    auto rowsets = IndexDiskUsageReader::capture_rowsets(tablet, 3);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();
    EXPECT_EQ(1, syncs);
    EXPECT_EQ((std::vector<Version> {{0, 1}, {2, 3}}), versions_of(rowsets.value()));
    EXPECT_EQ(compacted->rowset_id(), rowsets.value().back()->rowset_id());
}

// A compaction that also covers a version newer than the scan version keeps the rowsets it
// replaced as stale rowsets, so the scan version can still be captured.
TEST_F(IndexDiskUsageCaptureTest, CaptureSurvivesCompactionPastScanVersion) {
    auto tablet = create_tablet(3);
    int syncs = 0;
    sync_returns(tablet, create_rowset({2, 4}), &syncs);

    auto rowsets = IndexDiskUsageReader::capture_rowsets(tablet, 3);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();
    EXPECT_EQ(1, syncs);
    EXPECT_NE(nullptr, tablet->get_rowset_by_version(Version(2, 4)));
    EXPECT_EQ((std::vector<Version> {{0, 1}, {2, 2}, {3, 3}}), versions_of(rowsets.value()));
}

} // namespace doris
