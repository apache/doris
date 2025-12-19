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

#include "io/cache/block_file_cache_ttl_mgr.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/cache_block_meta_store.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "olap/base_tablet.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "util/slice.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris::io {

namespace fs = std::filesystem;

namespace {

class FakeTablet : public BaseTablet {
public:
    FakeTablet(int64_t creation_time, int64_t ttl_seconds)
            : BaseTablet(create_meta(creation_time, ttl_seconds)) {}

    void set_creation_time(int64_t value) { _tablet_meta->set_creation_time(value); }

    void set_ttl_seconds(int64_t value) { _tablet_meta->set_ttl_seconds(value); }

    std::string tablet_path() const override { return ""; }

    bool exceed_version_limit(int32_t /*limit*/) override { return false; }

    Result<std::unique_ptr<RowsetWriter>> create_rowset_writer(RowsetWriterContext& /*context*/,
                                                               bool /*vertical*/) override {
        return ResultError(Status::NotSupported("fake tablet"));
    }

    Result<std::unique_ptr<RowsetWriter>> create_transient_rowset_writer(
            const Rowset& /*rowset*/, std::shared_ptr<PartialUpdateInfo> /*partial_update_info*/,
            int64_t /*txn_expiration*/ = 0) override {
        return ResultError(Status::NotSupported("fake tablet"));
    }

    Status capture_rs_readers(const Version& /*spec_version*/,
                              std::vector<RowSetSplits>* /*rs_splits*/,
                              const CaptureRowsetOps& /*opts*/) override {
        return Status::NotSupported("fake tablet");
    }

    Status save_delete_bitmap(const TabletTxnInfo* /*txn_info*/, int64_t /*txn_id*/,
                              DeleteBitmapPtr /*delete_bitmap*/, RowsetWriter* /*rowset_writer*/,
                              const RowsetIdUnorderedSet& /*cur_rowset_ids*/,
                              int64_t /*lock_id*/ = -1,
                              int64_t /*next_visible_version*/ = -1) override {
        return Status::NotSupported("fake tablet");
    }

    CalcDeleteBitmapExecutor* calc_delete_bitmap_executor() override { return nullptr; }

    void clear_cache() override {}

    Versions calc_missed_versions(int64_t /*spec_version*/,
                                  Versions /*existing_versions*/) const override {
        return {};
    }

    size_t tablet_footprint() override { return 0; }

private:
    static TabletMetaSharedPtr create_meta(int64_t creation_time, int64_t ttl_seconds) {
        auto schema = std::make_shared<TabletSchema>();
        auto meta = std::make_shared<TabletMeta>(schema);
        meta->set_creation_time(creation_time);
        meta->set_ttl_seconds(ttl_seconds);
        return meta;
    }
};

class FakeStorageEngine : public BaseStorageEngine {
public:
    FakeStorageEngine() : BaseStorageEngine(BaseStorageEngine::Type::LOCAL, UniqueId::gen_uid()) {}

    Status open() override { return Status::OK(); }

    void stop() override {}

    bool stopped() override { return false; }

    Status start_bg_threads(std::shared_ptr<WorkloadGroup> /*wg_sptr*/ = nullptr) override {
        return Status::OK();
    }

    Result<BaseTabletSPtr> get_tablet(int64_t tablet_id, SyncRowsetStats* /*sync_stats*/ = nullptr,
                                      bool /*force_use_cache*/ = false,
                                      bool /*cache_on_miss*/ = true) override {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _tablets.find(tablet_id);
        if (it == _tablets.end()) {
            return ResultError(Status::NotFound("tablet {} not found", tablet_id));
        }
        return it->second;
    }

    Status get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta,
                           bool /*force_use_only_cached*/ = false) override {
        auto tablet_res = get_tablet(tablet_id);
        if (!tablet_res.has_value()) {
            return tablet_res.error();
        }
        if (tablet_meta != nullptr) {
            *tablet_meta = tablet_res.value()->tablet_meta();
        }
        return Status::OK();
    }

    Status set_cluster_id(int32_t /*cluster_id*/) override { return Status::OK(); }

    void add_tablet(int64_t tablet_id, const BaseTabletSPtr& tablet) {
        std::lock_guard<std::mutex> lock(_mutex);
        _tablets[tablet_id] = tablet;
    }

private:
    std::mutex _mutex;
    std::unordered_map<int64_t, BaseTabletSPtr> _tablets;
};

std::vector<FileBlockSPtr> blocks_from_holder(const FileBlocksHolder& holder) {
    return std::vector<FileBlockSPtr>(holder.file_blocks.begin(), holder.file_blocks.end());
}

template <class Predicate>
bool wait_for_condition(Predicate&& predicate, std::chrono::milliseconds timeout,
                        std::chrono::milliseconds interval = std::chrono::milliseconds(20)) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(interval);
    }
    return predicate();
}

} // namespace

class BlockFileCacheTtlMgrTest : public testing::Test {
protected:
    void SetUp() override {
        _orig_ttl_update_interval = config::file_cache_background_ttl_info_update_interval_ms;
        _orig_ttl_gc_interval = config::file_cache_background_ttl_gc_interval_ms;
        _orig_tablet_flush_interval = config::file_cache_background_tablet_id_flush_interval_ms;

        config::file_cache_background_ttl_info_update_interval_ms = 20;
        config::file_cache_background_ttl_gc_interval_ms = 20;
        config::file_cache_background_tablet_id_flush_interval_ms = 5;

        _test_root = fs::temp_directory_path() / "block_file_cache_ttl_mgr_test";
        if (fs::exists(_test_root)) {
            fs::remove_all(_test_root);
        }
        fs::create_directories(_test_root);
        _cache_dir = (_test_root / "cache").string();
        _meta_dir = (_test_root / "meta").string();

        auto engine = std::make_unique<FakeStorageEngine>();
        _fake_engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        _meta_store = std::make_unique<CacheBlockMetaStore>(_meta_dir, 32);
        ASSERT_TRUE(_meta_store->init().ok());

        FileCacheSettings settings;
        settings.capacity = 4 * 1024 * 1024;
        settings.max_file_block_size = 1024;
        settings.ttl_queue_size = settings.capacity;
        settings.ttl_queue_elements = 128;
        settings.query_queue_size = settings.capacity;
        settings.query_queue_elements = 128;
        settings.index_queue_size = settings.capacity;
        settings.index_queue_elements = 128;
        settings.disposable_queue_size = settings.capacity;
        settings.disposable_queue_elements = 128;

        _cache = std::make_unique<BlockFileCache>(_cache_dir, settings);
        ASSERT_TRUE(_cache->initialize());
        ASSERT_TRUE(wait_for_condition([this]() { return _cache->get_async_open_success(); },
                                       std::chrono::seconds(5)));
    }

    void TearDown() override {
        _ttl_mgr.reset();
        _cache.reset();
        _meta_store.reset();

        if (_fake_engine != nullptr) {
            ExecEnv::GetInstance()->set_storage_engine(nullptr);
            _fake_engine = nullptr;
        }

        if (!_test_root.empty() && fs::exists(_test_root)) {
            fs::remove_all(_test_root);
        }

        config::file_cache_background_ttl_info_update_interval_ms = _orig_ttl_update_interval;
        config::file_cache_background_ttl_gc_interval_ms = _orig_ttl_gc_interval;
        config::file_cache_background_tablet_id_flush_interval_ms = _orig_tablet_flush_interval;
    }

    FileBlockSPtr create_block(int64_t tablet_id, const std::string& cache_key, size_t offset,
                               size_t size, UInt128Wrapper* out_hash) {
        auto hash = BlockFileCache::hash(cache_key);
        if (out_hash != nullptr) {
            *out_hash = hash;
        }

        CacheContext context;
        ReadStatistics stats;
        context.stats = &stats;
        context.cache_type = FileCacheType::NORMAL;
        context.tablet_id = tablet_id;

        auto holder = _cache->get_or_set(hash, offset, size, context);
        auto blocks = blocks_from_holder(holder);

        EXPECT_FALSE(blocks.empty());
        if (blocks.empty()) {
            return nullptr;
        }
        // Some cache configurations may split the requested range into multiple
        // file blocks. Pick the file block that contains the requested offset
        // (the one the caller will start reading from).
        auto it = std::find_if(
                blocks.begin(), blocks.end(), [offset](const FileBlockSPtr& candidate) {
                    return candidate->range().left <= offset && candidate->range().right >= offset;
                });
        EXPECT_NE(it, blocks.end());
        if (it == blocks.end()) {
            return nullptr;
        }
        auto block = *it;
        EXPECT_TRUE(block);
        EXPECT_EQ(FileCacheType::NORMAL, block->cache_type());
        EXPECT_EQ(FileBlock::get_caller_id(), block->get_or_set_downloader());
        // Only append up to the selected file block's size. The requested
        // range may be split into multiple file blocks, so appending the
        // original requested size could overflow the selected block.
        size_t write_size = block->range().size();
        std::string data(write_size, 'a');
        EXPECT_TRUE(block->append(Slice(data.data(), data.size())).ok());
        EXPECT_TRUE(block->finalize().ok());
        return block;
    }

    void persist_block_meta(int64_t tablet_id, const UInt128Wrapper& hash, size_t offset,
                            size_t size) {
        BlockMetaKey key {tablet_id, hash, offset};
        BlockMeta meta {FileCacheType::NORMAL, size, 0};
        _meta_store->put(key, meta);
        ASSERT_TRUE(wait_for_condition([this, &key]() { return _meta_store->get(key).has_value(); },
                                       std::chrono::seconds(2)));
    }

    FakeStorageEngine* fake_engine() const { return _fake_engine; }

    std::unique_ptr<CacheBlockMetaStore> _meta_store;
    std::unique_ptr<BlockFileCache> _cache;
    std::unique_ptr<BlockFileCacheTtlMgr> _ttl_mgr;
    FakeStorageEngine* _fake_engine = nullptr;

private:
    fs::path _test_root;
    std::string _cache_dir;
    std::string _meta_dir;
    int64_t _orig_ttl_update_interval = 0;
    int64_t _orig_ttl_gc_interval = 0;
    int64_t _orig_tablet_flush_interval = 0;
};

TEST_F(BlockFileCacheTtlMgrTest, BlocksSwitchToTtlWhenTabletHasTtl) {
    constexpr int64_t kTabletId = 1001;
    auto tablet = std::make_shared<FakeTablet>(UnixSeconds(), 60);
    fake_engine()->add_tablet(kTabletId, tablet);

    UInt128Wrapper hash;
    auto block = create_block(kTabletId, "ttl-tablet", 0, 1024, &hash);
    persist_block_meta(kTabletId, hash, block->range().left, block->range().size());

    _ttl_mgr = std::make_unique<BlockFileCacheTtlMgr>(_cache.get(), _meta_store.get());
    _ttl_mgr->register_tablet_id(kTabletId);

    ASSERT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::TTL; },
                                   std::chrono::seconds(5)));
}

TEST_F(BlockFileCacheTtlMgrTest, ExpiredTabletMovesBlocksBackToNormal) {
    constexpr int64_t kTabletId = 2002;
    auto tablet = std::make_shared<FakeTablet>(UnixSeconds(), 120);
    fake_engine()->add_tablet(kTabletId, tablet);

    UInt128Wrapper hash;
    auto block = create_block(kTabletId, "ttl-expire", 0, 2048, &hash);
    persist_block_meta(kTabletId, hash, block->range().left, block->range().size());

    _ttl_mgr = std::make_unique<BlockFileCacheTtlMgr>(_cache.get(), _meta_store.get());
    _ttl_mgr->register_tablet_id(kTabletId);

    ASSERT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::TTL; },
                                   std::chrono::seconds(5)));

    tablet->set_creation_time(UnixSeconds() - 120);
    tablet->set_ttl_seconds(1);
    _ttl_mgr->register_tablet_id(kTabletId);

    ASSERT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::NORMAL; },
                                   std::chrono::seconds(5)));
}

} // namespace doris::io
