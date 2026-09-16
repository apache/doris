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
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/cache_block_meta_store.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "runtime/exec_env.h"
#include "runtime/workload_group/workload_group_fwd.h"
#include "storage/storage_engine.h"
#include "storage/tablet/base_tablet.h"
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
        _get_tablet_meta_call_count.fetch_add(1, std::memory_order_relaxed);
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

    int64_t get_tablet_meta_call_count() const {
        return _get_tablet_meta_call_count.load(std::memory_order_relaxed);
    }

private:
    std::mutex _mutex;
    std::unordered_map<int64_t, BaseTabletSPtr> _tablets;
    std::atomic<int64_t> _get_tablet_meta_call_count {0};
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

        // initialize() starts a TTL manager of its own against this cache. Every case below
        // drives one it owns, and two of them converting the same blocks makes both the
        // conversions and the scan counts nondeterministic -- a block can be demoted before
        // the case has finished setting up the state it means to exercise.
        if (auto* cache_owned_ttl_mgr = _cache->get_ttl_mgr()) {
            cache_owned_ttl_mgr->stop();
        }
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
                               size_t size, UInt128Wrapper* out_hash,
                               FileCacheType cache_type = FileCacheType::NORMAL,
                               uint64_t expiration_time = 0) {
        auto hash = BlockFileCache::hash(cache_key);
        if (out_hash != nullptr) {
            *out_hash = hash;
        }

        CacheContext context;
        ReadStatistics stats;
        context.stats = &stats;
        context.cache_type = cache_type;
        context.expiration_time = expiration_time;
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
        EXPECT_EQ(cache_type, block->cache_type());
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
                            size_t size, FileCacheType cache_type = FileCacheType::NORMAL,
                            uint64_t expiration_time = 0) {
        BlockMetaKey key {tablet_id, hash, offset};
        BlockMeta meta {cache_type, size, expiration_time};
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

TEST_F(BlockFileCacheTtlMgrTest, NonTtlTabletWithoutPriorTtlInfoSkipsBlockScan) {
    config::file_cache_background_ttl_info_update_interval_ms = 100;

    constexpr int64_t kTabletId = 3003;
    auto tablet = std::make_shared<FakeTablet>(UnixSeconds(), 0);
    fake_engine()->add_tablet(kTabletId, tablet);

    UInt128Wrapper hash;
    auto block = create_block(kTabletId, "non-ttl-tablet", 0, 1024, &hash);
    persist_block_meta(kTabletId, hash, block->range().left, block->range().size());

    // Held by value in the callback: the background threads outlive this stack frame, and
    // neither the guard nor disable_processing() synchronizes with a callback in flight.
    auto block_scan_count = std::make_shared<std::atomic<int64_t>>(0);
    auto* sync_point = SyncPoint::get_instance();
    sync_point->clear_all_call_backs();
    sync_point->clear_trace();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "BlockFileCacheTtlMgr::get_file_blocks_from_tablet_id",
            [block_scan_count](std::vector<std::any>&& args) {
                if (doris::try_any_cast<int64_t>(args[0]) == kTabletId) {
                    block_scan_count->fetch_add(1, std::memory_order_relaxed);
                }
            },
            &guard);
    sync_point->enable_processing();

    _ttl_mgr = std::make_unique<BlockFileCacheTtlMgr>(_cache.get(), _meta_store.get());
    _ttl_mgr->register_tablet_id(kTabletId);

    bool update_thread_observed = wait_for_condition(
            [this]() { return fake_engine()->get_tablet_meta_call_count() >= 2; },
            std::chrono::seconds(5));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // Join the background threads before the callback and its captures go away.
    _ttl_mgr.reset();
    sync_point->disable_processing();
    sync_point->clear_trace();

    EXPECT_TRUE(update_thread_observed);
    EXPECT_EQ(0, block_scan_count->load(std::memory_order_relaxed));
    EXPECT_EQ(FileCacheType::NORMAL, block->cache_type());
}

TEST_F(BlockFileCacheTtlMgrTest, PeriodicReconcileDemotesTtlBlockWithoutPriorTtlInfo) {
    constexpr int64_t kTabletId = 4004;
    auto tablet = std::make_shared<FakeTablet>(UnixSeconds(), 0);
    fake_engine()->add_tablet(kTabletId, tablet);

    const uint64_t expiration_time = UnixSeconds() + 3600;
    UInt128Wrapper hash;
    auto block = create_block(kTabletId, "ttl-block-without-info", 0, 1024, &hash,
                              FileCacheType::TTL, expiration_time);
    persist_block_meta(kTabletId, hash, block->range().left, block->range().size(),
                       FileCacheType::TTL, expiration_time);
    ASSERT_EQ(FileCacheType::TTL, block->cache_type());

    // Held by value in the callback: the background threads outlive this stack frame, and
    // neither the guard nor disable_processing() synchronizes with a callback in flight.
    auto block_scan_count = std::make_shared<std::atomic<int64_t>>(0);
    auto* sync_point = SyncPoint::get_instance();
    sync_point->clear_all_call_backs();
    sync_point->clear_trace();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "BlockFileCacheTtlMgr::get_file_blocks_from_tablet_id",
            [block_scan_count](std::vector<std::any>&& args) {
                if (doris::try_any_cast<int64_t>(args[0]) == kTabletId) {
                    block_scan_count->fetch_add(1, std::memory_order_relaxed);
                }
            },
            &guard);
    sync_point->enable_processing();

    _ttl_mgr = std::make_unique<BlockFileCacheTtlMgr>(_cache.get(), _meta_store.get());
    _ttl_mgr->register_tablet_id(kTabletId);

    bool demoted =
            wait_for_condition([&]() { return block->cache_type() == FileCacheType::NORMAL; },
                               std::chrono::seconds(5));
    // Join the background threads before the callback and its captures go away.
    _ttl_mgr.reset();
    sync_point->disable_processing();
    sync_point->clear_trace();

    EXPECT_TRUE(demoted);
    EXPECT_GE(block_scan_count->load(std::memory_order_relaxed), 1);
}

TEST_F(BlockFileCacheTtlMgrTest, TabletTtlRemovedMovesBlocksBackToNormal) {
    constexpr int64_t kTabletId = 5005;
    auto tablet = std::make_shared<FakeTablet>(UnixSeconds(), 120);
    fake_engine()->add_tablet(kTabletId, tablet);

    UInt128Wrapper hash;
    auto block = create_block(kTabletId, "ttl-remove", 0, 1024, &hash);
    persist_block_meta(kTabletId, hash, block->range().left, block->range().size());

    _ttl_mgr = std::make_unique<BlockFileCacheTtlMgr>(_cache.get(), _meta_store.get());
    _ttl_mgr->register_tablet_id(kTabletId);

    ASSERT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::TTL; },
                                   std::chrono::seconds(5)));

    tablet->set_ttl_seconds(0);
    _ttl_mgr->register_tablet_id(kTabletId);

    ASSERT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::NORMAL; },
                                   std::chrono::seconds(5)));
}

TEST_F(BlockFileCacheTtlMgrTest, ExpiredTtlExtendedMovesBlocksBackToTtl) {
    constexpr int64_t kTabletId = 6006;
    auto tablet = std::make_shared<FakeTablet>(UnixSeconds(), 120);
    fake_engine()->add_tablet(kTabletId, tablet);

    UInt128Wrapper hash;
    auto block = create_block(kTabletId, "ttl-extend-after-expire", 0, 1024, &hash);
    persist_block_meta(kTabletId, hash, block->range().left, block->range().size());

    _ttl_mgr = std::make_unique<BlockFileCacheTtlMgr>(_cache.get(), _meta_store.get());
    _ttl_mgr->register_tablet_id(kTabletId);

    ASSERT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::TTL; },
                                   std::chrono::seconds(5)));

    // Let the TTL expire. The block goes back to NORMAL while the manager keeps a non-zero TTL
    // recorded for the tablet, which is the state that used to wedge the promotion path.
    tablet->set_creation_time(UnixSeconds() - 120);
    tablet->set_ttl_seconds(1);
    ASSERT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::NORMAL; },
                                   std::chrono::seconds(5)));

    // Extending an already expired TTL to one that has not expired has to bring the block back.
    tablet->set_ttl_seconds(30758400);
    ASSERT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::TTL; },
                                   std::chrono::seconds(5)));
}

TEST_F(BlockFileCacheTtlMgrTest, ExtendedTtlThatIsStillExpiredKeepsBlocksNormal) {
    constexpr int64_t kTabletId = 7007;
    const int64_t creation_time = UnixSeconds() - 7200;
    auto tablet = std::make_shared<FakeTablet>(creation_time, 60);
    fake_engine()->add_tablet(kTabletId, tablet);

    UInt128Wrapper hash;
    auto block = create_block(kTabletId, "ttl-extend-still-expired", 0, 1024, &hash);
    persist_block_meta(kTabletId, hash, block->range().left, block->range().size());

    _ttl_mgr = std::make_unique<BlockFileCacheTtlMgr>(_cache.get(), _meta_store.get());
    _ttl_mgr->register_tablet_id(kTabletId);

    int64_t call_count = fake_engine()->get_tablet_meta_call_count();
    ASSERT_TRUE(wait_for_condition(
            [&]() { return fake_engine()->get_tablet_meta_call_count() >= call_count + 2; },
            std::chrono::seconds(5)));
    ASSERT_EQ(FileCacheType::NORMAL, block->cache_type());

    // A longer TTL that is still in the past must not promote anything.
    tablet->set_ttl_seconds(120);
    call_count = fake_engine()->get_tablet_meta_call_count();
    ASSERT_TRUE(wait_for_condition(
            [&]() { return fake_engine()->get_tablet_meta_call_count() >= call_count + 3; },
            std::chrono::seconds(5)));
    EXPECT_EQ(FileCacheType::NORMAL, block->cache_type());
}

TEST_F(BlockFileCacheTtlMgrTest, RewritingTtlToAnotherValidValueDoesNotRescanBlocks) {
    constexpr int64_t kTabletId = 8008;
    auto tablet = std::make_shared<FakeTablet>(UnixSeconds(), 3600);
    fake_engine()->add_tablet(kTabletId, tablet);

    UInt128Wrapper hash;
    auto block = create_block(kTabletId, "ttl-rewrite-valid", 0, 1024, &hash);
    persist_block_meta(kTabletId, hash, block->range().left, block->range().size());

    _ttl_mgr = std::make_unique<BlockFileCacheTtlMgr>(_cache.get(), _meta_store.get());
    _ttl_mgr->register_tablet_id(kTabletId);

    ASSERT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::TTL; },
                                   std::chrono::seconds(5)));

    // The promotion is recorded only after every block has been converted, so the manager has
    // not necessarily finished with the tablet at the moment the type flips. Let a couple of
    // rounds pass before counting, or the tail of the promotion is charged to the rewrites.
    int64_t settled_after = fake_engine()->get_tablet_meta_call_count();
    ASSERT_TRUE(wait_for_condition(
            [&]() { return fake_engine()->get_tablet_meta_call_count() >= settled_after + 2; },
            std::chrono::seconds(5)));

    // Held by value in the callback: the background threads outlive this stack frame, and
    // neither the guard nor disable_processing() synchronizes with a callback in flight.
    auto block_scan_count = std::make_shared<std::atomic<int64_t>>(0);
    auto* sync_point = SyncPoint::get_instance();
    sync_point->clear_all_call_backs();
    sync_point->clear_trace();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "BlockFileCacheTtlMgr::get_file_blocks_from_tablet_id",
            [block_scan_count](std::vector<std::any>&& args) {
                if (doris::try_any_cast<int64_t>(args[0]) == kTabletId) {
                    block_scan_count->fetch_add(1, std::memory_order_relaxed);
                }
            },
            &guard);
    sync_point->enable_processing();

    // Automated jobs rewrite this property regularly. As long as the tablet stays in the same
    // state, none of those rewrites may trigger another walk of the meta store.
    for (int64_t ttl : {7200, 1800, 5400}) {
        tablet->set_ttl_seconds(ttl);
        int64_t call_count = fake_engine()->get_tablet_meta_call_count();
        ASSERT_TRUE(wait_for_condition(
                [&]() { return fake_engine()->get_tablet_meta_call_count() >= call_count + 2; },
                std::chrono::seconds(5)));
    }

    // Join the background threads before the callback and its captures go away.
    _ttl_mgr.reset();
    sync_point->disable_processing();
    sync_point->clear_trace();

    EXPECT_EQ(0, block_scan_count->load(std::memory_order_relaxed));
    EXPECT_EQ(FileCacheType::TTL, block->cache_type());
}

TEST_F(BlockFileCacheTtlMgrTest, TtlExtensionWinsOverConcurrentExpirationScan) {
    constexpr int64_t kTabletId = 9009;
    auto tablet = std::make_shared<FakeTablet>(UnixSeconds(), 3600);
    fake_engine()->add_tablet(kTabletId, tablet);

    UInt128Wrapper hash;
    auto block = create_block(kTabletId, "ttl-extend-during-demote", 0, 1024, &hash);
    persist_block_meta(kTabletId, hash, block->range().left, block->range().size());

    _ttl_mgr = std::make_unique<BlockFileCacheTtlMgr>(_cache.get(), _meta_store.get());
    _ttl_mgr->register_tablet_id(kTabletId);

    ASSERT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::TTL; },
                                   std::chrono::seconds(5)));

    // Stall the demotion scan midway so the TTL can be extended underneath it, reproducing the
    // window where the expiration check acts on a view of the tablet that is already stale.
    // Held by value in the callback rather than captured by reference: the background threads
    // outlive this stack frame, and neither the guard nor disable_processing() synchronizes
    // with a callback already in flight. A callback that stalls makes that window wide.
    auto demote_scan_entered = std::make_shared<std::atomic<bool>>(false);
    auto release_demote_scan = std::make_shared<std::atomic<bool>>(false);
    auto* sync_point = SyncPoint::get_instance();
    sync_point->clear_all_call_backs();
    sync_point->clear_trace();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "BlockFileCacheTtlMgr::get_file_blocks_from_tablet_id",
            [demote_scan_entered, release_demote_scan](std::vector<std::any>&& args) {
                if (doris::try_any_cast<int64_t>(args[0]) != kTabletId) {
                    return;
                }
                if (demote_scan_entered->exchange(true, std::memory_order_acq_rel)) {
                    return;
                }
                while (!release_demote_scan->load(std::memory_order_acquire)) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }
            },
            &guard);
    sync_point->enable_processing();

    tablet->set_creation_time(UnixSeconds() - 3600);
    tablet->set_ttl_seconds(1);

    bool scan_stalled = wait_for_condition(
            [&]() { return demote_scan_entered->load(std::memory_order_acquire); },
            std::chrono::seconds(10));

    // Extend the TTL while the demotion is still in flight.
    tablet->set_creation_time(UnixSeconds());
    tablet->set_ttl_seconds(30758400);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // Must come before the join below, or stop() waits on a thread parked in the callback.
    release_demote_scan->store(true, std::memory_order_release);

    bool ends_as_ttl = wait_for_condition(
            [&]() { return block->cache_type() == FileCacheType::TTL; }, std::chrono::seconds(10));
    _ttl_mgr.reset();
    sync_point->disable_processing();
    sync_point->clear_trace();

    ASSERT_TRUE(scan_stalled);
    EXPECT_TRUE(ends_as_ttl);
}

// _ttl_info_map is rebuilt in memory only, while the cache type of each block survives on disk.
// A tablet whose TTL expired while this BE was down therefore comes back with TTL blocks and no
// recorded state, and nothing later in the tablet's life re-examines them.
TEST_F(BlockFileCacheTtlMgrTest, ExpiredTabletDemotesTtlBlocksRestoredFromDisk) {
    constexpr int64_t kTabletId = 10010;
    const int64_t creation_time = UnixSeconds() - 7200;
    auto tablet = std::make_shared<FakeTablet>(creation_time, 60);
    fake_engine()->add_tablet(kTabletId, tablet);

    const uint64_t expiration_time = static_cast<uint64_t>(creation_time) + 60;
    UInt128Wrapper hash;
    auto block = create_block(kTabletId, "ttl-restored-expired", 0, 1024, &hash, FileCacheType::TTL,
                              expiration_time);
    // Asserted before the block is published to the meta store: a scan can only reach a block
    // that is listed there, so until then no manager can convert it out from under us.
    ASSERT_EQ(FileCacheType::TTL, block->cache_type());
    persist_block_meta(kTabletId, hash, block->range().left, block->range().size(),
                       FileCacheType::TTL, expiration_time);

    _ttl_mgr = std::make_unique<BlockFileCacheTtlMgr>(_cache.get(), _meta_store.get());
    _ttl_mgr->register_tablet_id(kTabletId);

    EXPECT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::NORMAL; },
                                   std::chrono::seconds(5)));
}

// Blocks can still land in the TTL queue after a tablet's existing ones were demoted, and the
// recorded state is per tablet, so it cannot tell that they have. They have to be collected too.
TEST_F(BlockFileCacheTtlMgrTest, ExpiredTabletDemotesTtlBlocksCachedAfterDemotion) {
    constexpr int64_t kTabletId = 11011;
    auto tablet = std::make_shared<FakeTablet>(UnixSeconds(), 120);
    fake_engine()->add_tablet(kTabletId, tablet);

    UInt128Wrapper hash;
    auto block = create_block(kTabletId, "ttl-expire-then-cache", 0, 1024, &hash);
    persist_block_meta(kTabletId, hash, block->range().left, block->range().size());

    _ttl_mgr = std::make_unique<BlockFileCacheTtlMgr>(_cache.get(), _meta_store.get());
    _ttl_mgr->register_tablet_id(kTabletId);

    ASSERT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::TTL; },
                                   std::chrono::seconds(5)));

    tablet->set_creation_time(UnixSeconds() - 120);
    tablet->set_ttl_seconds(1);
    ASSERT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::NORMAL; },
                                   std::chrono::seconds(5)));

    // A fresh block lands in the TTL queue after the demotion.
    const uint64_t expiration_time = UnixSeconds() + 3600;
    UInt128Wrapper late_hash;
    auto late_block = create_block(kTabletId, "ttl-expire-then-cache-late", 0, 1024, &late_hash,
                                   FileCacheType::TTL, expiration_time);
    // Same ordering as above, and here it matters: the manager is running by this point and
    // sweeps this tablet every round, so publishing first would race the assertion.
    ASSERT_EQ(FileCacheType::TTL, late_block->cache_type());
    persist_block_meta(kTabletId, late_hash, late_block->range().left, late_block->range().size(),
                       FileCacheType::TTL, expiration_time);

    EXPECT_TRUE(
            wait_for_condition([&]() { return late_block->cache_type() == FileCacheType::NORMAL; },
                               std::chrono::seconds(5)));
}

// Dropping the TTL of a tablet that had already expired leaves nothing to convert, but the
// tablet still has to stop being tracked -- otherwise it holds a map entry for the life of the
// process and never again qualifies for the periodic reconcile.
TEST_F(BlockFileCacheTtlMgrTest, TtlClearedAfterExpiryStopsTrackingTablet) {
    constexpr int64_t kTabletId = 12012;
    auto tablet = std::make_shared<FakeTablet>(UnixSeconds(), 120);
    fake_engine()->add_tablet(kTabletId, tablet);

    UInt128Wrapper hash;
    auto block = create_block(kTabletId, "ttl-cleared-after-expiry", 0, 1024, &hash);
    persist_block_meta(kTabletId, hash, block->range().left, block->range().size());

    _ttl_mgr = std::make_unique<BlockFileCacheTtlMgr>(_cache.get(), _meta_store.get());
    _ttl_mgr->register_tablet_id(kTabletId);

    ASSERT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::TTL; },
                                   std::chrono::seconds(5)));
    ASSERT_TRUE(wait_for_condition([&]() { return _ttl_mgr->tracked_tablet_num() == 1; },
                                   std::chrono::seconds(5)));

    tablet->set_creation_time(UnixSeconds() - 120);
    tablet->set_ttl_seconds(1);
    ASSERT_TRUE(wait_for_condition([&]() { return block->cache_type() == FileCacheType::NORMAL; },
                                   std::chrono::seconds(5)));

    tablet->set_ttl_seconds(0);
    EXPECT_TRUE(wait_for_condition([&]() { return _ttl_mgr->tracked_tablet_num() == 0; },
                                   std::chrono::seconds(5)));
    EXPECT_EQ(FileCacheType::NORMAL, block->cache_type());
}

} // namespace doris::io
