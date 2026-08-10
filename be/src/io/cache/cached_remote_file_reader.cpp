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

#include "io/cache/cached_remote_file_reader.h"

#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "cloud/cloud_cluster_info.h"
#include "cloud/cloud_warm_up_manager.h"
#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/block_file_cache_profile.h"
#include "io/cache/file_block.h"
#include "io/cache/peer_file_cache_reader.h"
#include "io/cache/remote_scan_cache_write_limiter.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_profile.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/io_throttle.h"
#include "service/backend_options.h"
#include "util/bit_util.h"
#include "util/brpc_client_cache.h" // BrpcClientCache
#include "util/bthread_utils.h"
#include "util/client_cache.h"
#include "util/concurrency_stats.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris::io {

bvar::Adder<uint64_t> s3_read_counter("cached_remote_reader_s3_read");
bvar::Adder<uint64_t> peer_read_counter("cached_remote_reader_peer_read");
bvar::LatencyRecorder g_skip_cache_num("cached_remote_reader_skip_cache_num");
bvar::Adder<uint64_t> g_skip_cache_sum("cached_remote_reader_skip_cache_sum");
bvar::Adder<uint64_t> g_skip_local_cache_io_sum_bytes(
        "cached_remote_reader_skip_local_cache_io_sum_bytes");
bvar::Adder<uint64_t> g_read_cache_direct_whole_num("cached_remote_reader_cache_direct_whole_num");
bvar::Adder<uint64_t> g_read_cache_direct_partial_num(
        "cached_remote_reader_cache_direct_partial_num");
bvar::Adder<uint64_t> g_read_cache_indirect_num("cached_remote_reader_cache_indirect_num");
bvar::Adder<uint64_t> g_read_cache_direct_whole_bytes(
        "cached_remote_reader_cache_direct_whole_bytes");
bvar::Adder<uint64_t> g_read_cache_direct_partial_bytes(
        "cached_remote_reader_cache_direct_partial_bytes");
bvar::Adder<uint64_t> g_read_cache_indirect_bytes("cached_remote_reader_cache_indirect_bytes");
bvar::Adder<uint64_t> g_read_cache_indirect_total_bytes(
        "cached_remote_reader_cache_indirect_total_bytes");
bvar::Adder<uint64_t> g_read_cache_self_heal_on_not_found(
        "cached_remote_reader_self_heal_on_not_found");
bvar::Window<bvar::Adder<uint64_t>> g_read_cache_indirect_bytes_1min_window(
        "cached_remote_reader_indirect_bytes_1min_window", &g_read_cache_indirect_bytes, 60);
bvar::Window<bvar::Adder<uint64_t>> g_read_cache_indirect_total_bytes_1min_window(
        "cached_remote_reader_indirect_total_bytes_1min_window", &g_read_cache_indirect_total_bytes,
        60);
bvar::Adder<uint64_t> g_failed_get_peer_addr_counter(
        "cached_remote_reader_failed_get_peer_addr_counter");

static std::atomic<int> g_active_peer_races {0};
bvar::PassiveStatus<int> g_active_peer_races_bvar(
        "peer_race_active_count",
        [](void*) { return g_active_peer_races.load(std::memory_order_relaxed); }, nullptr);
// Cross-CG peer read race statistics
bvar::Adder<uint64_t> g_peer_race_peer_win("peer_race_peer_win");
bvar::Adder<uint64_t> g_peer_race_s3_win("peer_race_s3_win");
bvar::Adder<uint64_t> g_peer_race_both_fail("peer_race_both_fail");
bvar::Adder<uint64_t> g_peer_cross_compute_group_read("peer_cross_compute_group_read");
bvar::Adder<uint64_t> g_peer_same_compute_group_read("peer_same_compute_group_read");
bvar::Adder<uint64_t> g_peer_lazy_fetch_triggered("peer_lazy_fetch_triggered");

FileScannerV2ReaderLocalCache::FileScannerV2ReaderLocalCache(
        size_t capacity, std::shared_ptr<doris::MemTrackerLimiter> query_mem_tracker)
        : _capacity(capacity),
          _query_mem_tracker(std::move(query_mem_tracker)),
          _memory_tracker(std::make_shared<doris::MemTracker>("FileScannerV2ReaderLocalCache")) {}

FileScannerV2ReaderLocalCache::~FileScannerV2ReaderLocalCache() {
    // Destructors are noexcept; drain the registry in place so teardown cannot allocate a snapshot
    // vector and terminate the query process under memory pressure.
    try {
        std::lock_guard registry_lock(_registry_mutex);
        for (const auto& file : _files) {
            if (auto live_file = file.lock(); live_file != nullptr) {
                live_file->_drain(this);
            }
        }
    } catch (...) {
        return;
    }
    std::lock_guard lock(_budget_mutex);
    DORIS_CHECK(_memory_bytes == 0);
    DORIS_CHECK(_reserved_bytes == 0);
}

std::shared_ptr<FileScannerV2ReaderLocalFileCache>
FileScannerV2ReaderLocalCache::create_file_cache() {
    if (_capacity == 0) {
        return nullptr;
    }
    std::shared_ptr<FileScannerV2ReaderLocalFileCache> file_cache;
    try {
        file_cache = std::shared_ptr<FileScannerV2ReaderLocalFileCache>(
                new FileScannerV2ReaderLocalFileCache(shared_from_this()));
    } catch (const doris::Exception&) {
        return nullptr;
    } catch (const std::bad_alloc&) {
        return nullptr;
    }
    {
        std::lock_guard lock(_registry_mutex);
        try {
            std::erase_if(_files, [](const auto& file) { return file.expired(); });
            _files.emplace_back(file_cache);
        } catch (const doris::Exception&) {
            return nullptr;
        } catch (const std::bad_alloc&) {
            return nullptr;
        }
    }
    return file_cache;
}

bool FileScannerV2ReaderLocalCache::_try_reserve(size_t bytes) {
    std::lock_guard lock(_budget_mutex);
    if (bytes > _capacity) {
        return false;
    }
    if (_memory_bytes + _reserved_bytes + bytes > _capacity) {
        return false;
    }
    if (_query_mem_tracker != nullptr && _query_mem_tracker->limit() >= 0 &&
        _query_mem_tracker->consumption() + cast_set<int64_t>(_reserved_bytes + bytes) >
                _query_mem_tracker->limit()) {
        return false;
    }
    if (GlobalMemoryArbitrator::is_exceed_soft_mem_limit(
                cast_set<int64_t>(_reserved_bytes + bytes))) {
        return false;
    }
    _reserved_bytes += bytes;
    return true;
}

bool FileScannerV2ReaderLocalCache::_reserve(size_t bytes,
                                             FileScannerV2ReaderLocalFileCache* requester,
                                             size_t* evicted) {
    if (_try_reserve(bytes)) {
        return true;
    }
    // A stream may recycle its own cold blocks, but it must never evict another stream's hot
    // block map; physical-stream ownership provides that isolation without a global registry.
    while (requester->_evict_one()) {
        ++*evicted;
        if (_try_reserve(bytes)) {
            return true;
        }
    }
    return false;
}

void FileScannerV2ReaderLocalCache::_commit(size_t bytes) {
    {
        std::lock_guard lock(_budget_mutex);
        DORIS_CHECK(_reserved_bytes >= bytes);
        _reserved_bytes -= bytes;
        _memory_bytes += bytes;
    }
    _memory_tracker->consume(cast_set<int64_t>(bytes));
}

void FileScannerV2ReaderLocalCache::_cancel_reservation(size_t bytes) {
    std::lock_guard lock(_budget_mutex);
    DORIS_CHECK(_reserved_bytes >= bytes);
    _reserved_bytes -= bytes;
}

void FileScannerV2ReaderLocalCache::_release(size_t bytes) {
    {
        std::lock_guard lock(_budget_mutex);
        DORIS_CHECK(_memory_bytes >= bytes);
        _memory_bytes -= bytes;
    }
    _memory_tracker->release(cast_set<int64_t>(bytes));
}

std::vector<std::shared_ptr<FileScannerV2ReaderLocalFileCache>>
FileScannerV2ReaderLocalCache::_file_caches() const {
    std::vector<std::shared_ptr<FileScannerV2ReaderLocalFileCache>> files;
    std::lock_guard lock(_registry_mutex);
    files.reserve(_files.size());
    for (const auto& file : _files) {
        if (auto live_file = file.lock(); live_file != nullptr) {
            files.push_back(std::move(live_file));
        }
    }
    return files;
}

size_t FileScannerV2ReaderLocalCache::entry_count() const {
    size_t count = 0;
    for (const auto& file : _file_caches()) {
        count += file->entry_count();
    }
    return count;
}

size_t FileScannerV2ReaderLocalCache::memory_usage() const {
    std::lock_guard lock(_budget_mutex);
    return _memory_bytes;
}

int64_t FileScannerV2ReaderLocalCache::tracked_memory() const {
    return _memory_tracker->consumption();
}

FileScannerV2ReaderLocalFileCache::FileScannerV2ReaderLocalFileCache(
        std::shared_ptr<FileScannerV2ReaderLocalCache> owner)
        : _owner(std::move(owner)) {}

FileScannerV2ReaderLocalFileCache::~FileScannerV2ReaderLocalFileCache() {
    if (auto owner = _owner.lock(); owner != nullptr) {
        _drain(owner.get());
    }
}

void FileScannerV2ReaderLocalFileCache::_drain(FileScannerV2ReaderLocalCache* owner) {
    DORIS_CHECK(owner != nullptr);
    size_t memory_bytes = 0;
    size_t reserved_bytes = 0;
    auto clear_entries = [&]() {
        std::unique_lock lock(_mutex);
        for (auto& [_, entry] : _entries) {
            if (entry->data != nullptr) {
                memory_bytes += entry->data->size();
            }
            reserved_bytes += entry->reserved_bytes;
        }
        _entries.clear();
        _lru.clear();
    };
    try {
        std::optional<SwitchThreadMemTrackerLimiter> switch_query_tracker;
        if (owner->_query_mem_tracker != nullptr) {
            switch_query_tracker.emplace(owner->_query_mem_tracker);
        }
        clear_entries();
    } catch (...) {
        // Destructors cannot propagate memory-tracker setup failures during query cancellation.
        clear_entries();
    }
    if (memory_bytes > 0) {
        owner->_release(memory_bytes);
    }
    if (reserved_bytes > 0) {
        owner->_cancel_reservation(reserved_bytes);
    }
}

void FileScannerV2ReaderLocalFileCache::_touch_locked(const std::shared_ptr<Entry>& entry) {
    if (entry->in_lru) {
        _lru.splice(_lru.begin(), _lru, entry->lru_position);
    }
}

bool FileScannerV2ReaderLocalFileCache::_evict_one() {
    const auto owner = _owner.lock();
    if (owner == nullptr) {
        return false;
    }
    std::shared_ptr<std::vector<char>> data;
    {
        std::lock_guard lock(_mutex);
        size_t candidates = _lru.size();
        while (candidates-- > 0 && !_lru.empty()) {
            const size_t victim_offset = _lru.back();
            const auto victim = _entries.find(victim_offset);
            DORIS_CHECK(victim != _entries.end());
            DORIS_CHECK(!victim->second->loading);
            if (victim->second->data.use_count() > 1) {
                // Keep pinned blocks discoverable so another reader cannot start a duplicate fill.
                _lru.splice(_lru.begin(), _lru, victim->second->lru_position);
                continue;
            }
            data = std::move(victim->second->data);
            _lru.pop_back();
            _entries.erase(victim);
            break;
        }
    }
    if (data == nullptr) {
        return false;
    }
    const size_t bytes = data->size();
    // Eviction and destruction are noexcept cleanup paths. A stack guard avoids a second heap
    // allocation while releasing memory under pressure; if tracker switching itself fails, the
    // block is still released and the explicit cache budget remains consistent.
    try {
        std::optional<SwitchThreadMemTrackerLimiter> switch_query_tracker;
        if (owner->_query_mem_tracker != nullptr) {
            switch_query_tracker.emplace(owner->_query_mem_tracker);
        }
        data.reset();
    } catch (...) {
        data.reset();
    }
    owner->_release(bytes);
    return true;
}

void FileScannerV2ReaderLocalFileCache::_abort_load(size_t block_offset,
                                                    const std::shared_ptr<Entry>& entry) {
    size_t reserved_bytes = 0;
    {
        std::unique_lock lock(_mutex);
        reserved_bytes = entry->reserved_bytes;
        entry->reserved_bytes = 0;
        entry->loading = false;
        const auto it = _entries.find(block_offset);
        if (it != _entries.end() && it->second == entry) {
            _entries.erase(it);
        }
    }
    if (reserved_bytes != 0) {
        if (const auto owner = _owner.lock(); owner != nullptr) {
            owner->_cancel_reservation(reserved_bytes);
        }
    }
    // A loader must publish every exit, including allocation and tracker exceptions, otherwise a
    // same-block waiter can remain asleep after the scan has already fallen back to FileCache.
    entry->ready.notify_all();
}

bool FileScannerV2ReaderLocalFileCache::pin_if_present(size_t block_offset, size_t read_offset,
                                                       size_t read_size, LookupResult* lookup) {
    DORIS_CHECK(lookup != nullptr);
    *lookup = {};
    std::shared_ptr<Entry> entry;
    bool touch_lru = false;
    {
        std::shared_lock lock(_mutex);
        const auto it = _entries.find(block_offset);
        if (it == _entries.end() || it->second->loading || !it->second->load_status.ok() ||
            it->second->data == nullptr || read_offset < block_offset ||
            read_offset - block_offset > it->second->data->size() ||
            read_size > it->second->data->size() - (read_offset - block_offset)) {
            return false;
        }
        entry = it->second;
        lookup->data = entry->data;
        lookup->admitted = true;
        lookup->hit = true;
        touch_lru =
                entry->hit_count.fetch_add(1, std::memory_order_relaxed) % LRU_TOUCH_INTERVAL == 0;
        if (touch_lru) {
            lookup->file_block_to_touch = entry->source_file_block.lock();
        }
    }
    if (touch_lru) {
        std::unique_lock lock(_mutex);
        const auto it = _entries.find(block_offset);
        if (it != _entries.end() && it->second == entry) {
            _touch_locked(entry);
        }
    }
    return true;
}

bool FileScannerV2ReaderLocalFileCache::read_if_present(size_t block_offset, size_t read_offset,
                                                        Slice result, LookupResult* lookup) {
    if (!pin_if_present(block_offset, read_offset, result.size, lookup)) {
        return false;
    }
    memcpy(result.data, lookup->data->data() + read_offset - block_offset, result.size);
    return true;
}

Status FileScannerV2ReaderLocalFileCache::get_or_load(size_t block_offset, size_t block_size,
                                                      const FileBlockSPtr& file_block,
                                                      size_t file_block_offset,
                                                      LookupResult* lookup) {
    DORIS_CHECK(lookup != nullptr);
    *lookup = {};
    const auto owner = _owner.lock();
    if (owner == nullptr) {
        return Status::OK();
    }
    std::shared_ptr<Entry> entry;
    std::shared_ptr<std::vector<char>> retired_data;
    bool load = false;
    {
        std::unique_lock lock(_mutex);
        const auto it = _entries.find(block_offset);
        if (it == _entries.end()) {
            try {
                entry = std::make_shared<Entry>();
            } catch (const doris::Exception&) {
                return Status::OK();
            } catch (const std::bad_alloc&) {
                return Status::OK();
            }
            try {
                _entries.emplace(block_offset, entry);
            } catch (const doris::Exception&) {
                return Status::OK();
            } catch (const std::bad_alloc&) {
                return Status::OK();
            }
            load = true;
        } else {
            entry = it->second;
            lookup->admitted = true;
            if (entry->loading) {
                lookup->waited = true;
                TEST_SYNC_POINT("CachedRemoteFileReader::reader_local_cache_before_wait");
                MonotonicStopWatch wait_watch;
                wait_watch.start();
                entry->ready.wait(lock, [&entry]() { return !entry->loading; });
                lookup->wait_time = wait_watch.elapsed_time();
            }
            RETURN_IF_ERROR(entry->load_status);
            if (entry->data == nullptr || entry->data->size() < block_size) {
                // FileCache ranges can end at different boundaries for the same aligned offset.
                // Replace an unpinned short promotion so a stable wider cell does not miss forever.
                if (entry->data == nullptr || entry->data.use_count() > 1) {
                    lookup->admitted = false;
                    return Status::OK();
                }
                retired_data = std::move(entry->data);
                if (entry->in_lru) {
                    _lru.erase(entry->lru_position);
                }
                _entries.erase(it);
                try {
                    entry = std::make_shared<Entry>();
                    _entries.emplace(block_offset, entry);
                    load = true;
                    lookup->admitted = false;
                } catch (const doris::Exception&) {
                    entry.reset();
                } catch (const std::bad_alloc&) {
                    entry.reset();
                }
                if (!load) {
                    lock.unlock();
                    owner->_release(retired_data->size());
                    return Status::OK();
                }
            } else {
                lookup->hit = true;
                lookup->data = entry->data;
                _touch_locked(entry);
            }
        }
    }

    if (retired_data != nullptr) {
        const size_t retired_size = retired_data->size();
        retired_data.reset();
        owner->_release(retired_size);
    }

    if (!load) {
        return Status::OK();
    }

    std::shared_ptr<std::vector<char>> data;
    std::optional<SwitchThreadMemTrackerLimiter> switch_query_tracker;
    try {
        if (!owner->_reserve(block_size, this, &lookup->evicted)) {
            // Allocation failures remain best-effort misses; this flag records policy admission
            // rejection from cache/query/global memory limits.
            lookup->admission_rejected = true;
            _abort_load(block_offset, entry);
            return Status::OK();
        }
        entry->reserved_bytes = block_size;
        lookup->admitted = true;

        if (owner->_query_mem_tracker != nullptr) {
            switch_query_tracker.emplace(owner->_query_mem_tracker);
        }
        data = std::make_shared<std::vector<char>>(block_size);
        MonotonicStopWatch fill_watch;
        fill_watch.start();
        TEST_SYNC_POINT("CachedRemoteFileReader::reader_local_cache_before_fill");
        const Status load_status =
                file_block->read(Slice(data->data(), data->size()), file_block_offset);
        lookup->fill_time = fill_watch.elapsed_time();
        {
            std::unique_lock lock(_mutex);
            entry->load_status = load_status;
            entry->loading = false;
            if (load_status.ok()) {
                entry->data = data;
                entry->source_file_block = file_block;
                try {
                    _lru.push_front(block_offset);
                } catch (...) {
                    owner->_cancel_reservation(entry->reserved_bytes);
                    entry->reserved_bytes = 0;
                    entry->data.reset();
                    data.reset();
                    _entries.erase(block_offset);
                    entry->ready.notify_all();
                    lookup->admitted = false;
                    return Status::OK();
                }
                entry->lru_position = _lru.begin();
                entry->in_lru = true;
                owner->_commit(entry->reserved_bytes);
                entry->reserved_bytes = 0;
                lookup->data = std::move(data);
            } else {
                owner->_cancel_reservation(entry->reserved_bytes);
                entry->reserved_bytes = 0;
                _entries.erase(block_offset);
            }
            entry->ready.notify_all();
        }
        return load_status;
    } catch (...) {
        data.reset();
        _abort_load(block_offset, entry);
        lookup->admitted = false;
        return Status::OK();
    }
}

size_t FileScannerV2ReaderLocalFileCache::entry_count() const {
    std::lock_guard lock(_mutex);
    return _entries.size();
}

static bool use_remote_only_on_cache_miss(const IOContext* io_ctx) {
    if (io_ctx->file_cache_miss_policy == FileCacheMissPolicy::REMOTE_ONLY_ON_MISS) {
        return true;
    }
    auto* limiter = io_ctx->remote_scan_cache_write_limiter;
    return limiter != nullptr && limiter->remote_only_on_miss();
}

CachedRemoteFileReader::CachedRemoteFileReader(FileReaderSPtr remote_file_reader,
                                               const FileReaderOptions& opts)
        : _is_doris_table(opts.is_doris_table),
          _enable_reader_local_cache(opts.enable_reader_local_cache &&
                                     opts.reader_local_cache != nullptr && !opts.is_doris_table),
          _tablet_id(opts.tablet_id),
          _storage_resource_id(opts.storage_resource_id),
          _remote_file_reader(std::move(remote_file_reader)),
          _reader_local_cache(opts.reader_local_cache) {
    DCHECK(!_is_doris_table || _tablet_id > 0);
    if (_is_doris_table) {
        _init_doris_table_cache();
    } else {
        _init_external_table_cache(opts);
    }
    if (_enable_reader_local_cache) {
        // Bind each block map to one physical stream so file-registry objects outside the byte
        // budget cannot accumulate across a long scan.
        _reader_local_file_cache = _reader_local_cache->create_file_cache();
        _enable_reader_local_cache = _reader_local_file_cache != nullptr;
    }
}

void CachedRemoteFileReader::_init_doris_table_cache() {
    _cache_hash = BlockFileCache::hash(path().filename().native());
    _cache = FileCacheFactory::instance()->get_by_path(_cache_hash);
    if (_can_read_cache_file_directly()) {
        // this is designed for and test in doris table, external table need extra tests
        _cache_file_readers = _cache->get_blocks_by_key(_cache_hash);
    }
}

void CachedRemoteFileReader::_init_external_table_cache(const FileReaderOptions& opts) {
    const std::string& file_system_identity = opts.cache_file_system_identity.empty()
                                                      ? opts.storage_resource_id
                                                      : opts.cache_file_system_identity;
    // HDFS readers strip the nameservice from path(). Keep the filesystem identity in the key so
    // equal paths from different namespaces can never alias cached bytes.
    std::string unique_path =
            fmt::format("{}:{}:{}:{}:{}:{}", file_system_identity.size(), file_system_identity,
                        path().native().size(), path().native(), opts.mtime, opts.file_size);
    _cache_hash = BlockFileCache::hash(unique_path);
    if (opts.cache_base_path.empty()) {
        // If cache path is not specified by session variable, choose randomly.
        _cache = FileCacheFactory::instance()->get_by_path(_cache_hash);
        return;
    }

    // From query session variable: file_cache_base_path.
    _cache = FileCacheFactory::instance()->get_by_path(opts.cache_base_path);
    if (_cache != nullptr) {
        return;
    }

    LOG(WARNING) << "Can't get cache from base path: " << opts.cache_base_path
                 << ", using random instead.";
    _cache = FileCacheFactory::instance()->get_by_path(_cache_hash);
}

bool CachedRemoteFileReader::_can_read_cache_file_directly() const {
    return _is_doris_table && config::enable_read_cache_file_directly;
}

bool CachedRemoteFileReader::_should_read_from_peer(const IOContext* io_ctx) const {
    return doris::config::is_cloud_mode() && _is_doris_table && _tablet_id > 0 &&
           !io_ctx->is_warmup && !io_ctx->bypass_peer_read &&
           doris::config::enable_cache_read_from_peer;
}

void CachedRemoteFileReader::_insert_file_reader(FileBlockSPtr file_block) {
    if (_can_read_cache_file_directly()) {
        std::lock_guard lock(_mtx);
        DCHECK(file_block->state() == FileBlock::State::DOWNLOADED);
        file_block->_owned_by_cached_reader = true;
        _cache_file_readers.emplace(file_block->offset(), std::move(file_block));
    }
}

CachedRemoteFileReader::~CachedRemoteFileReader() {
    for (auto& it : _cache_file_readers) {
        it.second->_owned_by_cached_reader = false;
    }
    static_cast<void>(close());
}

Status CachedRemoteFileReader::close() {
    return _remote_file_reader->close();
}

std::pair<size_t, size_t> CachedRemoteFileReader::s_align_size(size_t offset, size_t read_size,
                                                               size_t length) {
    size_t left = offset;
    size_t right = offset + read_size - 1;
    size_t align_left =
            (left / config::file_cache_each_block_size) * config::file_cache_each_block_size;
    size_t align_right =
            (right / config::file_cache_each_block_size + 1) * config::file_cache_each_block_size;
    align_right = align_right < length ? align_right : length;
    size_t align_size = align_right - align_left;
    if (align_size < config::file_cache_each_block_size && align_left != 0) {
        align_size += config::file_cache_each_block_size;
        align_left -= config::file_cache_each_block_size;
    }
    return std::make_pair(align_left, align_size);
}

namespace {
struct PeerFetchLayout {
    std::vector<size_t> block_offsets;
    std::vector<size_t> block_sizes;
    size_t total_size = 0;
};

bool is_fill_not_found(const Status& st, bool request_fill) {
    return request_fill && st.is<ErrorCode::NOT_FOUND>();
}

bool contains_file_block(const PeerFetchedBlockSet& fetched_blocks, const FileBlockSPtr& block) {
    return fetched_blocks.contains(block.get());
}

size_t clip_peer_block_size(const FileBlock::Range& range, size_t file_size) {
    if (range.left >= file_size) {
        return 0;
    }
    return std::min(file_size - range.left, range.size());
}

PeerFetchLayout build_peer_fetch_layout(const std::vector<FileBlockSPtr>& blocks,
                                        size_t file_size) {
    PeerFetchLayout layout;
    layout.block_offsets.reserve(blocks.size());
    layout.block_sizes.reserve(blocks.size());
    for (const auto& block : blocks) {
        const size_t block_size = clip_peer_block_size(block->range(), file_size);
        layout.block_offsets.push_back(layout.total_size);
        layout.block_sizes.push_back(block_size);
        layout.total_size += block_size;
    }
    return layout;
}

Status write_peer_payloads_into_block(const FileBlockSPtr& block,
                                      std::vector<const PeerFetchChunk*>& chunks,
                                      size_t* block_size) {
    if (block_size == nullptr) {
        return Status::InvalidArgument("peer block write requires non-null block_size");
    }
    *block_size = 0;
    if (chunks.empty()) {
        return Status::OK();
    }
    std::sort(chunks.begin(), chunks.end(),
              [](const PeerFetchChunk* lhs, const PeerFetchChunk* rhs) {
                  return lhs->block_offset < rhs->block_offset;
              });
    butil::IOBuf payload;
    for (const auto* chunk : chunks) {
        *block_size += chunk->payload.length();
        payload.append(chunk->payload);
    }
    DCHECK(*block_size != 0);
    return block->append_iobuf(payload);
}

void copy_peer_chunk_to_result(const PeerFetchChunk& chunk, size_t offset, size_t right_offset,
                               size_t already_read, Slice result, size_t& indirect_read_bytes,
                               SourceReadBreakdown& source_read_breakdown) {
    const size_t payload_size = chunk.payload.length();
    if (payload_size == 0) {
        return;
    }
    const size_t chunk_left = chunk.block_offset;
    const size_t chunk_right = chunk_left + payload_size - 1;
    const size_t copy_left_offset = std::max(offset + already_read, chunk_left);
    const size_t copy_right_offset = std::min(right_offset, chunk_right);
    if (copy_left_offset > copy_right_offset) {
        return;
    }
    const size_t copy_offset = copy_left_offset - chunk_left;
    const size_t copy_size = copy_right_offset - copy_left_offset + 1;
    char* dst = result.data + (copy_left_offset - offset);
    chunk.payload.copy_to(dst, copy_size, copy_offset);
    indirect_read_bytes += copy_size;
    source_read_breakdown.peer_bytes += copy_size;
}

// Execute peer read targeting a specific host:port.
Status execute_peer_read(const std::vector<FileBlockSPtr>& empty_blocks,
                         PeerFetchResult* peer_result, const std::string& file_path,
                         size_t file_size, bool is_doris_table, ReadStatistics& stats,
                         const IOContext* io_ctx, const std::string& host, int32_t port) {
    VLOG_DEBUG << "PeerFileCacheReader read from peer, host=" << host << ", port=" << port
               << ", file_path=" << file_path;

    if (host.empty() || port == 0) {
        g_failed_get_peer_addr_counter << 1;
        LOG_EVERY_N(WARNING, 100) << "PeerFileCacheReader host or port is empty"
                                  << ", host=" << host << ", port=" << port
                                  << ", file_path=" << file_path;
        return Status::InternalError<false>("host or port is empty");
    }
    SCOPED_RAW_TIMER(&stats.peer_read_timer);
    peer_read_counter << 1;
    PeerFileCacheReader peer_reader(file_path, is_doris_table, host, port);
    // Serial peer read: source BE has the data from rebalance warm-up; no fill needed.
    auto st = peer_reader.fetch_blocks(empty_blocks, peer_result, file_size, io_ctx,
                                       /*request_fill=*/false);
    if (!st.ok()) {
        LOG_WARNING("PeerFileCacheReader read from peer failed")
                .tag("host", host)
                .tag("port", port)
                .tag("error", st.msg());
    }
    stats.from_peer_cache = st.ok();
    return st;
}

// Execute S3 read
Status execute_s3_read(size_t empty_start, size_t& size, std::unique_ptr<char[]>& buffer,
                       ReadStatistics& stats, const IOContext* io_ctx,
                       FileReaderSPtr remote_file_reader) {
    s3_read_counter << 1;
    SCOPED_RAW_TIMER(&stats.remote_read_timer);
    stats.from_peer_cache = false;
    return remote_file_reader->read_at(empty_start, Slice(buffer.get(), size), &size, io_ctx);
}

CloudWarmUpManager& get_warm_up_manager() {
    return ExecEnv::GetInstance()->storage_engine().to_cloud().cloud_warm_up_manager();
}

// Shared state for peer-vs-S3 winner race.
// Uses bthread primitives — never std::mutex/condition_variable in bthread context.
struct RaceState {
    bthread::Mutex mtx;
    bthread::ConditionVariable cv;
    int winner = -1; // 0=peer won, 1=s3 won, -1=undecided, -2=both failed
    bool peer_done = false;
    bool s3_done = false;
    Status peer_status;
    Status s3_status;
    std::unique_ptr<char[]> s3_buf;
    PeerFetchResult peer_res;
    std::string peer_winner_cg_id; // compute_group_id of the winning peer candidate
    std::string peer_winner_host;  // host of the winning peer candidate
    int64_t peer_elapsed_ns = 0;   // wall-clock time of the entire peer path (including retries)
    int64_t peer_winner_io_ns = 0; // I/O time of the winning candidate only
};

// Peer race logic: try candidates sequentially until one succeeds or all fail.
// NOTE: Do NOT capture io_ctx here — it points into the caller's stack which may be destroyed
// when S3 wins the race and the caller returns before this bthread finishes.
void run_peer_race(std::shared_ptr<RaceState> race, std::vector<FileBlockSPtr> empty_blocks,
                   const std::string& file_path, size_t file_sz, bool is_doris,
                   std::shared_ptr<CloudWarmUpManager> manager,
                   std::vector<doris::PeerCandidate> candidates, int64_t tablet_id,
                   std::string resource_id, std::shared_ptr<ResourceContext> parent_resource_ctx) {
    std::unique_ptr<AttachTask> attach_task;
    if (parent_resource_ctx != nullptr) {
        attach_task = std::make_unique<AttachTask>(parent_resource_ctx);
    }

    bool all_tried = true;
    MonotonicStopWatch peer_sw;
    peer_sw.start();

    for (size_t i = 0; i < candidates.size(); ++i) {
        // Before issuing the next RPC, check if S3 already won.
        if (i > 0) {
            TEST_SYNC_POINT("run_peer_race::between_candidates");
            std::unique_lock<bthread::Mutex> lk(race->mtx);
            if (race->winner > 0) {
                // S3 already won — stop, but not all candidates were tried.
                all_tried = false;
                break;
            }
        }

        const auto& cand = candidates[i];
        peer_read_counter << 1;
        PeerFileCacheReader peer_reader(file_path, is_doris, cand.host, cand.brpc_port);
        PeerFetchResult local_peer_res;
        const bool request_fill =
                !config::peer_cache_fill_compute_group_id.empty() &&
                cand.compute_group_id == config::peer_cache_fill_compute_group_id &&
                !resource_id.empty() && !file_path.empty();
        MonotonicStopWatch cand_sw;
        cand_sw.start();
        auto st = peer_reader.fetch_blocks(empty_blocks, &local_peer_res, file_sz,
                                           /*ctx=*/nullptr, request_fill, tablet_id, resource_id);
        if (st.ok()) {
            manager->update_peer_candidate_on_success(tablet_id, cand.compute_group_id);
            std::unique_lock<bthread::Mutex> lk(race->mtx);
            if (race->winner < 0) {
                race->winner = 0;
                race->peer_res = std::move(local_peer_res);
                race->peer_winner_cg_id = cand.compute_group_id;
                race->peer_winner_host = cand.host;
                race->peer_elapsed_ns = peer_sw.elapsed_time();
                race->peer_winner_io_ns = cand_sw.elapsed_time();
            }
            race->peer_done = true;
            race->peer_status = Status::OK();
            race->cv.notify_all();
            return;
        }

        // Handle per-candidate failure.
        if (st.template is<ErrorCode::TOO_MANY_TASKS>()) {
            all_tried = false;
            break;
        }
        if (is_fill_not_found(st, request_fill)) {
            // Pull-through fill already told us this designated fill CG could not serve the block
            // in time. Do not serially retry additional candidates in the same race; let S3 win
            // instead of paying more peer RPC latency.
            manager->rotate_peer_candidate_on_cache_miss(tablet_id, cand.host, cand.brpc_port);
            all_tried = false;
            break;
        }
        if (st.template is<ErrorCode::NOT_FOUND>()) {
            manager->rotate_peer_candidate_on_cache_miss(tablet_id, cand.host, cand.brpc_port);
        } else {
            manager->update_peer_candidate_on_rpc_failure(tablet_id, cand.host, cand.brpc_port);
        }
    }

    if (all_tried) {
        manager->record_peer_all_miss(tablet_id);
    }
    std::unique_lock<bthread::Mutex> lk(race->mtx);
    race->peer_done = true;
    race->peer_status = Status::InternalError<false>("peer: all candidates failed");
    if (race->winner < 0 && race->s3_done) {
        race->winner = race->s3_status.ok() ? 1 : -2;
    }
    race->cv.notify_all();
}

// Apply hedge delay, then submit S3 read to the thread pool (or run inline).
void launch_s3_race(std::shared_ptr<RaceState> race, size_t empty_start, size_t span_size,
                    const IOContext* io_ctx, FileReaderSPtr remote_reader,
                    std::shared_ptr<ResourceContext> parent_resource_ctx,
                    std::shared_ptr<CachedRemoteFileReader> owner) {
    // Raw S3 read body.
    // `owner` keeps the CachedRemoteFileReader alive until the S3 task finishes,
    // preventing close() from being called on remote_reader while we are still reading.
    // Do NOT capture io_ctx: it points into the caller's stack/iterator which may be
    // destroyed when the query is cancelled before this background task runs. The S3
    // leg of the race is a best-effort background task whose result is discarded if the
    // peer wins; passing nullptr is safe because S3FileReader::read_at_impl ignores it.
    auto do_s3_read = [race, empty_start, span_size, remote_reader, owner]() {
        (void)owner;
        auto s3_buf = std::make_unique<char[]>(span_size);
        size_t read_size = span_size;
        s3_read_counter << 1;
        TEST_SYNC_POINT("CachedRemoteFileReader::_execute_winner_race::s3_before_read");
        auto st = remote_reader->read_at(empty_start, Slice(s3_buf.get(), span_size), &read_size,
                                         nullptr);
        std::unique_lock<bthread::Mutex> lk(race->mtx);
        race->s3_done = true;
        race->s3_status = st;
        if (st.ok() && race->winner < 0) {
            race->winner = 1;
            race->s3_buf = std::move(s3_buf);
        }
        race->cv.notify_all();
    };

    // Hedge delay: give peer a head start, but wake up early if peer finishes.
    // Uses cv.wait_for() instead of bthread_usleep() so the calling thread is
    // unblocked as soon as the peer bthread signals completion, avoiding the
    // unconditional 20ms sleep that dominated latency on cache-miss-heavy queries.
    bool peer_already_won = false;
    if (config::peer_race_hedge_delay_ms > 0) {
        std::unique_lock<bthread::Mutex> lk(race->mtx);
        if (!race->peer_done) {
            race->cv.wait_for(lk, static_cast<long>(config::peer_race_hedge_delay_ms) * 1000);
        }
        peer_already_won = (race->winner == 0);
        if (peer_already_won) {
            race->s3_done = true;
            race->s3_status = Status::InternalError<false>("skipped: peer won during hedge delay");
        }
    }

    if (!peer_already_won) {
        auto s3_fn = [do_s3_read, parent_resource_ctx]() mutable {
            std::unique_ptr<AttachTask> attach_task;
            if (parent_resource_ctx != nullptr) {
                attach_task = std::make_unique<AttachTask>(parent_resource_ctx);
            }
            do_s3_read();
        };
        auto* s3_pool = ExecEnv::GetInstance()->peer_race_s3_thread_pool();
        if (s3_pool == nullptr || !s3_pool->submit_func(s3_fn).ok()) {
            do_s3_read();
        }
    }
}

// Wait for the race to finish and populate the output accordingly.
Status collect_race_result(std::shared_ptr<RaceState> race, size_t span_size,
                           std::unique_ptr<char[]>& buffer, PeerFetchResult* peer_result,
                           ReadStatistics& stats, const IOContext* io_ctx) {
    {
        std::unique_lock<bthread::Mutex> lk(race->mtx);
        while (race->winner < 0 && !(race->peer_done && race->s3_done)) {
            race->cv.wait(lk);
        }
    }
    g_active_peer_races.fetch_sub(1, std::memory_order_relaxed);

    const std::string self_cg_id =
            static_cast<CloudClusterInfo*>(ExecEnv::GetInstance()->cluster_info())
                    ->cloud_compute_group_id();

    if (race->winner == 0) {
        // Peer won.
        if (peer_result != nullptr) {
            *peer_result = std::move(race->peer_res);
        }
        stats.from_peer_cache = true;
        stats.peer_read_timer += race->peer_elapsed_ns;
        g_peer_race_peer_win << 1;
        const bool is_cross_cg =
                !race->peer_winner_cg_id.empty() && race->peer_winner_cg_id != self_cg_id;
        if (is_cross_cg) {
            g_peer_cross_compute_group_read << 1;
        } else {
            g_peer_same_compute_group_read << 1;
        }
        if (io_ctx != nullptr && io_ctx->file_cache_stats != nullptr) {
            io_ctx->file_cache_stats->num_peer_race_peer_win++;
            io_ctx->file_cache_stats->peer_hosts.insert(race->peer_winner_host);
            if (is_cross_cg) {
                io_ctx->file_cache_stats->num_cross_cg_peer_io_total++;
                io_ctx->file_cache_stats->bytes_read_from_cross_cg_peer += span_size;
                io_ctx->file_cache_stats->cross_cg_peer_io_timer += race->peer_winner_io_ns;
            } else {
                io_ctx->file_cache_stats->num_same_cg_peer_io_total++;
                io_ctx->file_cache_stats->bytes_read_from_same_cg_peer += span_size;
                io_ctx->file_cache_stats->same_cg_peer_io_timer += race->peer_winner_io_ns;
            }
        }
        return Status::OK();
    } else if (race->winner == 1) {
        // S3 won.
        buffer = std::move(race->s3_buf);
        stats.from_peer_cache = false;
        g_peer_race_s3_win << 1;
        if (io_ctx != nullptr && io_ctx->file_cache_stats != nullptr) {
            io_ctx->file_cache_stats->num_peer_race_s3_win++;
        }
        return Status::OK();
    }
    g_peer_race_both_fail << 1;
    return Status::InternalError<false>("peer race: both peer and s3 failed");
}

} // anonymous namespace

Status CachedRemoteFileReader::_execute_s3_fallback(size_t empty_start, size_t span_size,
                                                    std::unique_ptr<char[]>& buffer,
                                                    PeerFetchResult* peer_result,
                                                    ReadStatistics& stats,
                                                    const IOContext* io_ctx) {
    if (peer_result != nullptr) {
        peer_result->clear();
    }
    buffer.reset(new char[span_size]);
    size_t read_size = span_size;
    return execute_s3_read(empty_start, read_size, buffer, stats, io_ctx, _remote_file_reader);
}

Status CachedRemoteFileReader::_execute_sequential_peer_read(
        const std::vector<FileBlockSPtr>& empty_blocks, size_t empty_start, size_t span_size,
        std::unique_ptr<char[]>& buffer, PeerFetchResult* peer_result, ReadStatistics& stats,
        const IOContext* io_ctx, const std::vector<doris::PeerCandidate>& candidates,
        int64_t tablet_id) {
    // candidates[0] already reflects last_successful_compute_group_id affinity:
    // get_peer_candidates() applies stable_partition before returning.
    if (candidates.empty()) {
        return _execute_s3_fallback(empty_start, span_size, buffer, peer_result, stats, io_ctx);
    }

    auto& manager = get_warm_up_manager();
    PeerFetchResult serial_res;
    const int64_t timer_before = stats.peer_read_timer;
    auto st = execute_peer_read(empty_blocks, &serial_res, path().native(), this->size(),
                                _is_doris_table, stats, io_ctx, candidates[0].host,
                                candidates[0].brpc_port);
    if (st.ok()) {
        manager.update_peer_candidate_on_success(tablet_id, candidates[0].compute_group_id);
        if (peer_result != nullptr) {
            *peer_result = std::move(serial_res);
        }
        // Update profile counters for cross/same CG stats.
        const std::string self_cg_id =
                static_cast<CloudClusterInfo*>(ExecEnv::GetInstance()->cluster_info())
                        ->cloud_compute_group_id();
        const bool is_cross_cg = !candidates[0].compute_group_id.empty() &&
                                 candidates[0].compute_group_id != self_cg_id;
        if (is_cross_cg) {
            g_peer_cross_compute_group_read << 1;
        } else {
            g_peer_same_compute_group_read << 1;
        }
        if (io_ctx != nullptr && io_ctx->file_cache_stats != nullptr) {
            io_ctx->file_cache_stats->peer_hosts.insert(candidates[0].host);
            if (is_cross_cg) {
                io_ctx->file_cache_stats->num_cross_cg_peer_io_total++;
                io_ctx->file_cache_stats->bytes_read_from_cross_cg_peer += span_size;
                io_ctx->file_cache_stats->cross_cg_peer_io_timer +=
                        stats.peer_read_timer - timer_before;
            } else {
                io_ctx->file_cache_stats->num_same_cg_peer_io_total++;
                io_ctx->file_cache_stats->bytes_read_from_same_cg_peer += span_size;
                io_ctx->file_cache_stats->same_cg_peer_io_timer +=
                        stats.peer_read_timer - timer_before;
            }
        }
        return st;
    }
    // Track failure so affinity / eviction logic stays consistent with the race path.
    if (st.is<ErrorCode::TOO_MANY_TASKS>()) {
        // Server healthy but overloaded — don't penalize candidate.
    } else if (st.is<ErrorCode::NOT_FOUND>()) {
        manager.rotate_peer_candidate_on_cache_miss(tablet_id, candidates[0].host,
                                                    candidates[0].brpc_port);
    } else {
        manager.update_peer_candidate_on_rpc_failure(tablet_id, candidates[0].host,
                                                     candidates[0].brpc_port);
    }
    return _execute_s3_fallback(empty_start, span_size, buffer, peer_result, stats, io_ctx);
}

Status CachedRemoteFileReader::_execute_remote_read(const std::vector<FileBlockSPtr>& empty_blocks,
                                                    size_t empty_start, size_t span_size,
                                                    std::unique_ptr<char[]>& buffer,
                                                    PeerFetchResult* peer_result,
                                                    ReadStatistics& stats,
                                                    const IOContext* io_ctx) {
    // --- Non-peer path: direct S3 ---
    if (!_should_read_from_peer(io_ctx)) {
        return _execute_s3_fallback(empty_start, span_size, buffer, peer_result, stats, io_ctx);
    }

    // --- UT debug point: injected peer address ---
    DBUG_EXECUTE_IF("PeerFileCacheReader::_fetch_from_peer_cache_blocks", {
        std::string dp_host = dp->param<std::string>("host", "127.0.0.1");
        int32_t dp_port = dp->param("port", 9060);
        buffer.reset();
        DCHECK(peer_result != nullptr);
        peer_result->clear();
        auto st = execute_peer_read(empty_blocks, peer_result, path().native(), this->size(),
                                    _is_doris_table, stats, io_ctx, dp_host, dp_port);
        if (st.ok()) return st;
        return _execute_s3_fallback(empty_start, span_size, buffer, peer_result, stats, io_ctx);
    });

    // --- Resolve tablet and obtain peer candidates ---
    int64_t tablet_id = _tablet_id;
    auto& manager = get_warm_up_manager();
    auto candidates = manager.get_peer_candidates(tablet_id);
    if (candidates.empty()) {
        if (!manager.is_peer_cooldown(tablet_id)) {
            // Cold miss: trigger background FE fetch and fall back to S3.
            g_peer_lazy_fetch_triggered << 1;
            auto manager_ptr =
                    ExecEnv::GetInstance()->storage_engine().to_cloud().cloud_warm_up_manager_ptr();
            start_bthread([manager_ptr = std::move(manager_ptr), tablet_id]() {
                manager_ptr->fetch_candidates_from_fe(tablet_id);
            });
        }
        return _execute_s3_fallback(empty_start, span_size, buffer, peer_result, stats, io_ctx);
    }

    // --- Dispatch: concurrent race or sequential fallback ---
    // Candidates are already sorted by last_successful_compute_group_id affinity
    // (stable_partition in get_peer_candidates), so the winner race peer bthread
    // naturally tries the most promising candidate first — whether same-CG or cross-CG.
    if (config::enable_peer_s3_race) {
        return _execute_winner_race(empty_blocks, empty_start, span_size, buffer, peer_result,
                                    stats, io_ctx, candidates, tablet_id);
    }
    return _execute_sequential_peer_read(empty_blocks, empty_start, span_size, buffer, peer_result,
                                         stats, io_ctx, candidates, tablet_id);
}

Status CachedRemoteFileReader::_execute_winner_race(
        const std::vector<FileBlockSPtr>& empty_blocks, size_t empty_start, size_t span_size,
        std::unique_ptr<char[]>& buffer, PeerFetchResult* peer_result, ReadStatistics& stats,
        const IOContext* io_ctx, const std::vector<doris::PeerCandidate>& candidates,
        int64_t tablet_id) {
    // Reserve a race slot; degrade to sequential if at limit.
    if (g_active_peer_races.fetch_add(1, std::memory_order_relaxed) >=
        config::max_concurrent_peer_races) {
        g_active_peer_races.fetch_sub(1, std::memory_order_relaxed);
        return _execute_sequential_peer_read(empty_blocks, empty_start, span_size, buffer,
                                             peer_result, stats, io_ctx, candidates, tablet_id);
    }

    auto race = std::make_shared<RaceState>();
    auto manager = ExecEnv::GetInstance()->storage_engine().to_cloud().cloud_warm_up_manager_ptr();

    // Capture context for child threads.
    const std::string file_path = path().native();
    const size_t file_sz = this->size();
    const bool is_doris = _is_doris_table;
    auto remote_reader = _remote_file_reader;
    std::shared_ptr<ResourceContext> parent_resource_ctx;
    auto* parent_thread_context = thread_context();
    if (parent_thread_context != nullptr && parent_thread_context->is_attach_task()) {
        parent_resource_ctx = parent_thread_context->resource_ctx();
    }

    // Launch peer bthread.
    start_bthread(
            [race, empty_blocks = std::move(empty_blocks), file_path, file_sz, is_doris,
             manager = std::move(manager), candidates = std::move(candidates), tablet_id,
             resource_id = _storage_resource_id, parent_resource_ctx]() mutable {
                run_peer_race(race, std::move(empty_blocks), file_path, file_sz, is_doris,
                              std::move(manager), std::move(candidates), tablet_id,
                              std::move(resource_id), parent_resource_ctx);
            },
            /*init_thread_ctx=*/true);

    // Launch S3 (with optional hedge delay).
    // Pass shared_from_this() so the background S3 task holds a reference to this
    // reader, preventing destruction (and close()) until the S3 task completes.
    launch_s3_race(race, empty_start, span_size, io_ctx, remote_reader, parent_resource_ctx,
                   shared_from_this());

    // Collect race result.
    return collect_race_result(race, span_size, buffer, peer_result, stats, io_ctx);
}

bool CachedRemoteFileReader::_try_read_from_cached_files_directly(
        size_t offset, Slice result, size_t bytes_req, bool is_dryrun, ReadStatistics& stats,
        SourceReadBreakdown& source_read_breakdown, size_t& already_read, size_t* bytes_read) {
    if (!_can_read_cache_file_directly()) {
        return false;
    }

    SCOPED_RAW_TIMER(&stats.read_cache_file_directly_timer);
    size_t need_read_size = bytes_req;
    std::shared_lock lock(_mtx);
    if (_cache_file_readers.empty()) {
        return false;
    }

    auto iter = _cache_file_readers.upper_bound(offset);
    if (iter != _cache_file_readers.begin()) {
        --iter;
    }

    size_t current_offset = offset;
    while (need_read_size != 0 && iter != _cache_file_readers.end()) {
        if (iter->second->offset() > current_offset ||
            iter->second->range().right < current_offset) {
            break;
        }

        size_t file_offset = current_offset - iter->second->offset();
        size_t reserve_bytes = std::min(need_read_size, iter->second->range().size() - file_offset);
        if (is_dryrun) [[unlikely]] {
            g_skip_local_cache_io_sum_bytes << reserve_bytes;
        } else {
            SCOPED_RAW_TIMER(&stats.local_read_timer);
            if (!_read_local_block(iter->second, file_offset, current_offset,
                                   Slice(result.data + (current_offset - offset), reserve_bytes),
                                   stats)
                         .ok()) { // TODO: maybe read failed because block evict, should handle error
                break;
            }
            source_read_breakdown.local_bytes += reserve_bytes;
        }

        _cache->add_need_update_lru_block(iter->second);
        need_read_size -= reserve_bytes;
        current_offset += reserve_bytes;
        already_read += reserve_bytes;
        ++iter;
    }

    if (need_read_size == 0) {
        *bytes_read = bytes_req;
        stats.hit_cache = true;
        g_read_cache_direct_whole_num << 1;
        g_read_cache_direct_whole_bytes << bytes_req;
        return true;
    }

    g_read_cache_direct_partial_num << 1;
    g_read_cache_direct_partial_bytes << already_read;
    return false;
}

bool CachedRemoteFileReader::_read_from_memory_block_cache(size_t offset, Slice result,
                                                           ReadStatistics* stats) {
    if (!_enable_reader_local_cache || _reader_local_file_cache == nullptr || _cache == nullptr) {
        return false;
    }
    size_t current_offset = offset;
    const size_t request_end = offset + result.size;
    struct PinnedRead {
        size_t block_offset;
        size_t read_offset;
        size_t result_offset;
        size_t read_size;
        FileScannerV2ReaderLocalFileCache::LookupResult lookup;
    };
    // Parquet metadata and page reads normally span very few cache blocks. Keep their pins on the
    // stack so the direct-memory hot path does not replace FileCache locking with heap allocation.
    constexpr size_t INLINE_PINNED_READS = 4;
    std::array<PinnedRead, INLINE_PINNED_READS> inline_pinned_reads {};
    std::vector<PinnedRead> overflow_pinned_reads;
    size_t pinned_read_count = 0;
    while (current_offset < request_end) {
        const size_t block_offset =
                current_offset / READER_LOCAL_CACHE_BLOCK_BYTES * READER_LOCAL_CACHE_BLOCK_BYTES;
        const size_t read_end =
                std::min(request_end, block_offset + READER_LOCAL_CACHE_BLOCK_BYTES);
        const size_t read_size = read_end - current_offset;
        PinnedRead pinned {.block_offset = block_offset,
                           .read_offset = current_offset,
                           .result_offset = current_offset - offset,
                           .read_size = read_size,
                           .lookup = {}};
        if (!_reader_local_file_cache->pin_if_present(block_offset, current_offset, read_size,
                                                      &pinned.lookup)) {
            if (stats != nullptr && pinned_read_count != 0) {
                stats->num_reader_local_cache_partial_miss++;
            }
            return false;
        }
        try {
            if (pinned_read_count < INLINE_PINNED_READS) {
                inline_pinned_reads[pinned_read_count] = std::move(pinned);
            } else {
                overflow_pinned_reads.push_back(std::move(pinned));
            }
        } catch (...) {
            // Optional hot-cache bookkeeping must never fail the scan under memory pressure.
            return false;
        }
        ++pinned_read_count;
        current_offset = read_end;
    }
    // Pin the complete request before copying. A partial probe must leave the caller's buffer
    // untouched because the FileCache fallback will restart the request from its original offset.
    auto copy_pinned_read = [&](const PinnedRead& pinned) {
        memcpy(result.data + pinned.result_offset,
               pinned.lookup.data->data() + pinned.read_offset - pinned.block_offset,
               pinned.read_size);
        if (pinned.lookup.file_block_to_touch != nullptr) {
            _cache->add_need_update_lru_block(pinned.lookup.file_block_to_touch);
            if (stats != nullptr) {
                stats->num_reader_local_cache_disk_lru_touch++;
            }
        }
    };
    for (size_t i = 0; i < std::min(pinned_read_count, INLINE_PINNED_READS); ++i) {
        copy_pinned_read(inline_pinned_reads[i]);
    }
    for (const auto& pinned : overflow_pinned_reads) {
        copy_pinned_read(pinned);
    }
    if (stats != nullptr) {
        stats->num_reader_local_cache_total += cast_set<int64_t>(pinned_read_count);
        stats->num_reader_local_cache_hit += cast_set<int64_t>(pinned_read_count);
        stats->bytes_reader_local_cache_request += cast_set<int64_t>(result.size);
        stats->bytes_read_from_reader_local_cache += cast_set<int64_t>(result.size);
    }
    return true;
}

Status CachedRemoteFileReader::_read_local_block(const FileBlockSPtr& block, size_t file_offset,
                                                 size_t absolute_offset, Slice result,
                                                 ReadStatistics& stats,
                                                 bool bypass_reader_local_cache) {
    if (!_enable_reader_local_cache || _reader_local_file_cache == nullptr ||
        bypass_reader_local_cache) {
        return block->read(result, file_offset);
    }

    size_t current_offset = absolute_offset;
    const size_t request_end = absolute_offset + result.size;
    while (current_offset < request_end) {
        const size_t aligned_offset =
                current_offset / READER_LOCAL_CACHE_BLOCK_BYTES * READER_LOCAL_CACHE_BLOCK_BYTES;
        const size_t buffer_offset = std::max(aligned_offset, block->range().left);
        const size_t buffer_end = std::min({aligned_offset + READER_LOCAL_CACHE_BLOCK_BYTES,
                                            block->range().right + 1, size()});
        const size_t copy_end = std::min(request_end, buffer_end);
        const size_t copy_size = copy_end - current_offset;
        const size_t result_offset = current_offset - absolute_offset;

        stats.num_reader_local_cache_total++;
        stats.bytes_reader_local_cache_request += cast_set<int64_t>(copy_size);

        const size_t buffer_size = buffer_end - buffer_offset;
        FileScannerV2ReaderLocalFileCache::LookupResult lookup;
        RETURN_IF_ERROR(_reader_local_file_cache->get_or_load(
                buffer_offset, buffer_size, block, buffer_offset - block->range().left, &lookup));
        stats.num_reader_local_cache_evict += cast_set<int64_t>(lookup.evicted);
        stats.num_reader_local_cache_admission_reject += lookup.admission_rejected ? 1 : 0;
        stats.reader_local_cache_fill_timer += lookup.fill_time;
        if (lookup.waited) {
            stats.num_reader_local_cache_wait++;
            stats.reader_local_cache_wait_timer += lookup.wait_time;
        }
        if (!lookup.admitted) {
            // Cache memory is best-effort: preserve the FileCache hit when query or process memory
            // is tight instead of failing the scan for an optional promotion.
            RETURN_IF_ERROR(block->read(Slice(result.data + result_offset, copy_size),
                                        current_offset - block->range().left));
            stats.num_reader_local_cache_miss++;
            current_offset = copy_end;
            continue;
        }
        if (lookup.hit) {
            stats.num_reader_local_cache_hit++;
            stats.bytes_read_from_reader_local_cache += cast_set<int64_t>(copy_size);
        } else {
            stats.num_reader_local_cache_miss++;
            stats.num_reader_local_cache_fill++;
            stats.bytes_read_into_reader_local_cache += cast_set<int64_t>(buffer_size);
        }

        memcpy(result.data + result_offset, lookup.data->data() + current_offset - buffer_offset,
               copy_size);
        current_offset = copy_end;
    }
    return Status::OK();
}

std::vector<FileBlockSPtr> CachedRemoteFileReader::_collect_remote_read_blocks(
        const FileBlocksHolder& holder, ReadStatistics& stats) {
    std::vector<FileBlockSPtr> empty_blocks;
    for (auto& block : holder.file_blocks) {
        switch (block->state()) {
        case FileBlock::State::EMPTY:
            VLOG_DEBUG << fmt::format("Block EMPTY path={} hash={}:{}:{} offset={} cache_path={}",
                                      path().native(), _cache_hash.to_string(), _cache_hash.high(),
                                      _cache_hash.low(), block->offset(), block->get_cache_file());
            block->get_or_set_downloader();
            if (block->is_downloader()) {
                empty_blocks.push_back(block);
                TEST_SYNC_POINT_CALLBACK("CachedRemoteFileReader::EMPTY");
            }
            stats.hit_cache = false;
            break;
        case FileBlock::State::SKIP_CACHE:
            VLOG_DEBUG << fmt::format(
                    "Block SKIP_CACHE path={} hash={}:{}:{} offset={} cache_path={}",
                    path().native(), _cache_hash.to_string(), _cache_hash.high(), _cache_hash.low(),
                    block->offset(), block->get_cache_file());
            empty_blocks.push_back(block);
            stats.hit_cache = false;
            stats.skip_cache = true;
            break;
        case FileBlock::State::DOWNLOADING:
            stats.hit_cache = false;
            break;
        case FileBlock::State::DOWNLOADED:
            _insert_file_reader(block);
            break;
        }
    }
    return empty_blocks;
}

Status CachedRemoteFileReader::_read_remote_blocks_into_cache(
        const std::vector<FileBlockSPtr>& empty_blocks, size_t offset, size_t bytes_req,
        size_t already_read, Slice result, bool is_dryrun, ReadStatistics& stats,
        SourceReadBreakdown& source_read_breakdown, const IOContext* io_ctx,
        size_t& indirect_read_bytes, PeerFetchedBlockSet& fetched_blocks) {
    if (empty_blocks.empty()) {
        return Status::OK();
    }

    const size_t empty_start = empty_blocks.front()->range().left;
    const size_t empty_end = empty_blocks.back()->range().right;
    const size_t span_read_size = empty_end - empty_start + 1;
    const auto peer_fetch_layout = build_peer_fetch_layout(empty_blocks, size());
    std::unique_ptr<char[]> buffer;
    PeerFetchResult peer_result;

    RETURN_IF_ERROR(_execute_remote_read(empty_blocks, empty_start, span_read_size, buffer,
                                         &peer_result, stats, io_ctx));
    if (stats.from_peer_cache) {
        ++source_read_breakdown.peer_requests;
    } else {
        ++source_read_breakdown.remote_requests;
    }

    std::vector<std::vector<const PeerFetchChunk*>> peer_chunks_by_block;
    if (stats.from_peer_cache) {
        // Peer returns sparse payloads; remember the exact sparse blocks that were filled.
        peer_chunks_by_block.resize(empty_blocks.size());
        for (const auto& chunk : peer_result.chunks) {
            DCHECK_LT(chunk.block_index, empty_blocks.size());
            peer_chunks_by_block[chunk.block_index].push_back(&chunk);
        }
    }

    SCOPED_CONCURRENCY_COUNT(ConcurrencyStatsManager::instance().cached_remote_reader_write_back);
    fetched_blocks.reserve(fetched_blocks.size() + empty_blocks.size());
    for (size_t idx = 0; idx < empty_blocks.size(); ++idx) {
        auto& block = empty_blocks[idx];
        fetched_blocks.insert(block.get());
        if (block->state() == FileBlock::State::SKIP_CACHE) {
            continue;
        }

        SCOPED_RAW_TIMER(&stats.local_write_timer);
        size_t block_size = block->range().size();
        Status st;
        if (stats.from_peer_cache) {
            block_size = peer_fetch_layout.block_sizes[idx];
            if (block_size == 0) {
                continue;
            }
            st = write_peer_payloads_into_block(block, peer_chunks_by_block[idx], &block_size);
        } else {
            char* current_ptr = buffer.get() + block->range().left - empty_start;
            st = block->append(Slice(current_ptr, block_size));
        }
        if (st.ok()) {
            st = block->finalize();
        }
        if (!st.ok()) {
            LOG(WARNING) << "write data to file cache failed, source="
                         << (stats.from_peer_cache ? "peer" : "remote")
                         << ", path=" << path().native() << ", tablet_id=" << _tablet_id
                         << ", file_size=" << size() << ", cache_hash=" << _cache_hash.to_string()
                         << ", write_block_size=" << block_size
                         << ", block=" << block->get_info_for_log()
                         << ", cache_file=" << block->get_cache_file() << ", err=" << st;
        } else {
            _insert_file_reader(block);
            stats.bytes_write_into_file_cache += block_size;
        }
    }

    const size_t right_offset = offset + bytes_req - 1;
    if (stats.from_peer_cache) {
        if (is_dryrun) {
            return Status::OK();
        }
        for (const auto& chunk : peer_result.chunks) {
            copy_peer_chunk_to_result(chunk, offset, right_offset, already_read, result,
                                      indirect_read_bytes, source_read_breakdown);
        }
        return Status::OK();
    }

    if (empty_start <= right_offset && empty_end >= offset + already_read && !is_dryrun) {
        size_t copy_left_offset = std::max(offset + already_read, empty_start);
        size_t copy_right_offset = std::min(right_offset, empty_end);
        char* dst = result.data + (copy_left_offset - offset);
        char* src = buffer.get() + (copy_left_offset - empty_start);
        size_t copy_size = copy_right_offset - copy_left_offset + 1;
        memcpy(dst, src, copy_size);
        indirect_read_bytes += copy_size;
        source_read_breakdown.remote_bytes += copy_size;
    }
    return Status::OK();
}

Status CachedRemoteFileReader::_read_remaining_blocks_from_cache(
        const FileBlocksHolder& holder, size_t offset, size_t bytes_req, Slice result,
        bool is_dryrun, const PeerFetchedBlockSet& fetched_blocks, ReadStatistics& stats,
        SourceReadBreakdown& source_read_breakdown, size_t& indirect_read_bytes, size_t* bytes_read,
        const IOContext* io_ctx) {
    size_t current_offset = offset + *bytes_read;
    size_t end_offset = offset + bytes_req - 1;
    bool need_self_heal = false;
    for (auto& block : holder.file_blocks) {
        if (current_offset > end_offset) {
            break;
        }

        size_t left = block->range().left;
        size_t right = block->range().right;
        if (right < offset) {
            continue;
        }

        size_t read_size =
                end_offset > right ? right - current_offset + 1 : end_offset - current_offset + 1;
        if (contains_file_block(fetched_blocks, block)) {
            *bytes_read += read_size;
            current_offset = right + 1;
            continue;
        }

        FileBlock::State block_state = block->state();
        int64_t wait_time = 0;
        static int64_t max_wait_time = 10;
        TEST_SYNC_POINT_CALLBACK("CachedRemoteFileReader::max_wait_time", &max_wait_time);
        if (block_state != FileBlock::State::DOWNLOADED) {
            SCOPED_CONCURRENCY_COUNT(
                    ConcurrencyStatsManager::instance().cached_remote_reader_blocking);
            do {
                SCOPED_RAW_TIMER(&stats.remote_wait_timer);
                TEST_SYNC_POINT_CALLBACK("CachedRemoteFileReader::DOWNLOADING");
                block_state = block->wait();
                if (block_state != FileBlock::State::DOWNLOADING) {
                    break;
                }
            } while (++wait_time < max_wait_time);
        }
        if (wait_time == max_wait_time) [[unlikely]] {
            LOG_WARNING("Waiting too long for the download to complete");
        }

        Status st;
        /*
         * If block_state == EMPTY, the thread reads the data from remote.
         * If block_state == DOWNLOADED, when the cache file is deleted by the other process,
         * the thread reads the data from remote too.
         */
        if (block_state == FileBlock::State::DOWNLOADED) {
            if (is_dryrun) [[unlikely]] {
                g_skip_local_cache_io_sum_bytes << read_size;
            } else {
                size_t file_offset = current_offset - left;
                SCOPED_RAW_TIMER(&stats.local_read_timer);
                SCOPED_CONCURRENCY_COUNT(
                        ConcurrencyStatsManager::instance().cached_remote_reader_local_read);
                st = _read_local_block(block, file_offset, current_offset,
                                       Slice(result.data + (current_offset - offset), read_size),
                                       stats,
                                       io_ctx != nullptr && io_ctx->bypass_reader_local_cache);
                indirect_read_bytes += read_size;
                if (st.ok()) {
                    source_read_breakdown.local_bytes += read_size;
                }
            }
            if (block_state == FileBlock::State::DOWNLOADED && st.is<ErrorCode::NOT_FOUND>()) {
                need_self_heal = true;
                g_read_cache_self_heal_on_not_found << 1;
                LOG_EVERY_N(WARNING, 100)
                        << "Cache block file is missing, will self-heal by clearing cache hash. "
                        << "path=" << path().native() << ", hash=" << _cache_hash.to_string()
                        << ", offset=" << left << ", err=" << st.msg();
            }
        }
        if (!st || block_state != FileBlock::State::DOWNLOADED) {
            if (is_dryrun) [[unlikely]] {
                *bytes_read += read_size;
                current_offset = right + 1;
                continue;
            }
            LOG(WARNING) << "Read data failed from file cache downloaded by others. err="
                         << st.msg() << ", block state=" << block_state;
            size_t remote_bytes_read {0};
            stats.hit_cache = false;
            stats.from_peer_cache = false;
            s3_read_counter << 1;
            SCOPED_RAW_TIMER(&stats.remote_read_timer);
            RETURN_IF_ERROR(_remote_file_reader->read_at(
                    current_offset, Slice(result.data + (current_offset - offset), read_size),
                    &remote_bytes_read, io_ctx));
            indirect_read_bytes += read_size;
            source_read_breakdown.remote_bytes += remote_bytes_read;
            ++source_read_breakdown.remote_requests;
            DCHECK(remote_bytes_read == read_size);
        }

        *bytes_read += read_size;
        current_offset = right + 1;
    }
    if (need_self_heal && _cache != nullptr) {
        _cache->remove_if_cached_async(_cache_hash);
    }
    return Status::OK();
}

Status CachedRemoteFileReader::read_at_from_cache(size_t offset, Slice result, size_t* bytes_read,
                                                  bool* cache_hit, const IOContext* io_ctx) {
    IOContext default_io_ctx;
    if (io_ctx == nullptr) {
        io_ctx = &default_io_ctx;
    }
    const auto read_type =
            io_ctx->is_inverted_index
                    ? FileCacheReadType::INVERTED_INDEX
                    : (io_ctx->is_index_data ? FileCacheReadType::SEGMENT_FOOTER_INDEX
                                             : FileCacheReadType::DATA);
    auto publish_stats = [&](const ReadStatistics& stats,
                             const SourceReadBreakdown& source_read_breakdown) {
        if (io_ctx->is_dryrun) {
            return;
        }
        if (io_ctx->file_cache_stats != nullptr) {
            _update_stats(stats, source_read_breakdown, io_ctx->file_cache_stats, read_type);
        }
        if (!io_ctx->is_warmup) {
            FileCacheStatistics increment;
            _update_stats(stats, source_read_breakdown, &increment, read_type);
            FileCacheMetrics::instance().update(&increment);
        }
    };
    *bytes_read = 0;
    *cache_hit = false;
    const size_t bytes_req = std::min(result.size, size() - std::min(offset, size()));
    if (bytes_req == 0) {
        *cache_hit = true;
        return Status::OK();
    }
    ReadStatistics stats;
    stats.num_exact_cache_probe = 1;
    MonotonicStopWatch exact_cache_probe_watch;
    exact_cache_probe_watch.start();
    const bool bypass_reader_local_cache = io_ctx != nullptr && io_ctx->bypass_reader_local_cache;
    MonotonicStopWatch reader_local_probe_watch;
    reader_local_probe_watch.start();
    const bool reader_local_hit =
            !bypass_reader_local_cache &&
            _read_from_memory_block_cache(offset, Slice(result.data, bytes_req), &stats);
    stats.reader_local_cache_probe_timer += reader_local_probe_watch.elapsed_time();
    if (reader_local_hit) {
        *bytes_read = bytes_req;
        *cache_hit = true;
        stats.bytes_read = cast_set<int64_t>(bytes_req);
        SourceReadBreakdown source_read_breakdown;
        stats.num_exact_cache_probe_hit = 1;
        stats.exact_cache_probe_timer = exact_cache_probe_watch.elapsed_time();
        publish_stats(stats, source_read_breakdown);
        return Status::OK();
    }

    SourceReadBreakdown source_read_breakdown;
    stats.bytes_read = cast_set<int64_t>(bytes_req);
    const size_t block_size = cast_set<size_t>(config::file_cache_each_block_size);
    const size_t align_left = offset / block_size * block_size;
    const size_t request_end = offset + bytes_req;
    const size_t align_end =
            std::min((request_end + block_size - 1) / block_size * block_size, size());
    const size_t align_size = align_end - align_left;
    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    MonotonicStopWatch sw;
    sw.start();
    FileBlocks downloaded_blocks;
    bool fully_covered = false;
    RETURN_IF_ERROR(_cache->get_downloaded_blocks_if_fully_covered(
            _cache_hash, align_left, align_size, cache_context, &downloaded_blocks,
            &fully_covered));
    stats.cache_get_or_set_timer += sw.elapsed_time();
    if (!fully_covered) {
        stats.num_exact_cache_probe_miss = 1;
        stats.exact_cache_probe_timer = exact_cache_probe_watch.elapsed_time();
        publish_stats(stats, source_read_breakdown);
        return Status::OK();
    }
    FileBlocksHolder holder(std::move(downloaded_blocks));

    for (const auto& block : holder.file_blocks) {
        const size_t read_start = std::max(offset, block->range().left);
        const size_t read_end = std::min(request_end, block->range().right + 1);
        if (read_start >= read_end) {
            continue;
        }
        const size_t read_size = read_end - read_start;
        SCOPED_RAW_TIMER(&stats.local_read_timer);
        const Status st = _read_local_block(block, read_start - block->range().left, read_start,
                                            Slice(result.data + read_start - offset, read_size),
                                            stats, bypass_reader_local_cache);
        if (!st.ok()) {
            // A cache file can be evicted between the state check and the read. Preserve the
            // cache-only contract and let the normal path self-heal through remote storage.
            *bytes_read = 0;
            *cache_hit = false;
            _cache->remove_if_cached_async(_cache_hash);
            stats.num_exact_cache_probe_miss = 1;
            stats.exact_cache_probe_timer = exact_cache_probe_watch.elapsed_time();
            publish_stats(stats, source_read_breakdown);
            return Status::OK();
        }
        source_read_breakdown.local_bytes += cast_set<int64_t>(read_size);
        _cache->add_need_update_lru_block(block);
    }
    *bytes_read = bytes_req;
    *cache_hit = true;
    stats.num_exact_cache_probe_hit = 1;
    stats.exact_cache_probe_timer = exact_cache_probe_watch.elapsed_time();
    publish_stats(stats, source_read_breakdown);
    return Status::OK();
}

Status CachedRemoteFileReader::_read_from_indirect_cache(size_t offset, Slice result,
                                                         size_t bytes_req, size_t already_read,
                                                         bool is_dryrun, size_t* bytes_read,
                                                         ReadStatistics& stats,
                                                         SourceReadBreakdown& source_read_breakdown,
                                                         const IOContext* io_ctx) {
    g_read_cache_indirect_num << 1;
    size_t indirect_read_bytes = 0;
    auto [align_left, align_size] =
            s_align_size(offset + already_read, bytes_req - already_read, size());
    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    MonotonicStopWatch sw;
    sw.start();
    ConcurrencyStatsManager::instance().cached_remote_reader_get_or_set->increment();
    FileBlocksHolder holder =
            _cache->get_or_set(_cache_hash, align_left, align_size, cache_context);
    ConcurrencyStatsManager::instance().cached_remote_reader_get_or_set->decrement();
    stats.cache_get_or_set_timer += sw.elapsed_time();

    auto empty_blocks = _collect_remote_read_blocks(holder, stats);
    PeerFetchedBlockSet fetched_blocks;
    size_t run_start = 0;
    for (size_t index = 1; index <= empty_blocks.size(); ++index) {
        const bool end_of_run =
                index == empty_blocks.size() ||
                empty_blocks[index - 1]->range().right + 1 != empty_blocks[index]->range().left;
        if (!end_of_run) {
            continue;
        }
        // A cache hit is a hard merge boundary. Reading across it would redownload resident data
        // and violate the cache-aware miss coalescing invariant used by StarRocks.
        std::vector<FileBlockSPtr> contiguous_misses(empty_blocks.begin() + run_start,
                                                     empty_blocks.begin() + index);
        RETURN_IF_ERROR(_read_remote_blocks_into_cache(
                contiguous_misses, offset, bytes_req, already_read, result, is_dryrun, stats,
                source_read_breakdown, io_ctx, indirect_read_bytes, fetched_blocks));
        run_start = index;
    }
    *bytes_read = already_read;
    RETURN_IF_ERROR(_read_remaining_blocks_from_cache(holder, offset, bytes_req, result, is_dryrun,
                                                      fetched_blocks, stats, source_read_breakdown,
                                                      indirect_read_bytes, bytes_read, io_ctx));
    g_read_cache_indirect_bytes << indirect_read_bytes;
    g_read_cache_indirect_total_bytes << *bytes_read;
    DCHECK(*bytes_read == bytes_req);
    return Status::OK();
}

Status CachedRemoteFileReader::_read_remote_only_on_cache_miss(
        size_t offset, Slice result, size_t bytes_req, bool is_dryrun, size_t* bytes_read,
        ReadStatistics& stats, SourceReadBreakdown& source_read_breakdown,
        const IOContext* io_ctx) {
    auto read_remote = [&]() -> Status {
        stats.hit_cache = false;
        stats.from_peer_cache = false;
        stats.skip_cache = true;
        s3_read_counter << 1;
        if (is_dryrun) [[unlikely]] {
            *bytes_read = bytes_req;
            g_read_cache_indirect_bytes << 0;
            g_read_cache_indirect_total_bytes << bytes_req;
            return Status::OK();
        }

        size_t remote_bytes_read = bytes_req;
        SCOPED_RAW_TIMER(&stats.remote_read_timer);
        RETURN_IF_ERROR(_remote_file_reader->read_at(offset, Slice(result.data, bytes_req),
                                                     &remote_bytes_read, io_ctx));
        *bytes_read = remote_bytes_read;
        DCHECK_EQ(*bytes_read, bytes_req);
        source_read_breakdown.remote_bytes += remote_bytes_read;
        ++source_read_breakdown.remote_requests;
        g_read_cache_indirect_bytes << remote_bytes_read;
        g_read_cache_indirect_total_bytes << remote_bytes_read;
        return Status::OK();
    };

    g_read_cache_indirect_num << 1;
    CacheContext cache_context(io_ctx);
    cache_context.stats = &stats;
    cache_context.tablet_id = _tablet_id;
    FileBlocks file_blocks;
    bool fully_covered = false;
    {
        SCOPED_RAW_TIMER(&stats.get_timer);
        RETURN_IF_ERROR(_cache->get_downloaded_blocks_if_fully_covered(
                _cache_hash, offset, bytes_req, cache_context, &file_blocks, &fully_covered));
    }
    if (!fully_covered) {
        return read_remote();
    }

    size_t local_read_bytes = 0;
    size_t current_offset = offset;
    size_t end_offset = offset + bytes_req - 1;
    for (auto& block : file_blocks) {
        if (current_offset > end_offset) {
            break;
        }
        const auto& block_range = block->range();
        if (block_range.right < current_offset) {
            continue;
        }

        size_t read_left = std::max(current_offset, block_range.left);
        size_t read_right = std::min(end_offset, block_range.right);
        size_t read_size = read_right - read_left + 1;
        if (is_dryrun) [[unlikely]] {
            g_skip_local_cache_io_sum_bytes << read_size;
        } else {
            SCOPED_RAW_TIMER(&stats.local_read_timer);
            // Remote-only governs misses, not downloaded hits. Promote the full cache cell so
            // repeated Parquet page/header reads can reuse memory without another disk read.
            Status st =
                    _read_local_block(block, read_left - block_range.left, read_left,
                                      Slice(result.data + (read_left - offset), read_size), stats);
            if (!st.ok()) {
                if (st.is<ErrorCode::NOT_FOUND>()) {
                    _cache->remove_if_cached_async(_cache_hash);
                }
                LOG_EVERY_N(WARNING, 100)
                        << "Read data failed from file cache in remote-only-on-miss path. "
                        << "Fallback to remote. err=" << st.msg()
                        << ", block state=" << block->state();
                return read_remote();
            }
            source_read_breakdown.local_bytes += read_size;
            local_read_bytes += read_size;
        }
        current_offset = read_right + 1;
    }

    *bytes_read = bytes_req;
    stats.hit_cache = true;
    g_read_cache_indirect_bytes << local_read_bytes;
    g_read_cache_indirect_total_bytes << bytes_req;
    return Status::OK();
}

Status CachedRemoteFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                            const IOContext* io_ctx) {
    SCOPED_CONCURRENCY_COUNT(ConcurrencyStatsManager::instance().cached_remote_reader_read_at);
    IOContext default_io_ctx;
    if (io_ctx == nullptr) {
        io_ctx = &default_io_ctx;
    }
    DCHECK(io_ctx);
    DCHECK(!closed());

    const bool is_dryrun = io_ctx->is_dryrun;
    if (offset > size()) {
        return Status::InvalidArgument(
                fmt::format("offset exceeds file size(offset: {}, file size: {}, path: {})", offset,
                            size(), path().native()));
    }

    size_t bytes_req = std::min(result.size, size() - offset);
    if (UNLIKELY(bytes_req == 0)) {
        *bytes_read = 0;
        return Status::OK();
    }

    ReadStatistics stats;
    SourceReadBreakdown source_read_breakdown;
    Status read_st = Status::OK();
    MonotonicStopWatch read_at_sw;
    read_at_sw.start();
    stats.bytes_read += bytes_req;
    Defer defer {[&]() {
        if (config::print_stack_when_cache_miss) {
            if (io_ctx->file_cache_stats == nullptr && !stats.hit_cache && !io_ctx->is_warmup) {
                LOG_INFO("[verbose] {}", Status::InternalError<true>("not hit cache"));
            }
        }
        if (!stats.hit_cache && config::read_cluster_cache_opt_verbose_log) {
            LOG_INFO(
                    "[verbose] not hit cache, path: {}, offset: {}, size: {}, cost: {} ms, warmup: "
                    "{}",
                    path().native(), offset, bytes_req, read_at_sw.elapsed_time_milliseconds(),
                    io_ctx->is_warmup);
        }
        if (read_st.ok() && !is_dryrun) {
            // Only successful reads contribute to query profile and file-cache metrics.
            const auto file_cache_read_type =
                    io_ctx->is_inverted_index
                            ? FileCacheReadType::INVERTED_INDEX
                            : (io_ctx->is_index_data ? FileCacheReadType::SEGMENT_FOOTER_INDEX
                                                     : FileCacheReadType::DATA);
            if (io_ctx->file_cache_stats) {
                _update_stats(stats, source_read_breakdown, io_ctx->file_cache_stats,
                              file_cache_read_type);
                auto* limiter = io_ctx->remote_scan_cache_write_limiter;
                if (limiter != nullptr) {
                    io_ctx->file_cache_stats->remote_only_on_miss_triggered =
                            io_ctx->file_cache_stats->remote_only_on_miss_triggered ||
                            limiter->remote_only_on_miss();
                    io_ctx->file_cache_stats->remote_only_on_miss_threshold_bytes =
                            limiter->threshold_bytes();
                }
            }
            if (!io_ctx->is_warmup) {
                FileCacheStatistics fcache_stats_increment;
                _update_stats(stats, source_read_breakdown, &fcache_stats_increment,
                              file_cache_read_type);
                io::FileCacheMetrics::instance().update(&fcache_stats_increment);
            }
        }
    }};

    const bool bypass_reader_local_cache = io_ctx->bypass_reader_local_cache;
    MonotonicStopWatch reader_local_probe_watch;
    reader_local_probe_watch.start();
    const bool reader_local_hit =
            !is_dryrun && !bypass_reader_local_cache &&
            _read_from_memory_block_cache(offset, Slice(result.data, bytes_req), &stats);
    stats.reader_local_cache_probe_timer += reader_local_probe_watch.elapsed_time();
    if (reader_local_hit) {
        // A resident file-local block is authoritative for this immutable file identity; avoid
        // taking FileCache metadata locks again on the hot path.
        *bytes_read = bytes_req;
        return Status::OK();
    }

    if (use_remote_only_on_cache_miss(io_ctx)) {
        read_st = _read_remote_only_on_cache_miss(offset, result, bytes_req, is_dryrun, bytes_read,
                                                  stats, source_read_breakdown, io_ctx);
        return read_st;
    }

    size_t already_read = 0;
    if (_try_read_from_cached_files_directly(offset, result, bytes_req, is_dryrun, stats,
                                             source_read_breakdown, already_read, bytes_read)) {
        return Status::OK();
    }

    read_st = _read_from_indirect_cache(offset, result, bytes_req, already_read, is_dryrun,
                                        bytes_read, stats, source_read_breakdown, io_ctx);
    return read_st;
}

void CachedRemoteFileReader::prefetch_range(size_t offset, size_t size, const IOContext* io_ctx) {
    if (offset >= this->size() || size == 0) {
        return;
    }

    size = std::min(size, this->size() - offset);

    ThreadPool* pool = ExecEnv::GetInstance()->segment_prefetch_thread_pool();
    if (pool == nullptr) {
        return;
    }

    IOContext dryrun_ctx;
    if (io_ctx != nullptr) {
        dryrun_ctx = *io_ctx;
    }
    dryrun_ctx.is_dryrun = true;
    dryrun_ctx.query_id = nullptr;
    dryrun_ctx.file_cache_stats = nullptr;
    dryrun_ctx.file_reader_stats = nullptr;
    dryrun_ctx.remote_scan_cache_write_limiter = nullptr;

    LOG_IF(INFO, config::enable_segment_prefetch_verbose_log)
            << fmt::format("[verbose] Submitting prefetch task for offset={} size={}, file={}",
                           offset, size, path().filename().native());
    std::weak_ptr<CachedRemoteFileReader> weak_this = shared_from_this();
    auto st = pool->submit_func([weak_this, offset, size, dryrun_ctx]() {
        auto self = weak_this.lock();
        if (self == nullptr) {
            return;
        }
        size_t bytes_read = 0;
        Slice dummy_buffer((char*)nullptr, size);
        (void)self->read_at_impl(offset, dummy_buffer, &bytes_read, &dryrun_ctx);
        LOG_IF(INFO, config::enable_segment_prefetch_verbose_log)
                << fmt::format("[verbose] Prefetch task completed for offset={} size={}, file={}",
                               offset, size, self->path().filename().native());
    });

    if (!st.ok()) {
        VLOG_DEBUG << "Failed to submit prefetch task for offset=" << offset << " size=" << size
                   << " error=" << st.to_string();
    }
}

void CachedRemoteFileReader::_update_stats(const ReadStatistics& read_stats,
                                           const SourceReadBreakdown& source_read_breakdown,
                                           FileCacheStatistics* statis,
                                           FileCacheReadType read_type) const {
    if (statis == nullptr) {
        return;
    }
    const bool has_source_bytes = source_read_breakdown.local_bytes != 0 ||
                                  source_read_breakdown.remote_bytes != 0 ||
                                  source_read_breakdown.peer_bytes != 0;
    const bool has_source_activity = has_source_bytes ||
                                     source_read_breakdown.remote_requests != 0 ||
                                     source_read_breakdown.peer_requests != 0;
    const bool exact_probe_miss_without_io =
            read_stats.num_exact_cache_probe_miss != 0 && !has_source_bytes;
    const bool reader_local_only =
            read_stats.bytes_read != 0 &&
            read_stats.bytes_read_from_reader_local_cache == read_stats.bytes_read;
    if (has_source_activity) {
        if (source_read_breakdown.local_bytes != 0) {
            statis->num_local_io_total++;
            statis->bytes_read_from_local += source_read_breakdown.local_bytes;
        }
        if (source_read_breakdown.peer_bytes != 0 || source_read_breakdown.peer_requests != 0 ||
            read_stats.from_peer_cache) {
            // Count peer IO whenever peer was used, even if its fetched blocks were entirely
            // outside the copy range (e.g., backward-aligned prefetch block before
            // offset+already_read).  In that case peer_bytes==0 but the peer RPC did happen
            // and wrote data into the local file cache.
            statis->num_peer_io_total += std::max<int64_t>(source_read_breakdown.peer_requests, 1);
            statis->bytes_read_from_peer += source_read_breakdown.peer_bytes;
            statis->peer_io_timer += read_stats.peer_read_timer;
        }
        if (source_read_breakdown.remote_bytes != 0 || source_read_breakdown.remote_requests != 0) {
            statis->num_remote_io_total +=
                    std::max<int64_t>(source_read_breakdown.remote_requests, 1);
            statis->bytes_read_from_remote += source_read_breakdown.remote_bytes;
            statis->remote_io_timer += read_stats.remote_read_timer;
        }
    } else if (exact_probe_miss_without_io) {
        // A cache-only miss is a lookup result, not physical remote IO. MergeRange will account
        // the subsequent fallback read independently.
    } else if (reader_local_only) {
        // Reader-local memory hits have dedicated counters and must not inflate physical disk IO.
    } else if (read_stats.hit_cache) {
        statis->num_local_io_total++;
        statis->bytes_read_from_local += read_stats.bytes_read;
    } else if (read_stats.from_peer_cache) {
        statis->num_peer_io_total++;
        statis->bytes_read_from_peer += read_stats.bytes_read;
        statis->peer_io_timer += read_stats.peer_read_timer;
    } else {
        statis->num_remote_io_total++;
        statis->bytes_read_from_remote += read_stats.bytes_read;
        statis->remote_io_timer += read_stats.remote_read_timer;
    }
    statis->remote_wait_timer += read_stats.remote_wait_timer;
    statis->local_io_timer += read_stats.local_read_timer;
    statis->num_skip_cache_io_total += read_stats.skip_cache;
    statis->bytes_write_into_cache += read_stats.bytes_write_into_file_cache;
    statis->write_cache_io_timer += read_stats.local_write_timer;

    statis->read_cache_file_directly_timer += read_stats.read_cache_file_directly_timer;
    statis->cache_get_or_set_timer += read_stats.cache_get_or_set_timer;
    statis->lock_wait_timer += read_stats.lock_wait_timer;
    statis->get_timer += read_stats.get_timer;
    statis->set_timer += read_stats.set_timer;
    statis->num_reader_local_cache_total += read_stats.num_reader_local_cache_total;
    statis->num_reader_local_cache_hit += read_stats.num_reader_local_cache_hit;
    statis->num_reader_local_cache_miss += read_stats.num_reader_local_cache_miss;
    statis->num_reader_local_cache_fill += read_stats.num_reader_local_cache_fill;
    statis->num_reader_local_cache_evict += read_stats.num_reader_local_cache_evict;
    statis->num_reader_local_cache_wait += read_stats.num_reader_local_cache_wait;
    statis->num_reader_local_cache_admission_reject +=
            read_stats.num_reader_local_cache_admission_reject;
    statis->num_reader_local_cache_partial_miss += read_stats.num_reader_local_cache_partial_miss;
    statis->num_reader_local_cache_disk_lru_touch +=
            read_stats.num_reader_local_cache_disk_lru_touch;
    statis->bytes_reader_local_cache_request += read_stats.bytes_reader_local_cache_request;
    statis->bytes_read_from_reader_local_cache += read_stats.bytes_read_from_reader_local_cache;
    statis->bytes_read_into_reader_local_cache += read_stats.bytes_read_into_reader_local_cache;
    statis->reader_local_cache_fill_timer += read_stats.reader_local_cache_fill_timer;
    statis->reader_local_cache_wait_timer += read_stats.reader_local_cache_wait_timer;
    statis->reader_local_cache_probe_timer += read_stats.reader_local_cache_probe_timer;
    statis->num_exact_cache_probe += read_stats.num_exact_cache_probe;
    statis->num_exact_cache_probe_hit += read_stats.num_exact_cache_probe_hit;
    statis->num_exact_cache_probe_miss += read_stats.num_exact_cache_probe_miss;
    statis->exact_cache_probe_timer += read_stats.exact_cache_probe_timer;

    auto update_index_stats = [&](int64_t& num_local_io_total, int64_t& num_remote_io_total,
                                  int64_t& num_peer_io_total, int64_t& bytes_read_from_local,
                                  int64_t& bytes_read_from_remote, int64_t& bytes_read_from_peer,
                                  int64_t& local_io_timer, int64_t& remote_io_timer,
                                  int64_t& peer_io_timer, int64_t& write_cache_io_timer,
                                  int64_t& bytes_write_into_cache) {
        if (has_source_bytes) {
            if (source_read_breakdown.local_bytes != 0) {
                num_local_io_total++;
                bytes_read_from_local += source_read_breakdown.local_bytes;
            }
            if (source_read_breakdown.peer_bytes != 0 || read_stats.from_peer_cache) {
                num_peer_io_total++;
                bytes_read_from_peer += source_read_breakdown.peer_bytes;
                peer_io_timer += read_stats.peer_read_timer;
            }
            if (source_read_breakdown.remote_bytes != 0) {
                num_remote_io_total++;
                bytes_read_from_remote += source_read_breakdown.remote_bytes;
                remote_io_timer += read_stats.remote_read_timer;
            }
        } else if (read_stats.hit_cache) {
            num_local_io_total++;
            bytes_read_from_local += read_stats.bytes_read;
        } else if (read_stats.from_peer_cache) {
            num_peer_io_total++;
            bytes_read_from_peer += read_stats.bytes_read;
            peer_io_timer += read_stats.peer_read_timer;
        } else {
            num_remote_io_total++;
            bytes_read_from_remote += read_stats.bytes_read;
            remote_io_timer += read_stats.remote_read_timer;
        }
        local_io_timer += read_stats.local_read_timer;
        write_cache_io_timer += read_stats.local_write_timer;
        bytes_write_into_cache += read_stats.bytes_write_into_file_cache;
    };

    switch (read_type) {
    case FileCacheReadType::DATA:
        break;
    case FileCacheReadType::INVERTED_INDEX:
        update_index_stats(
                statis->inverted_index_num_local_io_total,
                statis->inverted_index_num_remote_io_total,
                statis->inverted_index_num_peer_io_total,
                statis->inverted_index_bytes_read_from_local,
                statis->inverted_index_bytes_read_from_remote,
                statis->inverted_index_bytes_read_from_peer, statis->inverted_index_local_io_timer,
                statis->inverted_index_remote_io_timer, statis->inverted_index_peer_io_timer,
                statis->inverted_index_write_cache_io_timer,
                statis->inverted_index_bytes_write_into_cache);
        break;
    case FileCacheReadType::SEGMENT_FOOTER_INDEX:
        update_index_stats(statis->segment_footer_index_num_local_io_total,
                           statis->segment_footer_index_num_remote_io_total,
                           statis->segment_footer_index_num_peer_io_total,
                           statis->segment_footer_index_bytes_read_from_local,
                           statis->segment_footer_index_bytes_read_from_remote,
                           statis->segment_footer_index_bytes_read_from_peer,
                           statis->segment_footer_index_local_io_timer,
                           statis->segment_footer_index_remote_io_timer,
                           statis->segment_footer_index_peer_io_timer,
                           statis->segment_footer_index_write_cache_io_timer,
                           statis->segment_footer_index_bytes_write_into_cache);
        break;
    }

    g_skip_cache_sum << read_stats.skip_cache;
}

} // namespace doris::io
