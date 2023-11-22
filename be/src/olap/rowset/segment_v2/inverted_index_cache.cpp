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

#include "olap/rowset/segment_v2/inverted_index_cache.h"

#include <CLucene/debug/mem.h>
#include <CLucene/search/IndexSearcher.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <string.h>
#include <sys/resource.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <iostream>

#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"

namespace doris {
namespace segment_v2 {

IndexSearcherPtr InvertedIndexSearcherCache::build_index_searcher(const io::FileSystemSPtr& fs,
                                                                  const std::string& index_dir,
                                                                  const std::string& file_name) {
    DorisCompoundReader* directory =
            new DorisCompoundReader(DorisCompoundDirectory::getDirectory(fs, index_dir.c_str()),
                                    file_name.c_str(), config::inverted_index_read_buffer_size);
    auto closeDirectory = true;
    auto reader = lucene::index::IndexReader::open(
            directory, config::inverted_index_read_buffer_size, closeDirectory);
    auto index_searcher = std::make_shared<lucene::search::IndexSearcher>(reader);
    // NOTE: need to cl_refcount-- here, so that directory will be deleted when
    // index_searcher is destroyed
    _CLDECDELETE(directory)
    return index_searcher;
}

InvertedIndexSearcherCache* InvertedIndexSearcherCache::create_global_instance(
        size_t capacity, uint32_t num_shards) {
    InvertedIndexSearcherCache* res = new InvertedIndexSearcherCache(capacity, num_shards);
    return res;
}

InvertedIndexSearcherCache::InvertedIndexSearcherCache(size_t capacity, uint32_t num_shards)
        : LRUCachePolicy(CachePolicy::CacheType::INVERTEDINDEX_SEARCHER_CACHE,
                         config::inverted_index_cache_stale_sweep_time_sec),
          _mem_tracker(std::make_unique<MemTracker>("InvertedIndexSearcherCache")) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    uint64_t fd_number = config::min_file_descriptor_number;
    struct rlimit l;
    int ret = getrlimit(RLIMIT_NOFILE, &l);
    if (ret != 0) {
        LOG(WARNING) << "call getrlimit() failed. errno=" << strerror(errno)
                     << ", use default configuration instead.";
    } else {
        fd_number = static_cast<uint64_t>(l.rlim_cur);
    }

    uint64_t open_searcher_limit = fd_number * config::inverted_index_fd_number_limit_percent / 100;
    LOG(INFO) << "fd_number: " << fd_number
              << ", inverted index open searcher limit: " << open_searcher_limit;
#ifdef BE_TEST
    open_searcher_limit = 2;
#endif

    if (config::enable_inverted_index_cache_check_timestamp) {
        auto get_last_visit_time = [](const void* value) -> int64_t {
            InvertedIndexSearcherCache::CacheValue* cache_value =
                    (InvertedIndexSearcherCache::CacheValue*)value;
            return cache_value->last_visit_time;
        };
        _cache = std::unique_ptr<Cache>(
                new ShardedLRUCache("InvertedIndexSearcherCache", capacity, LRUCacheType::SIZE,
                                    num_shards, get_last_visit_time, true, open_searcher_limit));
    } else {
        _cache = std::unique_ptr<Cache>(new ShardedLRUCache("InvertedIndexSearcherCache", capacity,
                                                            LRUCacheType::SIZE, num_shards,
                                                            open_searcher_limit));
    }
}

Status InvertedIndexSearcherCache::get_index_searcher(const io::FileSystemSPtr& fs,
                                                      const std::string& index_dir,
                                                      const std::string& file_name,
                                                      InvertedIndexCacheHandle* cache_handle,
                                                      OlapReaderStatistics* stats, bool use_cache) {
    auto file_path = index_dir + "/" + file_name;

    using namespace std::chrono;
    auto start_time = steady_clock::now();
    Defer cost {[&]() {
        int64_t cost = duration_cast<microseconds>(steady_clock::now() - start_time).count();
        VLOG_DEBUG << "finish get_index_searcher for " << file_path << ", cost=" << cost << "us";
    }};

    InvertedIndexSearcherCache::CacheKey cache_key(file_path);
    if (_lookup(cache_key, cache_handle)) {
        cache_handle->owned = false;
        return Status::OK();
    }

    cache_handle->owned = !use_cache;
    IndexSearcherPtr index_searcher = nullptr;
    auto mem_tracker =
            std::unique_ptr<MemTracker>(new MemTracker("InvertedIndexSearcherCacheWithRead"));
#ifndef BE_TEST
    {
        SCOPED_RAW_TIMER(&stats->inverted_index_searcher_open_timer);
        SCOPED_CONSUME_MEM_TRACKER(mem_tracker.get());
        index_searcher = build_index_searcher(fs, index_dir, file_name);
    }
#endif

    if (use_cache) {
        IndexCacheValuePtr cache_value = std::make_unique<InvertedIndexSearcherCache::CacheValue>();
        cache_value->index_searcher = std::move(index_searcher);
        cache_value->size = mem_tracker->consumption();
        *cache_handle =
                InvertedIndexCacheHandle(_cache.get(), _insert(cache_key, cache_value.release()));
    } else {
        cache_handle->index_searcher = std::move(index_searcher);
    }
    return Status::OK();
}

Status InvertedIndexSearcherCache::insert(const io::FileSystemSPtr& fs,
                                          const std::string& index_dir,
                                          const std::string& file_name) {
    auto file_path = index_dir + "/" + file_name;

    using namespace std::chrono;
    auto start_time = steady_clock::now();
    Defer cost {[&]() {
        int64_t cost = duration_cast<microseconds>(steady_clock::now() - start_time).count();
        VLOG_DEBUG << "finish insert index_searcher for " << file_path << ", cost=" << cost << "us";
    }};

    InvertedIndexSearcherCache::CacheKey cache_key(file_path);
    IndexCacheValuePtr cache_value = std::make_unique<InvertedIndexSearcherCache::CacheValue>();
    IndexSearcherPtr index_searcher = nullptr;
    auto mem_tracker =
            std::unique_ptr<MemTracker>(new MemTracker("InvertedIndexSearcherCacheWithInsert"));
#ifndef BE_TEST
    {
        SCOPED_CONSUME_MEM_TRACKER(mem_tracker.get());
        index_searcher = build_index_searcher(fs, index_dir, file_name);
    }
#endif

    cache_value->index_searcher = std::move(index_searcher);
    cache_value->size = mem_tracker->consumption();
    cache_value->last_visit_time = UnixMillis();
    auto lru_handle = _insert(cache_key, cache_value.release());
    _cache->release(lru_handle);
    return Status::OK();
}

Status InvertedIndexSearcherCache::erase(const std::string& index_file_path) {
    InvertedIndexSearcherCache::CacheKey cache_key(index_file_path);
    _cache->erase(cache_key.index_file_path);
    return Status::OK();
}

int64_t InvertedIndexSearcherCache::mem_consumption() {
    if (_cache) {
        return _cache->mem_consumption();
    }
    return 0L;
}

bool InvertedIndexSearcherCache::_lookup(const InvertedIndexSearcherCache::CacheKey& key,
                                         InvertedIndexCacheHandle* handle) {
    auto lru_handle = _cache->lookup(key.index_file_path);
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = InvertedIndexCacheHandle(_cache.get(), lru_handle);
    return true;
}

Cache::Handle* InvertedIndexSearcherCache::_insert(const InvertedIndexSearcherCache::CacheKey& key,
                                                   CacheValue* value) {
    auto deleter = [](const doris::CacheKey& key, void* value) {
        InvertedIndexSearcherCache::CacheValue* cache_value =
                (InvertedIndexSearcherCache::CacheValue*)value;
        delete cache_value;
    };

    Cache::Handle* lru_handle =
            _cache->insert(key.index_file_path, value, value->size, deleter, CachePriority::NORMAL);
    return lru_handle;
}

bool InvertedIndexQueryCache::lookup(const CacheKey& key, InvertedIndexQueryCacheHandle* handle) {
    if (key.encode().empty()) {
        return false;
    }
    auto lru_handle = _cache->lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = InvertedIndexQueryCacheHandle(_cache.get(), lru_handle);
    return true;
}

void InvertedIndexQueryCache::insert(const CacheKey& key, std::shared_ptr<roaring::Roaring> bitmap,
                                     InvertedIndexQueryCacheHandle* handle) {
    auto deleter = [](const doris::CacheKey& key, void* value) {
        delete (InvertedIndexQueryCache::CacheValue*)value;
    };

    std::unique_ptr<InvertedIndexQueryCache::CacheValue> cache_value_ptr =
            std::make_unique<InvertedIndexQueryCache::CacheValue>();
    cache_value_ptr->last_visit_time = UnixMillis();
    cache_value_ptr->bitmap = bitmap;
    cache_value_ptr->size = bitmap->getSizeInBytes();
    if (key.encode().empty()) {
        return;
    }

    auto lru_handle = _cache->insert(key.encode(), (void*)cache_value_ptr.release(),
                                     bitmap->getSizeInBytes(), deleter, CachePriority::NORMAL);
    *handle = InvertedIndexQueryCacheHandle(_cache.get(), lru_handle);
}

int64_t InvertedIndexQueryCache::mem_consumption() {
    if (_cache) {
        return _cache->mem_consumption();
    }
    return 0L;
}

} // namespace segment_v2
} // namespace doris
