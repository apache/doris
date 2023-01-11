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

#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "util/defer_op.h"

namespace doris {
namespace segment_v2 {

InvertedIndexSearcherCache* InvertedIndexSearcherCache::_s_instance = nullptr;

IndexSearcherPtr InvertedIndexSearcherCache::build_index_searcher(const io::FileSystemSPtr& fs,
                                                                  const std::string& index_dir,
                                                                  const std::string& file_name) {
    DorisCompoundReader* directory = new DorisCompoundReader(
            DorisCompoundDirectory::getDirectory(fs, index_dir.c_str()), file_name.c_str());
    auto closeDirectory = true;
    auto index_searcher =
            std::make_shared<lucene::search::IndexSearcher>(directory, closeDirectory);
    // NOTE: need to cl_refcount-- here, so that directory will be deleted when
    // index_searcher is destroyed
    _CLDECDELETE(directory)
    return index_searcher;
}

void InvertedIndexSearcherCache::create_global_instance(size_t capacity, uint32_t num_shards) {
    DCHECK(_s_instance == nullptr);
    static InvertedIndexSearcherCache instance(capacity, num_shards);
    _s_instance = &instance;
}

InvertedIndexSearcherCache::InvertedIndexSearcherCache(size_t capacity, uint32_t num_shards)
        : _mem_tracker(std::make_unique<MemTracker>("InvertedIndexSearcherCache")) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    if (config::enable_index_cache_check_timestamp) {
        auto get_last_visit_time = [](const void* value) -> int64_t {
            InvertedIndexSearcherCache::CacheValue* cache_value =
                    (InvertedIndexSearcherCache::CacheValue*)value;
            return cache_value->last_visit_time;
        };
        _cache = std::unique_ptr<Cache>(
                new ShardedLRUCache("InvertedIndexSearcher:InvertedIndexSearcherCache", capacity,
                                    LRUCacheType::SIZE, num_shards, get_last_visit_time, true));
    } else {
        _cache = std::unique_ptr<Cache>(
                new_lru_cache("InvertedIndexSearcher:InvertedIndexSearcherCache", capacity,
                              LRUCacheType::SIZE, num_shards));
    }
}

Status InvertedIndexSearcherCache::get_index_searcher(const io::FileSystemSPtr& fs,
                                                      const std::string& index_dir,
                                                      const std::string& file_name,
                                                      InvertedIndexCacheHandle* cache_handle,
                                                      bool use_cache) {
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

} // namespace segment_v2
} // namespace doris
