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

namespace doris {
namespace segment_v2 {

InvertedIndexSearcherCache* InvertedIndexSearcherCache::_s_instance = nullptr;

static IndexSearcherPtr build_index_searcher(const io::FileSystemSPtr& fs,
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

void InvertedIndexSearcherCache::create_global_instance(size_t capacity) {
    DCHECK(_s_instance == nullptr);
    static InvertedIndexSearcherCache instance(capacity);
    _s_instance = &instance;
}

InvertedIndexSearcherCache::InvertedIndexSearcherCache(size_t capacity)
        : _mem_tracker(std::make_unique<MemTracker>("InvertedIndexSearcherCache")) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    _cache = std::unique_ptr<Cache>(
            new_lru_cache("InvertedIndexSearcher:InvertedIndexSearcherCache", capacity));
}

Status InvertedIndexSearcherCache::get_index_searcher(const io::FileSystemSPtr& fs,
                                                      const std::string& index_dir,
                                                      const std::string& file_name,
                                                      InvertedIndexCacheHandle* cache_handle,
                                                      bool use_cache) {
    auto file_path = index_dir + "/" + file_name;
    InvertedIndexSearcherCache::CacheKey cache_key(file_path);
    if (_lookup(cache_key, cache_handle)) {
        cache_handle->owned = false;
        return Status::OK();
    }
    cache_handle->owned = !use_cache;
    IndexSearcherPtr index_searcher = nullptr;
    auto mem_tracker =
            std::unique_ptr<MemTracker>(new MemTracker("InvertedIndexSearcherCacheWithRead"));
    {
        SCOPED_CONSUME_MEM_TRACKER(mem_tracker.get());
        index_searcher = build_index_searcher(fs, index_dir, file_name);
    }

    if (use_cache) {
        InvertedIndexSearcherCache::CacheValue* cache_value =
                new InvertedIndexSearcherCache::CacheValue();
        cache_value->index_searcher = std::move(index_searcher);
        cache_value->size = mem_tracker->consumption();
        _insert(cache_key, *cache_value, cache_handle);
    } else {
        cache_handle->index_searcher = std::move(index_searcher);
    }
    return Status::OK();
}

Status InvertedIndexSearcherCache::insert(const io::FileSystemSPtr& fs,
                                          const std::string& index_dir,
                                          const std::string& file_name) {
    auto file_path = index_dir + "/" + file_name;
    InvertedIndexSearcherCache::CacheKey cache_key(file_path);
    InvertedIndexSearcherCache::CacheValue* cache_value =
            new InvertedIndexSearcherCache::CacheValue();
    IndexSearcherPtr index_searcher = nullptr;
    auto mem_tracker =
            std::unique_ptr<MemTracker>(new MemTracker("InvertedIndexSearcherCacheWithInsert"));
    {
        SCOPED_CONSUME_MEM_TRACKER(mem_tracker.get());
        index_searcher = build_index_searcher(fs, index_dir, file_name);
    }

    cache_value->index_searcher = std::move(index_searcher);
    cache_value->first_start_time = UnixMillis();
    cache_value->size = mem_tracker->consumption();

    InvertedIndexCacheHandle inverted_index_cache_handle;
    _insert(cache_key, *cache_value, &inverted_index_cache_handle);
    return Status::OK();
}

Status InvertedIndexSearcherCache::erase(const std::string& index_file_path) {
    InvertedIndexSearcherCache::CacheKey cache_key(index_file_path);
    _cache->erase(cache_key.index_file_path);
    return Status::OK();
}

Status InvertedIndexSearcherCache::prune() {
    const int64_t curtime = UnixMillis();
    auto pred = [curtime](const void* value) -> bool {
        InvertedIndexSearcherCache::CacheValue* cache_value =
                (InvertedIndexSearcherCache::CacheValue*)value;
        bool expired = false;
        if (cache_value->first_start_time != 0) {
            // add into cache after write index, but no visited in 15 minutes
            auto start_expired_time =
                    cache_value->first_start_time +
                    (config::index_searcher_cache_stale_sweep_time_sec / 2) * 1000;
            if (start_expired_time < curtime && cache_value->last_visit_time == 0) {
                expired = true;
            }
        }

        if (cache_value->last_visit_time != 0) {
            // no vistited in 30 minutes
            auto visit_expired_time = cache_value->last_visit_time +
                                      config::index_searcher_cache_stale_sweep_time_sec * 1000;
            if (visit_expired_time < curtime) {
                expired = true;
            }
        }

        return expired;
    };

    MonotonicStopWatch watch;
    watch.start();
    int64_t prune_num = _cache->prune_if(pred);
    if (prune_num > 0)
        LOG(INFO) << "prune " << prune_num << " entries in index_searcher cache. cost(ms): "
                  << watch.elapsed_time() / 1000 / 1000;
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

void InvertedIndexSearcherCache::_insert(const InvertedIndexSearcherCache::CacheKey& key,
                                         CacheValue& value, InvertedIndexCacheHandle* handle) {
    auto deleter = [](const doris::CacheKey& key, void* value) {
        InvertedIndexSearcherCache::CacheValue* cache_value =
                (InvertedIndexSearcherCache::CacheValue*)value;
        delete cache_value;
    };

    auto lru_handle =
            _cache->insert(key.index_file_path, &value, value.size, deleter, CachePriority::NORMAL);
    *handle = InvertedIndexCacheHandle(_cache.get(), lru_handle);
}

} // namespace segment_v2
} // namespace doris
