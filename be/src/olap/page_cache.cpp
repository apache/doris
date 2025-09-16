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

#include "olap/page_cache.h"

#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>

#include <ostream>

#include "runtime/exec_env.h"

namespace doris {

template <typename T>
MemoryTrackedPageBase<T>::MemoryTrackedPageBase(size_t size, bool use_cache,
                                                segment_v2::PageTypePB page_type)
        : _size(size) {
    if (use_cache) {
        _mem_tracker_by_allocator = StoragePageCache::instance()->mem_tracker(page_type);
    } else {
        _mem_tracker_by_allocator =
                thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker_sptr();
    }
}

MemoryTrackedPageWithPageEntity::MemoryTrackedPageWithPageEntity(size_t size, bool use_cache,
                                                                 segment_v2::PageTypePB page_type)
        : MemoryTrackedPageBase<char*>(size, use_cache, page_type), _capacity(size) {
    {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(this->_mem_tracker_by_allocator);
        this->_data = reinterpret_cast<char*>(
                Allocator<false>::alloc(this->_capacity, ALLOCATOR_ALIGNMENT_16));
    }
}

MemoryTrackedPageWithPageEntity::~MemoryTrackedPageWithPageEntity() {
    if (this->_data != nullptr) {
        DCHECK(this->_capacity != 0 && this->_size != 0);
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(this->_mem_tracker_by_allocator);
        Allocator<false>::free(this->_data, this->_capacity);
    }
}

template <typename T>
MemoryTrackedPageWithPagePtr<T>::MemoryTrackedPageWithPagePtr(size_t size,
                                                              segment_v2::PageTypePB page_type)
        : MemoryTrackedPageBase<std::shared_ptr<T>>(size, true, page_type) {
    DCHECK(this->_size > 0);
    this->_size = size;
    this->_mem_tracker_by_allocator->consume(this->_size);
}

template <typename T>
MemoryTrackedPageWithPagePtr<T>::~MemoryTrackedPageWithPagePtr() {
    DCHECK(this->_size > 0);
    this->_mem_tracker_by_allocator->release(this->_size);
}

StoragePageCache* StoragePageCache::create_global_cache(size_t capacity,
                                                        int32_t index_cache_percentage,
                                                        int64_t pk_index_cache_capacity,
                                                        uint32_t num_shards) {
    return new StoragePageCache(capacity, index_cache_percentage, pk_index_cache_capacity,
                                num_shards);
}

StoragePageCache::StoragePageCache(size_t capacity, int32_t index_cache_percentage,
                                   int64_t pk_index_cache_capacity, uint32_t num_shards)
        : _index_cache_percentage(index_cache_percentage) {
    if (index_cache_percentage == 0) {
        _data_page_cache = std::make_unique<DataPageCache>(capacity, num_shards);
    } else if (index_cache_percentage == 100) {
        _index_page_cache = std::make_unique<IndexPageCache>(capacity, num_shards);
    } else if (index_cache_percentage > 0 && index_cache_percentage < 100) {
        _data_page_cache = std::make_unique<DataPageCache>(
                capacity * (100 - index_cache_percentage) / 100, num_shards);
        _index_page_cache = std::make_unique<IndexPageCache>(
                capacity * index_cache_percentage / 100, num_shards);
    } else {
        CHECK(false) << "invalid index page cache percentage";
    }

    _pk_index_page_cache = std::make_unique<PKIndexPageCache>(pk_index_cache_capacity, num_shards);
}

bool StoragePageCache::lookup(const CacheKey& key, PageCacheHandle* handle,
                              segment_v2::PageTypePB page_type) {
    auto* cache = _get_page_cache(page_type);
    auto* lru_handle = cache->lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = PageCacheHandle(cache, lru_handle);
    return true;
}

void StoragePageCache::insert(const CacheKey& key, DataPage* data, PageCacheHandle* handle,
                              segment_v2::PageTypePB page_type, bool in_memory) {
    CachePriority priority = CachePriority::NORMAL;
    if (in_memory) {
        priority = CachePriority::DURABLE;
    }

    auto* cache = _get_page_cache(page_type);
    auto* lru_handle = cache->insert(key.encode(), data, data->capacity(), 0, priority);
    DCHECK(lru_handle != nullptr);
    *handle = PageCacheHandle(cache, lru_handle);
}

template <typename T>
void StoragePageCache::insert(const CacheKey& key, T data, size_t size, PageCacheHandle* handle,
                              segment_v2::PageTypePB page_type, bool in_memory) {
    static_assert(std::is_same<typename std::remove_cv<T>::type,
                               std::shared_ptr<typename T::element_type>>::value,
                  "Second argument must be a std::shared_ptr");
    using ValueType = typename T::element_type; // Type that shared_ptr points to

    CachePriority priority = CachePriority::NORMAL;
    if (in_memory) {
        priority = CachePriority::DURABLE;
    }

    auto* cache = _get_page_cache(page_type);
    // Lify cycle of page will be managed by StoragePageCache
    auto page = std::make_unique<MemoryTrackedPageWithPagePtr<ValueType>>(size, page_type);
    // Lify cycle of data will be managed by StoragePageCache and user at the same time.
    page->set_data(data);

    auto* lru_handle = cache->insert(key.encode(), page.get(), size, 0, priority);
    DCHECK(lru_handle != nullptr);
    *handle = PageCacheHandle(cache, lru_handle);
    // Now page is managed by StoragePageCache.
    page.release();
}

Slice PageCacheHandle::data() const {
    auto* cache_value = (DataPage*)_cache->value(_handle);
    return {cache_value->data(), cache_value->size()};
}

template void StoragePageCache::insert(const CacheKey& key,
                                       std::shared_ptr<segment_v2::SegmentFooterPB> data,
                                       size_t size, PageCacheHandle* handle,
                                       segment_v2::PageTypePB page_type, bool in_memory);

template class MemoryTrackedPageWithPagePtr<segment_v2::SegmentFooterPB>;

} // namespace doris
