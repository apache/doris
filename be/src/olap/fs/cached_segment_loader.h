#pragma once

#include <memory>
#include <string>
#include <utility>

#include "gutil/macros.h" // for DISALLOW_COPY_AND_ASSIGN
#include "olap/lru_cache.h"
#include "runtime/mem_tracker.h"
#include "util/time.h"

namespace doris {

class CachedSegmentCacheHandle;

// CachedSegmentLoader is used to load the cached segment of remote storage, like S3, HDFS, etc.
// An LRUCache is encapsulated inside it, which is used to cache the cached segments.
// The caller should use the following method to load and obtain the segment:
//
//  CachedSegmentCacheHandle cache_handle;
//  bool cached = StorageEngine::instance()->remote_file_cache()->load_cached_segment(done_file_path, &cache_handle);
//
// Make sure that cache_handle is valid during the segment usage period.
class CachedSegmentLoader {
public:

    // The cache key for cached segment lru cache
    // Holding cached segment done file path
    struct CacheKey {
        CacheKey(std::string done_file_path_) : done_file_path(done_file_path_) {}
        std::string done_file_path;

        // Encode to a flat binary which can be used as LRUCache's key
        std::string encode() const {
            return done_file_path;
        }
    };

    // The cache value of segment lru cache.
    // Holding cached segment file path
    struct CacheValue {
        CacheValue(std::string file_path_) : file_path(file_path_){}
        const std::string file_path;
    };

    CachedSegmentLoader(size_t capacity);

    // Load cached segment by done_file_path, return the "cache_handle" which contains segment file_path.
    // If use_cache is true, it will be loaded from _cache.
    bool load_cached_segment(const std::string done_file_path,
                             CachedSegmentCacheHandle* cache_handle, bool use_cache = true);

    // Insert k-v to cache
    void insert(const std::string done_file_path, const std::string file_psth,
                CachedSegmentCacheHandle* cache_handle);

    // Try to prune the segment cache if expired.
    Status prune();

private:
    CachedSegmentLoader();

    // Lookup the given segment in the cache.
    // If the rowset is found, the cache entry will be written into handle.
    // Return true if entry is found, otherwise return false.
    bool _lookup(const CacheKey& key, CachedSegmentCacheHandle* handle);

    // Insert a cache entry by key.
    // And the cache entry will be returned in handle.
    // This function is thread-safe.
    void _insert(const CacheKey& key, CacheValue* value, CachedSegmentCacheHandle* handle);

private:
    // A LRU cache to cache all cached segments
    std::unique_ptr<Cache> _cache = nullptr;
    std::shared_ptr<MemTracker> _mem_tracker = nullptr;
};

// A handle for a single segment lru cache.
// The handle can ensure that the segment is valid
// and will not be closed while the holder of the handle is accessing the segment.
// The handle will automatically release the cache entry when it is destroyed.
// So the caller need to make sure the handle is valid in lifecycle.
class CachedSegmentCacheHandle {
public:
    CachedSegmentCacheHandle() {}
    CachedSegmentCacheHandle(Cache* cache, Cache::Handle* handle) : _cache(cache), _handle(handle) {}

    ~CachedSegmentCacheHandle() {
        if (_handle != nullptr) {
            CHECK(_cache != nullptr);
            _cache->release(_handle);
        }
    }

    CachedSegmentCacheHandle(CachedSegmentCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
    }

    CachedSegmentCacheHandle& operator=(CachedSegmentCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        return *this;
    }

private:
    Cache* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(CachedSegmentCacheHandle);
};

} // namespace doris
