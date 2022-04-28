// specific language governing permissions and limitations
// under the License.

#include "olap/fs/cached_segment_loader.h"

#include "util/filesystem_util.h"
#include "util/stopwatch.hpp"

namespace doris {
CachedSegmentLoader::CachedSegmentLoader(size_t capacity)
        : _mem_tracker(MemTracker::create_tracker(
        capacity, "CachedSegmentLoader", nullptr, MemTrackerLevel::OVERVIEW)) {
    _cache = std::unique_ptr<Cache>(new_lru_cache("CachedSegmentCache", capacity));

}

bool CachedSegmentLoader::_lookup(const CachedSegmentLoader::CacheKey& key, CachedSegmentCacheHandle* handle) {
    auto lru_handle = _cache->lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = CachedSegmentCacheHandle(_cache.get(), lru_handle);
    return true;
}

void CachedSegmentLoader::_insert(const CachedSegmentLoader::CacheKey& key, CachedSegmentLoader::CacheValue* value,
                                  CachedSegmentCacheHandle* handle) {
    // When evicting one entry from cache, delete the file on disk.
    auto deleter = [](const doris::CacheKey& key, void* value) {
        CachedSegmentLoader::CacheValue* cache_value = (CachedSegmentLoader::CacheValue*)value;
        // remove file
        std::vector<std::string> files = {key.to_string(), cache_value->file_path};
        Status st = FileSystemUtil::remove_paths(files);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to remove files [" << key.to_string() << ", "
                         << cache_value->file_path << "], error_msg=" << st.get_error_msg();
        }
        LOG(INFO) << "Successfully remove cached segment files [" << key.to_string() << ", "
                  << cache_value->file_path << "].";
        delete cache_value;
    };

    auto lru_handle = _cache->insert(key.encode(), value, 1,
                                     deleter, CachePriority::NORMAL);
    *handle = CachedSegmentCacheHandle(_cache.get(), lru_handle);
}

bool CachedSegmentLoader::load_cached_segment(const std::string done_file_path,
                                              CachedSegmentCacheHandle* cache_handle, bool use_cache) {
    CachedSegmentLoader::CacheKey cache_key(done_file_path);
    if (_lookup(cache_key, cache_handle)) {
        return true;
    }

    return false;
}

void CachedSegmentLoader::insert(std::string done_file_path, std::string file_path,
                                 CachedSegmentCacheHandle* cache_handle) {
    CachedSegmentLoader::CacheKey cache_key(done_file_path);
    // memory of CachedSegmentLoader::CacheValue will be handled by CachedSegmentLoader
    CachedSegmentLoader::CacheValue* cache_value = new CachedSegmentLoader::CacheValue(file_path);
    _insert(cache_key, cache_value, cache_handle);
}

Status CachedSegmentLoader::prune() {
    MonotonicStopWatch watch;
    watch.start();
    _cache->prune();

    LOG(INFO) << "prune cached segment cache. cost(ms): " << watch.elapsed_time() / 1000 / 1000;
    return Status::OK();
}

} // namespace doris