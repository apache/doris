#pragma once

#include <vector>

#include "io/cloud/cloud_file_cache.h"
#include "io/cloud/cloud_file_cache_fwd.h"
#include "io/cloud/cloud_file_cache_settings.h"
namespace doris {
namespace io {

enum FileSegmentCacheType {
    NORMAL,
    DISPOSABLE,
};
/**
 * Creates a FileCache object for cache_base_path.
 */
class FileCacheFactory {
public:
    static FileCacheFactory& instance();

    Status create_file_cache(const std::string& cache_base_path,
                             const FileCacheSettings& file_cache_settings,
                             FileSegmentCacheType type);

    CloudFileCachePtr get_by_path(const IFileCache::Key& key);
    CloudFileCachePtr get_disposable_cache(const IFileCache::Key& key);
    std::vector<IFileCache::QueryContextHolderPtr> get_query_context_holders(
            const TUniqueId& query_id);
    FileCacheFactory() = default;
    FileCacheFactory& operator=(const FileCacheFactory&) = delete;
    FileCacheFactory(const FileCacheFactory&) = delete;

private:
    std::vector<std::unique_ptr<IFileCache>> _caches;
    std::vector<std::unique_ptr<IFileCache>> _disposable_cache;
};

} // namespace io
} // namespace doris
