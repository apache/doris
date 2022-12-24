#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <unordered_map>

#include "io/cloud/cloud_file_cache.h"
#include "io/cloud/cloud_file_segment.h"

namespace doris {
namespace io {

/**
 * Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
 * Implements LRU eviction policy.
 */
class LRUFileCache final : public IFileCache {
public:
    /**
     * cache_base_path: the file cache path
     * cache_settings: the file cache setttings
     */
    LRUFileCache(const std::string& cache_base_path, const FileCacheSettings& cache_settings);

    /**
     * get the files which range contain [offset, offset+size-1]
     */
    FileSegmentsHolder get_or_set(const Key& key, size_t offset, size_t size, bool is_persistent,
                                  const TUniqueId& query_id) override;

    // init file cache
    Status initialize() override;

    // remove the files belong to key
    void remove_if_exists(const Key& key, bool is_persistent) override;

    // remove the files only catched by cache
    void remove_if_releasable(bool is_persistent) override;

    std::vector<std::string> try_get_cache_paths(const Key& key, bool is_persistent) override;

    size_t get_used_cache_size(bool is_persistent) const override;

    size_t get_file_segments_num(bool is_persistent) const override;

private:
    struct FileSegmentCell {
        FileSegmentSPtr file_segment;

        /// Iterator is put here on first reservation attempt, if successful.
        std::optional<LRUQueue::Iterator> queue_iterator;

        /// Pointer to file segment is always hold by the cache itself.
        /// Apart from pointer in cache, it can be hold by cache users, when they call
        /// getorSet(), but cache users always hold it via FileSegmentsHolder.
        bool releasable() const { return file_segment.unique(); }

        size_t size() const { return file_segment->_segment_range.size(); }

        FileSegmentCell(FileSegmentSPtr file_segment_, LRUFileCache* cache,
                        std::lock_guard<std::mutex>& cache_lock);

        FileSegmentCell(FileSegmentCell&& other) noexcept
                : file_segment(std::move(other.file_segment)),
                  queue_iterator(other.queue_iterator) {}

        FileSegmentCell& operator=(const FileSegmentCell&) = delete;
        FileSegmentCell(const FileSegmentCell&) = delete;
    };

    using FileSegmentsByOffset = std::map<size_t, FileSegmentCell>;

    struct HashCachedFileKey {
        std::size_t operator()(const std::pair<Key, bool>& k) const { return KeyHash()(k.first); }
    };
    // key: <file key, is_persistent>
    using CachedFiles =
            std::unordered_map<std::pair<Key, bool>, FileSegmentsByOffset, HashCachedFileKey>;

    CachedFiles _files;
    LRUQueue _queue;
    LRUQueue _persistent_queue;

    FileSegments get_impl(const Key& key, const TUniqueId& query_id, bool is_persistent,
                          const FileSegment::Range& range, std::lock_guard<std::mutex>& cache_lock);

    FileSegmentCell* get_cell(const Key& key, bool is_persistent, size_t offset,
                              std::lock_guard<std::mutex>& cache_lock);

    FileSegmentCell* add_cell(const Key& key, bool is_persistent, size_t offset, size_t size,
                              FileSegment::State state, std::lock_guard<std::mutex>& cache_lock);

    void use_cell(const FileSegmentCell& cell, const TUniqueId& query_id, bool is_persistent,
                  FileSegments& result, std::lock_guard<std::mutex>& cache_lock);

    bool try_reserve(const Key& key, const TUniqueId& query_id, bool is_persistent, size_t offset,
                     size_t size, std::lock_guard<std::mutex>& cache_lock) override;

    bool try_reserve_for_main_list(const Key& key, QueryContextPtr query_context,
                                   bool is_persistent, size_t offset, size_t size,
                                   std::lock_guard<std::mutex>& cache_lock);

    void remove(const Key& key, bool is_persistent, size_t offset,
                std::lock_guard<std::mutex>& cache_lock,
                std::lock_guard<std::mutex>& segment_lock) override;

    size_t get_available_cache_size(bool is_persistent) const;

    void load_cache_info_into_memory(std::lock_guard<std::mutex>& cache_lock);

    FileSegments split_range_into_cells(const Key& key, const TUniqueId& query_id,
                                        bool is_persistent, size_t offset, size_t size,
                                        FileSegment::State state,
                                        std::lock_guard<std::mutex>& cache_lock);

    std::string dump_structure_unlocked(const Key& key, bool is_persistent,
                                        std::lock_guard<std::mutex>& cache_lock);

    void fill_holes_with_empty_file_segments(FileSegments& file_segments, const Key& key,
                                             const TUniqueId& query_id, bool is_persistent,
                                             const FileSegment::Range& range,
                                             std::lock_guard<std::mutex>& cache_lock);

    size_t get_used_cache_size_unlocked(bool is_persistent,
                                        std::lock_guard<std::mutex>& cache_lock) const;

    size_t get_available_cache_size_unlocked(bool is_persistent,
                                             std::lock_guard<std::mutex>& cache_lock) const;

    size_t get_file_segments_num_unlocked(bool is_persistent,
                                          std::lock_guard<std::mutex>& cache_lock) const;

public:
    std::string dump_structure(const Key& key, bool is_persistent) override;
};

} // namespace io
} // namespace doris
