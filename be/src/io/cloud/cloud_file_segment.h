#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>

#include "common/status.h"
#include "io/cloud/cloud_file_cache.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"

namespace doris {
namespace io {

class FileSegment;
using FileSegmentSPtr = std::shared_ptr<FileSegment>;
using FileSegments = std::list<FileSegmentSPtr>;

class FileSegment {
    friend class LRUFileCache;
    friend struct FileSegmentsHolder;

public:
    using Key = IFileCache::Key;
    using LocalWriterPtr = std::unique_ptr<FileWriter>;
    using LocalReaderPtr = std::shared_ptr<FileReader>;

    enum class State {
        DOWNLOADED,
        /**
         * When file segment is first created and returned to user, it has state EMPTY.
         * EMPTY state can become DOWNLOADING when getOrSetDownaloder is called successfully
         * by any owner of EMPTY state file segment.
         */
        EMPTY,
        /**
         * A newly created file segment never has DOWNLOADING state until call to getOrSetDownloader
         * because each cache user might acquire multiple file segments and reads them one by one,
         * so only user which actually needs to read this segment earlier than others - becomes a downloader.
         */
        DOWNLOADING,
        SKIP_CACHE,
    };

    FileSegment(size_t offset_, size_t size_, const Key& key_, IFileCache* cache_,
                State download_state_, bool is_persistent);

    ~FileSegment();

    State state() const;

    static std::string state_to_string(FileSegment::State state);

    /// Represents an interval [left, right] including both boundaries.
    struct Range {
        size_t left;
        size_t right;

        Range(size_t left_, size_t right_) : left(left_), right(right_) {}

        bool operator==(const Range& other) const {
            return left == other.left && right == other.right;
        }

        size_t size() const { return right - left + 1; }

        std::string to_string() const {
            return fmt::format("[{}, {}]", std::to_string(left), std::to_string(right));
        }
    };

    const Range& range() const { return _segment_range; }

    const Key& key() const { return _file_key; }

    size_t offset() const { return range().left; }

    State wait();

    // append data to cache file
    Status append(Slice data);

    // read data from cache file
    Status read_at(Slice buffer, size_t offset_);

    // finish write, release the file writer
    Status finalize_write();

    // set downloader if state == EMPTY
    std::string get_or_set_downloader();

    std::string get_downloader() const;

    void reset_downloader(std::lock_guard<std::mutex>& segment_lock);

    bool is_downloader() const;

    bool is_downloaded() const { return _is_downloaded.load(); }

    bool is_persistent() const { return _is_persistent; }

    static std::string get_caller_id();

    size_t get_download_offset() const;

    size_t get_downloaded_size() const;

    std::string get_info_for_log() const;

    std::string get_path_in_local_cache() const;

    FileSegment& operator=(const FileSegment&) = delete;
    FileSegment(const FileSegment&) = delete;

private:
    size_t get_downloaded_size(std::lock_guard<std::mutex>& segment_lock) const;
    std::string get_info_for_log_impl(std::lock_guard<std::mutex>& segment_lock) const;
    bool has_finalized_state() const;

    Status set_downloaded(std::lock_guard<std::mutex>& segment_lock);
    bool is_downloader_impl(std::lock_guard<std::mutex>& segment_lock) const;

    /// complete() without any completion state is called from destructor of
    /// FileSegmentsHolder. complete() might check if the caller of the method
    /// is the last alive holder of the segment. Therefore, complete() and destruction
    /// of the file segment pointer must be done under the same cache mutex.
    void complete(std::lock_guard<std::mutex>& cache_lock);
    void complete_unlocked(std::lock_guard<std::mutex>& cache_lock,
                           std::lock_guard<std::mutex>& segment_lock);

    void reset_downloader_impl(std::lock_guard<std::mutex>& segment_lock);

    const Range _segment_range;

    State _download_state;

    std::string _downloader_id;

    LocalWriterPtr _cache_writer;
    LocalReaderPtr _cache_reader;

    size_t _downloaded_size = 0;

    /// global locking order rule:
    /// 1. cache lock
    /// 2. segment lock

    mutable std::mutex _mutex;
    std::condition_variable _cv;

    /// Protects downloaded_size access with actual write into fs.
    /// downloaded_size is not protected by download_mutex in methods which
    /// can never be run in parallel to FileSegment::write() method
    /// as downloaded_size is updated only in FileSegment::write() method.
    /// Such methods are identified by isDownloader() check at their start,
    /// e.g. they are executed strictly by the same thread, sequentially.
    mutable std::mutex _download_mutex;

    Key _file_key;
    IFileCache* _cache;

    std::atomic<bool> _is_downloaded {false};
    bool _is_persistent = false;
};

struct FileSegmentsHolder {
    explicit FileSegmentsHolder(FileSegments&& file_segments_)
            : file_segments(std::move(file_segments_)) {}

    FileSegmentsHolder(FileSegmentsHolder&& other) noexcept
            : file_segments(std::move(other.file_segments)) {}

    FileSegmentsHolder& operator=(const FileSegmentsHolder&) = delete;
    FileSegmentsHolder(const FileSegmentsHolder&) = delete;

    ~FileSegmentsHolder();

    FileSegments file_segments {};

    std::string to_string();
};

} // namespace io
} // namespace doris
