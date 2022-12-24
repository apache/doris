#pragma once
#include <atomic>
#include <condition_variable>
#include <string>

#include "common/config.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_writer.h"
#include "runtime/tmp_file_mgr.h"

namespace doris::io {

struct TmpFileDirConfig {
    std::string path;
    uint64_t max_cache_bytes;
    uint64_t max_upload_bytes;
};

class TmpFileMgr {
public:
    static Status create_tmp_file_mgrs();

    static TmpFileMgr* instance() { return _s_instance; }

    TmpFileMgr(const std::vector<TmpFileDirConfig>& configs) {
        std::for_each(configs.begin(), configs.end(), [this](const TmpFileDirConfig& config) {
            if (_tmp_file_dirs_size == MAX_TMP_FILE_DIR_SIZE) {
                LOG(WARNING) << "THE MAX_TMP_FILE_DIR_SIZE is " << MAX_TMP_FILE_DIR_SIZE
                             << " and ignore the following tmp file dir " << config.path;
                return;
            }
            global_local_filesystem()->delete_directory(config.path);
            global_local_filesystem()->create_directory(config.path);
            _tmp_file_dirs[_tmp_file_dirs_size].path = config.path;
            _tmp_file_dirs[_tmp_file_dirs_size].max_cache_bytes = config.max_cache_bytes;
            _tmp_file_dirs[_tmp_file_dirs_size].max_upload_bytes = config.max_upload_bytes;
            _tmp_file_dirs_size++;
        });
        // TODO(liuchangliang): need to remove this after modify data_dir
        auto num_per_disk = config::compaction_task_num_per_disk;
        auto num_per_fast_disk = config::compaction_task_num_per_fast_disk;
        config::compaction_task_num_per_disk =
                _tmp_file_dirs_size > 0 ? num_per_disk * _tmp_file_dirs_size : num_per_disk;
        config::compaction_task_num_per_fast_disk =
                _tmp_file_dirs_size > 0 ? num_per_fast_disk * _tmp_file_dirs_size
                                        : num_per_fast_disk;
        LOG(WARNING) << "modify compaction_task_num_per_disk, original=" << num_per_disk
                     << " modified=" << config::compaction_task_num_per_disk;
        LOG(WARNING) << "modify compaction_task_num_per_fast_disk, original=" << num_per_fast_disk
                     << " modified=" << config::compaction_task_num_per_fast_disk;
    }

    const std::string& get_tmp_file_dir(const std::string& tmp_file_name) {
        return _tmp_file_dirs[std::hash<std::string>()(tmp_file_name) % _tmp_file_dirs_size].path;
    }

    bool insert_tmp_file(const Path& path, size_t file_size);

    FileReaderSPtr lookup_tmp_file(const Path& path);

    const std::string& get_tmp_file_dir() {
        size_t cur_index = _next_index.fetch_add(1);
        return _tmp_file_dirs[cur_index % _tmp_file_dirs_size].path;
    }

    // If true, flush thread can submit the upload task and return immediately
    // If false, flush thread will wait until the upload task completed
    bool check_if_has_enough_space_to_async_upload(const Path& path, uint64_t upload_file_size);

    // When upload completed, release the space to let other files upload async.
    void upload_complete(const Path& path, uint64_t upload_file_size, bool is_async_upload);

private:
    struct TmpFileDir {
        using file_key = std::pair<Path, uint64_t>;

        std::string path;
        uint64_t max_cache_bytes;
        uint64_t max_upload_bytes;
        std::mutex mtx;
        std::unordered_set<Path, PathHasher> file_set;
        std::list<file_key> file_list;
        uint64_t cur_cache_bytes {0};

        std::atomic_uint64_t cur_upload_bytes {0};
    };
    static constexpr size_t MAX_TMP_FILE_DIR_SIZE {32};
    std::array<TmpFileDir, MAX_TMP_FILE_DIR_SIZE> _tmp_file_dirs;
    size_t _tmp_file_dirs_size {0};
    std::atomic_size_t _next_index {0}; // use for round-robin
    static inline TmpFileMgr* _s_instance {nullptr};
};

} // namespace doris::io
