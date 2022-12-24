#include "io/cloud/tmp_file_mgr.h"

#include <rapidjson/document.h>

#include <mutex>

namespace doris::io {

static const char* TMP_FILE_DIR_PATH = "path";
static const char* MAX_CACHE_BYTES = "max_cache_bytes";
static const char* MAX_UPLOAD_BYTES = "max_upload_bytes";

Status TmpFileMgr::create_tmp_file_mgrs() {
    if (config::tmp_file_dirs.empty()) {
        LOG(ERROR) << "The config tmp_file_dirs is empty";
        return Status::InternalError("OLAP_ERR_INPUT_PARAMETER_ERROR");
    }
    using namespace rapidjson;
    Document document;
    document.Parse(config::tmp_file_dirs.c_str());
    if (!document.IsArray()) {
        LOG(ERROR) << "The config tmp_file_dirs need to be array";
        return Status::InternalError("OLAP_ERR_INPUT_PARAMETER_ERROR");
    }
    std::vector<TmpFileDirConfig> configs;
    for (auto& config : document.GetArray()) {
        TmpFileDirConfig tmp_file_mgr_config;
        auto map = config.GetObject();
        if (!map.HasMember(TMP_FILE_DIR_PATH)) {
            LOG(ERROR) << "The config doesn't have member 'path' ";
            return Status::InternalError("OLAP_ERR_INPUT_PARAMETER_ERROR");
        }
        tmp_file_mgr_config.path = map.FindMember(TMP_FILE_DIR_PATH)->value.GetString();
        tmp_file_mgr_config.max_cache_bytes =
                map.HasMember(MAX_CACHE_BYTES) ? map.FindMember(MAX_CACHE_BYTES)->value.GetInt64()
                                               : 0;
        tmp_file_mgr_config.max_upload_bytes =
                map.HasMember(MAX_UPLOAD_BYTES) ? map.FindMember(MAX_UPLOAD_BYTES)->value.GetInt64()
                                                : 0;
        if (tmp_file_mgr_config.max_upload_bytes <= 0) {
            LOG(WARNING) << "max_upload_bytes should not less than or equal to zero";
            return Status::InternalError("OLAP_ERR_INPUT_PARAMETER_ERROR");
        }
        if (tmp_file_mgr_config.max_cache_bytes < 0) {
            LOG(WARNING) << "max_cache_bytes should not less than zero";
            return Status::InternalError("OLAP_ERR_INPUT_PARAMETER_ERROR");
        }
        configs.push_back(tmp_file_mgr_config);
    }
    static TmpFileMgr factory {configs};
    _s_instance = &factory;
    return Status::OK();
}

FileReaderSPtr TmpFileMgr::lookup_tmp_file(const Path& path) {
    auto& tmp_file_dir = _tmp_file_dirs[std::hash<std::string>()(path.filename().native()) %
                                        _tmp_file_dirs_size];
    if (tmp_file_dir.max_cache_bytes == 0) {
        return nullptr;
    }
    {
        std::lock_guard lock(tmp_file_dir.mtx);
        if (tmp_file_dir.file_set.count(path) == 0) {
            return nullptr;
        }
    }
    FileReaderSPtr file_reader;
    auto st = global_local_filesystem()->open_file(path, &file_reader);
    if (!st.ok()) {
        LOG(WARNING) << "could not open tmp file. err=" << st;
        return nullptr;
    }
    return file_reader;
}

bool TmpFileMgr::insert_tmp_file(const Path& path, size_t file_size) {
    auto& tmp_file_dir = _tmp_file_dirs[std::hash<std::string>()(path.filename().native()) %
                                        _tmp_file_dirs_size];
    if (tmp_file_dir.max_cache_bytes == 0) {
        return false;
    }
    auto local_fs = global_local_filesystem();
    std::vector<Path> remove_paths;
    {
        std::lock_guard lock(tmp_file_dir.mtx);
        tmp_file_dir.cur_cache_bytes += file_size;
        while (tmp_file_dir.cur_cache_bytes > tmp_file_dir.max_cache_bytes) {
            auto& [remove_path, size] = tmp_file_dir.file_list.back();
            tmp_file_dir.file_set.erase(remove_path);
            tmp_file_dir.cur_cache_bytes -= size;
            remove_paths.push_back(std::move(remove_path));
            tmp_file_dir.file_list.pop_back();
        }
        tmp_file_dir.file_list.push_front(std::make_pair(path, file_size));
        tmp_file_dir.file_set.insert(path);
    }
    for (auto& remove_path : remove_paths) {
        auto st = local_fs->delete_file(remove_path);
        if (!st.ok()) {
            LOG(WARNING) << "could not remove tmp file. err=" << st;
        }
    }
    return true;
}

bool TmpFileMgr::check_if_has_enough_space_to_async_upload(const Path& path,
                                                           uint64_t upload_file_size) {
    auto& tmp_file_dir = _tmp_file_dirs[std::hash<std::string>()(path.filename().native()) %
                                        _tmp_file_dirs_size];
    uint64_t cur_upload_bytes, new_cur_upload_bytes;
    do {
        cur_upload_bytes = tmp_file_dir.cur_upload_bytes;
        new_cur_upload_bytes = cur_upload_bytes + upload_file_size;
        if (new_cur_upload_bytes > tmp_file_dir.max_upload_bytes) {
            return false;
        }
    } while (!tmp_file_dir.cur_upload_bytes.compare_exchange_strong(cur_upload_bytes,
                                                                    new_cur_upload_bytes));
    return true;
}

void TmpFileMgr::upload_complete(const Path& path, uint64_t upload_file_size,
                                 bool is_async_upload) {
    if (!is_async_upload) {
        return;
    }
    auto& tmp_file_dir = _tmp_file_dirs[std::hash<std::string>()(path.filename().native()) %
                                        _tmp_file_dirs_size];
    tmp_file_dir.cur_upload_bytes -= upload_file_size;
}

} // namespace doris::io
