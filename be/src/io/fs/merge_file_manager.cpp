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

#include "io/fs/merge_file_manager.h"

#include <bvar/bvar.h>
#include <bvar/passive_status.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <functional>
#include <optional>
#include <random>
#include <sstream>
#include <unordered_set>

#ifdef BE_TEST
#include "cpp/sync_point.h"
#endif

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "common/config.h"
#include "gen_cpp/cloud.pb.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "util/uid_util.h"

namespace doris::io {

namespace {

bvar::Adder<int64_t> g_merge_file_total_count("merge_file", "total_count");
bvar::Adder<int64_t> g_merge_file_total_small_file_count("merge_file", "total_small_file_num");
bvar::Adder<int64_t> g_merge_file_total_size_bytes("merge_file", "total_size_bytes");

double merge_file_avg_small_file_num(void*) {
    auto total = g_merge_file_total_count.get_value();
    if (total == 0) {
        return 0.0;
    }
    return static_cast<double>(g_merge_file_total_small_file_count.get_value()) / total;
}

double merge_file_avg_file_size(void*) {
    auto total = g_merge_file_total_count.get_value();
    if (total == 0) {
        return 0.0;
    }
    return static_cast<double>(g_merge_file_total_size_bytes.get_value()) / total;
}

bvar::PassiveStatus<double> g_merge_file_avg_small_file_num("merge_file_avg_small_file_num",
                                                            merge_file_avg_small_file_num, nullptr);
bvar::PassiveStatus<double> g_merge_file_avg_file_size_bytes("merge_file_avg_file_size_bytes",
                                                             merge_file_avg_file_size, nullptr);

} // namespace

MergeFileManager* MergeFileManager::instance() {
    static MergeFileManager instance;
    return &instance;
}

MergeFileManager::~MergeFileManager() {
    stop_background_manager();
}

Status MergeFileManager::init() {
    return Status::OK();
}

Status MergeFileManager::create_new_merge_file_state(
        const std::string& resource_id, std::unique_ptr<MergeFileState>& merge_file_state) {
    FileSystemSPtr file_system;
    RETURN_IF_ERROR(ensure_file_system(resource_id, &file_system));
    if (file_system == nullptr) {
        return Status::InternalError("File system is not available for merge file creation: " +
                                     resource_id);
    }

    auto uuid = generate_uuid_string();
    auto hash_val = std::hash<std::string> {}(uuid);
    uint16_t path_bucket = hash_val % 4096 + 1;
    std::stringstream path_stream;
    path_stream << "data/merge_file/" << path_bucket << "/" << uuid;

    merge_file_state = std::make_unique<MergeFileState>();
    const std::string relative_path = path_stream.str();
    merge_file_state->merge_file_path = relative_path;
    merge_file_state->create_time = std::time(nullptr);
    merge_file_state->create_timestamp = std::chrono::steady_clock::now();
    merge_file_state->state = MergeFileStateEnum::INIT;
    merge_file_state->resource_id = resource_id;
    merge_file_state->file_system = std::move(file_system);

    // Create file writer for the merge file
    FileWriterPtr new_writer;
    FileWriterOptions opts;
    RETURN_IF_ERROR(
            merge_file_state->file_system->create_file(Path(relative_path), &new_writer, &opts));
    merge_file_state->merge_file_path = new_writer->path().native();
    merge_file_state->writer = std::move(new_writer);

    return Status::OK();
}

Status MergeFileManager::ensure_file_system(const std::string& resource_id,
                                            FileSystemSPtr* file_system) {
    if (file_system == nullptr) {
        return Status::InvalidArgument("file_system output parameter is null");
    }

    {
        std::lock_guard<std::mutex> lock(_file_system_mutex);
        if (resource_id.empty()) {
            if (_default_file_system != nullptr) {
                *file_system = _default_file_system;
                return Status::OK();
            }
        } else {
            auto it = _file_systems.find(resource_id);
            if (it != _file_systems.end()) {
                *file_system = it->second;
                return Status::OK();
            }
        }
    }

    if (!config::is_cloud_mode()) {
        return Status::InternalError("Cloud file system is not available in local mode");
    }

    auto* exec_env = ExecEnv::GetInstance();
    if (exec_env == nullptr) {
        return Status::InternalError("ExecEnv instance is not initialized");
    }

    FileSystemSPtr resolved_fs;
    if (resource_id.empty()) {
        resolved_fs = exec_env->storage_engine().to_cloud().latest_fs();
        if (resolved_fs == nullptr) {
            return Status::InternalError("Cloud file system is not ready");
        }
    } else {
        auto storage_resource =
                exec_env->storage_engine().to_cloud().get_storage_resource(resource_id);
        if (!storage_resource.has_value() || storage_resource->fs == nullptr) {
            return Status::InternalError("Cloud file system is not ready for resource: " +
                                         resource_id);
        }
        resolved_fs = storage_resource->fs;
    }

    {
        std::lock_guard<std::mutex> lock(_file_system_mutex);
        if (resource_id.empty()) {
            _default_file_system = resolved_fs;
            *file_system = _default_file_system;
        } else {
            _file_systems[resource_id] = resolved_fs;
            *file_system = resolved_fs;
        }
    }

    return Status::OK();
}

Status MergeFileManager::append(const std::string& path, const Slice& data,
                                const MergeFileAppendInfo& info) {
    // Check if file is too large to be merged
    if (data.get_size() > config::small_file_threshold_bytes) {
        return Status::OK(); // Skip merging for large files
    }

    if (info.txn_id <= 0) {
        return Status::InvalidArgument("Missing valid txn id for merge file append: " + path);
    }

    std::lock_guard<std::timed_mutex> lock(_current_merge_file_mutex);

    auto& current_state = _current_merge_files[info.resource_id];
    if (!current_state || !current_state->writer) {
        RETURN_IF_ERROR(create_new_merge_file_state(info.resource_id, current_state));
    }

    // Check if we need to create a new merge file
    if (current_state->total_size + data.get_size() >= config::merge_file_size_threshold_bytes) {
        RETURN_IF_ERROR(mark_current_merge_file_for_upload_locked(info.resource_id));
        auto it = _current_merge_files.find(info.resource_id);
        if (it == _current_merge_files.end() || !it->second || !it->second->writer) {
            RETURN_IF_ERROR(create_new_merge_file_state(info.resource_id,
                                                        _current_merge_files[info.resource_id]));
        }
    }

    MergeFileState* active_state = current_state.get();

    // Write data to current merge file
    RETURN_IF_ERROR(active_state->writer->append(data));

    // Update index
    MergeFileSegmentIndex index;
    index.merge_file_path = active_state->merge_file_path;
    index.offset = active_state->current_offset;
    index.size = data.get_size();
    index.tablet_id = info.tablet_id;
    index.rowset_id = info.rowset_id;
    index.resource_id = info.resource_id;
    index.txn_id = info.txn_id;

    active_state->index_map[path] = index;
    active_state->current_offset += data.get_size();
    active_state->total_size += data.get_size();

    // Mark as active if this is the first write
    if (active_state->state == MergeFileStateEnum::INIT) {
        active_state->state = MergeFileStateEnum::ACTIVE;
    }

    // Update global index
    {
        std::lock_guard<std::mutex> global_lock(_global_index_mutex);
        _global_index_map[path] = index;
    }

    return Status::OK();
}

Status MergeFileManager::wait_for_merge_file_upload(MergeFileState* merge_file_ptr) {
    std::unique_lock<std::mutex> upload_lock(merge_file_ptr->upload_mutex);
    merge_file_ptr->upload_cv.wait(upload_lock, [merge_file_ptr] {
        auto state = merge_file_ptr->state.load(std::memory_order_acquire);
        return state == MergeFileStateEnum::UPLOADED || state == MergeFileStateEnum::FAILED;
    });
    if (merge_file_ptr->state == MergeFileStateEnum::FAILED) {
        std::string err = merge_file_ptr->last_error;
        if (err.empty()) {
            err = "Merge file upload failed";
        }
        return Status::InternalError(err);
    }
    return Status::OK();
}

Status MergeFileManager::wait_write_done(const std::string& path) {
    std::string merge_file_path;
    {
        std::lock_guard<std::mutex> global_lock(_global_index_mutex);
        auto it = _global_index_map.find(path);
        if (it == _global_index_map.end()) {
            return Status::InternalError("File not found in global index: " + path);
        }
        merge_file_path = it->second.merge_file_path;
    }

    // Find the merge file in uploaded files first - if already uploaded, no need to wait
    std::shared_ptr<MergeFileState> managed_merge_file;
    std::shared_ptr<MergeFileState> failed_merge_file;
    {
        std::lock_guard<std::mutex> lock(_merge_files_mutex);
        auto uploaded_it = _uploaded_merge_files.find(merge_file_path);
        if (uploaded_it != _uploaded_merge_files.end()) {
            auto state = uploaded_it->second->state.load(std::memory_order_acquire);
            if (state == MergeFileStateEnum::UPLOADED) {
                return Status::OK(); // Already uploaded, no need to wait
            }
            if (state == MergeFileStateEnum::FAILED) {
                failed_merge_file = uploaded_it->second;
            } else {
                managed_merge_file = uploaded_it->second;
            }
        }
    }

    if (failed_merge_file) {
        std::lock_guard<std::mutex> upload_lock(failed_merge_file->upload_mutex);
        std::string err = failed_merge_file->last_error;
        if (err.empty()) {
            err = "Merge file upload failed";
        }
        return Status::InternalError(err);
    }

    // Find the merge file in either current or uploading files
    MergeFileState* merge_file_ptr = nullptr;
    {
        std::lock_guard<std::timed_mutex> current_lock(_current_merge_file_mutex);
        for (auto& [resource_id, state] : _current_merge_files) {
            if (state && state->merge_file_path == merge_file_path) {
                merge_file_ptr = state.get();
                break;
            }
        }
    }

    if (!merge_file_ptr) {
        std::lock_guard<std::mutex> lock(_merge_files_mutex);
        auto uploading_it = _uploading_merge_files.find(merge_file_path);
        if (uploading_it != _uploading_merge_files.end()) {
            managed_merge_file = uploading_it->second;
            merge_file_ptr = managed_merge_file.get();
        } else {
            auto uploaded_it = _uploaded_merge_files.find(merge_file_path);
            if (uploaded_it != _uploaded_merge_files.end()) {
                managed_merge_file = uploaded_it->second;
                merge_file_ptr = managed_merge_file.get();
            }
        }
    }

    if (!merge_file_ptr) {
        // Merge file not found in any location, this is unexpected
        return Status::InternalError("Merge file not found for path: " + path);
    }

    Status wait_status = wait_for_merge_file_upload(merge_file_ptr);
    (void)managed_merge_file; // keep shared ownership alive during wait
    return wait_status;
}

Status MergeFileManager::get_merge_file_index(const std::string& path,
                                              MergeFileSegmentIndex* index) {
    std::lock_guard<std::mutex> lock(_global_index_mutex);
    auto it = _global_index_map.find(path);
    if (it == _global_index_map.end()) {
        return Status::NotFound("File not found in global merge index: {}", path);
    }

    *index = it->second;
    return Status::OK();
}

void MergeFileManager::start_background_manager() {
    if (_background_thread) {
        return; // Already started
    }

    _stop_background_thread = false;
    _background_thread = std::make_unique<std::thread>([this] { background_manager(); });
}

void MergeFileManager::stop_background_manager() {
    _stop_background_thread = true;
    if (_background_thread && _background_thread->joinable()) {
        _background_thread->join();
    }
    _background_thread.reset();
}

Status MergeFileManager::mark_current_merge_file_for_upload_locked(const std::string& resource_id) {
    auto it = _current_merge_files.find(resource_id);
    if (it == _current_merge_files.end() || !it->second || !it->second->writer) {
        return Status::OK(); // Nothing to mark for upload
    }

    auto& current = it->second;

    // Mark as ready for upload
    current->state = MergeFileStateEnum::READY_TO_UPLOADING;

    // Move to uploading files list
    {
        std::shared_ptr<MergeFileState> uploading_ptr =
                std::shared_ptr<MergeFileState>(std::move(current));
        std::lock_guard<std::mutex> lock(_merge_files_mutex);
        _uploading_merge_files[uploading_ptr->merge_file_path] = uploading_ptr;
    }

    // Create new merge file
    return create_new_merge_file_state(resource_id, it->second);
}

Status MergeFileManager::mark_current_merge_file_for_upload(const std::string& resource_id) {
    std::lock_guard<std::timed_mutex> lock(_current_merge_file_mutex);
    return mark_current_merge_file_for_upload_locked(resource_id);
}

void MergeFileManager::record_merge_file_metrics(const MergeFileState& merge_file) {
    g_merge_file_total_count << 1;
    g_merge_file_total_small_file_count << static_cast<int64_t>(merge_file.index_map.size());
    g_merge_file_total_size_bytes << merge_file.total_size;
}

void MergeFileManager::background_manager() {
    auto last_cleanup_time = std::chrono::steady_clock::now();

    while (!_stop_background_thread.load()) {
        int64_t check_interval_ms = std::max<int64_t>(1, config::merge_file_time_threshold_ms / 2);
        std::this_thread::sleep_for(std::chrono::milliseconds(check_interval_ms));

        // Check if current merge file should be closed due to time threshold
        std::vector<std::string> resources_to_mark;
        {
            std::unique_lock<std::timed_mutex> current_lock(_current_merge_file_mutex,
                                                            std::defer_lock);
            int64_t lock_wait_ms = std::max<int64_t>(0, config::merge_file_try_lock_timeout_ms);
            if (current_lock.try_lock_for(std::chrono::milliseconds(lock_wait_ms))) {
                for (auto& [resource_id, state] : _current_merge_files) {
                    if (!state || state->state != MergeFileStateEnum::ACTIVE) {
                        continue;
                    }
                    auto current_time = std::chrono::steady_clock::now();
                    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                              current_time - state->create_timestamp)
                                              .count();
                    if (elapsed_ms >= config::merge_file_time_threshold_ms) {
                        resources_to_mark.push_back(resource_id);
                    }
                }
            }
        }
        for (const auto& resource_id : resources_to_mark) {
            Status st = mark_current_merge_file_for_upload(resource_id);
            if (!st.ok()) {
                LOG(WARNING) << "Failed to close current merge file for resource " << resource_id
                             << ": " << st.to_string();
            }
        }

        // Process uploading files
        process_uploading_files();

        auto now = std::chrono::steady_clock::now();
        int64_t cleanup_interval_sec =
                std::max<int64_t>(1, config::merge_file_cleanup_interval_seconds);
        auto cleanup_interval = std::chrono::seconds(cleanup_interval_sec);
        if (now - last_cleanup_time >= cleanup_interval) {
            cleanup_expired_data();
            last_cleanup_time = now;
        }
    }
}

void MergeFileManager::process_uploading_files() {
    std::vector<std::shared_ptr<MergeFileState>> files_ready;
    std::vector<std::shared_ptr<MergeFileState>> files_uploading;

    {
        std::lock_guard<std::mutex> lock(_merge_files_mutex);
        for (auto& [merge_file_path, merge_file] : _uploading_merge_files) {
            auto state = merge_file->state.load(std::memory_order_acquire);
            if (state != MergeFileStateEnum::READY_TO_UPLOADING &&
                state != MergeFileStateEnum::UPLOADING) {
                continue;
            }
            if (state == MergeFileStateEnum::READY_TO_UPLOADING) {
                files_ready.emplace_back(merge_file);
            } else {
                files_uploading.emplace_back(merge_file);
            }
        }
    }

    auto handle_success = [&](const std::shared_ptr<MergeFileState>& merge_file) {
        VLOG_DEBUG << "Merge file upload completed: " << merge_file->merge_file_path;
        {
            std::lock_guard<std::mutex> upload_lock(merge_file->upload_mutex);
            merge_file->state = MergeFileStateEnum::UPLOADED;
            merge_file->upload_time = std::time(nullptr);
        }
        merge_file->upload_cv.notify_all();
        {
            std::lock_guard<std::mutex> lock(_merge_files_mutex);
            _uploading_merge_files.erase(merge_file->merge_file_path);
            _uploaded_merge_files[merge_file->merge_file_path] = merge_file;
        }
    };

    auto handle_failure = [&](const std::shared_ptr<MergeFileState>& merge_file,
                              const Status& status) {
        LOG(WARNING) << "Failed to upload merge file: " << merge_file->merge_file_path
                     << ", error: " << status.to_string();
        {
            std::lock_guard<std::mutex> upload_lock(merge_file->upload_mutex);
            merge_file->state = MergeFileStateEnum::FAILED;
            merge_file->last_error = status.to_string();
            merge_file->upload_time = std::time(nullptr);
        }
        merge_file->upload_cv.notify_all();
        {
            std::lock_guard<std::mutex> lock(_merge_files_mutex);
            _uploading_merge_files.erase(merge_file->merge_file_path);
            _uploaded_merge_files[merge_file->merge_file_path] = merge_file;
        }
    };

    for (auto& merge_file : files_ready) {
        const std::string& merge_file_path = merge_file->merge_file_path;
        cloud::MergedFileInfoPB merge_file_info;
        merge_file_info.set_ref_cnt(merge_file->index_map.size());
        merge_file_info.set_total_file_num(merge_file->index_map.size());
        merge_file_info.set_left_file_num(merge_file->index_map.size());
        merge_file_info.set_total_file_bytes(merge_file->total_size);
        merge_file_info.set_left_file_bytes(merge_file->total_size);
        merge_file_info.set_created_at_sec(merge_file->create_time);
        merge_file_info.set_corrected(false);
        merge_file_info.set_state(doris::cloud::MergedFileInfoPB::NORMAL);
        merge_file_info.set_resource_id(merge_file->resource_id);

        for (const auto& [small_file_path, index] : merge_file->index_map) {
            auto* small_file = merge_file_info.add_small_files();
            small_file->set_path(small_file_path);
            small_file->set_offset(index.offset);
            small_file->set_size(index.size);
            small_file->set_deleted(false);
            if (index.tablet_id != 0) {
                small_file->set_tablet_id(index.tablet_id);
            }
            if (!index.rowset_id.empty()) {
                small_file->set_rowset_id(index.rowset_id);
            }
            if (index.txn_id != 0) {
                small_file->set_txn_id(index.txn_id);
            }
        }

        Status meta_status = update_meta_service(merge_file->merge_file_path, merge_file_info);
        if (!meta_status.ok()) {
            LOG(WARNING) << "Failed to update meta service for merge file: "
                         << merge_file->merge_file_path << ", error: " << meta_status.to_string();
            handle_failure(merge_file, meta_status);
            continue;
        }

        // Record stats once the merge file metadata is persisted.
        record_merge_file_metrics(*merge_file);

        // Now upload the file
        if (!merge_file->index_map.empty()) {
            std::ostringstream oss;
            oss << "Uploading merge file " << merge_file_path << " with "
                << merge_file->index_map.size() << " small files: ";
            bool first = true;
            for (const auto& [small_file_path, index] : merge_file->index_map) {
                if (!first) {
                    oss << ", ";
                }
                first = false;
                oss << "[" << small_file_path << ", offset=" << index.offset
                    << ", size=" << index.size << "]";
            }
            LOG(INFO) << oss.str();
        } else {
            LOG(INFO) << "Uploading merge file " << merge_file_path << " with no small files";
        }

        Status upload_status =
                upload_merge_file(merge_file->merge_file_path, merge_file->writer.get());

        if (upload_status.is<ErrorCode::ALREADY_CLOSED>()) {
            handle_success(merge_file);
            continue;
        }
        if (!upload_status.ok()) {
            handle_failure(merge_file, upload_status);
            continue;
        }

        merge_file->state = MergeFileStateEnum::UPLOADING;
    }

    for (auto& merge_file : files_uploading) {
        if (!merge_file->writer) {
            handle_failure(merge_file,
                           Status::InternalError("File writer is null for merge file: {}",
                                                 merge_file->merge_file_path));
            continue;
        }

        if (merge_file->writer->state() != FileWriter::State::CLOSED) {
            continue;
        }

        Status status = merge_file->writer->close(true);
        if (status.is<ErrorCode::ALREADY_CLOSED>()) {
            handle_success(merge_file);
            continue;
        }
        if (status.ok()) {
            continue;
        }

        handle_failure(merge_file, status);
    }
}

Status MergeFileManager::upload_merge_file(const std::string& merge_file_path, FileWriter* writer) {
    if (writer == nullptr) {
        return Status::InternalError("File writer is null for merge file: " + merge_file_path);
    }

    return writer->close(true);
}

Status MergeFileManager::update_meta_service(const std::string& merge_file_path,
                                             const cloud::MergedFileInfoPB& merge_file_info) {
#ifdef BE_TEST
    TEST_SYNC_POINT_RETURN_WITH_VALUE("MergeFileManager::update_meta_service", Status::OK(),
                                      merge_file_path, &merge_file_info);
#endif
    VLOG_DEBUG << "Updating meta service for merge file: " << merge_file_path << " with "
               << merge_file_info.total_file_num() << " small files"
               << ", total bytes: " << merge_file_info.total_file_bytes();

    // Get CloudMetaMgr through StorageEngine
    if (!config::is_cloud_mode()) {
        return Status::InternalError("Storage engine is not cloud mode");
    }

    auto& storage_engine = ExecEnv::GetInstance()->storage_engine();
    auto& cloud_meta_mgr = storage_engine.to_cloud().meta_mgr();
    return cloud_meta_mgr.update_merge_file_info(merge_file_path, merge_file_info);
}

void MergeFileManager::cleanup_expired_data() {
    auto current_time = std::time(nullptr);

    // Clean up expired uploaded files
    {
        std::lock_guard<std::mutex> uploaded_lock(_merge_files_mutex);
        auto it = _uploaded_merge_files.begin();
        while (it != _uploaded_merge_files.end()) {
            if (it->second->state == MergeFileStateEnum::UPLOADED &&
                current_time - it->second->upload_time > config::uploaded_file_retention_seconds) {
                it = _uploaded_merge_files.erase(it);
            } else if (it->second->state == MergeFileStateEnum::FAILED &&
                       current_time - it->second->upload_time >
                               config::uploaded_file_retention_seconds) {
                it = _uploaded_merge_files.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Clean up expired global index entries
    {
        std::unordered_set<std::string> active_merge_files;
        {
            std::lock_guard<std::timed_mutex> current_lock(_current_merge_file_mutex);
            for (const auto& [resource_id, state] : _current_merge_files) {
                if (state) {
                    active_merge_files.insert(state->merge_file_path);
                }
            }
        }
        {
            std::lock_guard<std::mutex> merge_lock(_merge_files_mutex);
            for (const auto& [path, state] : _uploading_merge_files) {
                active_merge_files.insert(path);
            }
            for (const auto& [path, state] : _uploaded_merge_files) {
                active_merge_files.insert(path);
            }
        }

        std::lock_guard<std::mutex> global_lock(_global_index_mutex);
        auto it = _global_index_map.begin();
        while (it != _global_index_map.end()) {
            const auto& index = it->second;
            if (active_merge_files.find(index.merge_file_path) == active_merge_files.end()) {
                it = _global_index_map.erase(it);
            } else {
                ++it;
            }
        }
    }
}

#ifdef BE_TEST
namespace {
void reset_adder(bvar::Adder<int64_t>& adder) {
    auto current = adder.get_value();
    if (current != 0) {
        adder << (-current);
    }
}
} // namespace

void MergeFileManager::reset_merge_file_bvars_for_test() const {
    reset_adder(g_merge_file_total_count);
    reset_adder(g_merge_file_total_small_file_count);
    reset_adder(g_merge_file_total_size_bytes);
}

int64_t MergeFileManager::merge_file_total_count_for_test() const {
    return g_merge_file_total_count.get_value();
}

int64_t MergeFileManager::merge_file_total_small_file_num_for_test() const {
    return g_merge_file_total_small_file_count.get_value();
}

int64_t MergeFileManager::merge_file_total_size_bytes_for_test() const {
    return g_merge_file_total_size_bytes.get_value();
}

double MergeFileManager::merge_file_avg_small_file_num_for_test() const {
    return g_merge_file_avg_small_file_num.get_value();
}

double MergeFileManager::merge_file_avg_file_size_for_test() const {
    return g_merge_file_avg_file_size_bytes.get_value();
}

void MergeFileManager::record_merge_file_metrics_for_test(
        const MergeFileManager::MergeFileState* merge_file) {
    DCHECK(merge_file != nullptr);
    record_merge_file_metrics(*merge_file);
}
#endif

} // namespace doris::io
