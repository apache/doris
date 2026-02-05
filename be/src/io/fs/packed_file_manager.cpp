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

#include "io/fs/packed_file_manager.h"

#include <bvar/bvar.h>
#include <bvar/recorder.h>
#include <bvar/window.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <functional>
#include <limits>
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
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/packed_file_trailer.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "util/coding.h"
#include "util/slice.h"
#include "util/uid_util.h"

namespace doris::io {

namespace {

bvar::Adder<int64_t> g_packed_file_total_count("packed_file", "total_count");
bvar::Adder<int64_t> g_packed_file_total_small_file_count("packed_file", "total_small_file_num");
bvar::Adder<int64_t> g_packed_file_total_size_bytes("packed_file", "total_size_bytes");
bvar::IntRecorder g_packed_file_small_file_num_recorder;
bvar::IntRecorder g_packed_file_file_size_recorder;
bvar::Window<bvar::IntRecorder> g_packed_file_avg_small_file_num(
        "packed_file_avg_small_file_num", &g_packed_file_small_file_num_recorder,
        /*window_size=*/10);
bvar::Window<bvar::IntRecorder> g_packed_file_avg_file_size_bytes("packed_file_avg_file_size_bytes",
                                                                  &g_packed_file_file_size_recorder,
                                                                  /*window_size=*/10);
bvar::IntRecorder g_packed_file_active_to_ready_ms_recorder;
bvar::IntRecorder g_packed_file_ready_to_upload_ms_recorder;
bvar::IntRecorder g_packed_file_uploading_to_uploaded_ms_recorder;
bvar::Window<bvar::IntRecorder> g_packed_file_active_to_ready_ms_window(
        "packed_file_active_to_ready_ms", &g_packed_file_active_to_ready_ms_recorder,
        /*window_size=*/10);
bvar::Window<bvar::IntRecorder> g_packed_file_ready_to_upload_ms_window(
        "packed_file_ready_to_upload_ms", &g_packed_file_ready_to_upload_ms_recorder,
        /*window_size=*/10);
bvar::Window<bvar::IntRecorder> g_packed_file_uploading_to_uploaded_ms_window(
        "packed_file_uploading_to_uploaded_ms", &g_packed_file_uploading_to_uploaded_ms_recorder,
        /*window_size=*/10);

// Metrics for async small file cache write
bvar::Adder<int64_t> g_packed_file_cache_async_write_count("packed_file_cache",
                                                           "async_write_count");
bvar::Adder<int64_t> g_packed_file_cache_async_write_bytes("packed_file_cache",
                                                           "async_write_bytes");

Status append_packed_info_trailer(FileWriter* writer, const std::string& packed_file_path,
                                  const cloud::PackedFileInfoPB& packed_file_info) {
    if (writer == nullptr) {
        return Status::InternalError("File writer is null for packed file: {}", packed_file_path);
    }
    if (writer->state() == FileWriter::State::CLOSED) {
        return Status::InternalError("File writer already closed for packed file: {}",
                                     packed_file_path);
    }

    cloud::PackedFileFooterPB footer_pb;
    footer_pb.mutable_packed_file_info()->CopyFrom(packed_file_info);

    std::string serialized_footer;
    if (!footer_pb.SerializeToString(&serialized_footer)) {
        return Status::InternalError("Failed to serialize packed file footer info for {}",
                                     packed_file_path);
    }

    if (serialized_footer.size() >
        std::numeric_limits<uint32_t>::max() - kPackedFileTrailerSuffixSize) {
        return Status::InternalError("PackedFileFooterPB too large for {}", packed_file_path);
    }

    std::string trailer;
    trailer.reserve(serialized_footer.size() + kPackedFileTrailerSuffixSize);
    trailer.append(serialized_footer);
    put_fixed32_le(&trailer, static_cast<uint32_t>(serialized_footer.size()));
    put_fixed32_le(&trailer, kPackedFileTrailerVersion);

    return writer->append(Slice(trailer));
}

// write small file data to file cache
void do_write_to_file_cache(const std::string& small_file_path, const std::string& data,
                            int64_t tablet_id, uint64_t expiration_time) {
    if (data.empty()) {
        return;
    }

    // Generate cache key from small file path (e.g., "rowset_id_seg_id.dat")
    Path path(small_file_path);
    UInt128Wrapper cache_hash = BlockFileCache::hash(path.filename().native());

    VLOG_DEBUG << "packed_file_cache_write: path=" << small_file_path
               << " filename=" << path.filename().native() << " hash=" << cache_hash.to_string()
               << " size=" << data.size() << " tablet_id=" << tablet_id
               << " expiration_time=" << expiration_time;

    BlockFileCache* file_cache = FileCacheFactory::instance()->get_by_path(cache_hash);
    if (file_cache == nullptr) {
        return; // Cache not available, skip
    }

    // Allocate cache blocks
    CacheContext ctx;
    ctx.cache_type = expiration_time > 0 ? FileCacheType::TTL : FileCacheType::NORMAL;
    ctx.expiration_time = expiration_time;
    ctx.tablet_id = tablet_id;
    ReadStatistics stats;
    ctx.stats = &stats;

    FileBlocksHolder holder = file_cache->get_or_set(cache_hash, 0, data.size(), ctx);

    // Write data to cache blocks
    size_t data_offset = 0;
    for (auto& block : holder.file_blocks) {
        if (data_offset >= data.size()) {
            break;
        }
        size_t block_size = block->range().size();
        size_t write_size = std::min(block_size, data.size() - data_offset);

        if (block->state() == FileBlock::State::EMPTY) {
            block->get_or_set_downloader();
            if (block->is_downloader()) {
                Slice s(data.data() + data_offset, write_size);
                Status st = block->append(s);
                if (st.ok()) {
                    st = block->finalize();
                }
                if (!st.ok()) {
                    LOG(WARNING) << "Write small file to cache failed: " << st.msg();
                }
            }
        }
        data_offset += write_size;
    }
}

// Async wrapper: submit cache write task to thread pool
// The data is copied into the lambda capture to ensure its lifetime extends beyond
// the async task execution. The original Slice may reference a buffer that gets
// reused or freed before the async task runs.
void write_small_file_to_cache_async(const std::string& small_file_path, const Slice& data,
                                     int64_t tablet_id, uint64_t expiration_time) {
    if (!config::enable_file_cache || data.size == 0) {
        return;
    }

    // Copy data since original buffer may be reused before async task executes
    // For small files (< 1MB), copy overhead is acceptable
    std::string data_copy(data.data, data.size);
    size_t data_size = data.size;

    auto* thread_pool = ExecEnv::GetInstance()->s3_file_upload_thread_pool();
    if (thread_pool == nullptr) {
        // Fallback to sync write if thread pool not available
        do_write_to_file_cache(small_file_path, data_copy, tablet_id, expiration_time);
        return;
    }

    // Track async write count and bytes
    g_packed_file_cache_async_write_count << 1;
    g_packed_file_cache_async_write_bytes << static_cast<int64_t>(data_size);

    Status st = thread_pool->submit_func([path = small_file_path, data = std::move(data_copy),
                                          tablet_id, data_size, expiration_time]() {
        do_write_to_file_cache(path, data, tablet_id, expiration_time);
        // Decrement async write count after completion
        g_packed_file_cache_async_write_count << -1;
        g_packed_file_cache_async_write_bytes << -static_cast<int64_t>(data_size);
    });

    if (!st.ok()) {
        // Revert metrics since task was not submitted
        g_packed_file_cache_async_write_count << -1;
        g_packed_file_cache_async_write_bytes << -static_cast<int64_t>(data_size);
        LOG(WARNING) << "Failed to submit cache write task for " << small_file_path << ": "
                     << st.msg();
        // Don't block on failure, cache write is best-effort
    }
}

} // namespace

PackedFileManager* PackedFileManager::instance() {
    static PackedFileManager instance;
    return &instance;
}

PackedFileManager::~PackedFileManager() {
    stop_background_manager();
}

Status PackedFileManager::init() {
    return Status::OK();
}

Status PackedFileManager::create_new_packed_file_context(
        const std::string& resource_id, std::unique_ptr<PackedFileContext>& packed_file_ctx) {
    FileSystemSPtr file_system;
    RETURN_IF_ERROR(ensure_file_system(resource_id, &file_system));
    if (file_system == nullptr) {
        return Status::InternalError("File system is not available for packed file creation: " +
                                     resource_id);
    }

    auto uuid = generate_uuid_string();
    auto hash_val = std::hash<std::string> {}(uuid);
    uint16_t path_bucket = hash_val % 4096 + 1;
    std::stringstream path_stream;
    path_stream << "data/packed_file/" << path_bucket << "/" << uuid << ".bin";

    packed_file_ctx = std::make_unique<PackedFileContext>();
    const std::string relative_path = path_stream.str();
    packed_file_ctx->packed_file_path = relative_path;
    packed_file_ctx->create_time = std::time(nullptr);
    packed_file_ctx->create_timestamp = std::chrono::steady_clock::now();
    packed_file_ctx->state = PackedFileState::INIT;
    packed_file_ctx->resource_id = resource_id;
    packed_file_ctx->file_system = std::move(file_system);

    // Create file writer for the packed file
    FileWriterPtr new_writer;
    FileWriterOptions opts;
    // Disable write_file_cache for packed file itself.
    // We write file cache for each small file separately in append_small_file()
    // using the small file path as cache key, ensuring cache entries can be
    // properly cleaned up when stale rowsets are removed.
    opts.write_file_cache = false;
    RETURN_IF_ERROR(
            packed_file_ctx->file_system->create_file(Path(relative_path), &new_writer, &opts));
    packed_file_ctx->writer = std::move(new_writer);

    return Status::OK();
}

Status PackedFileManager::ensure_file_system(const std::string& resource_id,
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

Status PackedFileManager::append_small_file(const std::string& path, const Slice& data,
                                            const PackedAppendContext& info) {
    // Check if file is too large to be merged
    if (data.get_size() > config::small_file_threshold_bytes) {
        return Status::OK(); // Skip merging for large files
    }

    if (info.txn_id <= 0) {
        return Status::InvalidArgument("Missing valid txn id for packed file append: " + path);
    }

    std::lock_guard<std::timed_mutex> lock(_current_packed_file_mutex);

    auto& current_state = _current_packed_files[info.resource_id];
    if (!current_state || !current_state->writer) {
        RETURN_IF_ERROR(create_new_packed_file_context(info.resource_id, current_state));
    }

    // Check if we need to create a new packed file
    if (current_state->total_size + data.get_size() >= config::packed_file_size_threshold_bytes) {
        RETURN_IF_ERROR(mark_current_packed_file_for_upload_locked(info.resource_id));
        auto it = _current_packed_files.find(info.resource_id);
        if (it == _current_packed_files.end() || !it->second || !it->second->writer) {
            RETURN_IF_ERROR(create_new_packed_file_context(
                    info.resource_id, _current_packed_files[info.resource_id]));
        }
    }

    PackedFileContext* active_state = current_state.get();

    // Write data to current packed file
    RETURN_IF_ERROR(active_state->writer->append(data));

    // Async write data to file cache using small file path as cache key.
    // This ensures cache key matches the cleanup key in Rowset::clear_cache(),
    // allowing proper cache cleanup when stale rowsets are removed.
    write_small_file_to_cache_async(path, data, info.tablet_id, info.expiration_time);

    // Update index
    PackedSliceLocation location;
    location.packed_file_path = active_state->packed_file_path;
    location.offset = active_state->current_offset;
    location.size = data.get_size();
    location.create_time = std::time(nullptr);
    location.tablet_id = info.tablet_id;
    location.rowset_id = info.rowset_id;
    location.resource_id = info.resource_id;
    location.txn_id = info.txn_id;

    active_state->slice_locations[path] = location;
    active_state->current_offset += data.get_size();
    active_state->total_size += data.get_size();

    // Rotate packed file when small file count reaches threshold
    if (config::packed_file_small_file_count_threshold > 0 &&
        static_cast<int64_t>(active_state->slice_locations.size()) >=
                config::packed_file_small_file_count_threshold) {
        RETURN_IF_ERROR(mark_current_packed_file_for_upload_locked(info.resource_id));
    }

    // Mark as active if this is the first write
    if (!active_state->first_append_timestamp.has_value()) {
        active_state->first_append_timestamp = std::chrono::steady_clock::now();
    }
    if (active_state->state == PackedFileState::INIT) {
        active_state->state = PackedFileState::ACTIVE;
    }

    // Update global index
    {
        std::lock_guard<std::mutex> global_lock(_global_index_mutex);
        _global_slice_locations[path] = location;
    }

    return Status::OK();
}

Status PackedFileManager::wait_for_packed_file_upload(PackedFileContext* packed_file_ptr) {
    std::unique_lock<std::mutex> upload_lock(packed_file_ptr->upload_mutex);
    packed_file_ptr->upload_cv.wait(upload_lock, [packed_file_ptr] {
        auto state = packed_file_ptr->state.load(std::memory_order_acquire);
        return state == PackedFileState::UPLOADED || state == PackedFileState::FAILED;
    });
    if (packed_file_ptr->state == PackedFileState::FAILED) {
        std::string err = packed_file_ptr->last_error;
        if (err.empty()) {
            err = "Packed file upload failed";
        }
        return Status::InternalError(err);
    }
    return Status::OK();
}

Status PackedFileManager::wait_upload_done(const std::string& path) {
    std::string packed_file_path;
    {
        std::lock_guard<std::mutex> global_lock(_global_index_mutex);
        auto it = _global_slice_locations.find(path);
        if (it == _global_slice_locations.end()) {
            return Status::InternalError("File not found in global index: " + path);
        }
        packed_file_path = it->second.packed_file_path;
    }

    // Find the packed file in uploaded files first - if already uploaded, no need to wait
    std::shared_ptr<PackedFileContext> managed_packed_file;
    std::shared_ptr<PackedFileContext> failed_packed_file;
    {
        std::lock_guard<std::mutex> lock(_packed_files_mutex);
        auto uploaded_it = _uploaded_packed_files.find(packed_file_path);
        if (uploaded_it != _uploaded_packed_files.end()) {
            auto state = uploaded_it->second->state.load(std::memory_order_acquire);
            if (state == PackedFileState::UPLOADED) {
                return Status::OK(); // Already uploaded, no need to wait
            }
            if (state == PackedFileState::FAILED) {
                failed_packed_file = uploaded_it->second;
            } else {
                managed_packed_file = uploaded_it->second;
            }
        }
    }

    if (failed_packed_file) {
        std::lock_guard<std::mutex> upload_lock(failed_packed_file->upload_mutex);
        std::string err = failed_packed_file->last_error;
        if (err.empty()) {
            err = "Merge file upload failed";
        }
        return Status::InternalError(err);
    }

    // Find the packed file in either current or uploading files
    PackedFileContext* packed_file_ptr = nullptr;
    {
        std::lock_guard<std::timed_mutex> current_lock(_current_packed_file_mutex);
        for (auto& [resource_id, state] : _current_packed_files) {
            if (state && state->packed_file_path == packed_file_path) {
                packed_file_ptr = state.get();
                break;
            }
        }
    }

    if (!packed_file_ptr) {
        std::lock_guard<std::mutex> lock(_packed_files_mutex);
        auto uploading_it = _uploading_packed_files.find(packed_file_path);
        if (uploading_it != _uploading_packed_files.end()) {
            managed_packed_file = uploading_it->second;
            packed_file_ptr = managed_packed_file.get();
        } else {
            auto uploaded_it = _uploaded_packed_files.find(packed_file_path);
            if (uploaded_it != _uploaded_packed_files.end()) {
                managed_packed_file = uploaded_it->second;
                packed_file_ptr = managed_packed_file.get();
            }
        }
    }

    if (!packed_file_ptr) {
        // Packed file not found in any location, this is unexpected
        return Status::InternalError("Packed file not found for path: " + path);
    }

    Status wait_status = wait_for_packed_file_upload(packed_file_ptr);
    (void)managed_packed_file; // keep shared ownership alive during wait
    return wait_status;
}

Status PackedFileManager::get_packed_slice_location(const std::string& path,
                                                    PackedSliceLocation* location) {
    std::lock_guard<std::mutex> lock(_global_index_mutex);
    auto it = _global_slice_locations.find(path);
    if (it == _global_slice_locations.end()) {
        return Status::NotFound("File not found in global packed index: {}", path);
    }

    *location = it->second;
    return Status::OK();
}

void PackedFileManager::start_background_manager() {
    if (_background_thread) {
        return; // Already started
    }

    _stop_background_thread = false;
    _background_thread = std::make_unique<std::thread>([this] { background_manager(); });
}

void PackedFileManager::stop_background_manager() {
    _stop_background_thread = true;
    if (_background_thread && _background_thread->joinable()) {
        _background_thread->join();
    }
    _background_thread.reset();
}

Status PackedFileManager::mark_current_packed_file_for_upload_locked(
        const std::string& resource_id) {
    auto it = _current_packed_files.find(resource_id);
    if (it == _current_packed_files.end() || !it->second || !it->second->writer) {
        return Status::OK(); // Nothing to mark for upload
    }

    auto& current = it->second;

    // Mark as ready for upload
    current->state = PackedFileState::READY_TO_UPLOAD;
    if (!current->ready_to_upload_timestamp.has_value()) {
        auto now = std::chrono::steady_clock::now();
        current->ready_to_upload_timestamp = now;
        int64_t active_to_ready_ms = -1;
        if (current->first_append_timestamp.has_value()) {
            active_to_ready_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                         now - *current->first_append_timestamp)
                                         .count();
            g_packed_file_active_to_ready_ms_recorder << active_to_ready_ms;
            if (auto* sampler = g_packed_file_active_to_ready_ms_recorder.get_sampler()) {
                sampler->take_sample();
            }
        }
        VLOG_DEBUG << "Packed file " << current->packed_file_path
                   << " transition ACTIVE->READY_TO_UPLOAD; active_to_ready_ms="
                   << active_to_ready_ms;
    }

    // Move to uploading files list
    {
        std::shared_ptr<PackedFileContext> uploading_ptr =
                std::shared_ptr<PackedFileContext>(std::move(current));
        std::lock_guard<std::mutex> lock(_packed_files_mutex);
        _uploading_packed_files[uploading_ptr->packed_file_path] = uploading_ptr;
    }

    // Create new packed file
    return create_new_packed_file_context(resource_id, it->second);
}

Status PackedFileManager::mark_current_packed_file_for_upload(const std::string& resource_id) {
    std::lock_guard<std::timed_mutex> lock(_current_packed_file_mutex);
    return mark_current_packed_file_for_upload_locked(resource_id);
}

void PackedFileManager::record_packed_file_metrics(const PackedFileContext& packed_file) {
    g_packed_file_total_count << 1;
    g_packed_file_total_small_file_count
            << static_cast<int64_t>(packed_file.slice_locations.size());
    g_packed_file_total_size_bytes << packed_file.total_size;
    g_packed_file_small_file_num_recorder
            << static_cast<int64_t>(packed_file.slice_locations.size());
    g_packed_file_file_size_recorder << packed_file.total_size;
    // Flush samplers immediately so the window bvar reflects the latest packed file.
    if (auto* sampler = g_packed_file_small_file_num_recorder.get_sampler()) {
        sampler->take_sample();
    }
    if (auto* sampler = g_packed_file_file_size_recorder.get_sampler()) {
        sampler->take_sample();
    }
}

void PackedFileManager::background_manager() {
    auto last_cleanup_time = std::chrono::steady_clock::now();

    while (!_stop_background_thread.load()) {
        int64_t check_interval_ms =
                std::max<int64_t>(1, config::packed_file_time_threshold_ms / 10);
        std::this_thread::sleep_for(std::chrono::milliseconds(check_interval_ms));

        // Check if current packed file should be closed due to time threshold
        std::vector<std::string> resources_to_mark;
        {
            std::unique_lock<std::timed_mutex> current_lock(_current_packed_file_mutex,
                                                            std::defer_lock);
            int64_t lock_wait_ms = std::max<int64_t>(0, config::packed_file_try_lock_timeout_ms);
            if (current_lock.try_lock_for(std::chrono::milliseconds(lock_wait_ms))) {
                for (auto& [resource_id, state] : _current_packed_files) {
                    if (!state || state->state != PackedFileState::ACTIVE) {
                        continue;
                    }
                    if (!state->first_append_timestamp.has_value()) {
                        continue;
                    }
                    auto current_time = std::chrono::steady_clock::now();
                    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                              current_time - *(state->first_append_timestamp))
                                              .count();
                    if (elapsed_ms >= config::packed_file_time_threshold_ms) {
                        resources_to_mark.push_back(resource_id);
                    }
                }
            }
        }
        for (const auto& resource_id : resources_to_mark) {
            Status st = mark_current_packed_file_for_upload(resource_id);
            if (!st.ok()) {
                LOG(WARNING) << "Failed to close current packed file for resource " << resource_id
                             << ": " << st.to_string();
            }
        }

        // Process uploading files
        process_uploading_packed_files();

        auto now = std::chrono::steady_clock::now();
        int64_t cleanup_interval_sec =
                std::max<int64_t>(1, config::packed_file_cleanup_interval_seconds);
        auto cleanup_interval = std::chrono::seconds(cleanup_interval_sec);
        if (now - last_cleanup_time >= cleanup_interval) {
            cleanup_expired_data();
            last_cleanup_time = now;
        }
    }
}

void PackedFileManager::process_uploading_packed_files() {
    std::vector<std::shared_ptr<PackedFileContext>> files_ready;
    std::vector<std::shared_ptr<PackedFileContext>> files_uploading;
    auto record_ready_to_upload = [&](const std::shared_ptr<PackedFileContext>& packed_file) {
        if (!packed_file->uploading_timestamp.has_value()) {
            packed_file->uploading_timestamp = std::chrono::steady_clock::now();
            int64_t duration_ms = -1;
            if (packed_file->ready_to_upload_timestamp.has_value()) {
                duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                      *packed_file->uploading_timestamp -
                                      *packed_file->ready_to_upload_timestamp)
                                      .count();
                g_packed_file_ready_to_upload_ms_recorder << duration_ms;
                if (auto* sampler = g_packed_file_ready_to_upload_ms_recorder.get_sampler()) {
                    sampler->take_sample();
                }
            }
            VLOG_DEBUG << "Packed file " << packed_file->packed_file_path
                       << " transition READY_TO_UPLOAD->UPLOADING; "
                          "ready_to_upload_ms="
                       << duration_ms;
        }
    };

    {
        std::lock_guard<std::mutex> lock(_packed_files_mutex);
        for (auto& [packed_file_path, packed_file] : _uploading_packed_files) {
            auto state = packed_file->state.load(std::memory_order_acquire);
            if (state != PackedFileState::READY_TO_UPLOAD && state != PackedFileState::UPLOADING) {
                continue;
            }
            if (state == PackedFileState::READY_TO_UPLOAD) {
                files_ready.emplace_back(packed_file);
            } else {
                files_uploading.emplace_back(packed_file);
            }
        }
    }

    auto handle_success = [&](const std::shared_ptr<PackedFileContext>& packed_file) {
        auto now = std::chrono::steady_clock::now();
        int64_t active_to_ready_ms = -1;
        int64_t ready_to_upload_ms = -1;
        int64_t uploading_to_uploaded_ms = -1;
        if (packed_file->first_append_timestamp.has_value() &&
            packed_file->ready_to_upload_timestamp.has_value()) {
            active_to_ready_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                         *packed_file->ready_to_upload_timestamp -
                                         *packed_file->first_append_timestamp)
                                         .count();
        }
        if (packed_file->ready_to_upload_timestamp.has_value() &&
            packed_file->uploading_timestamp.has_value()) {
            ready_to_upload_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                         *packed_file->uploading_timestamp -
                                         *packed_file->ready_to_upload_timestamp)
                                         .count();
        }
        if (packed_file->uploading_timestamp.has_value()) {
            uploading_to_uploaded_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                               now - *packed_file->uploading_timestamp)
                                               .count();
            g_packed_file_uploading_to_uploaded_ms_recorder << uploading_to_uploaded_ms;
            if (auto* sampler = g_packed_file_uploading_to_uploaded_ms_recorder.get_sampler()) {
                sampler->take_sample();
            }
        }
        int64_t total_ms = -1;
        if (packed_file->first_append_timestamp.has_value()) {
            total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               now - *packed_file->first_append_timestamp)
                               .count();
        }
        std::ostringstream slices_stream;
        bool first_slice = true;
        for (const auto& [small_file_path, index] : packed_file->slice_locations) {
            if (!first_slice) {
                slices_stream << "; ";
            }
            first_slice = false;
            slices_stream << small_file_path << "(txn=" << index.txn_id
                          << ", offset=" << index.offset << ", size=" << index.size << ")";

            // Update packed_file_size in global index
            {
                std::lock_guard<std::mutex> global_lock(_global_index_mutex);
                auto it = _global_slice_locations.find(small_file_path);
                if (it != _global_slice_locations.end()) {
                    it->second.packed_file_size = packed_file->total_size;
                }
            }
        }
        LOG(INFO) << "Packed file " << packed_file->packed_file_path
                  << " uploaded; slices=" << packed_file->slice_locations.size()
                  << ", total_bytes=" << packed_file->total_size << ", slice_detail=["
                  << slices_stream.str() << "]"
                  << ", active_to_ready_ms=" << active_to_ready_ms
                  << ", ready_to_upload_ms=" << ready_to_upload_ms
                  << ", uploading_to_uploaded_ms=" << uploading_to_uploaded_ms
                  << ", total_ms=" << total_ms;
        {
            std::lock_guard<std::mutex> upload_lock(packed_file->upload_mutex);
            packed_file->state = PackedFileState::UPLOADED;
            packed_file->upload_time = std::time(nullptr);
        }
        packed_file->upload_cv.notify_all();
        {
            std::lock_guard<std::mutex> lock(_packed_files_mutex);
            _uploading_packed_files.erase(packed_file->packed_file_path);
            _uploaded_packed_files[packed_file->packed_file_path] = packed_file;
        }
    };

    auto handle_failure = [&](const std::shared_ptr<PackedFileContext>& packed_file,
                              const Status& status) {
        LOG(WARNING) << "Failed to upload packed file: " << packed_file->packed_file_path
                     << ", error: " << status.to_string();
        {
            std::lock_guard<std::mutex> upload_lock(packed_file->upload_mutex);
            packed_file->state = PackedFileState::FAILED;
            packed_file->last_error = status.to_string();
            packed_file->upload_time = std::time(nullptr);
        }
        packed_file->upload_cv.notify_all();
        {
            std::lock_guard<std::mutex> lock(_packed_files_mutex);
            _uploading_packed_files.erase(packed_file->packed_file_path);
            _uploaded_packed_files[packed_file->packed_file_path] = packed_file;
        }
    };

    for (auto& packed_file : files_ready) {
        const std::string& packed_file_path = packed_file->packed_file_path;
        cloud::PackedFileInfoPB packed_file_info;
        packed_file_info.set_ref_cnt(packed_file->slice_locations.size());
        packed_file_info.set_total_slice_num(packed_file->slice_locations.size());
        packed_file_info.set_total_slice_bytes(packed_file->total_size);
        packed_file_info.set_remaining_slice_bytes(packed_file->total_size);
        packed_file_info.set_created_at_sec(packed_file->create_time);
        packed_file_info.set_corrected(false);
        packed_file_info.set_state(doris::cloud::PackedFileInfoPB::NORMAL);
        packed_file_info.set_resource_id(packed_file->resource_id);

        for (const auto& [small_file_path, index] : packed_file->slice_locations) {
            auto* small_file = packed_file_info.add_slices();
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

        Status meta_status = update_meta_service(packed_file->packed_file_path, packed_file_info);
        if (!meta_status.ok()) {
            LOG(WARNING) << "Failed to update meta service for packed file: "
                         << packed_file->packed_file_path << ", error: " << meta_status.to_string();
            handle_failure(packed_file, meta_status);
            continue;
        }

        // Record stats once the packed file metadata is persisted.
        record_packed_file_metrics(*packed_file);

        Status trailer_status = append_packed_info_trailer(
                packed_file->writer.get(), packed_file->packed_file_path, packed_file_info);
        if (!trailer_status.ok()) {
            handle_failure(packed_file, trailer_status);
            continue;
        }

        // Now upload the file
        if (!packed_file->slice_locations.empty()) {
            std::ostringstream oss;
            oss << "Uploading packed file " << packed_file_path << " with "
                << packed_file->slice_locations.size() << " small files: ";
            bool first = true;
            for (const auto& [small_file_path, index] : packed_file->slice_locations) {
                if (!first) {
                    oss << ", ";
                }
                first = false;
                oss << "[" << small_file_path << ", offset=" << index.offset
                    << ", size=" << index.size << "]";
            }
            VLOG_DEBUG << oss.str();
        } else {
            VLOG_DEBUG << "Uploading packed file " << packed_file_path << " with no small files";
        }

        Status upload_status = finalize_packed_file_upload(packed_file->packed_file_path,
                                                           packed_file->writer.get());

        if (upload_status.is<ErrorCode::ALREADY_CLOSED>()) {
            record_ready_to_upload(packed_file);
            handle_success(packed_file);
            continue;
        }
        if (!upload_status.ok()) {
            handle_failure(packed_file, upload_status);
            continue;
        }

        record_ready_to_upload(packed_file);
        packed_file->state = PackedFileState::UPLOADING;
    }

    for (auto& packed_file : files_uploading) {
        if (!packed_file->writer) {
            handle_failure(packed_file,
                           Status::InternalError("File writer is null for packed file: {}",
                                                 packed_file->packed_file_path));
            continue;
        }

        if (packed_file->writer->state() != FileWriter::State::CLOSED) {
            continue;
        }

        Status status = packed_file->writer->close(true);
        if (status.is<ErrorCode::ALREADY_CLOSED>()) {
            handle_success(packed_file);
            continue;
        }
        if (status.ok()) {
            continue;
        }

        handle_failure(packed_file, status);
    }
}

Status PackedFileManager::finalize_packed_file_upload(const std::string& packed_file_path,
                                                      FileWriter* writer) {
    if (writer == nullptr) {
        return Status::InternalError("File writer is null for packed file: " + packed_file_path);
    }

    return writer->close(true);
}

Status PackedFileManager::update_meta_service(const std::string& packed_file_path,
                                              const cloud::PackedFileInfoPB& packed_file_info) {
#ifdef BE_TEST
    TEST_SYNC_POINT_RETURN_WITH_VALUE("PackedFileManager::update_meta_service", Status::OK(),
                                      packed_file_path, &packed_file_info);
#endif
    VLOG_DEBUG << "Updating meta service for packed file: " << packed_file_path << " with "
               << packed_file_info.total_slice_num() << " small files"
               << ", total bytes: " << packed_file_info.total_slice_bytes();

    // Get CloudMetaMgr through StorageEngine
    if (!config::is_cloud_mode()) {
        return Status::InternalError("Storage engine is not cloud mode");
    }

    auto& storage_engine = ExecEnv::GetInstance()->storage_engine();
    auto& cloud_meta_mgr = storage_engine.to_cloud().meta_mgr();
    return cloud_meta_mgr.update_packed_file_info(packed_file_path, packed_file_info);
}

void PackedFileManager::cleanup_expired_data() {
    auto current_time = std::time(nullptr);

    // Clean up expired uploaded files
    {
        std::lock_guard<std::mutex> uploaded_lock(_packed_files_mutex);
        auto it = _uploaded_packed_files.begin();
        while (it != _uploaded_packed_files.end()) {
            if (it->second->state == PackedFileState::UPLOADED &&
                current_time - it->second->upload_time > config::uploaded_file_retention_seconds) {
                it = _uploaded_packed_files.erase(it);
            } else if (it->second->state == PackedFileState::FAILED &&
                       current_time - it->second->upload_time >
                               config::uploaded_file_retention_seconds) {
                it = _uploaded_packed_files.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Clean up expired global index entries
    {
        std::lock_guard<std::mutex> global_lock(_global_index_mutex);
        auto it = _global_slice_locations.begin();
        while (it != _global_slice_locations.end()) {
            const auto& index = it->second;
            if (index.create_time > 0 &&
                current_time - index.create_time > config::uploaded_file_retention_seconds) {
                it = _global_slice_locations.erase(it);
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

void PackedFileManager::reset_packed_file_bvars_for_test() const {
    reset_adder(g_packed_file_total_count);
    reset_adder(g_packed_file_total_small_file_count);
    reset_adder(g_packed_file_total_size_bytes);
    g_packed_file_small_file_num_recorder.reset();
    g_packed_file_file_size_recorder.reset();
    g_packed_file_active_to_ready_ms_recorder.reset();
    g_packed_file_ready_to_upload_ms_recorder.reset();
    g_packed_file_uploading_to_uploaded_ms_recorder.reset();
    if (auto* sampler = g_packed_file_small_file_num_recorder.get_sampler()) {
        sampler->take_sample();
    }
    if (auto* sampler = g_packed_file_file_size_recorder.get_sampler()) {
        sampler->take_sample();
    }
    if (auto* sampler = g_packed_file_active_to_ready_ms_recorder.get_sampler()) {
        sampler->take_sample();
    }
    if (auto* sampler = g_packed_file_ready_to_upload_ms_recorder.get_sampler()) {
        sampler->take_sample();
    }
    if (auto* sampler = g_packed_file_uploading_to_uploaded_ms_recorder.get_sampler()) {
        sampler->take_sample();
    }
}

int64_t PackedFileManager::packed_file_total_count_for_test() const {
    return g_packed_file_total_count.get_value();
}

int64_t PackedFileManager::packed_file_total_small_file_num_for_test() const {
    return g_packed_file_total_small_file_count.get_value();
}

int64_t PackedFileManager::packed_file_total_size_bytes_for_test() const {
    return g_packed_file_total_size_bytes.get_value();
}

double PackedFileManager::packed_file_avg_small_file_num_for_test() const {
    return g_packed_file_avg_small_file_num.get_value().get_average_double();
}

double PackedFileManager::packed_file_avg_file_size_for_test() const {
    return g_packed_file_avg_file_size_bytes.get_value().get_average_double();
}

void PackedFileManager::record_packed_file_metrics_for_test(
        const PackedFileManager::PackedFileContext* packed_file) {
    DCHECK(packed_file != nullptr);
    record_packed_file_metrics(*packed_file);
}

void PackedFileManager::clear_state_for_test() {
    std::lock_guard<std::timed_mutex> cur_lock(_current_packed_file_mutex);
    _current_packed_files.clear();
    {
        std::lock_guard<std::mutex> lock(_packed_files_mutex);
        _uploading_packed_files.clear();
        _uploaded_packed_files.clear();
    }
    {
        std::lock_guard<std::mutex> lock(_global_index_mutex);
        _global_slice_locations.clear();
    }
}
#endif

} // namespace doris::io
