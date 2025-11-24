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

#pragma once

#include <butil/macros.h>
#include <gen_cpp/cloud.pb.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris::io {

struct MergeFileSegmentIndex {
    std::string merge_file_path;
    int64_t offset;
    int64_t size;
    int64_t tablet_id = 0;
    std::string rowset_id;
    std::string resource_id;
    int64_t txn_id = 0;
};

struct MergeFileAppendInfo {
    std::string resource_id;
    int64_t tablet_id = 0;
    std::string rowset_id;
    int64_t txn_id = 0;
};

// Global object that manages merging small files into large files for S3 optimization
class MergeFileManager {
    struct MergeFileState;

public:
    static MergeFileManager* instance();

    // Initialize manager state; file system will be resolved lazily
    Status init();

    // Write a small file to the current merge file
    Status append(const std::string& path, const Slice& data, const MergeFileAppendInfo& info);

    // Block until the small file's merge file is uploaded to S3
    Status wait_write_done(const std::string& path);

    // Get merge file index information for a small file
    Status get_merge_file_index(const std::string& path, MergeFileSegmentIndex* index);

    // Start the background management thread
    void start_background_manager();

    // Stop the background management thread
    void stop_background_manager();

    // Mark current merge file for upload and create new one
    Status mark_current_merge_file_for_upload(const std::string& resource_id);

    // Internal helper; expects caller holds _current_merge_file_mutex
    Status mark_current_merge_file_for_upload_locked(const std::string& resource_id);

    void record_merge_file_metrics(const MergeFileState& merge_file);

private:
    MergeFileManager() = default;
    ~MergeFileManager();

    DISALLOW_COPY_AND_ASSIGN(MergeFileManager);

    // Background thread function for managing merge file lifecycle
    void background_manager();

    // Upload merge file to S3 and update meta service
    Status upload_merge_file(const std::string& merge_file_path, FileWriter* writer);

    // Update meta service with merge file information
    Status update_meta_service(const std::string& merge_file_path,
                               const cloud::MergedFileInfoPB& merge_file_info);

    // Process uploading files
    void process_uploading_files();

    // Clean up expired data
    void cleanup_expired_data();

    // Internal structure to track merge file state
    enum class MergeFileStateEnum {
        INIT,               // Initial state, no files written yet
        ACTIVE,             // Has files but doesn't meet upload conditions
        READY_TO_UPLOADING, // Ready for upload, metadata still being prepared
        UPLOADING,          // Upload triggered, waiting for writer close to finish
        UPLOADED,           // Upload completed
        FAILED              // Upload failed
    };

    struct MergeFileState {
        std::string merge_file_path;
        std::unique_ptr<FileWriter> writer;
        std::unordered_map<std::string, MergeFileSegmentIndex> index_map;
        int64_t current_offset = 0;
        int64_t total_size = 0;
        int64_t create_time;
        int64_t upload_time = 0;
        std::chrono::steady_clock::time_point create_timestamp;
        std::optional<std::chrono::steady_clock::time_point> first_append_timestamp;
        std::atomic<MergeFileStateEnum> state {MergeFileStateEnum::INIT};
        std::condition_variable upload_cv;
        std::mutex upload_mutex;
        std::string last_error;
        std::string resource_id;
        FileSystemSPtr file_system;
    };

    // Create a new merge file state with file writer
    Status create_new_merge_file_state(const std::string& resource_id,
                                       std::unique_ptr<MergeFileState>& merge_file_state);

    Status ensure_file_system(const std::string& resource_id, FileSystemSPtr* file_system);

    // Helper function to wait for merge file upload completion
    Status wait_for_merge_file_upload(MergeFileState* merge_file_ptr);

    // Thread management
    std::atomic<bool> _stop_background_thread {false};
    std::unique_ptr<std::thread> _background_thread;

    // File system
    FileSystemSPtr _default_file_system;
    std::unordered_map<std::string, FileSystemSPtr> _file_systems;
    std::mutex _file_system_mutex;

    // Current active merge file
    std::unordered_map<std::string, std::unique_ptr<MergeFileState>> _current_merge_files;
    std::timed_mutex _current_merge_file_mutex;

    // Merge files ready for upload or being processed
    std::unordered_map<std::string, std::shared_ptr<MergeFileState>> _uploading_merge_files;

    // Uploaded merge files (kept for some time for wait_write_done)
    std::unordered_map<std::string, std::shared_ptr<MergeFileState>> _uploaded_merge_files;
    std::mutex _merge_files_mutex;

    // Global index mapping small file path to merge file index
    std::unordered_map<std::string, MergeFileSegmentIndex> _global_index_map;
    std::mutex _global_index_mutex;

#ifdef BE_TEST
public:
    void reset_merge_file_bvars_for_test() const;
    int64_t merge_file_total_count_for_test() const;
    int64_t merge_file_total_small_file_num_for_test() const;
    int64_t merge_file_total_size_bytes_for_test() const;
    double merge_file_avg_small_file_num_for_test() const;
    double merge_file_avg_file_size_for_test() const;
    void record_merge_file_metrics_for_test(const MergeFileState* merge_file);
#endif
};

} // namespace doris::io
