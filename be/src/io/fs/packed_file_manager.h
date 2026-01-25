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

struct PackedSliceLocation {
    std::string packed_file_path;
    int64_t offset;
    int64_t size;
    int64_t create_time = 0;
    int64_t tablet_id = 0;
    std::string rowset_id;
    std::string resource_id;
    int64_t txn_id = 0;
    int64_t packed_file_size = -1; // Total size of the packed file, -1 means not set
};

struct PackedAppendContext {
    std::string resource_id;
    int64_t tablet_id = 0;
    std::string rowset_id;
    int64_t txn_id = 0;
    uint64_t expiration_time = 0; // TTL expiration time in seconds since epoch, 0 means no TTL
};

// Global object that manages packing small files into larger files for S3 optimization
class PackedFileManager {
    struct PackedFileContext;

public:
    static PackedFileManager* instance();

    // Initialize manager state; file system will be resolved lazily
    Status init();

    // Write a small file to the current packed file
    Status append_small_file(const std::string& path, const Slice& data,
                             const PackedAppendContext& info);

    // Block until the small file's packed file is uploaded to S3
    Status wait_upload_done(const std::string& path);

    // Get packed file index information for a small file
    Status get_packed_slice_location(const std::string& path, PackedSliceLocation* location);

    // Start the background management thread
    void start_background_manager();

    // Stop the background management thread
    void stop_background_manager();

    // Mark current packed file for upload and create new one
    Status mark_current_packed_file_for_upload(const std::string& resource_id);

    // Internal helper; expects caller holds _current_packed_file_mutex
    Status mark_current_packed_file_for_upload_locked(const std::string& resource_id);

    void record_packed_file_metrics(const PackedFileContext& packed_file);

private:
    PackedFileManager() = default;
    ~PackedFileManager();

    DISALLOW_COPY_AND_ASSIGN(PackedFileManager);

    // Background thread function for managing packed file lifecycle
    void background_manager();

    // Upload packed file to S3 and update meta service
    Status finalize_packed_file_upload(const std::string& packed_file_path, FileWriter* writer);

    // Update meta service with packed file information
    Status update_meta_service(const std::string& packed_file_path,
                               const cloud::PackedFileInfoPB& packed_file_info);

    // Process uploading files
    void process_uploading_packed_files();

    // Clean up expired data
    void cleanup_expired_data();

    // Internal structure to track packed file state
    enum class PackedFileState {
        INIT,            // Initial state, no files written yet
        ACTIVE,          // Has files but doesn't meet upload conditions
        READY_TO_UPLOAD, // Ready for upload, metadata still being prepared
        UPLOADING,       // Upload triggered, waiting for writer close to finish
        UPLOADED,        // Upload completed
        FAILED           // Upload failed
    };

    struct PackedFileContext {
        std::string packed_file_path;
        std::unique_ptr<FileWriter> writer;
        std::unordered_map<std::string, PackedSliceLocation> slice_locations;
        int64_t current_offset = 0;
        int64_t total_size = 0;
        int64_t create_time;
        int64_t upload_time = 0;
        std::chrono::steady_clock::time_point create_timestamp;
        std::optional<std::chrono::steady_clock::time_point> first_append_timestamp;
        std::optional<std::chrono::steady_clock::time_point> ready_to_upload_timestamp;
        std::optional<std::chrono::steady_clock::time_point> uploading_timestamp;
        std::atomic<PackedFileState> state {PackedFileState::INIT};
        std::condition_variable upload_cv;
        std::mutex upload_mutex;
        std::string last_error;
        std::string resource_id;
        FileSystemSPtr file_system;
    };

    // Create a new packed file state with file writer
    Status create_new_packed_file_context(const std::string& resource_id,
                                          std::unique_ptr<PackedFileContext>& packed_file_ctx);

    Status ensure_file_system(const std::string& resource_id, FileSystemSPtr* file_system);

    // Helper function to wait for packed file upload completion
    Status wait_for_packed_file_upload(PackedFileContext* packed_file_ptr);

    // Thread management
    std::atomic<bool> _stop_background_thread {false};
    std::unique_ptr<std::thread> _background_thread;

    // File system
    FileSystemSPtr _default_file_system;
    std::unordered_map<std::string, FileSystemSPtr> _file_systems;
    std::mutex _file_system_mutex;

    // Current active packed file
    std::unordered_map<std::string, std::unique_ptr<PackedFileContext>> _current_packed_files;
    std::timed_mutex _current_packed_file_mutex;

    // Merge files ready for upload or being processed
    std::unordered_map<std::string, std::shared_ptr<PackedFileContext>> _uploading_packed_files;

    // Uploaded packed files (kept for some time for wait_write_done)
    std::unordered_map<std::string, std::shared_ptr<PackedFileContext>> _uploaded_packed_files;
    std::mutex _packed_files_mutex;

    // Global index mapping small file path to packed file index
    std::unordered_map<std::string, PackedSliceLocation> _global_slice_locations;
    std::mutex _global_index_mutex;

#ifdef BE_TEST
public:
    void reset_packed_file_bvars_for_test() const;
    int64_t packed_file_total_count_for_test() const;
    int64_t packed_file_total_small_file_num_for_test() const;
    int64_t packed_file_total_size_bytes_for_test() const;
    double packed_file_avg_small_file_num_for_test() const;
    double packed_file_avg_file_size_for_test() const;
    void record_packed_file_metrics_for_test(const PackedFileContext* packed_file);

    // Test-only helpers to introspect/clear internal state
    void clear_state_for_test();
    auto& current_packed_files_for_test() { return _current_packed_files; }
    auto& uploading_packed_files_for_test() { return _uploading_packed_files; }
    auto& uploaded_packed_files_for_test() { return _uploaded_packed_files; }
    auto& global_slice_locations_for_test() { return _global_slice_locations; }
    auto& file_systems_for_test() { return _file_systems; }
    FileSystemSPtr& default_file_system_for_test() { return _default_file_system; }
    Status create_new_packed_file_state_for_test(const std::string& resource_id,
                                                 std::unique_ptr<PackedFileContext>& ctx) {
        return create_new_packed_file_context(resource_id, ctx);
    }
#endif
};

} // namespace doris::io
