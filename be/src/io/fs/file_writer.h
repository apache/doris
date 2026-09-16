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

#include <atomic>
#include <functional>
#include <future>
#include <memory>

#include "common/status.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris::io {
class FileSystem;
struct FileCacheAllocatorBuilder;
struct EncryptionInfo;

// Request statistics reported by remote file writers when the caller passes an instance
// through FileWriterOptions::remote_write_stats. All fields are cumulative.
struct RemoteWriteStats {
    std::atomic<int64_t> put_object_requests {0};
    std::atomic<int64_t> create_multipart_requests {0};
    std::atomic<int64_t> upload_part_requests {0};
    std::atomic<int64_t> complete_multipart_requests {0};
    std::atomic<int64_t> head_requests {0};
    std::atomic<int64_t> failed_requests {0};
    // Bytes acknowledged by object storage (PutObject and UploadPart payloads).
    std::atomic<int64_t> uploaded_bytes {0};
    // Sum of request latencies. Requests run concurrently, so this is not wall-clock time.
    std::atomic<int64_t> request_time_ns {0};

    int64_t total_requests() const {
        return put_object_requests + create_multipart_requests + upload_part_requests +
               complete_multipart_requests + head_requests;
    }
};

// Only affects remote file writers
struct FileWriterOptions {
    // S3 committer will start multipart uploading all files on BE side,
    // and then complete multipart upload these files on FE side.
    // If you do not complete multi parts of a file, the file will not be visible.
    // So in this way, the atomicity of a single file can be guaranteed. But it still cannot
    // guarantee the atomicity of multiple files.
    // Because hive committers have best-effort semantics,
    // this shortens the inconsistent time window.
    bool used_by_s3_committer = false;
    bool write_file_cache = false;
    bool allow_adaptive_file_cache_write = true;
    bool is_cold_data = false;
    bool sync_file_data = true;              // Whether flush data into storage system
    uint64_t file_cache_expiration_time = 0; // Absolute time, 0 means no TTL
    uint64_t approximate_bytes_to_write = 0; // Approximate bytes to write, used for file cache
    // Upload flow control, honoured by S3FileWriter only (other writers ignore both hooks).
    //
    // upload_submit_gate is called on the appending thread (appendv, or close for the last
    // buffer) right before a data buffer is submitted for upload, with the allocated capacity
    // of the buffer (s3_write_buffer_size, also for a partially filled last buffer) so that a
    // budget built on it bounds memory, not payload. It may block. A non-OK status fails the
    // writer: the buffer is dropped, no further data is accepted and close() reports the error.
    //
    // upload_done_callback is called exactly once, with the same capacity, for every buffer
    // that passed the gate, when
    // the upload of that buffer has finished (success, provider error, or skipped because an
    // earlier buffer failed) and also when its submission failed. It runs on the upload thread
    // strictly before the buffer's status is published, so it always happens before the writer
    // reports a final close status or is destroyed. It must not block and must not touch the
    // FileWriter. Buffers that fail before being submitted (e.g. a checksum mismatch detected
    // inside the upload buffer) do not call back; callers that need exact accounting reconcile
    // after the writer reached its final state.
    std::function<Status(size_t)> upload_submit_gate = nullptr;
    std::function<void(size_t)> upload_done_callback = nullptr;
    // Optional sink for per-request statistics of remote file writers.
    std::shared_ptr<RemoteWriteStats> remote_write_stats = nullptr;
};

struct AsyncCloseStatusPack {
    std::promise<Status> promise;
    std::future<Status> future;
};

class FileWriter {
public:
    enum class State : uint8_t {
        OPENED = 0,
        ASYNC_CLOSING,
        CLOSED,
    };
    FileWriter() = default;
    virtual ~FileWriter() = default;

    FileWriter(const FileWriter&) = delete;
    const FileWriter& operator=(const FileWriter&) = delete;

    // Normal close. Wait for all data to persist before returning.
    // If there is no data appended, an empty file will be persisted.
    virtual Status close(bool non_block = false) = 0;

    // Non-blocking probe for a previous close(true).
    // OK means close finished successfully. NeedSendAgain means close is still running.
    // Other errors mean close finished with error or the writer does not support this API.
    // NOTE: This method consumes the async close result when it is ready. The caller must
    // use it as the only completion path for that async close; mixing it with close(false)
    // or another try_finish_close consumer is not supported.
    virtual Status try_finish_close() {
        return Status::NotSupported("try_finish_close is not supported");
    }

    Status append(const Slice& data) { return appendv(&data, 1); }

    virtual Status appendv(const Slice* data, size_t data_cnt) = 0;

    virtual const Path& path() const = 0;

    virtual size_t bytes_appended() const = 0;

    virtual State state() const = 0;

    // Returns true if this file's data was written to a packed file.
    // Used to determine whether to collect packed slice location from PackedFileManager.
    virtual bool is_in_packed_file() const { return false; }

    FileCacheAllocatorBuilder* cache_builder() const {
        return _cache_builder == nullptr ? nullptr : _cache_builder.get();
    }

protected:
    void init_cache_builder(const FileWriterOptions* opts, const Path& path) {
        if (!config::enable_file_cache || opts == nullptr) {
            return;
        }

        io::UInt128Wrapper path_hash = BlockFileCache::hash(path.filename().native());
        BlockFileCache* file_cache_ptr = FileCacheFactory::instance()->get_by_path(path_hash);

        bool has_enough_file_cache_space = opts->allow_adaptive_file_cache_write &&
                                           config::enable_file_cache_adaptive_write &&
                                           (opts->approximate_bytes_to_write > 0) &&
                                           (file_cache_ptr->approximate_available_cache_size() >
                                            opts->approximate_bytes_to_write);

        VLOG_DEBUG << "path:" << path.filename().native()
                   << ", write_file_cache:" << opts->write_file_cache
                   << ", allow_adaptive_file_cache_write:" << opts->allow_adaptive_file_cache_write
                   << ", has_enough_file_cache_space:" << has_enough_file_cache_space
                   << ", approximate_bytes_to_write:" << opts->approximate_bytes_to_write
                   << ", file_cache_available_size:"
                   << file_cache_ptr->approximate_available_cache_size();
        if (opts->write_file_cache || has_enough_file_cache_space) {
            _cache_builder = std::make_unique<FileCacheAllocatorBuilder>(FileCacheAllocatorBuilder {
                    opts ? opts->is_cold_data : false, opts ? opts->file_cache_expiration_time : 0,
                    path_hash, file_cache_ptr});
        }
        return;
    }

    std::unique_ptr<FileCacheAllocatorBuilder> _cache_builder =
            nullptr; // nullptr if disable write file cache
};

} // namespace doris::io
