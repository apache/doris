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

#include "io/fs/hdfs_file_writer.h"

#include <fcntl.h>
#include <fmt/core.h>

#include <chrono>
#include <filesystem>
#include <ostream>
#include <string>
#include <thread>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "common/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/err_utils.h"
#include "io/fs/hdfs_file_system.h"
#include "io/hdfs_util.h"
#include "service/backend_options.h"
#include "util/bvar_helper.h"
#include "util/jni-util.h"

namespace doris::io {

bvar::Adder<uint64_t> hdfs_file_writer_total("hdfs_file_writer_total_num");
bvar::Adder<uint64_t> hdfs_bytes_written_total("hdfs_file_writer_bytes_written");
bvar::Adder<uint64_t> hdfs_file_created_total("hdfs_file_writer_file_created");
bvar::Adder<uint64_t> hdfs_file_being_written("hdfs_file_writer_file_being_written");

class HdfsMaxConnectionLimiter {
public:
    HdfsMaxConnectionLimiter() = default;
    ~HdfsMaxConnectionLimiter() = default;

    void add_inflight_writer() {
        std::unique_lock lck {_latch};
        _cv.wait(lck, [&]() {
            return _cur_inflight_writer < config::max_inflight_hdfs_write_connection;
        });
        _cur_inflight_writer++;
    }

    void reduce_inflight_writer() {
        std::unique_lock lck {_latch};
        _cur_inflight_writer--;
        _cv.notify_one();
    }

private:
    std::mutex _latch;
    std::condition_variable _cv;
    int64_t _cur_inflight_writer {0};
};

static constexpr size_t MB = 1024 * 1024;
static constexpr size_t CLIENT_WRITE_PACKET_SIZE = 64 * 1024; // 64 KB

// In practice, we've found that if the import frequency to HDFS is too fast,
// it can cause an OutOfMemoryError (OOM) in the JVM started by the JNI.
// For this, we should have a method to monitor how much JVM memory is currently being used.
// The HdfsWriteMemUsageRecorder class increments a recorded value during hdfsWrite when writing to HDFS.
// The HDFS client will blockingly call hdfsHsync or hdfsCloseFile
// which ensures that the client's buffer is sent to the data node and returned with an acknowledgment before returning to the caller.
// HdfsWriteMemUsageRecorder would reduce the mem usage at that time.
// If the current usage exceeds the maximum set by the user, the current mem acquire would return failure.
// The caller could do sleep to wait for free memory.
class HdfsWriteMemUsageRecorder {
public:
    HdfsWriteMemUsageRecorder()
            : max_jvm_heap_size(JniUtil::get_max_jni_heap_memory_size()), cur_memory_comsuption(0) {
        LOG_INFO("the max jvm heap size is {}", max_jvm_heap_size);
    }
    ~HdfsWriteMemUsageRecorder() = default;
    size_t max_usage() const {
        return static_cast<size_t>(max_jvm_heap_size *
                                   config::max_hdfs_wirter_jni_heap_usage_ratio);
    }
    Status acquire_memory(size_t memory_size) {
#ifdef USE_LIBHDFS3
        return Status::OK();
#elif BE_TEST
        return Status::OK();
#else
        if (cur_memory_comsuption + memory_size > max_usage()) {
            return Status::InternalError("Run out of Jni jvm heap space, current limit size is {}",
                                         max_usage());
        }
        cur_memory_comsuption.fetch_add(memory_size);
        return Status::OK();
#endif
    }

    void release_memory(size_t memory_size) {
#ifdef USE_LIBHDFS3
        return;
#elif BE_TEST
#else
        cur_memory_comsuption.fetch_sub(memory_size);
#endif
    }

private:
    size_t max_jvm_heap_size;
    std::atomic_uint64_t cur_memory_comsuption;
};

static HdfsWriteMemUsageRecorder g_hdfs_write_rate_limiter;
static HdfsMaxConnectionLimiter g_hdfs_max_write_connection_limiter;

HdfsFileWriter::HdfsFileWriter(Path path, std::shared_ptr<HdfsHandler> handler, hdfsFile hdfs_file,
                               std::string fs_name, const FileWriterOptions* opts)
        : _path(std::move(path)),
          _hdfs_handler(std::move(handler)),
          _hdfs_file(hdfs_file),
          _fs_name(std::move(fs_name)),
          _sync_file_data(opts ? opts->sync_file_data : true),
          _batch_buffer(MB * config::hdfs_write_batch_buffer_size_mb) {
    if (config::enable_file_cache && opts != nullptr && opts->write_file_cache) {
        _cache_builder = std::make_unique<FileCacheAllocatorBuilder>(FileCacheAllocatorBuilder {
                opts ? opts->is_cold_data : false, opts ? opts->file_cache_expiration : 0,
                BlockFileCache::hash(_path.filename().native()),
                FileCacheFactory::instance()->get_by_path(
                        BlockFileCache::hash(_path.filename().native()))});
    }
    hdfs_file_writer_total << 1;
    hdfs_file_being_written << 1;

    TEST_SYNC_POINT("HdfsFileWriter");
}

HdfsFileWriter::~HdfsFileWriter() {
    if (_hdfs_file) {
        SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_close_latency);
        hdfsCloseFile(_hdfs_handler->hdfs_fs, _hdfs_file);
        g_hdfs_max_write_connection_limiter.reduce_inflight_writer();
        _flush_and_reset_approximate_jni_buffer_size();
    }

    hdfs_file_being_written << -1;
}

void HdfsFileWriter::_flush_and_reset_approximate_jni_buffer_size() {
    g_hdfs_write_rate_limiter.release_memory(_approximate_jni_buffer_size);
    _approximate_jni_buffer_size = 0;
}

Status HdfsFileWriter::_acquire_jni_memory(size_t size) {
#ifdef USE_LIBHDFS3
    return Status::OK();
#endif
    size_t actual_size = std::max(CLIENT_WRITE_PACKET_SIZE, size);
    if (auto st = g_hdfs_write_rate_limiter.acquire_memory(actual_size); !st.ok()) {
        auto ret = SYNC_POINT_HOOK_RETURN_VALUE(hdfsHSync(_hdfs_handler->hdfs_fs, _hdfs_file),
                                                "HdfsFileWriter::close::hdfsHSync");
        _flush_and_reset_approximate_jni_buffer_size();
        if (ret != 0) {
            return Status::InternalError(
                    "Write hdfs file failed. (BE: {}) namenode:{}, path:{}, err: {}, file_size={}",
                    BackendOptions::get_localhost(), _fs_name, _path.native(), hdfs_error(),
                    bytes_appended());
        }
        // Other hdfs writers might have occupied too much memory, we need to sleep for a while to wait for them
        // releasing their memory
        for (int i = 0; i < config::hdfs_jni_write_max_retry_time; i++) {
            if (g_hdfs_write_rate_limiter.acquire_memory(actual_size).ok()) {
                _approximate_jni_buffer_size += actual_size;
                return Status::OK();
            }
            std::this_thread::sleep_for(
                    std::chrono::milliseconds(config::hdfs_jni_write_sleep_milliseconds));
        }
        return st;
    }

    _approximate_jni_buffer_size += actual_size;
    return Status::OK();
}

Status HdfsFileWriter::close() {
    if (_closed) {
        return Status::OK();
    }
    _closed = true;
    if (_batch_buffer.size() != 0) {
        RETURN_IF_ERROR(_flush_buffer());
    }
    int ret;
    if (_sync_file_data) {
        {
            SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_hsync_latency);
#ifdef USE_LIBHDFS3
            ret = SYNC_POINT_HOOK_RETURN_VALUE(hdfsSync(_hdfs_handler->hdfs_fs, _hdfs_file),
                                               "HdfsFileWriter::close::hdfsHSync");
#else
            ret = SYNC_POINT_HOOK_RETURN_VALUE(hdfsHSync(_hdfs_handler->hdfs_fs, _hdfs_file),
                                               "HdfsFileWriter::close::hdfsHSync");
            _flush_and_reset_approximate_jni_buffer_size();
#endif
        }
        TEST_INJECTION_POINT_RETURN_WITH_VALUE("HdfsFileWriter::hdfsSync",
                                               Status::InternalError("failed to sync hdfs file"));

        if (ret != 0) {
            return Status::InternalError(
                    "failed to sync hdfs file. fs_name={} path={} : {}, file_size={}", _fs_name,
                    _path.native(), hdfs_error(), bytes_appended());
        }
    }

    {
        SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_close_latency);
        // The underlying implementation will invoke `hdfsHFlush` to flush buffered data and wait for
        // the HDFS response, but won't guarantee the synchronization of data to HDFS.
        ret = SYNC_POINT_HOOK_RETURN_VALUE(hdfsCloseFile(_hdfs_handler->hdfs_fs, _hdfs_file),
                                           "HdfsFileWriter::close::hdfsCloseFile");
        _flush_and_reset_approximate_jni_buffer_size();
    }
    _hdfs_file = nullptr;
    TEST_INJECTION_POINT_RETURN_WITH_VALUE("HdfsFileWriter::hdfsCloseFile",
                                           Status::InternalError("failed to close hdfs file"));
    if (ret != 0) {
        return Status::InternalError(
                "Write hdfs file failed. (BE: {}) namenode:{}, path:{}, err: {}, file_size={}",
                BackendOptions::get_localhost(), _fs_name, _path.native(), hdfs_error(),
                bytes_appended());
    }
    hdfs_file_created_total << 1;
    return Status::OK();
}

HdfsFileWriter::BatchBuffer::BatchBuffer(size_t capacity) {
    _batch_buffer.reserve(capacity);
}

bool HdfsFileWriter::BatchBuffer::full() const {
    return size() == capacity();
}

const char* HdfsFileWriter::BatchBuffer::data() const {
    return _batch_buffer.data();
}

size_t HdfsFileWriter::BatchBuffer::capacity() const {
    return _batch_buffer.capacity();
}

size_t HdfsFileWriter::BatchBuffer::size() const {
    return _batch_buffer.size();
}

void HdfsFileWriter::BatchBuffer::clear() {
    _batch_buffer.clear();
}

// TODO(ByteYue): Refactor Upload Buffer to reduce this duplicate code
void HdfsFileWriter::_write_into_local_file_cache() {
    auto holder = _cache_builder->allocate_cache_holder(_bytes_appended - _batch_buffer.size(),
                                                        _batch_buffer.capacity());
    size_t pos = 0;
    size_t data_remain_size = _batch_buffer.size();
    for (auto& block : holder->file_blocks) {
        if (data_remain_size == 0) {
            break;
        }
        size_t block_size = block->range().size();
        size_t append_size = std::min(data_remain_size, block_size);
        if (block->state() == FileBlock::State::EMPTY) {
            block->get_or_set_downloader();
            if (block->is_downloader()) {
                Slice s(_batch_buffer.data() + pos, append_size);
                Status st = block->append(s);
                if (st.ok()) {
                    st = block->finalize();
                }
                if (!st.ok()) {
                    LOG_WARNING("failed to append data to file cache").error(st);
                }
            }
        }
        data_remain_size -= append_size;
        pos += append_size;
    }
}

Status HdfsFileWriter::append_hdfs_file(std::string_view content) {
    RETURN_IF_ERROR(_acquire_jni_memory(content.size()));
    while (!content.empty()) {
        int64_t written_bytes;
        {
            TEST_INJECTION_POINT_CALLBACK("HdfsFileWriter::append_hdfs_file_delay");
            SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_write_latency);
            written_bytes = SYNC_POINT_HOOK_RETURN_VALUE(
                    hdfsWrite(_hdfs_handler->hdfs_fs, _hdfs_file, content.data(), content.size()),
                    "HdfsFileWriter::append_hdfs_file::hdfsWrite", content);
            {
                TEST_INJECTION_POINT_RETURN_WITH_VALUE(
                        "HdfsFileWriter::append_hdfs_file_error",
                        Status::InternalError(
                                "write hdfs failed. fs_name: {}, path: {}, error: inject error",
                                _fs_name, _path.native()));
            }
        }
        if (written_bytes < 0) {
            return Status::InternalError(
                    "write hdfs failed. fs_name: {}, path: {}, error: {}, file_size={}", _fs_name,
                    _path.native(), hdfs_error(), bytes_appended());
        }
        hdfs_bytes_written_total << written_bytes;
        content.remove_prefix(written_bytes);
    }
    return Status::OK();
}

Status HdfsFileWriter::_flush_buffer() {
    RETURN_IF_ERROR(append_hdfs_file(_batch_buffer.content()));
    if (_cache_builder != nullptr) {
        _write_into_local_file_cache();
    }
    _batch_buffer.clear();
    return Status::OK();
}

size_t HdfsFileWriter::BatchBuffer::append(std::string_view content) {
    size_t append_size = std::min(capacity() - size(), content.size());
    _batch_buffer.append(content.data(), append_size);
    return append_size;
}

std::string_view HdfsFileWriter::BatchBuffer::content() const {
    return _batch_buffer;
}

Status HdfsFileWriter::_append(std::string_view content) {
    while (!content.empty()) {
        if (_batch_buffer.full()) {
            auto error_msg = fmt::format("invalid batch buffer status, capacity {}, size {}",
                                         _batch_buffer.capacity(), _batch_buffer.size());
            DCHECK(false) << error_msg;
            return Status::InternalError(error_msg);
        }
        size_t append_size = _batch_buffer.append(content);
        content.remove_prefix(append_size);
        _bytes_appended += append_size;
        if (_batch_buffer.full()) {
            RETURN_IF_ERROR(_flush_buffer());
        }
    }
    return Status::OK();
}

Status HdfsFileWriter::appendv(const Slice* data, size_t data_cnt) {
    if (_closed) [[unlikely]] {
        return Status::InternalError("append to closed file: {}", _path.native());
    }

    for (size_t i = 0; i < data_cnt; i++) {
        RETURN_IF_ERROR(_append({data[i].get_data(), data[i].get_size()}));
    }
    return Status::OK();
}

// Call this method when there is no more data to write.
Status HdfsFileWriter::finalize() {
    if (_closed) [[unlikely]] {
        return Status::InternalError("finalize closed file: {}, file_size={}", _path.native(),
                                     bytes_appended());
    }
    if (_batch_buffer.size() != 0) {
        RETURN_IF_ERROR(_flush_buffer());
    }

    // Flush buffered data to HDFS without waiting for HDFS response
    int ret = SYNC_POINT_HOOK_RETURN_VALUE(hdfsFlush(_hdfs_handler->hdfs_fs, _hdfs_file),
                                           "HdfsFileWriter::finalize::hdfsFlush");
    TEST_INJECTION_POINT_RETURN_WITH_VALUE("HdfsFileWriter::hdfsFlush",
                                           Status::InternalError("failed to flush hdfs file"));
    if (ret != 0) {
        return Status::InternalError(
                "failed to flush hdfs file. fs_name={} path={} : {}, file_size={}", _fs_name,
                _path.native(), hdfs_error(), bytes_appended());
    }

    return Status::OK();
}

Result<FileWriterPtr> HdfsFileWriter::create(Path full_path, std::shared_ptr<HdfsHandler> handler,
                                             const std::string& fs_name,
                                             const FileWriterOptions* opts) {
    auto path = convert_path(full_path, fs_name);
#ifdef USE_LIBHDFS3
    std::string hdfs_dir = path.parent_path().string();
    int exists = hdfsExists(handler->hdfs_fs, hdfs_dir.c_str());
    if (exists != 0) {
        VLOG_NOTICE << "hdfs dir doesn't exist, create it: " << hdfs_dir;
        int ret = hdfsCreateDirectory(handler->hdfs_fs, hdfs_dir.c_str());
        if (ret != 0) {
            std::stringstream ss;
            ss << "create dir failed. "
               << " fs_name: " << fs_name << " path: " << hdfs_dir << ", err: " << hdfs_error();
            LOG(WARNING) << ss.str();
            return ResultError(Status::InternalError(ss.str()));
        }
    }
#endif
    // open file
    hdfsFile hdfs_file = nullptr;
    g_hdfs_max_write_connection_limiter.add_inflight_writer();
    {
        SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_open_latency);
        hdfs_file = hdfsOpenFile(handler->hdfs_fs, path.c_str(), O_WRONLY, 0, 0, 0);
    }
    if (hdfs_file == nullptr) {
        std::stringstream ss;
        ss << "open file failed. "
           << " fs_name:" << fs_name << " path:" << path << ", err: " << hdfs_error();
        LOG(WARNING) << ss.str();
        return ResultError(Status::InternalError(ss.str()));
    }
    VLOG_NOTICE << "open file. fs_name:" << fs_name << ", path:" << path;
    return std::make_unique<HdfsFileWriter>(std::move(path), handler, hdfs_file, fs_name, opts);
}

} // namespace doris::io
