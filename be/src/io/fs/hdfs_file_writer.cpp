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
#include <random>
#include <string>
#include <thread>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/err_utils.h"
#include "io/fs/file_writer.h"
#include "io/fs/hdfs_file_system.h"
#include "io/hdfs_util.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "util/bvar_helper.h"
#include "util/jni-util.h"

namespace doris::io {

bvar::Adder<uint64_t> hdfs_file_writer_total("hdfs_file_writer_total_num");
bvar::Adder<uint64_t> hdfs_bytes_written_total("hdfs_file_writer_bytes_written");
bvar::Adder<uint64_t> hdfs_file_created_total("hdfs_file_writer_file_created");
bvar::Adder<uint64_t> inflight_hdfs_file_writer("inflight_hdfs_file_writer");

static constexpr size_t MB = 1024 * 1024;
#ifndef USE_LIBHDFS3
static constexpr size_t CLIENT_WRITE_PACKET_SIZE = 64 * 1024; // 64 KB
#endif

inline std::default_random_engine make_random_engine() {
    return std::default_random_engine(
            static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
}

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
    HdfsWriteMemUsageRecorder() = default;
    ~HdfsWriteMemUsageRecorder() = default;
    size_t max_usage() const {
        return static_cast<size_t>(max_jvm_heap_size() *
                                   config::max_hdfs_wirter_jni_heap_usage_ratio);
    }
    Status acquire_memory(size_t memory_size, int try_time) {
#if defined(USE_LIBHDFS3) || defined(BE_TEST)
        return Status::OK();
#else
        if (!config::enable_hdfs_mem_limiter) {
            return Status::OK();
        }
        auto unit = config::hdfs_jni_write_sleep_milliseconds;
        std::default_random_engine rng = make_random_engine();
        std::uniform_int_distribution<uint32_t> u(unit, 2 * unit);
        std::uniform_int_distribution<uint32_t> u2(2 * unit, 4 * unit);
        auto duration_ms =
                try_time < (config::hdfs_jni_write_max_retry_time / 2) ? u(rng) : u2(rng);
        std::unique_lock lck {cur_memory_latch};
        cv.wait_for(lck, std::chrono::milliseconds(duration_ms),
                    [&]() { return cur_memory_comsuption + memory_size <= max_usage(); });
        if (cur_memory_comsuption + memory_size > max_usage()) {
            lck.unlock();
            return Status::InternalError<false>(
                    "Run out of Jni jvm heap space, current limit size is {}, max heap size is {}, "
                    "ratio is {}",
                    max_usage(), max_jvm_heap_size(), config::max_hdfs_wirter_jni_heap_usage_ratio);
        }
        cur_memory_comsuption += memory_size;
        return Status::OK();
#endif
    }

    void release_memory(size_t memory_size) {
#if defined(USE_LIBHDFS3) || defined(BE_TEST)
#else
        if (!config::enable_hdfs_mem_limiter) {
            return;
        }
        std::unique_lock lck {cur_memory_latch};
        size_t origin_size = cur_memory_comsuption;
        cur_memory_comsuption -= memory_size;
        if (cur_memory_comsuption < max_usage() && origin_size > max_usage()) {
            cv.notify_all();
        }
#endif
    }

private:
    size_t max_jvm_heap_size() const { return JniUtil::get_max_jni_heap_memory_size(); }
    [[maybe_unused]] std::size_t cur_memory_comsuption {0};
    std::mutex cur_memory_latch;
    std::condition_variable cv;
};

static HdfsWriteMemUsageRecorder g_hdfs_write_rate_limiter;

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

    TEST_SYNC_POINT("HdfsFileWriter");
}

HdfsFileWriter::~HdfsFileWriter() {
    if (_async_close_pack != nullptr) {
        // For thread safety
        std::ignore = _async_close_pack->future.get();
        _async_close_pack = nullptr;
    }
    if (_hdfs_file) {
        SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_close_latency);
        hdfsCloseFile(_hdfs_handler->hdfs_fs, _hdfs_file);
        inflight_hdfs_file_writer << -1;
        _flush_and_reset_approximate_jni_buffer_size();
    }
}

void HdfsFileWriter::_flush_and_reset_approximate_jni_buffer_size() {
    g_hdfs_write_rate_limiter.release_memory(_approximate_jni_buffer_size);
    _approximate_jni_buffer_size = 0;
}

Status HdfsFileWriter::_acquire_jni_memory(size_t size) {
#ifdef USE_LIBHDFS3
    return Status::OK();
#else
    size_t actual_size = std::max(CLIENT_WRITE_PACKET_SIZE, size);
    int try_time = 0;
    if (auto st = g_hdfs_write_rate_limiter.acquire_memory(actual_size, try_time); !st.ok()) {
        if (_approximate_jni_buffer_size > 0) {
            int ret;
            {
                SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_hflush_latency);
                ret = SYNC_POINT_HOOK_RETURN_VALUE(hdfsHFlush(_hdfs_handler->hdfs_fs, _hdfs_file),
                                                   "HdfsFileWriter::close::hdfsHFlush");
            }
            _flush_and_reset_approximate_jni_buffer_size();
            if (ret != 0) {
                return Status::InternalError(
                        "Write hdfs file failed. (BE: {}) namenode:{}, path:{}, err: {}, "
                        "file_size={}",
                        BackendOptions::get_localhost(), _fs_name, _path.native(), hdfs_error(),
                        bytes_appended());
            }
        }
        // Other hdfs writers might have occupied too much memory, we need to sleep for a while to wait for them
        // releasing their memory
        for (; try_time < config::hdfs_jni_write_max_retry_time; try_time++) {
            if (g_hdfs_write_rate_limiter.acquire_memory(actual_size, try_time).ok()) {
                _approximate_jni_buffer_size += actual_size;
                return Status::OK();
            }
        }
        return st;
    }

    _approximate_jni_buffer_size += actual_size;
    return Status::OK();
#endif
}

Status HdfsFileWriter::close(bool non_block) {
    if (state() == State::CLOSED) {
        return Status::InternalError("HdfsFileWriter already closed, file path {}, fs name {}",
                                     _path.native(), _fs_name);
    }
    if (state() == State::ASYNC_CLOSING) {
        if (non_block) {
            return Status::InternalError("Don't submit async close multi times");
        }
        CHECK(_async_close_pack != nullptr);
        _st = _async_close_pack->future.get();
        _async_close_pack = nullptr;
        // We should wait for all the pre async task to be finished
        _state = State::CLOSED;
        // The next time we call close() with no matter non_block true or false, it would always return the
        // '_st' value because this writer is already closed.
        return _st;
    }
    if (non_block) {
        _state = State::ASYNC_CLOSING;
        _async_close_pack = std::make_unique<AsyncCloseStatusPack>();
        _async_close_pack->future = _async_close_pack->promise.get_future();
        return ExecEnv::GetInstance()->non_block_close_thread_pool()->submit_func(
                [&]() { _async_close_pack->promise.set_value(_close_impl()); });
    }
    _st = _close_impl();
    _state = State::CLOSED;
    return _st;
}

Status HdfsFileWriter::_close_impl() {
    if (_batch_buffer.size() != 0) {
        if (_st = _flush_buffer(); !_st.ok()) {
            return _st;
        }
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
            _st = Status::InternalError(
                    "failed to sync hdfs file. fs_name={} path={} : {}, file_size={}", _fs_name,
                    _path.native(), hdfs_error(), bytes_appended());
            return _st;
        }
    }

    {
        SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_close_latency);
        // The underlying implementation will invoke `hdfsHFlush` to flush buffered data and wait for
        // the HDFS response, but won't guarantee the synchronization of data to HDFS.
        ret = SYNC_POINT_HOOK_RETURN_VALUE(hdfsCloseFile(_hdfs_handler->hdfs_fs, _hdfs_file),
                                           "HdfsFileWriter::close::hdfsCloseFile");
        inflight_hdfs_file_writer << -1;
        _flush_and_reset_approximate_jni_buffer_size();
    }
    _hdfs_file = nullptr;
    TEST_INJECTION_POINT_RETURN_WITH_VALUE("HdfsFileWriter::hdfsCloseFile",
                                           Status::InternalError("failed to close hdfs file"));
    if (ret != 0) {
        _st = Status::InternalError(
                "Write hdfs file failed. (BE: {}) namenode:{}, path:{}, err: {}, file_size={}",
                BackendOptions::get_localhost(), _fs_name, _path.native(), hdfs_error(),
                bytes_appended());
        return _st;
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
    if (_state != State::OPENED) [[unlikely]] {
        return Status::InternalError("append to closed file: {}", _path.native());
    }

    for (size_t i = 0; i < data_cnt; i++) {
        RETURN_IF_ERROR(_append({data[i].get_data(), data[i].get_size()}));
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
    inflight_hdfs_file_writer << 1;
    return std::make_unique<HdfsFileWriter>(std::move(path), handler, hdfs_file, fs_name, opts);
}

} // namespace doris::io
