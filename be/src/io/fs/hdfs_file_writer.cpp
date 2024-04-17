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

#include <filesystem>
#include <ostream>
#include <string>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/err_utils.h"
#include "io/fs/hdfs_file_system.h"
#include "io/hdfs_util.h"
#include "service/backend_options.h"
#include "util/bvar_helper.h"

namespace doris::io {

bvar::Adder<uint64_t> hdfs_file_writer_total("hdfs_file_writer", "total_num");
bvar::Adder<uint64_t> hdfs_bytes_written_total("hdfs_file_writer", "bytes_written");
bvar::Adder<uint64_t> hdfs_file_created_total("hdfs_file_writer", "file_created");
bvar::Adder<uint64_t> hdfs_file_being_written("hdfs_file_writer", "file_being_written");

HdfsFileWriter::HdfsFileWriter(Path path, HdfsHandler* handler, hdfsFile hdfs_file,
                               std::string fs_name, const FileWriterOptions* opts)
        : _path(std::move(path)),
          _hdfs_handler(handler),
          _hdfs_file(hdfs_file),
          _fs_name(std::move(fs_name)),
          _sync_file_data(opts ? opts->sync_file_data : true),
          _batch_buffer(config::hdfs_write_batch_buffer_size) {
    if (config::enable_file_cache && (opts ? opts->write_file_cache : false)) {
        _batch_buffer._write_file_cache = true;
        _batch_buffer._expiration_time = opts ? opts->file_cache_expiration : 0;
        _batch_buffer._is_cold_data = opts ? opts->is_cold_data : false;
        _batch_buffer._cache_hash = BlockFileCache::hash(_path.filename().native());
        _batch_buffer._cache = FileCacheFactory::instance()->get_by_path(_batch_buffer._cache_hash);
    }
    hdfs_file_writer_total << 1;
    hdfs_file_being_written << 1;
}

HdfsFileWriter::~HdfsFileWriter() {
    if (_hdfs_file) {
        SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_close_latency);
        hdfsCloseFile(_hdfs_handler->hdfs_fs, _hdfs_file);
    }

    if (_hdfs_handler->from_cache) {
        _hdfs_handler->dec_ref();
    } else {
        delete _hdfs_handler;
    }
    hdfs_file_being_written << -1;
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
            ret = hdfsHSync(_hdfs_handler->hdfs_fs, _hdfs_file);
        }

        if (ret != 0) {
            return Status::InternalError("failed to sync hdfs file. fs_name={} path={} : {}",
                                         _fs_name, _path.native(), hdfs_error());
        }
    }

    {
        SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_flush_latency);
        // The underlying implementation will invoke `hdfsHFlush` to flush buffered data and wait for
        // the HDFS response, but won't guarantee the synchronization of data to HDFS.
        ret = hdfsCloseFile(_hdfs_handler->hdfs_fs, _hdfs_file);
    }
    _hdfs_file = nullptr;
    if (ret != 0) {
        return Status::InternalError(
                "Write hdfs file failed. (BE: {}) namenode:{}, path:{}, err: {}",
                BackendOptions::get_localhost(), _fs_name, _path.native(), hdfs_error());
    }
    hdfs_file_created_total << 1;
    return Status::OK();
}

HdfsFileWriter::CachedBatchBuffer::CachedBatchBuffer(size_t capacity) {
    _batch_buffer.reserve(capacity);
}

bool HdfsFileWriter::CachedBatchBuffer::full() const {
    return size() == capacity();
}

const char* HdfsFileWriter::CachedBatchBuffer::data() const {
    return _batch_buffer.data();
}

size_t HdfsFileWriter::CachedBatchBuffer::capacity() const {
    return _batch_buffer.capacity();
}

size_t HdfsFileWriter::CachedBatchBuffer::size() const {
    return _batch_buffer.size();
}

void HdfsFileWriter::CachedBatchBuffer::clear() {
    _batch_buffer.clear();
}

FileBlocksHolderPtr HdfsFileWriter::CachedBatchBuffer::allocate_cache_holder(size_t offset) {
    CacheContext ctx;
    ctx.cache_type = _expiration_time == 0 ? FileCacheType::NORMAL : FileCacheType::TTL;
    ctx.expiration_time = _expiration_time;
    ctx.is_cold_data = _is_cold_data;
    auto holder = _cache->get_or_set(_cache_hash, offset, capacity(), ctx);
    return std::make_unique<FileBlocksHolder>(std::move(holder));
}

// TODO(ByteYue): Refactor Upload Buffer to reduce this duplicate code
void HdfsFileWriter::_write_into_local_file_cache() {
    auto _holder = _batch_buffer.allocate_cache_holder(_bytes_appended - _batch_buffer.size());
    size_t pos = 0;
    size_t data_remain_size = _batch_buffer.size();
    for (auto& block : _holder->file_blocks) {
        if (data_remain_size == 0) {
            break;
        }
        size_t block_size = block->range().size();
        size_t append_size = std::min(data_remain_size, block_size);
        if (block->state() == FileBlock::State::EMPTY) {
            if (_index_offset != 0 && block->range().right >= _index_offset) {
                static_cast<void>(block->change_cache_type_self(FileCacheType::INDEX));
            }
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
    while (!content.empty()) {
        int64_t written_bytes;
        {
            SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_write_latency);
            written_bytes =
                    hdfsWrite(_hdfs_handler->hdfs_fs, _hdfs_file, content.data(), content.size());
        }
        if (written_bytes < 0) {
            return Status::InternalError("write hdfs failed. fs_name: {}, path: {}, error: {}",
                                         _fs_name, _path.native(), hdfs_error());
        }
        hdfs_bytes_written_total << written_bytes;
        content.remove_prefix(written_bytes);
    }
    return Status::OK();
}

Status HdfsFileWriter::_flush_buffer() {
    RETURN_IF_ERROR(append_hdfs_file(_batch_buffer._batch_buffer));
    if (_batch_buffer._write_file_cache) {
        _write_into_local_file_cache();
    }
    _batch_buffer.clear();
    return Status::OK();
}

size_t HdfsFileWriter::CachedBatchBuffer::append(std::string_view content) {
    size_t append_size = std::min(capacity() - size(), content.size());
    _batch_buffer.append(content.substr(0, append_size));
    return append_size;
}

Status HdfsFileWriter::_append(std::string_view content) {
    while (!content.empty()) {
        if (_batch_buffer.full()) {
            DCHECK(false) << "invalid batch buffer status";
            return Status::InternalError("invalid batch buffer status");
        }
        size_t append_size = _batch_buffer.append(content);
        content.remove_prefix(append_size);
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
        _bytes_appended += data[i].get_size();
    }
    return Status::OK();
}

// Call this method when there is no more data to write.
Status HdfsFileWriter::finalize() {
    if (_closed) [[unlikely]] {
        return Status::InternalError("finalize closed file: {}", _path.native());
    }
    if (_batch_buffer.size() != 0) {
        RETURN_IF_ERROR(_flush_buffer());
    }

    // Flush buffered data to HDFS without waiting for HDFS response
    int ret = hdfsFlush(_hdfs_handler->hdfs_fs, _hdfs_file);
    if (ret != 0) {
        return Status::InternalError("failed to flush hdfs file. fs_name={} path={} : {}", _fs_name,
                                     _path.native(), hdfs_error());
    }

    return Status::OK();
}

Result<FileWriterPtr> HdfsFileWriter::create(Path full_path, HdfsHandler* handler,
                                             const std::string& fs_name,
                                             const FileWriterOptions* opts) {
    auto path = convert_path(full_path, fs_name);
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
    return std::make_unique<HdfsFileWriter>(std::move(path), handler, hdfs_file, fs_name, opts);
}

} // namespace doris::io
