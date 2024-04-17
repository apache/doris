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
          _expiration_time(opts == nullptr ? 0 : opts->file_cache_expiration),
          _is_cold_data(opts == nullptr ? false : opts->is_cold_data),
          _write_file_cache(opts == nullptr ? false : opts->write_file_cache) {
    _batch_buffer.reserve(config::hdfs_write_batch_buffer_size);
    if (config::enable_file_cache && _write_file_cache) {
        _cache_hash = BlockFileCache::hash(_path.filename().native());
        _cache = FileCacheFactory::instance()->get_by_path(_cache_hash);
    }
    hdfs_file_writer_total << 1;
    hdfs_file_being_written << 1;
}

HdfsFileWriter::~HdfsFileWriter() {
    if (_hdfs_file) {
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
    if (_sync_file_data) {
        int ret = hdfsHSync(_hdfs_handler->hdfs_fs, _hdfs_file);
        if (ret != 0) {
            return Status::InternalError("failed to sync hdfs file. fs_name={} path={} : {}",
                                         _fs_name, _path.native(), hdfs_error());
        }
    }
    
    int ret;
    {
        SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_flush_latency);
        // The underlying implementation will invoke `hdfsHFlush` to flush buffered data and wait for
        // the HDFS response, but won't guarantee the synchronization of data to HDFS.
        result = hdfsFlush(_hdfs_handler->hdfs_fs, _hdfs_file);
    }
    if (ret == -1) {
        std::stringstream ss;
        ss << "failed to flush hdfs file. "
           << "fs_name:" << _fs_name << " path:" << _path << ", err: " << hdfs_error();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    {
        SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_close_latency);
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

static FileBlocksHolderPtr allocate_cache_holder(BlockFileCache* cache, UInt128Wrapper cache_hash,
                                                 size_t offset, uint64_t expiration_time,
                                                 bool is_cold) {
    CacheContext ctx;
    ctx.cache_type = expiration_time == 0 ? FileCacheType::NORMAL : FileCacheType::TTL;
    ctx.expiration_time = expiration_time;
    ctx.is_cold_data = is_cold;
    auto holder = cache->get_or_set(cache_hash, offset, config::hdfs_write_batch_buffer_size, ctx);
    return std::make_unique<FileBlocksHolder>(std::move(holder));
}

// TODO(ByteYue): Refactor Upload Buffer to reduce this duplicate code
void HdfsFileWriter::_write_into_local_file_cache() {
    auto _holder =
            allocate_cache_holder(_cache, _cache_hash, _bytes_appended - _batch_buffer.size(),
                                  _expiration_time, _is_cold_data);
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

Status HdfsFileWriter::_write_into_batch(Slice data) {
    while (!data.empty()) {
        size_t append_size = std::min(config::hdfs_write_batch_buffer_size - _batch_buffer.size(),
                                      data.get_size());
        std::string_view sv(data.get_data(), append_size);
        _batch_buffer.append(sv);
        data.remove_prefix(append_size);
        if (_batch_buffer.size() == config::hdfs_write_batch_buffer_size) {
            size_t left_bytes = config::hdfs_write_batch_buffer_size;
            const char* p = _batch_buffer.data();
            while (left_bytes > 0) {
                int64_t written_bytes;
                {
                    SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_write_latency);
                    written_bytes = hdfsWrite(_hdfs_handler->hdfs_fs, _hdfs_file, p, left_bytes);
                }
                if (written_bytes < 0) {
                    return Status::InternalError(
                            "write hdfs failed. fs_name: {}, path: {}, error: {}", _fs_name,
                            _path.native(), hdfs_error());
                }
                hdfs_bytes_written_total << written_bytes;
                left_bytes -= written_bytes;
                p += written_bytes;
                _bytes_appended += written_bytes;
            }
            _write_into_local_file_cache();
            _batch_buffer.clear();
        }
    }
    return Status::OK();
}

Status HdfsFileWriter::appendv(const Slice* data, size_t data_cnt) {
    if (_closed) [[unlikely]] {
        return Status::InternalError("append to closed file: {}", _path.native());
    }

    for (size_t i = 0; i < data_cnt; i++) {
        RETURN_IF_ERROR(_write_into_batch(data[i]));
    }
    return Status::OK();
}

// Call this method when there is no more data to write.
Status HdfsFileWriter::finalize() {
    if (_closed) [[unlikely]] {
        return Status::InternalError("finalize closed file: {}", _path.native());
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
    std::string hdfs_dir = path.parent_path().string();
    int exists = hdfsExists(handler->hdfs_fs, hdfs_dir.c_str());
    if (exists != 0) {
        // FIXME(plat1ko): Directly return error here?
        VLOG_NOTICE << "hdfs dir doesn't exist, create it: " << hdfs_dir;
        int ret;
        {
            SCOPED_BVAR_LATENCY(hdfs_bvar::hdfs_create_dir_latency);
            ret = hdfsCreateDirectory(handler->hdfs_fs, hdfs_dir.c_str());
        }
        if (ret != 0) {
            // TODO(plat1ko): Normalized error handling
            std::stringstream ss;
            ss << "create dir failed. "
               << " fs_name: " << fs_name << " path: " << hdfs_dir << ", err: " << hdfs_error();
            LOG(WARNING) << ss.str();
            return ResultError(Status::InternalError(ss.str()));
        }
    }
    // open file
    struct hdfsFile_internal* hdfs_file = nullptr;
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
