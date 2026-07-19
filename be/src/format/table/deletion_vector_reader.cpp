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

#include "format/table/deletion_vector_reader.h"

#include <limits>

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "util/block_compression.h"

namespace doris {
namespace {

constexpr int64_t ICEBERG_DELETION_VECTOR_MIN_BYTES =
        static_cast<int64_t>(ICEBERG_DELETION_VECTOR_BLOB_OVERHEAD_BYTES);
constexpr int64_t PAIMON_DELETION_VECTOR_MIN_BYTES = 8;
constexpr int64_t PAIMON_LENGTH_PREFIX_BYTES = 4;

enum class DeletionVectorSizeLimitStatus {
    DATA_QUALITY,
    NOT_SUPPORTED,
};

Status validate_deletion_vector_read_range(const char* description, int64_t offset, int64_t size,
                                           int64_t min_size, int64_t max_size,
                                           DeletionVectorSizeLimitStatus size_limit_status,
                                           size_t& bytes_read) {
    if (offset < 0) {
        return Status::DataQualityError("{} offset must be non-negative: {}", description, offset);
    }
    if (size < min_size) {
        return Status::DataQualityError("{} size too small: {}, minimum: {}", description, size,
                                        min_size);
    }
    if (size > max_size) {
        if (size_limit_status == DeletionVectorSizeLimitStatus::NOT_SUPPORTED) {
            return Status::NotSupported("{} size exceeds Doris supported limit: {}, limit: {}",
                                        description, size, max_size);
        }
        return Status::DataQualityError("{} size exceeds limit: {}, limit: {}", description, size,
                                        max_size);
    }
    if (offset > std::numeric_limits<int64_t>::max() - size) {
        return Status::DataQualityError("{} offset plus size overflows: offset {}, size {}",
                                        description, offset, size);
    }
    bytes_read = static_cast<size_t>(size);
    return Status::OK();
}

} // namespace

Status validate_iceberg_deletion_vector_read_range(int64_t offset, int64_t size,
                                                   size_t& bytes_read) {
    return validate_deletion_vector_read_range(
            "Iceberg deletion vector", offset, size, ICEBERG_DELETION_VECTOR_MIN_BYTES,
            MAX_ICEBERG_DELETION_VECTOR_BYTES, DeletionVectorSizeLimitStatus::NOT_SUPPORTED,
            bytes_read);
}

Status validate_paimon_deletion_vector_read_range(int64_t offset, int64_t length,
                                                  size_t& bytes_read) {
    if (length < 0) {
        return Status::DataQualityError("Paimon deletion vector length must be non-negative: {}",
                                        length);
    }
    if (length > std::numeric_limits<int64_t>::max() - PAIMON_LENGTH_PREFIX_BYTES) {
        return Status::DataQualityError("Paimon deletion vector length overflows: {}", length);
    }
    return validate_deletion_vector_read_range(
            "Paimon deletion vector", offset, length + PAIMON_LENGTH_PREFIX_BYTES,
            PAIMON_DELETION_VECTOR_MIN_BYTES, MAX_PAIMON_DELETION_VECTOR_BYTES,
            DeletionVectorSizeLimitStatus::DATA_QUALITY, bytes_read);
}

DeletionVectorReader::~DeletionVectorReader() {
    // The file reader may retain the child IOContext. Destroy it before merging and before the
    // child statistics storage goes away.
    _file_reader.reset();
    _merge_io_statistics();
}

void DeletionVectorReader::_init_io_context() {
    if (_parent_io_ctx == nullptr) {
        return;
    }
    _reader_io_ctx = *_parent_io_ctx;
    _reader_io_ctx.file_cache_stats = &_file_cache_stats;
    _reader_io_ctx.file_reader_stats = &_file_reader_stats;
    _io_ctx = &_reader_io_ctx;
}

void DeletionVectorReader::_merge_io_statistics() {
    if (_statistics_merged || _parent_io_ctx == nullptr) {
        return;
    }
    if (_parent_io_ctx->file_cache_stats != nullptr) {
        _parent_io_ctx->file_cache_stats->merge_from(_file_cache_stats);
    }
    if (_parent_io_ctx->file_reader_stats != nullptr) {
        _parent_io_ctx->file_reader_stats->read_calls += _file_reader_stats.read_calls;
        _parent_io_ctx->file_reader_stats->read_bytes += _file_reader_stats.read_bytes;
        _parent_io_ctx->file_reader_stats->read_time_ns += _file_reader_stats.read_time_ns;
        _parent_io_ctx->file_reader_stats->read_rows += _file_reader_stats.read_rows;
    }
    _statistics_merged = true;
}

Status DeletionVectorReader::open() {
    if (_is_opened) [[unlikely]] {
        return Status::OK();
    }

    if (_desc.start_offset < 0 || _desc.size < 0) {
        return Status::DataQualityError(
                "Deletion vector range must be non-negative: path {}, offset {}, size {}",
                _desc.path, _desc.start_offset, _desc.size);
    }

    _init_system_properties();
    _init_file_description();
    RETURN_IF_ERROR(_create_file_reader());

    const size_t file_size = _file_reader->size();
    const size_t start_offset = static_cast<size_t>(_desc.start_offset);
    const size_t range_size = static_cast<size_t>(_desc.size);
    if (start_offset > file_size || range_size > file_size - start_offset) {
        return Status::DataQualityError(
                "Deletion vector range exceeds file size: path {}, offset {}, size {}, file size "
                "{}",
                _desc.path, _desc.start_offset, _desc.size, file_size);
    }
    _is_opened = true;
    return Status::OK();
}

Status DeletionVectorReader::read_at(size_t offset, Slice result) {
    if (UNLIKELY(_parent_io_ctx && _parent_io_ctx->should_stop)) {
        return Status::EndOfFile("stop read.");
    }
    if (_io_ctx != nullptr) {
        _io_ctx->should_stop = _parent_io_ctx->should_stop;
    }
    size_t bytes_read = 0;
    RETURN_IF_ERROR(_file_reader->read_at(offset, result, &bytes_read, _io_ctx));
    if (bytes_read != result.size) [[unlikely]] {
        return Status::IOError("Failed to read fully at offset {}, expected {}, got {}", offset,
                               result.size, bytes_read);
    }
    return Status::OK();
}

Status DeletionVectorReader::_create_file_reader() {
    if (UNLIKELY(_parent_io_ctx && _parent_io_ctx->should_stop)) {
        return Status::EndOfFile("stop read.");
    }

    _file_description.mtime = _desc.modification_time;
    // Keep DV blobs on the normal remote-file path. When file cache is enabled this creates a
    // CachedRemoteFileReader, so a Puffin/delete-file range is persisted in the disk File Cache;
    // the query-local decoded cache above it only owns the Roaring bitmap.
    io::FileReaderOptions reader_options =
            FileFactory::get_reader_options(_state->query_options(), _file_description);
    _file_reader = DORIS_TRY(io::DelegateReader::create_file_reader(
            _profile, _system_properties, _file_description, reader_options,
            io::DelegateReader::AccessMode::RANDOM, _io_ctx));
    return Status::OK();
}

void DeletionVectorReader::_init_file_description() {
    _file_description.path = _desc.path;
    _file_description.file_size = _desc.file_size;
    _file_description.fs_name = _desc.fs_name;
}

void DeletionVectorReader::_init_system_properties() {
    _system_properties.system_type = _params.file_type;
    _system_properties.properties = _params.properties;
    _system_properties.hdfs_params = _params.hdfs_params;
    if (_params.__isset.broker_addresses) {
        _system_properties.broker_addresses.assign(_params.broker_addresses.begin(),
                                                   _params.broker_addresses.end());
    }
}

} // namespace doris
