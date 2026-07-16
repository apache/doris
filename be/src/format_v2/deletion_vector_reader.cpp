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

#include "format_v2/deletion_vector_reader.h"

#include <fmt/format.h>

#include <cstring>
#include <stdexcept>

#include "exec/common/endian.h"

namespace doris::format {

std::string build_iceberg_deletion_vector_cache_key(const std::string& data_file_path,
                                                    const TIcebergDeleteFileDesc& delete_file) {
    return fmt::format("delete_dv_{}:{}{}:{}#{}#{}", data_file_path.size(), data_file_path,
                       delete_file.path.size(), delete_file.path, delete_file.content_offset,
                       delete_file.content_size_in_bytes);
}

Status decode_iceberg_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                             DeletionVector* rows_to_delete) {
    if (buf == nullptr || rows_to_delete == nullptr) {
        return Status::InvalidArgument("invalid deletion vector decode arguments");
    }
    if (buffer_size < 12) {
        return Status::DataQualityError("Deletion vector file size too small: {}", buffer_size);
    }

    const auto total_length = BigEndian::Load32(buf);
    if (total_length + 8 != buffer_size) {
        return Status::DataQualityError("Deletion vector length mismatch, expected: {}, actual: {}",
                                        total_length + 8, buffer_size);
    }

    constexpr static char MAGIC_NUMBER[] = {'\xD1', '\xD3', '\x39', '\x64'};
    if (std::memcmp(buf + sizeof(total_length), MAGIC_NUMBER, 4) != 0) {
        return Status::DataQualityError("Deletion vector magic number mismatch");
    }

    try {
        *rows_to_delete |= roaring::Roaring64Map::readSafe(buf + 8, buffer_size - 12);
    } catch (const std::runtime_error& e) {
        return Status::DataQualityError("Decode roaring bitmap failed, {}", e.what());
    }
    return Status::OK();
}

std::string build_paimon_deletion_vector_cache_key(const TPaimonDeletionFileDesc& deletion_file) {
    return fmt::format("paimon_dv_{}#{}#{}", deletion_file.path, deletion_file.offset,
                       deletion_file.length);
}

Status decode_paimon_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                            DeletionVector* deletion_vector) {
    if (deletion_vector == nullptr) {
        return Status::InvalidArgument("deletion_vector must not be null");
    }
    if (buffer_size < 8) [[unlikely]] {
        return Status::DataQualityError("Deletion vector file size too small: {}", buffer_size);
    }
    const uint32_t actual_length = BigEndian::Load32(buf);
    if (actual_length + 4 != buffer_size) [[unlikely]] {
        return Status::RuntimeError(
                "DeletionVector deserialize error: length not match, actual length: {}, expect "
                "length: {}",
                actual_length, buffer_size - 4);
    }
    constexpr char PAIMON_BITMAP_MAGIC[] = {'\x5E', '\x43', '\xF2', '\xD0'};
    if (std::memcmp(buf + sizeof(actual_length), PAIMON_BITMAP_MAGIC, 4) != 0) [[unlikely]] {
        return Status::RuntimeError("DeletionVector deserialize error: invalid magic number {}",
                                    BigEndian::Load32(buf + sizeof(actual_length)));
    }
    roaring::Roaring roaring_bitmap;
    try {
        roaring_bitmap = roaring::Roaring::readSafe(buf + 8, buffer_size - 8);
    } catch (const std::runtime_error& e) {
        return Status::RuntimeError(
                "DeletionVector deserialize error: failed to deserialize roaring bitmap, {}",
                e.what());
    }
    *deletion_vector |= DeletionVector(std::move(roaring_bitmap));
    return Status::OK();
}

DeletionVectorReader::~DeletionVectorReader() {
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

    _init_system_properties();
    _init_file_description();
    RETURN_IF_ERROR(_create_file_reader());

    _file_size = _file_reader->size();
    _is_opened = true;
    return Status::OK();
}

Status DeletionVectorReader::read_at(size_t offset, Slice result) {
    if (UNLIKELY(_parent_io_ctx != nullptr && _parent_io_ctx->should_stop)) {
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
    if (UNLIKELY(_parent_io_ctx != nullptr && _parent_io_ctx->should_stop)) {
        return Status::EndOfFile("stop read.");
    }

    _file_description.mtime = _desc.modification_time;
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

} // namespace doris::format
