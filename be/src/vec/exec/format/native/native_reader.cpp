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

#include "vec/exec/format/native/native_reader.h"

#include <gen_cpp/data.pb.h>

#include "io/file_factory.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "io/fs/tracing_file_reader.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/format/native/native_format.h"

namespace doris::vectorized {

#include "common/compile_check_begin.h"

NativeReader::NativeReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                           const TFileRangeDesc& range, io::IOContext* io_ctx, RuntimeState* state)
        : _profile(profile),
          _scan_params(params),
          _scan_range(range),
          _io_ctx(io_ctx),
          _state(state) {}

NativeReader::~NativeReader() {
    (void)close();
}

namespace {

Status validate_and_consume_header(io::FileReaderSPtr file_reader, const TFileRangeDesc& range,
                                   int64_t* file_size, int64_t* current_offset, bool* eof) {
    *file_size = file_reader->size();
    *current_offset = 0;
    *eof = (*file_size == 0);

    // Validate and consume Doris Native file header.
    // Expected layout:
    // [magic bytes "DORISN1\0"][uint32_t format_version][uint64_t block_size]...
    static constexpr size_t HEADER_SIZE = sizeof(DORIS_NATIVE_MAGIC) + sizeof(uint32_t);
    if (*eof || *file_size < static_cast<int64_t>(HEADER_SIZE)) {
        return Status::InternalError(
                "invalid Doris Native file {}, file size {} is smaller than header size {}",
                range.path, *file_size, HEADER_SIZE);
    }

    char header[HEADER_SIZE];
    Slice header_slice(header, sizeof(header));
    size_t bytes_read = 0;
    RETURN_IF_ERROR(file_reader->read_at(0, header_slice, &bytes_read));
    if (bytes_read != sizeof(header)) {
        return Status::InternalError(
                "failed to read Doris Native header from file {}, expect {} bytes, got {} bytes",
                range.path, sizeof(header), bytes_read);
    }

    if (memcmp(header, DORIS_NATIVE_MAGIC, sizeof(DORIS_NATIVE_MAGIC)) != 0) {
        return Status::InternalError("invalid Doris Native magic header in file {}", range.path);
    }

    uint32_t version = 0;
    memcpy(&version, header + sizeof(DORIS_NATIVE_MAGIC), sizeof(uint32_t));
    if (version != DORIS_NATIVE_FORMAT_VERSION) {
        return Status::InternalError(
                "unsupported Doris Native format version {} in file {}, expect {}", version,
                range.path, DORIS_NATIVE_FORMAT_VERSION);
    }

    *current_offset = sizeof(header);
    *eof = (*file_size == *current_offset);
    return Status::OK();
}

} // namespace

Status NativeReader::init_reader() {
    if (_file_reader != nullptr) {
        return Status::OK();
    }

    // Create underlying file reader. For now we always use random access mode.
    io::FileSystemProperties system_properties;
    io::FileDescription file_description;
    file_description.file_size = -1;
    if (_scan_range.__isset.file_size) {
        file_description.file_size = _scan_range.file_size;
    }
    file_description.path = _scan_range.path;
    if (_scan_range.__isset.fs_name) {
        file_description.fs_name = _scan_range.fs_name;
    }
    if (_scan_range.__isset.modification_time) {
        file_description.mtime = _scan_range.modification_time;
    } else {
        file_description.mtime = 0;
    }

    if (_scan_range.__isset.file_type) {
        // For compatibility with older FE.
        system_properties.system_type = _scan_range.file_type;
    } else {
        system_properties.system_type = _scan_params.file_type;
    }
    system_properties.properties = _scan_params.properties;
    system_properties.hdfs_params = _scan_params.hdfs_params;
    if (_scan_params.__isset.broker_addresses) {
        system_properties.broker_addresses.assign(_scan_params.broker_addresses.begin(),
                                                  _scan_params.broker_addresses.end());
    }

    io::FileReaderOptions reader_options =
            FileFactory::get_reader_options(_state, file_description);
    auto reader_res = io::DelegateReader::create_file_reader(
            _profile, system_properties, file_description, reader_options,
            io::DelegateReader::AccessMode::RANDOM, _io_ctx);
    if (!reader_res.has_value()) {
        return reader_res.error();
    }
    _file_reader = reader_res.value();

    if (_io_ctx) {
        _file_reader =
                std::make_shared<io::TracingFileReader>(_file_reader, _io_ctx->file_reader_stats);
    }

    RETURN_IF_ERROR(validate_and_consume_header(_file_reader, _scan_range, &_file_size,
                                                &_current_offset, &_eof));
    return Status::OK();
}

Status NativeReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_eof) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(init_reader());

    std::string buff;
    bool local_eof = false;

    // If we have already loaded the first block for schema probing, use it first.
    if (_first_block_loaded && !_first_block_consumed) {
        buff = _first_block_buf;
        local_eof = false;
    } else {
        RETURN_IF_ERROR(_read_next_pblock(&buff, &local_eof));
    }

    // If we reach EOF and also read no data for this call, the whole file is considered finished.
    if (local_eof && buff.empty()) {
        *read_rows = 0;
        *eof = true;
        _eof = true;
        return Status::OK();
    }
    // If buffer is empty but we have not reached EOF yet, treat this as an error.
    if (buff.empty()) {
        return Status::InternalError("read empty native block from file {}", _scan_range.path);
    }

    PBlock pblock;
    if (!pblock.ParseFromArray(buff.data(), static_cast<int>(buff.size()))) {
        return Status::InternalError("Failed to parse native PBlock from file {}",
                                     _scan_range.path);
    }

    // Initialize schema from first block if not done yet.
    if (!_schema_inited) {
        RETURN_IF_ERROR(_init_schema_from_pblock(pblock));
    }

    size_t uncompressed_bytes = 0;
    int64_t decompress_time = 0;
    RETURN_IF_ERROR(block->deserialize(pblock, &uncompressed_bytes, &decompress_time));

    // For external file scan / TVF scenarios, unify all columns as nullable to match
    // GenericReader/SlotDescriptor convention. This ensures schema consistency when
    // some writers emit non-nullable columns.
    for (size_t i = 0; i < block->columns(); ++i) {
        auto& col_with_type = block->get_by_position(i);
        if (!col_with_type.type->is_nullable()) {
            col_with_type.column = make_nullable(col_with_type.column);
            col_with_type.type = make_nullable(col_with_type.type);
        }
    }

    *read_rows = block->rows();
    *eof = false;

    if (_first_block_loaded && !_first_block_consumed) {
        _first_block_consumed = true;
    }

    // If we reached the physical end of file, mark eof for subsequent calls.
    if (_current_offset >= _file_size) {
        _eof = true;
    }

    return Status::OK();
}

Status NativeReader::get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                                 std::unordered_set<std::string>* missing_cols) {
    missing_cols->clear();
    RETURN_IF_ERROR(init_reader());

    if (!_schema_inited) {
        // Load first block lazily to initialize schema.
        if (!_first_block_loaded) {
            bool local_eof = false;
            RETURN_IF_ERROR(_read_next_pblock(&_first_block_buf, &local_eof));
            // Treat file as empty only if we reach EOF and there is no block data at all.
            if (local_eof && _first_block_buf.empty()) {
                return Status::EndOfFile("empty native file {}", _scan_range.path);
            }
            // Non-EOF but empty buffer means corrupted native file.
            if (_first_block_buf.empty()) {
                return Status::InternalError("first native block is empty {}", _scan_range.path);
            }
            _first_block_loaded = true;
        }

        PBlock pblock;
        if (!pblock.ParseFromArray(_first_block_buf.data(),
                                   static_cast<int>(_first_block_buf.size()))) {
            return Status::InternalError("Failed to parse native PBlock for schema from file {}",
                                         _scan_range.path);
        }
        RETURN_IF_ERROR(_init_schema_from_pblock(pblock));
    }

    for (size_t i = 0; i < _schema_col_names.size(); ++i) {
        name_to_type->emplace(_schema_col_names[i], _schema_col_types[i]);
    }
    return Status::OK();
}

Status NativeReader::init_schema_reader() {
    RETURN_IF_ERROR(init_reader());
    return Status::OK();
}

Status NativeReader::get_parsed_schema(std::vector<std::string>* col_names,
                                       std::vector<DataTypePtr>* col_types) {
    RETURN_IF_ERROR(init_reader());

    if (!_schema_inited) {
        if (!_first_block_loaded) {
            bool local_eof = false;
            RETURN_IF_ERROR(_read_next_pblock(&_first_block_buf, &local_eof));
            // Treat file as empty only if we reach EOF and there is no block data at all.
            if (local_eof && _first_block_buf.empty()) {
                return Status::EndOfFile("empty native file {}", _scan_range.path);
            }
            // Non-EOF but empty buffer means corrupted native file.
            if (_first_block_buf.empty()) {
                return Status::InternalError("first native block is empty {}", _scan_range.path);
            }
            _first_block_loaded = true;
        }

        PBlock pblock;
        if (!pblock.ParseFromArray(_first_block_buf.data(),
                                   static_cast<int>(_first_block_buf.size()))) {
            return Status::InternalError("Failed to parse native PBlock for schema from file {}",
                                         _scan_range.path);
        }
        RETURN_IF_ERROR(_init_schema_from_pblock(pblock));
    }

    *col_names = _schema_col_names;
    *col_types = _schema_col_types;
    return Status::OK();
}

Status NativeReader::close() {
    _file_reader.reset();
    return Status::OK();
}

Status NativeReader::_read_next_pblock(std::string* buff, bool* eof) {
    *eof = false;
    buff->clear();

    if (_file_reader == nullptr) {
        RETURN_IF_ERROR(init_reader());
    }

    if (_current_offset >= _file_size) {
        *eof = true;
        return Status::OK();
    }

    uint64_t len = 0;
    Slice len_slice(reinterpret_cast<char*>(&len), sizeof(len));
    size_t bytes_read = 0;
    RETURN_IF_ERROR(_file_reader->read_at(_current_offset, len_slice, &bytes_read));
    if (bytes_read == 0) {
        *eof = true;
        return Status::OK();
    }
    if (bytes_read != sizeof(len)) {
        return Status::InternalError(
                "Failed to read native block length from file {}, expect {}, "
                "actual {}",
                _scan_range.path, sizeof(len), bytes_read);
    }

    _current_offset += sizeof(len);
    if (len == 0) {
        // Empty block, nothing to read.
        *eof = (_current_offset >= _file_size);
        return Status::OK();
    }

    buff->assign(len, '\0');
    Slice data_slice(buff->data(), len);
    bytes_read = 0;
    RETURN_IF_ERROR(_file_reader->read_at(_current_offset, data_slice, &bytes_read));
    if (bytes_read != len) {
        return Status::InternalError(
                "Failed to read native block body from file {}, expect {}, "
                "actual {}",
                _scan_range.path, len, bytes_read);
    }

    _current_offset += len;
    *eof = (_current_offset >= _file_size);
    return Status::OK();
}

Status NativeReader::_init_schema_from_pblock(const PBlock& pblock) {
    _schema_col_names.clear();
    _schema_col_types.clear();

    for (const auto& pcol_meta : pblock.column_metas()) {
        DataTypePtr type = make_nullable(DataTypeFactory::instance().create_data_type(pcol_meta));
        VLOG_DEBUG << "init_schema_from_pblock, name=" << pcol_meta.name()
                   << ", type=" << type->get_name();
        _schema_col_names.emplace_back(pcol_meta.name());
        _schema_col_types.emplace_back(type);
    }
    _schema_inited = true;
    return Status::OK();
}

#include "common/compile_check_end.h"

} // namespace doris::vectorized
