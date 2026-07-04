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

#include "format_v2/native/native_reader.h"

#include <cstring>
#include <utility>

#include "common/cast_set.h"
#include "core/block/block.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "format/native/native_format.h"
#include "format_v2/column_mapper.h"
#include "format_v2/materialized_reader_util.h"
#include "io/file_factory.h"
#include "io/fs/tracing_file_reader.h"
#include "runtime/runtime_state.h"
#include "util/slice.h"

namespace doris::format::native {
namespace {

Status parse_native_pblock(const std::string& buffer, const std::string& path, PBlock* pblock) {
    DORIS_CHECK(pblock != nullptr);
    if (!pblock->ParseFromArray(buffer.data(), cast_set<int>(buffer.size()))) {
        return Status::InternalError("Failed to parse native PBlock from file {}", path);
    }
    return Status::OK();
}

} // namespace

NativeReader::NativeReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                           std::unique_ptr<io::FileDescription>& file_description,
                           std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile)
        : FileReader(system_properties, file_description, std::move(io_ctx), profile) {}

NativeReader::~NativeReader() {
    static_cast<void>(close());
}

Status NativeReader::init(RuntimeState* state) {
    _runtime_state = state;
    if (_file_description == nullptr) {
        return Status::InvalidArgument("Native v2 reader requires file description");
    }
    RETURN_IF_ERROR(FileReader::init(state));
    RETURN_IF_ERROR(_validate_and_consume_header());
    return Status::OK();
}

Status NativeReader::get_schema(std::vector<ColumnDefinition>* file_schema) const {
    if (file_schema == nullptr) {
        return Status::InvalidArgument("Native v2 file_schema is null");
    }
    RETURN_IF_ERROR(_ensure_schema_loaded());
    *file_schema = _file_schema;
    return Status::OK();
}

std::unique_ptr<TableColumnMapper> NativeReader::create_column_mapper(
        TableColumnMapperOptions options) const {
    return std::make_unique<MaterializedColumnMapper>(std::move(options));
}

Status NativeReader::open(std::shared_ptr<FileScanRequest> request) {
    RETURN_IF_ERROR(FileReader::open(std::move(request)));
    DORIS_CHECK(_request != nullptr);
    _first_block_consumed = false;
    _reader_eof = false;
    _eof = false;
    return Status::OK();
}

Status NativeReader::get_block(Block* file_block, size_t* rows, bool* eof) {
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    DORIS_CHECK(eof != nullptr);
    if (_request == nullptr) {
        return Status::InternalError("Native v2 reader is not open");
    }

    *rows = 0;
    *eof = false;
    if (_reader_eof) {
        *eof = true;
        _eof = true;
        return Status::OK();
    }

    std::string buffer;
    bool local_eof = false;
    if (_first_block_loaded && !_first_block_consumed) {
        buffer = _first_block_buffer;
    } else {
        RETURN_IF_ERROR(_read_next_pblock(&buffer, &local_eof));
    }

    if (local_eof && buffer.empty()) {
        _reader_eof = true;
        *eof = true;
        _eof = true;
        return Status::OK();
    }
    if (buffer.empty()) {
        return Status::InternalError("read empty native block from file {}",
                                     _file_description->path);
    }

    PBlock pblock;
    RETURN_IF_ERROR(parse_native_pblock(buffer, _file_description->path, &pblock));
    if (!_schema_inited) {
        RETURN_IF_ERROR(_init_schema_from_pblock(pblock));
    }

    Block source_block;
    size_t uncompressed_bytes = 0;
    int64_t decompress_time = 0;
    RETURN_IF_ERROR(source_block.deserialize(pblock, &uncompressed_bytes, &decompress_time));
    RETURN_IF_ERROR(_materialize_requested_columns(source_block, file_block));
    *rows = file_block->rows();
    RETURN_IF_ERROR(_apply_filters(file_block, rows));
    _reader_statistics.read_rows += *rows;

    if (_first_block_loaded && !_first_block_consumed) {
        _first_block_consumed = true;
    }
    if (_current_offset >= _file_size) {
        _reader_eof = true;
    }
    *eof = _reader_eof && *rows == 0;
    _eof = *eof;
    return Status::OK();
}

Status NativeReader::close() {
    _file_reader.reset();
    _tracing_file_reader.reset();
    _request.reset();
    _reader_eof = true;
    _eof = true;
    return Status::OK();
}

Status NativeReader::_validate_and_consume_header() {
    DORIS_CHECK(_tracing_file_reader != nullptr);
    _file_size = _tracing_file_reader->size();
    _current_offset = 0;
    _reader_eof = (_file_size == 0);

    static constexpr size_t HEADER_SIZE = sizeof(DORIS_NATIVE_MAGIC) + sizeof(uint32_t);
    if (_reader_eof || _file_size < cast_set<int64_t>(HEADER_SIZE)) {
        return Status::InternalError(
                "invalid Doris Native file {}, file size {} is smaller than header size {}",
                _file_description->path, _file_size, HEADER_SIZE);
    }

    char header[HEADER_SIZE];
    Slice header_slice(header, sizeof(header));
    size_t bytes_read = 0;
    RETURN_IF_ERROR(_tracing_file_reader->read_at(0, header_slice, &bytes_read, _io_ctx.get()));
    if (bytes_read != sizeof(header)) {
        return Status::InternalError(
                "failed to read Doris Native header from file {}, expect {} bytes, got {} bytes",
                _file_description->path, sizeof(header), bytes_read);
    }
    if (std::memcmp(header, DORIS_NATIVE_MAGIC, sizeof(DORIS_NATIVE_MAGIC)) != 0) {
        return Status::InternalError("invalid Doris Native magic header in file {}",
                                     _file_description->path);
    }

    uint32_t version = 0;
    std::memcpy(&version, header + sizeof(DORIS_NATIVE_MAGIC), sizeof(uint32_t));
    if (version != DORIS_NATIVE_FORMAT_VERSION) {
        return Status::InternalError(
                "unsupported Doris Native format version {} in file {}, expect {}", version,
                _file_description->path, DORIS_NATIVE_FORMAT_VERSION);
    }

    _current_offset = sizeof(header);
    _reader_eof = (_file_size == _current_offset);
    return Status::OK();
}

Status NativeReader::_ensure_schema_loaded() const {
    if (_schema_inited) {
        return Status::OK();
    }
    if (!_first_block_loaded) {
        bool local_eof = false;
        RETURN_IF_ERROR(_read_next_pblock(&_first_block_buffer, &local_eof));
        if (local_eof && _first_block_buffer.empty()) {
            return Status::EndOfFile("empty native file {}", _file_description->path);
        }
        if (_first_block_buffer.empty()) {
            return Status::InternalError("first native block is empty {}", _file_description->path);
        }
        _first_block_loaded = true;
    }

    PBlock pblock;
    RETURN_IF_ERROR(parse_native_pblock(_first_block_buffer, _file_description->path, &pblock));
    RETURN_IF_ERROR(_init_schema_from_pblock(pblock));
    return Status::OK();
}

Status NativeReader::_read_next_pblock(std::string* buffer, bool* eof) const {
    DORIS_CHECK(buffer != nullptr);
    DORIS_CHECK(eof != nullptr);
    DORIS_CHECK(_tracing_file_reader != nullptr);
    buffer->clear();
    *eof = false;

    if (_current_offset >= _file_size) {
        *eof = true;
        return Status::OK();
    }

    uint64_t block_len = 0;
    Slice len_slice(reinterpret_cast<char*>(&block_len), sizeof(block_len));
    size_t bytes_read = 0;
    RETURN_IF_ERROR(
            _tracing_file_reader->read_at(_current_offset, len_slice, &bytes_read, _io_ctx.get()));
    if (bytes_read == 0) {
        *eof = true;
        return Status::OK();
    }
    if (bytes_read != sizeof(block_len)) {
        return Status::InternalError(
                "Failed to read native block length from file {}, expect {}, actual {}",
                _file_description->path, sizeof(block_len), bytes_read);
    }
    _current_offset += sizeof(block_len);
    if (block_len == 0) {
        *eof = (_current_offset >= _file_size);
        return Status::OK();
    }

    buffer->assign(block_len, '\0');
    Slice data_slice(buffer->data(), block_len);
    bytes_read = 0;
    RETURN_IF_ERROR(
            _tracing_file_reader->read_at(_current_offset, data_slice, &bytes_read, _io_ctx.get()));
    if (bytes_read != block_len) {
        return Status::InternalError(
                "Failed to read native block body from file {}, expect {}, actual {}",
                _file_description->path, block_len, bytes_read);
    }
    _current_offset += block_len;
    *eof = (_current_offset >= _file_size);
    return Status::OK();
}

Status NativeReader::_init_schema_from_pblock(const PBlock& pblock) const {
    _file_schema.clear();
    _file_schema.reserve(pblock.column_metas_size());
    for (int idx = 0; idx < pblock.column_metas_size(); ++idx) {
        const auto& meta = pblock.column_metas(idx);
        ColumnDefinition field;
        field.identifier = Field::create_field<TYPE_STRING>(meta.name());
        field.local_id = idx;
        field.name = meta.name();
        field.type = make_nullable(DataTypeFactory::instance().create_data_type(meta));
        _file_schema.push_back(std::move(field));
    }
    _schema_inited = true;
    return Status::OK();
}

Status NativeReader::_materialize_requested_columns(const Block& source_block,
                                                    Block* file_block) const {
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(_request != nullptr);
    for (const auto& [file_column_id, block_position] : _request->local_positions) {
        const auto source_idx = file_column_id.value();
        if (source_idx < 0 || cast_set<size_t>(source_idx) >= source_block.columns()) {
            return Status::InternalError("native file {} does not contain local column id {}",
                                         _file_description->path, source_idx);
        }
        if (block_position.value() >= file_block->columns()) {
            return Status::InternalError("native v2 request has invalid block position {}",
                                         block_position.value());
        }
        const auto& target = file_block->get_by_position(block_position.value());
        auto column = source_block.get_by_position(source_idx).column;
        column = make_column_nullable_if_needed(std::move(column), target.type);
        file_block->replace_by_position(block_position.value(), IColumn::mutate(std::move(column)));
    }
    return Status::OK();
}

Status NativeReader::_apply_filters(Block* file_block, size_t* rows) const {
    return apply_materialized_reader_filters(_request.get(), _io_ctx.get(), file_block, rows);
}

} // namespace doris::format::native
