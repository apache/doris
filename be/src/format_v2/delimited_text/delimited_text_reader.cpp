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

#include "format_v2/delimited_text/delimited_text_reader.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "common/cast_set.h"
#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "exprs/vexpr_context.h"
#include "format/line_reader.h"
#include "io/file_factory.h"
#include "io/fs/tracing_file_reader.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/decompressor.h"
#include "util/string_util.h"

namespace doris::format {
namespace {

DataTypePtr nullable_type(DataTypePtr type) {
    return type != nullptr && type->is_nullable() ? std::move(type)
                                                  : make_nullable(std::move(type));
}

} // namespace

DelimitedTextReader::DelimitedTextReader(
        std::shared_ptr<io::FileSystemProperties>& system_properties,
        std::unique_ptr<io::FileDescription>& file_description,
        std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
        const TFileScanRangeParams* scan_params,
        const std::vector<SlotDescriptor*>& file_slot_descs, std::string reader_name)
        : FileReader(system_properties, file_description, std::move(io_ctx), profile),
          _scan_params(scan_params),
          _source_file_slot_descs(file_slot_descs),
          _reader_name(std::move(reader_name)) {}

DelimitedTextReader::~DelimitedTextReader() {
    static_cast<void>(close());
}

Status DelimitedTextReader::init(RuntimeState* state) {
    _init_profile();
    _runtime_state = state;
    if (_scan_params == nullptr) {
        return Status::InvalidArgument("{} v2 reader requires scan params", _reader_name);
    }
    if (_file_description == nullptr) {
        return Status::InvalidArgument("{} v2 reader requires file description", _reader_name);
    }
    if (!_scan_params->__isset.file_attributes ||
        !_scan_params->file_attributes.__isset.text_params) {
        return Status::InvalidArgument("{} v2 reader requires text file attributes", _reader_name);
    }

    RETURN_IF_ERROR(_init_format_state());

    // Delimited text files have no physical column ids. FE sends `column_idxs` to describe how
    // each physical file slot maps to a field ordinal in the text row. The local id exposed in the
    // file schema is therefore the text-field ordinal, not the slot vector position.
    _source_column_idxs.clear();
    if (_scan_params->__isset.column_idxs && !_scan_params->column_idxs.empty()) {
        if (_scan_params->column_idxs.size() != _source_file_slot_descs.size()) {
            return Status::InvalidArgument(
                    "{} v2 reader column_idxs size {} does not match file slot size {}",
                    _reader_name, _scan_params->column_idxs.size(), _source_file_slot_descs.size());
        }
        _source_column_idxs.reserve(_scan_params->column_idxs.size());
        for (const auto column_idx : _scan_params->column_idxs) {
            _source_column_idxs.push_back(column_idx);
        }
    } else {
        _source_column_idxs.reserve(_source_file_slot_descs.size());
        for (size_t i = 0; i < _source_file_slot_descs.size(); ++i) {
            _source_column_idxs.push_back(static_cast<int32_t>(i));
        }
    }

    _source_serdes = create_data_type_serdes(_source_file_slot_descs);
    _file_schema.clear();
    _file_schema.reserve(_source_file_slot_descs.size());
    for (size_t i = 0; i < _source_file_slot_descs.size(); ++i) {
        const auto* slot = _source_file_slot_descs[i];
        DORIS_CHECK(slot != nullptr);
        ColumnDefinition field;
        field.identifier = Field::create_field<TYPE_STRING>(slot->col_name());
        field.local_id = _source_column_idxs[i];
        field.name = slot->col_name();
        field.type = nullable_type(slot->get_data_type_ptr());
        _file_schema.push_back(std::move(field));
    }
    _eof = false;
    return Status::OK();
}

Status DelimitedTextReader::get_schema(std::vector<ColumnDefinition>* file_schema) const {
    if (file_schema == nullptr) {
        return Status::InvalidArgument("{} v2 file_schema is null", _reader_name);
    }
    *file_schema = _file_schema;
    return Status::OK();
}

Status DelimitedTextReader::open(std::shared_ptr<FileScanRequest> request) {
    RETURN_IF_ERROR(FileReader::open(std::move(request)));
    DORIS_CHECK(_request != nullptr);
    RETURN_IF_ERROR(_build_requested_columns(*_request, &_requested_columns));
    RETURN_IF_ERROR(_open_file());
    RETURN_IF_ERROR(_create_decompressor());
    RETURN_IF_ERROR(_create_line_reader());
    _line_reader_eof = false;
    _bom_removed = false;
    _eof = false;
    return Status::OK();
}

Status DelimitedTextReader::get_block(Block* file_block, size_t* rows, bool* eof) {
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    DORIS_CHECK(eof != nullptr);
    if (_line_reader == nullptr) {
        return Status::InternalError("{} v2 reader is not open", _reader_name);
    }

    const auto batch_size = _runtime_state != nullptr ? _runtime_state->batch_size() : 4096;
    const auto max_block_bytes = _runtime_state != nullptr
                                         ? _runtime_state->preferred_block_size_bytes()
                                         : std::numeric_limits<size_t>::max();
    *rows = 0;
    *eof = false;

    {
        auto columns_guard = file_block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        // Delimited text readers are column-pruned but not lazy materialized: all file-local
        // columns requested by TableReader are decoded before file-local conjuncts are evaluated.
        while (*rows < batch_size && !_line_reader_eof &&
               Block::columns_byte_size(columns) < max_block_bytes) {
            Slice line;
            bool line_eof = false;
            RETURN_IF_ERROR(_read_next_line(&line, &line_eof));
            if (line_eof) {
                break;
            }
            RETURN_IF_ERROR(_fill_columns_from_line(line, &columns, rows));
        }
    }

    const auto rows_before_filter = *rows;
    if (_request != nullptr && *rows > 0 && !_request->delete_conjuncts.empty()) {
        RETURN_IF_ERROR(VExprContext::filter_block(_request->delete_conjuncts, file_block,
                                                   file_block->columns()));
    }
    if (_request != nullptr && *rows > 0 && !_request->conjuncts.empty()) {
        RETURN_IF_ERROR(
                VExprContext::filter_block(_request->conjuncts, file_block, file_block->columns()));
    }

    *rows = file_block->columns() == 0 ? rows_before_filter : file_block->rows();
    _reader_statistics.read_rows += *rows;
    *eof = _line_reader_eof && *rows == 0;
    _eof = *eof;
    return Status::OK();
}

Status DelimitedTextReader::get_aggregate_result(const FileAggregateRequest& request,
                                                 FileAggregateResult* result) {
    DORIS_CHECK(result != nullptr);
    if (request.agg_type != TPushAggOp::type::COUNT) {
        return Status::NotSupported("{} v2 reader only supports COUNT aggregate pushdown",
                                    _reader_name);
    }
    if (_line_reader == nullptr) {
        return Status::InternalError("{} v2 reader is not open", _reader_name);
    }

    int64_t count = 0;
    while (!_line_reader_eof) {
        Slice line;
        bool line_eof = false;
        RETURN_IF_ERROR(_read_next_line(&line, &line_eof));
        if (line_eof) {
            break;
        }
        if (line.size == 0) {
            if (_runtime_state != nullptr && _runtime_state->is_read_csv_empty_line_as_null()) {
                ++count;
            }
            continue;
        }
        RETURN_IF_ERROR(_validate_line(line));
        ++count;
    }
    result->count = count;
    result->columns.clear();
    _reader_statistics.read_rows += count;
    _eof = true;
    return Status::OK();
}

Status DelimitedTextReader::close() {
    if (_line_reader != nullptr) {
        _line_reader->close();
        _line_reader.reset();
    }
    _decompressor.reset();
    _file_reader.reset();
    _tracing_file_reader.reset();
    _requested_columns.clear();
    return Status::OK();
}

Status DelimitedTextReader::_build_requested_columns(const FileScanRequest& request,
                                                     std::vector<RequestedColumn>* columns) const {
    DORIS_CHECK(columns != nullptr);
    columns->clear();

    // `request.local_positions` is keyed by FileReader schema local id. For delimited text readers
    // that local id is the field ordinal from column_idxs, so reverse-map it to the source slot
    // descriptor before choosing the serde.
    std::vector<RequestedColumn> by_position(request.local_positions.size());
    for (const auto& [file_column_id, block_position] : request.local_positions) {
        const auto source_it = std::find(_source_column_idxs.begin(), _source_column_idxs.end(),
                                         file_column_id.value());
        if (source_it == _source_column_idxs.end()) {
            return Status::InvalidArgument("{} v2 request references unknown local column id {}",
                                           _reader_name, file_column_id.value());
        }
        const auto source_index = std::distance(_source_column_idxs.begin(), source_it);
        DORIS_CHECK(source_index >= 0 &&
                    static_cast<size_t>(source_index) < _source_file_slot_descs.size());
        if (block_position.value() >= by_position.size()) {
            return Status::InvalidArgument("{} v2 request has invalid block position {}",
                                           _reader_name, block_position.value());
        }
        const auto* slot = _source_file_slot_descs[source_index];
        const auto type = slot->get_data_type_ptr();
        RequestedColumn requested_column;
        requested_column.file_column_id = file_column_id;
        requested_column.block_position = block_position;
        requested_column.slot_desc = slot;
        requested_column.serde = _source_serdes[source_index];
        requested_column.nullable_string_fast_path =
                type->is_nullable() && is_string_type(type->get_primitive_type());
        by_position[block_position.value()] = std::move(requested_column);
    }

    for (size_t i = 0; i < by_position.size(); ++i) {
        if (!by_position[i].file_column_id.is_valid()) {
            return Status::InvalidArgument("{} v2 request misses block position {}", _reader_name,
                                           i);
        }
    }
    *columns = std::move(by_position);
    return Status::OK();
}

Status DelimitedTextReader::_open_file() {
    _start_offset = _file_description->range_start_offset;
    _size = _file_description->range_size;
    // Some tests and callers use -1 to mean "read to EOF". NewPlainTextLineReader needs a concrete
    // byte budget, so normalize it to the remaining file size when the file metadata is known.
    if (_size <= 0 && _file_description->file_size >= 0) {
        _size = _file_description->file_size - _start_offset;
    }
    if (_size < 0) {
        return Status::InvalidArgument("{} v2 reader requires a valid split size for {}",
                                       _reader_name, _file_description->path);
    }
    _skip_lines = 0;
    if (_start_offset == 0) {
        if (_scan_params->file_attributes.__isset.header_type &&
            !_scan_params->file_attributes.header_type.empty()) {
            const auto header_type = to_lower(_scan_params->file_attributes.header_type);
            if (header_type == BeConsts::CSV_WITH_NAMES) {
                _skip_lines = 1;
            } else if (header_type == BeConsts::CSV_WITH_NAMES_AND_TYPES) {
                _skip_lines = 2;
            }
        } else if (_scan_params->file_attributes.__isset.skip_lines) {
            _skip_lines = _scan_params->file_attributes.skip_lines;
        }
    } else {
        if (!_can_split()) {
            return Status::InternalError<false>("For now we do not support split compressed file");
        }
        // Non-first splits normally start in the middle of a record. Pre-read at most one line
        // delimiter byte range, then skip one line in `_read_next_line()`, so the first returned
        // row is always complete. Example with '\n':
        //   file bytes:  "1,a\n2,b\n"
        //   split start:     ^
        //   pre-read:     ^
        //   skipped line: "a"
        //   returned row: "2,b"
        const int64_t pre_read_len =
                std::min(static_cast<int64_t>(_line_delimiter.size()), _start_offset);
        _start_offset -= pre_read_len;
        _size += pre_read_len;
        _skip_lines = 1;
    }

    auto reader_options =
            FileFactory::get_reader_options(_runtime_state->query_options(), *_file_description);
    auto file_reader = DORIS_TRY(FileFactory::create_file_reader(
            *_system_properties, *_file_description, reader_options, _profile));
    _file_reader = _io_ctx && _io_ctx->file_reader_stats
                           ? std::make_shared<io::TracingFileReader>(std::move(file_reader),
                                                                     _io_ctx->file_reader_stats)
                           : file_reader;
    if (_file_reader->size() == 0 && _scan_params->file_type != TFileType::FILE_BROKER) {
        return Status::EndOfFile("init reader failed, empty {} file: {}", _reader_name,
                                 _file_description->path);
    }
    return Status::OK();
}

Status DelimitedTextReader::_read_next_line(Slice* line, bool* eof) {
    DORIS_CHECK(line != nullptr);
    DORIS_CHECK(eof != nullptr);
    while (true) {
        const uint8_t* ptr = nullptr;
        size_t size = 0;
        RETURN_IF_ERROR(_line_reader->read_line(&ptr, &size, &_line_reader_eof, _io_ctx.get()));
        if (_line_reader_eof && size == 0) {
            *eof = true;
            return Status::OK();
        }
        if (_skip_lines == 0 && !_bom_removed) {
            // BOM is stripped only from the first logical data line. Header lines are skipped
            // before this branch, so a BOM inside a skipped header does not leak into user data.
            ptr = _remove_bom(ptr, &size);
            _bom_removed = true;
        }
        if (_skip_lines > 0) {
            --_skip_lines;
            _bom_removed = true;
            continue;
        }
        *line = Slice(ptr, size);
        *eof = false;
        return Status::OK();
    }
}

Status DelimitedTextReader::_fill_columns_from_line(const Slice& line,
                                                    std::vector<MutableColumnPtr>* columns,
                                                    size_t* rows) {
    DORIS_CHECK(columns != nullptr);
    if (line.size == 0) {
        if (_runtime_state != nullptr && _runtime_state->is_read_csv_empty_line_as_null()) {
            for (const auto& column : _requested_columns) {
                RETURN_IF_ERROR(_append_null((*columns)[column.block_position.value()].get()));
            }
            ++(*rows);
        }
        return Status::OK();
    }
    RETURN_IF_ERROR(_validate_line(line));

    _split_line(line);
    for (const auto& column : _requested_columns) {
        auto* output = (*columns)[column.block_position.value()].get();
        const int32_t field_index = column.file_column_id.value();
        // Missing trailing fields are query-compatible with the old readers: they become NULL
        // rather than shifting subsequent projected columns or rejecting the row.
        Slice value = field_index >= 0 && static_cast<size_t>(field_index) < _split_values.size()
                              ? _split_values[field_index]
                              : Slice(_options.null_format, _options.null_len);
        RETURN_IF_ERROR(_deserialize_one_cell(column, output, _normalize_value(value)));
    }
    ++(*rows);
    return Status::OK();
}

Status DelimitedTextReader::_validate_line(const Slice& line) {
    (void)line;
    return Status::OK();
}

Slice DelimitedTextReader::_normalize_value(Slice value) const {
    return value;
}

bool DelimitedTextReader::_can_split() const {
    return _file_compress_type == TFileCompressType::PLAIN;
}

Status DelimitedTextReader::_append_null(IColumn* output) {
    DORIS_CHECK(output != nullptr);
    auto* nullable = assert_cast<ColumnNullable*>(output);
    nullable->insert_data(nullptr, 0);
    return Status::OK();
}

const uint8_t* DelimitedTextReader::_remove_bom(const uint8_t* ptr, size_t* size) {
    DORIS_CHECK(size != nullptr);
    if (ptr != nullptr && *size >= 3 && static_cast<uint8_t>(ptr[0]) == 0xEF &&
        static_cast<uint8_t>(ptr[1]) == 0xBB && static_cast<uint8_t>(ptr[2]) == 0xBF) {
        *size -= 3;
        return ptr + 3;
    }
    return ptr;
}

} // namespace doris::format
