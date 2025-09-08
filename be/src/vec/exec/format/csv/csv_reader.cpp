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

#include "csv_reader.h"

#include <fmt/format.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <ostream>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/consts.h"
#include "common/status.h"
#include "exec/decompressor.h"
#include "exec/line_reader.h"
#include "io/file_factory.h"
#include "io/fs/broker_file_reader.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "io/fs/s3_file_reader.h"
#include "io/fs/tracing_file_reader.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/string_util.h"
#include "util/utf8_check.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/file_reader/new_plain_binary_line_reader.h"
#include "vec/exec/format/file_reader/new_plain_text_line_reader.h"
#include "vec/exec/scan/scanner.h"

namespace doris {
class RuntimeProfile;
namespace vectorized {
class IColumn;
} // namespace vectorized
namespace io {
struct IOContext;
enum class FileCachePolicy : uint8_t;
} // namespace io
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

void EncloseCsvTextFieldSplitter::do_split(const Slice& line, std::vector<Slice>* splitted_values) {
    const char* data = line.data;
    const auto& column_sep_positions = _text_line_reader_ctx->column_sep_positions();
    size_t value_start_offset = 0;
    for (auto idx : column_sep_positions) {
        process_value_func(data, value_start_offset, idx - value_start_offset, _trimming_char,
                           splitted_values);
        value_start_offset = idx + _value_sep_len;
    }
    if (line.size >= value_start_offset) {
        // process the last column
        process_value_func(data, value_start_offset, line.size - value_start_offset, _trimming_char,
                           splitted_values);
    }
}

void PlainCsvTextFieldSplitter::_split_field_single_char(const Slice& line,
                                                         std::vector<Slice>* splitted_values) {
    const char* data = line.data;
    const size_t size = line.size;
    size_t value_start = 0;
    for (size_t i = 0; i < size; ++i) {
        if (data[i] == _value_sep[0]) {
            process_value_func(data, value_start, i - value_start, _trimming_char, splitted_values);
            value_start = i + _value_sep_len;
        }
    }
    process_value_func(data, value_start, size - value_start, _trimming_char, splitted_values);
}

void PlainCsvTextFieldSplitter::_split_field_multi_char(const Slice& line,
                                                        std::vector<Slice>* splitted_values) {
    size_t start = 0;  // point to the start pos of next col value.
    size_t curpos = 0; // point to the start pos of separator matching sequence.

    // value_sep : AAAA
    // line.data : 1234AAAA5678
    // -> 1234,5678

    //    start   start
    //      ▼       ▼
    //      1234AAAA5678\0
    //          ▲       ▲
    //      curpos     curpos

    //kmp
    std::vector<int> next(_value_sep_len);
    next[0] = -1;
    for (int i = 1, j = -1; i < _value_sep_len; i++) {
        while (j > -1 && _value_sep[i] != _value_sep[j + 1]) {
            j = next[j];
        }
        if (_value_sep[i] == _value_sep[j + 1]) {
            j++;
        }
        next[i] = j;
    }

    for (int i = 0, j = -1; i < line.size; i++) {
        // i : line
        // j : _value_sep
        while (j > -1 && line[i] != _value_sep[j + 1]) {
            j = next[j];
        }
        if (line[i] == _value_sep[j + 1]) {
            j++;
        }
        if (j == _value_sep_len - 1) {
            curpos = i - _value_sep_len + 1;

            /*
             * column_separator : "xx"
             * data.csv :  data1xxxxdata2
             *
             * Parse incorrectly:
             *      data1[xx]xxdata2
             *      data1x[xx]xdata2
             *      data1xx[xx]data2
             * The string "xxxx" is parsed into three "xx" delimiters.
             *
             * Parse correctly:
             *      data1[xx]xxdata2
             *      data1xx[xx]data2
             */

            if (curpos >= start) {
                process_value_func(line.data, start, curpos - start, _trimming_char,
                                   splitted_values);
                start = i + 1;
            }

            j = next[j];
        }
    }
    process_value_func(line.data, start, line.size - start, _trimming_char, splitted_values);
}

void PlainCsvTextFieldSplitter::do_split(const Slice& line, std::vector<Slice>* splitted_values) {
    if (is_single_char_delim) {
        _split_field_single_char(line, splitted_values);
    } else {
        _split_field_multi_char(line, splitted_values);
    }
}

CsvReader::CsvReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
                     const TFileScanRangeParams& params, const TFileRangeDesc& range,
                     const std::vector<SlotDescriptor*>& file_slot_descs, io::IOContext* io_ctx)
        : _profile(profile),
          _params(params),
          _file_reader(nullptr),
          _line_reader(nullptr),
          _decompressor(nullptr),
          _state(state),
          _counter(counter),
          _range(range),
          _file_slot_descs(file_slot_descs),
          _line_reader_eof(false),
          _skip_lines(0),
          _io_ctx(io_ctx) {
    _file_format_type = _params.format_type;
    _is_proto_format = _file_format_type == TFileFormatType::FORMAT_PROTO;
    if (_range.__isset.compress_type) {
        // for compatibility
        _file_compress_type = _range.compress_type;
    } else {
        _file_compress_type = _params.compress_type;
    }
    _size = _range.size;

    _split_values.reserve(_file_slot_descs.size());
    _init_system_properties();
    _init_file_description();
    _serdes = vectorized::create_data_type_serdes(_file_slot_descs);
}

void CsvReader::_init_system_properties() {
    if (_range.__isset.file_type) {
        // for compatibility
        _system_properties.system_type = _range.file_type;
    } else {
        _system_properties.system_type = _params.file_type;
    }
    _system_properties.properties = _params.properties;
    _system_properties.hdfs_params = _params.hdfs_params;
    if (_params.__isset.broker_addresses) {
        _system_properties.broker_addresses.assign(_params.broker_addresses.begin(),
                                                   _params.broker_addresses.end());
    }
}

void CsvReader::_init_file_description() {
    _file_description.path = _range.path;
    _file_description.file_size = _range.__isset.file_size ? _range.file_size : -1;
    if (_range.__isset.fs_name) {
        _file_description.fs_name = _range.fs_name;
    }
}

Status CsvReader::init_reader(bool is_load) {
    // set the skip lines and start offset
    _start_offset = _range.start_offset;
    if (_start_offset == 0) {
        // check header typer first
        if (_params.__isset.file_attributes && _params.file_attributes.__isset.header_type &&
            !_params.file_attributes.header_type.empty()) {
            std::string header_type = to_lower(_params.file_attributes.header_type);
            if (header_type == BeConsts::CSV_WITH_NAMES) {
                _skip_lines = 1;
            } else if (header_type == BeConsts::CSV_WITH_NAMES_AND_TYPES) {
                _skip_lines = 2;
            }
        } else if (_params.file_attributes.__isset.skip_lines) {
            _skip_lines = _params.file_attributes.skip_lines;
        }
    } else if (_start_offset != 0) {
        if ((_file_compress_type != TFileCompressType::PLAIN) ||
            (_file_compress_type == TFileCompressType::UNKNOWN &&
             _file_format_type != TFileFormatType::FORMAT_CSV_PLAIN)) {
            return Status::InternalError<false>("For now we do not support split compressed file");
        }
        // pre-read to promise first line skipped always read
        int64_t pre_read_len = std::min(
                static_cast<int64_t>(_params.file_attributes.text_params.line_delimiter.size()),
                _start_offset);
        _start_offset -= pre_read_len;
        _size += pre_read_len;
        // not first range will always skip one line
        _skip_lines = 1;
    }

    _use_nullable_string_opt.resize(_file_slot_descs.size());
    for (int i = 0; i < _file_slot_descs.size(); ++i) {
        auto data_type_ptr = _file_slot_descs[i]->get_data_type_ptr();
        if (data_type_ptr->is_nullable() && is_string_type(data_type_ptr->get_primitive_type())) {
            _use_nullable_string_opt[i] = 1;
        }
    }

    RETURN_IF_ERROR(_init_options());
    RETURN_IF_ERROR(_create_file_reader(false));
    RETURN_IF_ERROR(_create_decompressor());
    RETURN_IF_ERROR(_create_line_reader());

    _is_load = is_load;
    if (!_is_load) {
        // For query task, there are 2 slot mapping.
        // One is from file slot to values in line.
        //      eg, the file_slot_descs is k1, k3, k5, and values in line are k1, k2, k3, k4, k5
        //      the _col_idxs will save: 0, 2, 4
        // The other is from file slot to columns in output block
        //      eg, the file_slot_descs is k1, k3, k5, and columns in block are p1, k1, k3, k5
        //      where "p1" is the partition col which does not exist in file
        //      the _file_slot_idx_map will save: 1, 2, 3
        DCHECK(_params.__isset.column_idxs);
        _col_idxs = _params.column_idxs;
        int idx = 0;
        for (const auto& slot_info : _params.required_slots) {
            if (slot_info.is_file_slot) {
                _file_slot_idx_map.push_back(idx);
            }
            idx++;
        }
    } else {
        // For load task, the column order is same as file column order
        int i = 0;
        for (const auto& desc [[maybe_unused]] : _file_slot_descs) {
            _col_idxs.push_back(i++);
        }
    }

    _line_reader_eof = false;
    return Status::OK();
}

Status CsvReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_line_reader_eof) {
        *eof = true;
        return Status::OK();
    }

    const int batch_size = std::max(_state->batch_size(), (int)_MIN_BATCH_SIZE);
    size_t rows = 0;

    bool success = false;
    bool is_remove_bom = false;
    if (_push_down_agg_type == TPushAggOp::type::COUNT) {
        while (rows < batch_size && !_line_reader_eof) {
            const uint8_t* ptr = nullptr;
            size_t size = 0;
            RETURN_IF_ERROR(_line_reader->read_line(&ptr, &size, &_line_reader_eof, _io_ctx));

            // _skip_lines == 0 means this line is the actual data beginning line for the entire file
            // is_remove_bom means _remove_bom should only execute once
            if (_skip_lines == 0 && !is_remove_bom) {
                ptr = _remove_bom(ptr, size);
                is_remove_bom = true;
            }

            // _skip_lines > 0 means we do not need to remove bom
            if (_skip_lines > 0) {
                _skip_lines--;
                is_remove_bom = true;
                continue;
            }
            if (size == 0) {
                if (!_line_reader_eof && _state->is_read_csv_empty_line_as_null()) {
                    ++rows;
                }
                // Read empty line, continue
                continue;
            }

            RETURN_IF_ERROR(_validate_line(Slice(ptr, size), &success));
            ++rows;
        }
        auto mutate_columns = block->mutate_columns();
        for (auto& col : mutate_columns) {
            col->resize(rows);
        }
        block->set_columns(std::move(mutate_columns));
    } else {
        auto columns = block->mutate_columns();
        while (rows < batch_size && !_line_reader_eof) {
            const uint8_t* ptr = nullptr;
            size_t size = 0;
            RETURN_IF_ERROR(_line_reader->read_line(&ptr, &size, &_line_reader_eof, _io_ctx));

            // _skip_lines == 0 means this line is the actual data beginning line for the entire file
            // is_remove_bom means _remove_bom should only execute once
            if (!is_remove_bom && _skip_lines == 0) {
                ptr = _remove_bom(ptr, size);
                is_remove_bom = true;
            }

            // _skip_lines > 0 means we do not remove bom
            if (_skip_lines > 0) {
                _skip_lines--;
                is_remove_bom = true;
                continue;
            }
            if (size == 0) {
                if (!_line_reader_eof && _state->is_read_csv_empty_line_as_null()) {
                    RETURN_IF_ERROR(_fill_empty_line(block, columns, &rows));
                }
                // Read empty line, continue
                continue;
            }

            RETURN_IF_ERROR(_validate_line(Slice(ptr, size), &success));
            if (!success) {
                continue;
            }
            RETURN_IF_ERROR(_fill_dest_columns(Slice(ptr, size), block, columns, &rows));
        }
        block->set_columns(std::move(columns));
    }

    *eof = (rows == 0);
    *read_rows = rows;

    return Status::OK();
}

Status CsvReader::get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                              std::unordered_set<std::string>* missing_cols) {
    for (const auto& slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

// init decompressor, file reader and line reader for parsing schema
Status CsvReader::init_schema_reader() {
    _start_offset = _range.start_offset;
    if (_start_offset != 0) {
        return Status::InvalidArgument(
                "start offset of TFileRangeDesc must be zero in get parsered schema");
    }
    if (_params.file_type == TFileType::FILE_BROKER) {
        return Status::InternalError<false>(
                "Getting parsered schema from csv file do not support stream load and broker "
                "load.");
    }

    // csv file without names line and types line.
    _read_line = 1;
    _is_parse_name = false;

    if (_params.__isset.file_attributes && _params.file_attributes.__isset.header_type &&
        !_params.file_attributes.header_type.empty()) {
        std::string header_type = to_lower(_params.file_attributes.header_type);
        if (header_type == BeConsts::CSV_WITH_NAMES) {
            _is_parse_name = true;
        } else if (header_type == BeConsts::CSV_WITH_NAMES_AND_TYPES) {
            _read_line = 2;
            _is_parse_name = true;
        }
    }

    RETURN_IF_ERROR(_init_options());
    RETURN_IF_ERROR(_create_file_reader(true));
    RETURN_IF_ERROR(_create_decompressor());
    RETURN_IF_ERROR(_create_line_reader());
    return Status::OK();
}

Status CsvReader::get_parsed_schema(std::vector<std::string>* col_names,
                                    std::vector<DataTypePtr>* col_types) {
    if (_read_line == 1) {
        if (!_is_parse_name) { //parse csv file without names and types
            size_t col_nums = 0;
            RETURN_IF_ERROR(_parse_col_nums(&col_nums));
            for (size_t i = 0; i < col_nums; ++i) {
                col_names->emplace_back("c" + std::to_string(i + 1));
            }
        } else { // parse csv file with names
            RETURN_IF_ERROR(_parse_col_names(col_names));
        }

        for (size_t j = 0; j < col_names->size(); ++j) {
            col_types->emplace_back(
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, true));
        }
    } else { // parse csv file with names and types
        RETURN_IF_ERROR(_parse_col_names(col_names));
        RETURN_IF_ERROR(_parse_col_types(col_names->size(), col_types));
    }
    return Status::OK();
}

Status CsvReader::_deserialize_nullable_string(IColumn& column, Slice& slice) {
    auto& null_column = assert_cast<ColumnNullable&>(column);
    if (_empty_field_as_null) {
        if (slice.size == 0) {
            null_column.insert_data(nullptr, 0);
            return Status::OK();
        }
    }
    if (_options.null_len > 0 && !(_options.converted_from_string && slice.trim_double_quotes())) {
        if (slice.compare(Slice(_options.null_format, _options.null_len)) == 0) {
            null_column.insert_data(nullptr, 0);
            return Status::OK();
        }
    }
    static DataTypeStringSerDe stringSerDe;
    auto st = stringSerDe.deserialize_one_cell_from_csv(null_column.get_nested_column(), slice,
                                                        _options);
    if (!st.ok()) {
        // fill null if fail
        null_column.insert_data(nullptr, 0); // 0 is meaningless here
        return Status::OK();
    }
    // fill not null if success
    null_column.get_null_map_data().push_back(0);
    return Status::OK();
}

Status CsvReader::_init_options() {
    // get column_separator and line_delimiter
    _value_separator = _params.file_attributes.text_params.column_separator;
    _value_separator_length = _value_separator.size();
    _line_delimiter = _params.file_attributes.text_params.line_delimiter;
    _line_delimiter_length = _line_delimiter.size();
    if (_params.file_attributes.text_params.__isset.enclose) {
        _enclose = _params.file_attributes.text_params.enclose;
    }
    if (_params.file_attributes.text_params.__isset.escape) {
        _escape = _params.file_attributes.text_params.escape;
    }

    _trim_tailing_spaces =
            (_state != nullptr && _state->trim_tailing_spaces_for_external_table_query());

    _options.escape_char = _escape;
    _options.quote_char = _enclose;

    if (_params.file_attributes.text_params.collection_delimiter.empty()) {
        _options.collection_delim = ',';
    } else {
        _options.collection_delim = _params.file_attributes.text_params.collection_delimiter[0];
    }
    if (_params.file_attributes.text_params.mapkv_delimiter.empty()) {
        _options.map_key_delim = ':';
    } else {
        _options.map_key_delim = _params.file_attributes.text_params.mapkv_delimiter[0];
    }

    if (_params.file_attributes.text_params.__isset.null_format) {
        _options.null_format = _params.file_attributes.text_params.null_format.data();
        _options.null_len = _params.file_attributes.text_params.null_format.length();
    }

    if (_params.file_attributes.__isset.trim_double_quotes) {
        _trim_double_quotes = _params.file_attributes.trim_double_quotes;
    }
    _options.converted_from_string = _trim_double_quotes;

    if (_state != nullptr) {
        _keep_cr = _state->query_options().keep_carriage_return;
    }

    if (_params.file_attributes.text_params.__isset.empty_field_as_null) {
        _empty_field_as_null = _params.file_attributes.text_params.empty_field_as_null;
    }
    return Status::OK();
}

Status CsvReader::_create_decompressor() {
    if (_file_compress_type != TFileCompressType::UNKNOWN) {
        RETURN_IF_ERROR(Decompressor::create_decompressor(_file_compress_type, &_decompressor));
    } else {
        RETURN_IF_ERROR(Decompressor::create_decompressor(_file_format_type, &_decompressor));
    }

    return Status::OK();
}

Status CsvReader::_create_file_reader(bool need_schema) {
    if (_params.file_type == TFileType::FILE_STREAM) {
        RETURN_IF_ERROR(FileFactory::create_pipe_reader(_range.load_id, &_file_reader, _state,
                                                        need_schema));
    } else {
        _file_description.mtime = _range.__isset.modification_time ? _range.modification_time : 0;
        io::FileReaderOptions reader_options =
                FileFactory::get_reader_options(_state, _file_description);
        auto file_reader = DORIS_TRY(io::DelegateReader::create_file_reader(
                _profile, _system_properties, _file_description, reader_options,
                io::DelegateReader::AccessMode::SEQUENTIAL, _io_ctx,
                io::PrefetchRange(_range.start_offset, _range.start_offset + _range.size)));
        _file_reader = _io_ctx ? std::make_shared<io::TracingFileReader>(std::move(file_reader),
                                                                         _io_ctx->file_reader_stats)
                               : file_reader;
    }
    if (_file_reader->size() == 0 && _params.file_type != TFileType::FILE_STREAM &&
        _params.file_type != TFileType::FILE_BROKER) {
        return Status::EndOfFile("init reader failed, empty csv file: " + _range.path);
    }
    return Status::OK();
}

Status CsvReader::_create_line_reader() {
    std::shared_ptr<TextLineReaderContextIf> text_line_reader_ctx;
    if (_enclose == 0) {
        text_line_reader_ctx = std::make_shared<PlainTextLineReaderCtx>(
                _line_delimiter, _line_delimiter_length, _keep_cr);
        _fields_splitter = std::make_unique<PlainCsvTextFieldSplitter>(
                _trim_tailing_spaces, false, _value_separator, _value_separator_length, -1);

    } else {
        // in load task, the _file_slot_descs is empty vector, so we need to set col_sep_num to 0
        size_t col_sep_num = _file_slot_descs.size() > 1 ? _file_slot_descs.size() - 1 : 0;
        text_line_reader_ctx = std::make_shared<EncloseCsvLineReaderCtx>(
                _line_delimiter, _line_delimiter_length, _value_separator, _value_separator_length,
                col_sep_num, _enclose, _escape, _keep_cr);

        _fields_splitter = std::make_unique<EncloseCsvTextFieldSplitter>(
                _trim_tailing_spaces, true,
                std::static_pointer_cast<EncloseCsvLineReaderCtx>(text_line_reader_ctx),
                _value_separator_length, _enclose);
    }
    switch (_file_format_type) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        [[fallthrough]];
    case TFileFormatType::FORMAT_CSV_GZ:
        [[fallthrough]];
    case TFileFormatType::FORMAT_CSV_BZ2:
        [[fallthrough]];
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
        [[fallthrough]];
    case TFileFormatType::FORMAT_CSV_LZ4BLOCK:
        [[fallthrough]];
    case TFileFormatType::FORMAT_CSV_LZOP:
        [[fallthrough]];
    case TFileFormatType::FORMAT_CSV_SNAPPYBLOCK:
        [[fallthrough]];
    case TFileFormatType::FORMAT_CSV_DEFLATE:
        _line_reader =
                NewPlainTextLineReader::create_unique(_profile, _file_reader, _decompressor.get(),
                                                      text_line_reader_ctx, _size, _start_offset);

        break;
    case TFileFormatType::FORMAT_PROTO:
        _fields_splitter = std::make_unique<CsvProtoFieldSplitter>();
        _line_reader = NewPlainBinaryLineReader::create_unique(_file_reader);
        break;
    default:
        return Status::InternalError<false>(
                "Unknown format type, cannot init line reader in csv reader, type={}",
                _file_format_type);
    }
    return Status::OK();
}

Status CsvReader::_deserialize_one_cell(DataTypeSerDeSPtr serde, IColumn& column, Slice& slice) {
    return serde->deserialize_one_cell_from_csv(column, slice, _options);
}

Status CsvReader::_fill_dest_columns(const Slice& line, Block* block,
                                     std::vector<MutableColumnPtr>& columns, size_t* rows) {
    bool is_success = false;

    RETURN_IF_ERROR(_line_split_to_values(line, &is_success));
    if (UNLIKELY(!is_success)) {
        // If not success, which means we met an invalid row, filter this row and return.
        return Status::OK();
    }

    for (int i = 0; i < _file_slot_descs.size(); ++i) {
        int col_idx = _col_idxs[i];
        // col idx is out of range, fill with null format
        auto value = col_idx < _split_values.size()
                             ? _split_values[col_idx]
                             : Slice(_options.null_format, _options.null_len);

        IColumn* col_ptr = columns[i].get();
        if (!_is_load) {
            col_ptr = const_cast<IColumn*>(
                    block->get_by_position(_file_slot_idx_map[i]).column.get());
        }

        if (_use_nullable_string_opt[i]) {
            // For load task, we always read "string" from file.
            // So serdes[i] here must be DataTypeNullableSerDe, and DataTypeNullableSerDe -> nested_serde must be DataTypeStringSerDe.
            // So we use deserialize_nullable_string and stringSerDe to reduce virtual function calls.
            RETURN_IF_ERROR(_deserialize_nullable_string(*col_ptr, value));
        } else {
            RETURN_IF_ERROR(_deserialize_one_cell(_serdes[i], *col_ptr, value));
        }
    }
    ++(*rows);

    return Status::OK();
}

Status CsvReader::_fill_empty_line(Block* block, std::vector<MutableColumnPtr>& columns,
                                   size_t* rows) {
    for (int i = 0; i < _file_slot_descs.size(); ++i) {
        IColumn* col_ptr = columns[i].get();
        if (!_is_load) {
            col_ptr = const_cast<IColumn*>(
                    block->get_by_position(_file_slot_idx_map[i]).column.get());
        }
        auto& null_column = assert_cast<ColumnNullable&>(*col_ptr);
        null_column.insert_data(nullptr, 0);
    }
    ++(*rows);
    return Status::OK();
}

Status CsvReader::_validate_line(const Slice& line, bool* success) {
    if (!_is_proto_format && !validate_utf8(_params, line.data, line.size)) {
        if (!_is_load) {
            return Status::InternalError<false>("Only support csv data in utf8 codec");
        } else {
            _counter->num_rows_filtered++;
            *success = false;
            RETURN_IF_ERROR(_state->append_error_msg_to_file(
                    [&]() -> std::string { return std::string(line.data, line.size); },
                    [&]() -> std::string {
                        fmt::memory_buffer error_msg;
                        fmt::format_to(error_msg, "{}{}",
                                       "Unable to display, only support csv data in utf8 codec",
                                       ", please check the data encoding");
                        return fmt::to_string(error_msg);
                    }));
            return Status::OK();
        }
    }
    *success = true;
    return Status::OK();
}

Status CsvReader::_line_split_to_values(const Slice& line, bool* success) {
    _split_line(line);

    if (_is_load) {
        // Only check for load task. For query task, the non exist column will be filled "null".
        // if actual column number in csv file is not equal to _file_slot_descs.size()
        // then filter this line.
        bool ignore_col = false;
        ignore_col = _params.__isset.file_attributes &&
                     _params.file_attributes.__isset.ignore_csv_redundant_col &&
                     _params.file_attributes.ignore_csv_redundant_col;

        if ((!ignore_col && _split_values.size() != _file_slot_descs.size()) ||
            (ignore_col && _split_values.size() < _file_slot_descs.size())) {
            std::string cmp_str =
                    _split_values.size() > _file_slot_descs.size() ? "more than" : "less than";
            _counter->num_rows_filtered++;
            *success = false;
            RETURN_IF_ERROR(_state->append_error_msg_to_file(
                    [&]() -> std::string { return std::string(line.data, line.size); },
                    [&]() -> std::string {
                        fmt::memory_buffer error_msg;
                        fmt::format_to(error_msg, "{} {} {}",
                                       "actual column number in csv file is ", cmp_str,
                                       " schema column number.");
                        fmt::format_to(error_msg, "actual number: {}, schema column number: {}; ",
                                       _split_values.size(), _file_slot_descs.size());
                        fmt::format_to(error_msg, "line delimiter: [{}], column separator: [{}], ",
                                       _line_delimiter, _value_separator);
                        if (_enclose != 0) {
                            fmt::format_to(error_msg, "enclose:[{}] ", _enclose);
                        }
                        if (_escape != 0) {
                            fmt::format_to(error_msg, "escape:[{}] ", _escape);
                        }
                        fmt::memory_buffer values;
                        for (const auto& value : _split_values) {
                            fmt::format_to(values, "{}, ", value.to_string());
                        }
                        fmt::format_to(error_msg, "result values:[{}]", fmt::to_string(values));
                        return fmt::to_string(error_msg);
                    }));
            return Status::OK();
        }
    }

    *success = true;
    return Status::OK();
}

void CsvReader::_split_line(const Slice& line) {
    _split_values.clear();
    _fields_splitter->split_line(line, &_split_values);
}

Status CsvReader::_parse_col_nums(size_t* col_nums) {
    const uint8_t* ptr = nullptr;
    size_t size = 0;
    RETURN_IF_ERROR(_line_reader->read_line(&ptr, &size, &_line_reader_eof, _io_ctx));
    if (size == 0) {
        return Status::InternalError<false>(
                "The first line is empty, can not parse column numbers");
    }
    if (!validate_utf8(_params, const_cast<char*>(reinterpret_cast<const char*>(ptr)), size)) {
        return Status::InternalError<false>("Only support csv data in utf8 codec");
    }
    ptr = _remove_bom(ptr, size);
    _split_line(Slice(ptr, size));
    *col_nums = _split_values.size();
    return Status::OK();
}

Status CsvReader::_parse_col_names(std::vector<std::string>* col_names) {
    const uint8_t* ptr = nullptr;
    size_t size = 0;
    // no use of _line_reader_eof
    RETURN_IF_ERROR(_line_reader->read_line(&ptr, &size, &_line_reader_eof, _io_ctx));
    if (size == 0) {
        return Status::InternalError<false>("The first line is empty, can not parse column names");
    }
    if (!validate_utf8(_params, const_cast<char*>(reinterpret_cast<const char*>(ptr)), size)) {
        return Status::InternalError<false>("Only support csv data in utf8 codec");
    }
    ptr = _remove_bom(ptr, size);
    _split_line(Slice(ptr, size));
    for (auto _split_value : _split_values) {
        col_names->emplace_back(_split_value.to_string());
    }
    return Status::OK();
}

// TODO(ftw): parse type
Status CsvReader::_parse_col_types(size_t col_nums, std::vector<DataTypePtr>* col_types) {
    // delete after.
    for (size_t i = 0; i < col_nums; ++i) {
        col_types->emplace_back(make_nullable(std::make_shared<DataTypeString>()));
    }

    // 1. check _line_reader_eof
    // 2. read line
    // 3. check utf8
    // 4. check size
    // 5. check _split_values.size must equal to col_nums.
    // 6. fill col_types
    return Status::OK();
}

const uint8_t* CsvReader::_remove_bom(const uint8_t* ptr, size_t& size) {
    if (size >= 3 && ptr[0] == 0xEF && ptr[1] == 0xBB && ptr[2] == 0xBF) {
        LOG(INFO) << "remove bom";
        size -= 3;
        return ptr + 3;
    }
    return ptr;
}

Status CsvReader::close() {
    if (_line_reader) {
        _line_reader->close();
    }

    if (_file_reader) {
        RETURN_IF_ERROR(_file_reader->close());
    }

    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
