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

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/internal_service.pb.h>

#include "common/consts.h"
#include "common/status.h"
#include "exec/decompressor.h"
#include "exec/plain_binary_line_reader.h"
#include "exec/plain_text_line_reader.h"
#include "exec/text_converter.h"
#include "exec/text_converter.hpp"
#include "util/string_util.h"
#include "util/utf8_check.h"
#include "vec/core/block.h"
#include "vec/exec/scan/vfile_scanner.h"

namespace doris::vectorized {
CsvReader::CsvReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
                     const TFileScanRangeParams& params, const TFileRangeDesc& range,
                     const std::vector<SlotDescriptor*>& file_slot_descs)
        : _state(state),
          _profile(profile),
          _counter(counter),
          _params(params),
          _range(range),
          _file_slot_descs(file_slot_descs),
          _line_reader(nullptr),
          _line_reader_eof(false),
          _text_converter(nullptr),
          _decompressor(nullptr),
          _skip_lines(0) {
    _file_format_type = _params.format_type;
    _file_compress_type = _params.compress_type;
    _size = _range.size;

    _text_converter.reset(new (std::nothrow) TextConverter('\\'));
    _split_values.reserve(sizeof(Slice) * _file_slot_descs.size());
}

CsvReader::~CsvReader() {}

Status CsvReader::init_reader() {
    // set the skip lines and start offset
    int64_t start_offset = _range.start_offset;
    if (start_offset == 0 && _params.__isset.file_attributes &&
        _params.file_attributes.__isset.header_type &&
        _params.file_attributes.header_type.size() > 0) {
        std::string header_type = to_lower(_params.file_attributes.header_type);
        if (header_type == BeConsts::CSV_WITH_NAMES) {
            _skip_lines = 1;
        } else if (header_type == BeConsts::CSV_WITH_NAMES_AND_TYPES) {
            _skip_lines = 2;
        }
    } else if (start_offset != 0) {
        if (_file_format_type != TFileFormatType::FORMAT_CSV_PLAIN ||
            (_file_compress_type != TFileCompressType::UNKNOWN &&
             _file_compress_type != TFileCompressType::PLAIN)) {
            return Status::InternalError("For now we do not support split compressed file");
        }
        start_offset -= 1;
        _size += 1;
        // not first range will always skip one line
        _skip_lines = 1;
    }

    // create and open file reader
    FileReader* real_reader = nullptr;
    if (_params.file_type == TFileType::FILE_STREAM) {
        RETURN_IF_ERROR(FileFactory::create_pipe_reader(_range.load_id, _file_reader_s));
        real_reader = _file_reader_s.get();
    } else {
        RETURN_IF_ERROR(FileFactory::create_file_reader(
                _profile, _params, _range.path, start_offset, _range.file_size, 0, _file_reader));
        real_reader = _file_reader.get();
    }
    RETURN_IF_ERROR(real_reader->open());
    if (real_reader->size() == 0 && _params.file_type != TFileType::FILE_STREAM &&
        _params.file_type != TFileType::FILE_BROKER) {
        return Status::EndOfFile("Empty File");
    }

    // get column_separator and line_delimiter
    _value_separator = _params.file_attributes.text_params.column_separator;
    _value_separator_length = _value_separator.size();
    _line_delimiter = _params.file_attributes.text_params.line_delimiter;
    _line_delimiter_length = _line_delimiter.size();

    // create decompressor.
    // _decompressor may be nullptr if this is not a compressed file
    RETURN_IF_ERROR(_create_decompressor());

    switch (_file_format_type) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
    case TFileFormatType::FORMAT_CSV_GZ:
    case TFileFormatType::FORMAT_CSV_BZ2:
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
    case TFileFormatType::FORMAT_CSV_LZOP:
    case TFileFormatType::FORMAT_CSV_DEFLATE:
        _line_reader.reset(new PlainTextLineReader(_profile, real_reader, _decompressor.get(),
                                                   _size, _line_delimiter, _line_delimiter_length));

        break;
    default:
        return Status::InternalError(
                "Unknown format type, cannot init line reader in csv reader, type={}",
                _file_format_type);
    }

    _line_reader_eof = false;
    return Status::OK();
}

Status CsvReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_line_reader_eof == true) {
        *eof = true;
        return Status::OK();
    }

    const int batch_size = _state->batch_size();
    auto columns = block->mutate_columns();

    while (columns[0]->size() < batch_size && !_line_reader_eof) {
        const uint8_t* ptr = nullptr;
        size_t size = 0;
        RETURN_IF_ERROR(_line_reader->read_line(&ptr, &size, &_line_reader_eof));
        if (_skip_lines > 0) {
            _skip_lines--;
            continue;
        }
        if (size == 0) {
            // Read empty row, just continue
            continue;
        }

        // TODO(ftw): check read_rows?
        ++(*read_rows);
        RETURN_IF_ERROR(_fill_dest_columns(Slice(ptr, size), columns));

        if (_line_reader_eof == true) {
            *eof = true;
            break;
        }
    }
    columns.clear();

    return Status::OK();
}

Status CsvReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                              std::unordered_set<std::string>* missing_cols) {
    for (auto& slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

Status CsvReader::_create_decompressor() {
    CompressType compress_type;
    if (_file_compress_type != TFileCompressType::UNKNOWN) {
        switch (_file_compress_type) {
        case TFileCompressType::PLAIN:
            compress_type = CompressType::UNCOMPRESSED;
            break;
        case TFileCompressType::GZ:
            compress_type = CompressType::GZIP;
            break;
        case TFileCompressType::LZO:
            compress_type = CompressType::LZOP;
            break;
        case TFileCompressType::BZ2:
            compress_type = CompressType::BZIP2;
            break;
        case TFileCompressType::LZ4FRAME:
            compress_type = CompressType::LZ4FRAME;
            break;
        case TFileCompressType::DEFLATE:
            compress_type = CompressType::DEFLATE;
            break;
        default:
            return Status::InternalError("unknown compress type: {}", _file_compress_type);
        }
    } else {
        switch (_file_format_type) {
        case TFileFormatType::FORMAT_CSV_PLAIN:
            compress_type = CompressType::UNCOMPRESSED;
            break;
        case TFileFormatType::FORMAT_CSV_GZ:
            compress_type = CompressType::GZIP;
            break;
        case TFileFormatType::FORMAT_CSV_BZ2:
            compress_type = CompressType::BZIP2;
            break;
        case TFileFormatType::FORMAT_CSV_LZ4FRAME:
            compress_type = CompressType::LZ4FRAME;
            break;
        case TFileFormatType::FORMAT_CSV_LZOP:
            compress_type = CompressType::LZOP;
            break;
        case TFileFormatType::FORMAT_CSV_DEFLATE:
            compress_type = CompressType::DEFLATE;
            break;
        default:
            return Status::InternalError("unknown format type: {}", _file_format_type);
        }
    }
    Decompressor* decompressor;
    RETURN_IF_ERROR(Decompressor::create_decompressor(compress_type, &decompressor));
    _decompressor.reset(decompressor);

    return Status::OK();
}

Status CsvReader::_fill_dest_columns(const Slice& line, std::vector<MutableColumnPtr>& columns) {
    bool is_success = false;

    RETURN_IF_ERROR(_line_split_to_values(line, &is_success));
    if (UNLIKELY(!is_success)) {
        // If not success, which means we met an invalid row, filter this row and return.
        return Status::OK();
    }

    RETURN_IF_ERROR(_check_array_format(_split_values, &is_success));
    if (UNLIKELY(!is_success)) {
        // If not success, which means we met an invalid row, filter this row and return.
        return Status::OK();
    }

    // if _split_values.size > _file_slot_descs.size()
    // we only take the first few columns
    for (int i = 0; i < _file_slot_descs.size(); ++i) {
        // TODO(ftw): no need of src_slot_desc
        auto src_slot_desc = _file_slot_descs[i];
        const Slice& value = _split_values[i];
        _text_converter->write_string_column(src_slot_desc, &columns[i], value.data, value.size);
    }

    return Status::OK();
}

Status CsvReader::_line_split_to_values(const Slice& line, bool* success) {
    if (!validate_utf8(line.data, line.size)) {
        RETURN_IF_ERROR(_state->append_error_msg_to_file(
                []() -> std::string { return "Unable to display"; },
                []() -> std::string {
                    fmt::memory_buffer error_msg;
                    fmt::format_to(error_msg, "{}", "Unable to display");
                    return fmt::to_string(error_msg);
                },
                &_line_reader_eof));
        _counter->num_rows_filtered++;
        *success = false;
        return Status::OK();
    }

    _split_line(line);

    // if actual column number in csv file is not equal to _file_slot_descs.size()
    // then filter this line.
    if (_split_values.size() != _file_slot_descs.size()) {
        std::string cmp_str =
                _split_values.size() > _file_slot_descs.size() ? "more than" : "less than";
        RETURN_IF_ERROR(_state->append_error_msg_to_file(
                [&]() -> std::string { return std::string(line.data, line.size); },
                [&]() -> std::string {
                    fmt::memory_buffer error_msg;
                    fmt::format_to(error_msg, "{} {} {}", "actual column number in csv file is ",
                                   cmp_str, " schema column number.");
                    fmt::format_to(error_msg, "actual number: {}, column separator: [{}], ",
                                   _split_values.size(), _value_separator);
                    fmt::format_to(error_msg, "line delimiter: [{}], schema column number: {}; ",
                                   _line_delimiter, _file_slot_descs.size());
                    return fmt::to_string(error_msg);
                },
                &_line_reader_eof));
        _counter->num_rows_filtered++;
        *success = false;
        return Status::OK();
    }

    *success = true;
    return Status::OK();
}

void CsvReader::_split_line(const Slice& line) {
    _split_values.clear();
    if (_file_format_type == TFileFormatType::FORMAT_PROTO) {
        PDataRow** ptr = reinterpret_cast<PDataRow**>(line.data);
        PDataRow* row = *ptr;
        for (const PDataColumn& col : (row)->col()) {
            int len = col.value().size();
            uint8_t* buf = new uint8_t[len];
            memcpy(buf, col.value().c_str(), len);
            _split_values.emplace_back(buf, len);
        }
        delete row;
        delete[] ptr;
    } else {
        const char* value = line.data;
        size_t start = 0;     // point to the start pos of next col value.
        size_t curpos = 0;    // point to the start pos of separator matching sequence.
        size_t p1 = 0;        // point to the current pos of separator matching sequence.
        size_t non_space = 0; // point to the last pos of non_space charactor.

        // Separator: AAAA
        //
        //    p1
        //     ▼
        //     AAAA
        //   1000AAAA2000AAAA
        //   ▲   ▲
        // Start │
        //     curpos

        while (curpos < line.size) {
            if (*(value + curpos + p1) != _value_separator[p1]) {
                // Not match, move forward:
                curpos += (p1 == 0 ? 1 : p1);
                p1 = 0;
            } else {
                p1++;
                if (p1 == _value_separator_length) {
                    // Match a separator
                    non_space = curpos;
                    // Trim tailing spaces. Be consistent with hive and trino's behavior.
                    if (_state->trim_tailing_spaces_for_external_table_query()) {
                        while (non_space > start && *(value + non_space - 1) == ' ') {
                            non_space--;
                        }
                    }
                    _split_values.emplace_back(value + start, non_space - start);
                    start = curpos + _value_separator_length;
                    curpos = start;
                    p1 = 0;
                    non_space = 0;
                }
            }
        }

        CHECK(curpos == line.size) << curpos << " vs " << line.size;
        non_space = curpos;
        if (_state->trim_tailing_spaces_for_external_table_query()) {
            while (non_space > start && *(value + non_space - 1) == ' ') {
                non_space--;
            }
        }
        _split_values.emplace_back(value + start, non_space - start);
    }
}

Status CsvReader::_check_array_format(std::vector<Slice>& split_values, bool* is_success) {
    // if not the array format, filter this line and return error url
    for (int j = 0; j < _file_slot_descs.size(); ++j) {
        auto slot_desc = _file_slot_descs[j];
        if (!slot_desc->is_materialized()) {
            continue;
        }
        const Slice& value = split_values[j];
        if (slot_desc->type().is_array_type() && !_is_null(value) && !_is_array(value)) {
            RETURN_IF_ERROR(_state->append_error_msg_to_file(
                    [&]() -> std::string { return std::string(value.data, value.size); },
                    [&]() -> std::string {
                        fmt::memory_buffer err_msg;
                        fmt::format_to(err_msg, "Invalid format for array column({})",
                                       slot_desc->col_name());
                        return fmt::to_string(err_msg);
                    },
                    &_line_reader_eof));
            _counter->num_rows_filtered++;
            *is_success = false;
            return Status::OK();
        }
    }
    *is_success = true;
    return Status::OK();
}

bool CsvReader::_is_null(const Slice& slice) {
    return slice.size == 2 && slice.data[0] == '\\' && slice.data[1] == 'N';
}

bool CsvReader::_is_array(const Slice& slice) {
    return slice.size > 1 && slice.data[0] == '[' && slice.data[slice.size - 1] == ']';
}

} // namespace doris::vectorized
