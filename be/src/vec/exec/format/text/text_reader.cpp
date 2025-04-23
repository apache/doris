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

#include "text_reader.h"

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "exec/decompressor.h"
#include "exec/line_reader.h"
#include "io/file_factory.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "io/fs/s3_file_reader.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/file_reader/new_plain_text_line_reader.h"
#include "vec/exec/scan/scanner.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

void HiveCsvTextFieldSplitter::do_split(const Slice& line, std::vector<Slice>* splitted_values) {
    const char* data = line.data;
    const size_t size = line.size;
    size_t value_start = 0;
    for (size_t i = 0; i < size; ++i) {
        if (data[i] == _value_sep[0]) {
            // hive will escape the field separator in string
            if (_escape_char != 0 && i > 0 && data[i - 1] == _escape_char) {
                continue;
            }
            process_value_func(data, value_start, i - value_start, _trimming_char, splitted_values);
            value_start = i + _value_sep_len;
        }
    }
    process_value_func(data, value_start, size - value_start, _trimming_char, splitted_values);
}

TextReader::TextReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
                       const TFileScanRangeParams& params, const TFileRangeDesc& range,
                       const std::vector<SlotDescriptor*>& file_slot_descs, io::IOContext* io_ctx)
        : _state(state),
          _profile(profile),
          _counter(counter),
          _params(params),
          _range(range),
          _file_slot_descs(file_slot_descs),
          _io_ctx(io_ctx) {
    _file_format_type = _params.format_type;
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

TextReader::~TextReader() = default;

void TextReader::_init_system_properties() {
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

void TextReader::_init_file_description() {
    _file_description.path = _range.path;
    _file_description.file_size = _range.__isset.file_size ? _range.file_size : -1;
    if (_range.__isset.fs_name) {
        _file_description.fs_name = _range.fs_name;
    }
}

Status TextReader::init_reader(bool is_load) {
    RETURN_IF_ERROR(_init_options());
    RETURN_IF_ERROR(_create_file_reader());
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

Status TextReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_line_reader_eof) {
        *eof = true;
        return Status::OK();
    }

    const int batch_size = std::max(_state->batch_size(), (int)_MIN_BATCH_SIZE);
    size_t rows = 0;

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
                // Read empty line, continue
                continue;
            }
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
                // Read empty line, continue
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

Status TextReader::get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                               std::unordered_set<std::string>* missing_cols) {
    for (const auto& slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

Status TextReader::get_parsed_schema(std::vector<std::string>* col_names,
                                     std::vector<DataTypePtr>* col_types) {
    int64_t start_offset = _range.start_offset;
    if (start_offset != 0) {
        return Status::InvalidArgument(
                "start offset of TFileRangeDesc must be zero in get parsered schema");
    }
    if (_params.file_type == TFileType::FILE_BROKER) {
        return Status::InternalError<false>(
                "Getting parsered schema from text file do not support stream load and broker "
                "load.");
    }
    RETURN_IF_ERROR(_init_options());
    RETURN_IF_ERROR(_create_file_reader());
    RETURN_IF_ERROR(_create_decompressor());
    RETURN_IF_ERROR(_create_line_reader());

    // for text file, we don't consider header line
    // just to parse one line to get the column number
    // TODO: parse all fields of hive text file as string may cause some problems
    size_t col_nums = 0;
    RETURN_IF_ERROR(_parse_col_nums(&col_nums));
    for (size_t i = 0; i < col_nums; ++i) {
        col_names->emplace_back("c" + std::to_string(i + 1));
    }

    for (size_t j = 0; j < col_names->size(); ++j) {
        col_types->emplace_back(
                DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, true));
    }

    return Status::OK();
}

Status TextReader::_parse_col_nums(size_t* col_nums) {
    const uint8_t* ptr = nullptr;
    size_t size = 0;
    RETURN_IF_ERROR(_line_reader->read_line(&ptr, &size, &_line_reader_eof, _io_ctx));
    if (size == 0) {
        return Status::InternalError<false>(
                "The first line is empty, can not parse column numbers");
    }
    ptr = _remove_bom(ptr, size);
    _split_line(Slice(ptr, size));
    *col_nums = _split_values.size();
    return Status::OK();
}

Status TextReader::_init_options() {
    // get column_separator and line_delimiter
    _value_separator = _params.file_attributes.text_params.column_separator;
    _value_separator_length = _value_separator.size();
    _line_delimiter = _params.file_attributes.text_params.line_delimiter;
    _line_delimiter_length = _line_delimiter.size();

    if (_params.file_attributes.text_params.__isset.escape) {
        _escape = _params.file_attributes.text_params.escape;
    }
    _options.escape_char = _escape;

    _options.collection_delim = _params.file_attributes.text_params.collection_delimiter[0];
    _options.map_key_delim = _params.file_attributes.text_params.mapkv_delimiter[0];

    _options.null_format = _params.file_attributes.text_params.null_format.data();
    _options.null_len = _params.file_attributes.text_params.null_format.length();

    return Status::OK();
}

Status TextReader::_create_decompressor() {
    // for compatibility
    if (_file_compress_type != TFileCompressType::UNKNOWN) {
        RETURN_IF_ERROR(Decompressor::create_decompressor(_file_compress_type, &_decompressor));
    } else {
        RETURN_IF_ERROR(Decompressor::create_decompressor(_file_format_type, &_decompressor));
    }
    return Status::OK();
}

Status TextReader::_create_file_reader() {
    if (_params.file_type == TFileType::FILE_STREAM) {
        RETURN_IF_ERROR(
                FileFactory::create_pipe_reader(_range.load_id, &_file_reader, _state, false));
    } else {
        _file_description.mtime = _range.__isset.modification_time ? _range.modification_time : 0;
        io::FileReaderOptions reader_options =
                FileFactory::get_reader_options(_state, _file_description);
        _file_reader = DORIS_TRY(io::DelegateReader::create_file_reader(
                _profile, _system_properties, _file_description, reader_options,
                io::DelegateReader::AccessMode::SEQUENTIAL, _io_ctx,
                io::PrefetchRange(_range.start_offset, _range.start_offset + _range.size)));
    }
    if (_file_reader->size() == 0 && _params.file_type != TFileType::FILE_STREAM &&
        _params.file_type != TFileType::FILE_BROKER) {
        return Status::EndOfFile("init reader failed, empty csv file: " + _range.path);
    }
    return Status::OK();
}

Status TextReader::_create_line_reader() {
    // set the skip lines and start offset
    _start_offset = _range.start_offset;
    if (_start_offset == 0) {
        // check header typer first
        if (_params.file_attributes.__isset.skip_lines) {
            _skip_lines = _params.file_attributes.skip_lines;
        }
    } else {
        // not first range, check if the file is compressed
        if (_file_compress_type != TFileCompressType::UNKNOWN &&
            _file_compress_type != TFileCompressType::PLAIN) {
            return Status::InternalError<false>("For now we do not support split compressed file");
        }
        _start_offset -= 1;
        _size += 1;
        // not first range will always skip one line
        _skip_lines = 1;
    }

    std::shared_ptr<TextLineReaderContextIf> text_line_reader_ctx;

    text_line_reader_ctx = std::make_shared<PlainTextLineReaderCtx>(_line_delimiter,
                                                                    _line_delimiter_length, false);

    _fields_splitter = std::make_unique<HiveCsvTextFieldSplitter>(
            false, false, _value_separator, _value_separator_length, -1, _escape);

    _line_reader =
            NewPlainTextLineReader::create_unique(_profile, _file_reader, _decompressor.get(),
                                                  text_line_reader_ctx, _size, _start_offset);

    return Status::OK();
}

Status TextReader::_fill_dest_columns(const Slice& line, Block* block,
                                      std::vector<MutableColumnPtr>& columns, size_t* rows) {
    bool is_success = false;

    RETURN_IF_ERROR(_line_split_to_values(line, &is_success));
    if (UNLIKELY(!is_success)) {
        // If not success, which means we met an invalid row, filter this row and return.
        return Status::OK();
    }

    for (int i = 0; i < _file_slot_descs.size(); ++i) {
        int col_idx = _col_idxs[i];
        // col idx is out of range, fill with null.
        auto value = col_idx < _split_values.size()
                             ? _split_values[col_idx]
                             : Slice(_options.null_format, _options.null_len);

        IColumn* col_ptr = columns[i].get();
        if (!_is_load) {
            col_ptr = const_cast<IColumn*>(
                    block->get_by_position(_file_slot_idx_map[i]).column.get());
        }
        RETURN_IF_ERROR(_serdes[i]->deserialize_one_cell_from_hive_text(*col_ptr, value, _options));
    }

    ++(*rows);

    return Status::OK();
}

Status TextReader::_line_split_to_values(const Slice& line, bool* success) {
    _split_values.clear();
    _fields_splitter->split_line(line, &_split_values);
    if (_is_load) {
        // Only check for load task. For query task, the non exist column will be filled "null".
        // if actual column number in text file is not equal to _file_slot_descs.size()
        // then filter this line.
        bool ignore_col = false;
        // TODO: check the props of hive text load task
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

void TextReader::_split_line(const Slice& line) {
    _split_values.clear();
    _fields_splitter->split_line(line, &_split_values);
}

const uint8_t* TextReader::_remove_bom(const uint8_t* ptr, size_t& size) {
    if (size >= 3 && ptr[0] == 0xEF && ptr[1] == 0xBB && ptr[2] == 0xBF) {
        LOG(INFO) << "remove bom";
        size -= 3;
        return ptr + 3;
    }
    return ptr;
}

Status TextReader::close() {
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
