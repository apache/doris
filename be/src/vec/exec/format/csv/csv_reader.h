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

#pragma once

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/internal_service.pb.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exec/decompressor.h"
#include "io/file_factory.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "util/slice.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/file_reader/new_plain_text_line_reader.h"
#include "vec/exec/format/generic_reader.h"

namespace doris {
#include "common/compile_check_begin.h"

class SlotDescriptor;
class RuntimeProfile;
class RuntimeState;

namespace io {
struct IOContext;
} // namespace io
struct TypeDescriptor;

namespace vectorized {
struct ScannerCounter;
class Block;

class LineFieldSplitterIf {
public:
    virtual ~LineFieldSplitterIf() = default;

    virtual void split_line(const Slice& line, std::vector<Slice>* splitted_values) = 0;
};

template <typename Splitter>
class BaseLineFieldSplitter : public LineFieldSplitterIf {
public:
    inline void split_line(const Slice& line, std::vector<Slice>* splitted_values) final {
        static_cast<Splitter*>(this)->split_line_impl(line, splitted_values);
    }
};

class CsvProtoFieldSplitter final : public BaseLineFieldSplitter<CsvProtoFieldSplitter> {
public:
    inline void split_line_impl(const Slice& line, std::vector<Slice>* splitted_values) {
        auto** row_ptr = reinterpret_cast<PDataRow**>(line.data);
        PDataRow* row = *row_ptr;
        for (const PDataColumn& col : row->col()) {
            splitted_values->emplace_back(col.value());
        }
    }
};

template <typename Splitter>
class BaseCsvTextFieldSplitter : public BaseLineFieldSplitter<BaseCsvTextFieldSplitter<Splitter>> {
    // using a function ptr to decrease the overhead (found very effective during test).
    using ProcessValueFunc = void (*)(const char*, size_t, size_t, char, std::vector<Slice>*);

public:
    explicit BaseCsvTextFieldSplitter(bool trim_tailing_space, bool trim_ends,
                                      size_t value_sep_len = 1, char trimming_char = 0)
            : _trimming_char(trimming_char), _value_sep_len(value_sep_len) {
        if (trim_tailing_space) {
            if (trim_ends) {
                process_value_func = &BaseCsvTextFieldSplitter::_process_value<true, true>;
            } else {
                process_value_func = &BaseCsvTextFieldSplitter::_process_value<true, false>;
            }
        } else {
            if (trim_ends) {
                process_value_func = &BaseCsvTextFieldSplitter::_process_value<false, true>;
            } else {
                process_value_func = &BaseCsvTextFieldSplitter::_process_value<false, false>;
            }
        }
    }

    inline void split_line_impl(const Slice& line, std::vector<Slice>* splitted_values) {
        static_cast<Splitter*>(this)->do_split(line, splitted_values);
    }

protected:
    const char _trimming_char;
    const size_t _value_sep_len;
    ProcessValueFunc process_value_func;

private:
    template <bool TrimTailingSpace, bool TrimEnds>
    inline static void _process_value(const char* data, size_t start_offset, size_t value_len,
                                      char trimming_char, std::vector<Slice>* splitted_values) {
        if constexpr (TrimTailingSpace) {
            while (value_len > 0 && *(data + start_offset + value_len - 1) == ' ') {
                --value_len;
            }
        }
        if constexpr (TrimEnds) {
            const bool trim_cond = value_len > 1 && *(data + start_offset) == trimming_char &&
                                   *(data + start_offset + value_len - 1) == trimming_char;
            if (trim_cond) {
                ++(start_offset);
                value_len -= 2;
            }
        }
        splitted_values->emplace_back(data + start_offset, value_len);
    }
};

class EncloseCsvTextFieldSplitter : public BaseCsvTextFieldSplitter<EncloseCsvTextFieldSplitter> {
public:
    explicit EncloseCsvTextFieldSplitter(bool trim_tailing_space, bool trim_ends,
                                         std::shared_ptr<EncloseCsvLineReaderCtx> line_reader_ctx,
                                         size_t value_sep_len = 1, char trimming_char = 0)
            : BaseCsvTextFieldSplitter(trim_tailing_space, trim_ends, value_sep_len, trimming_char),
              _text_line_reader_ctx(std::move(line_reader_ctx)) {}

    void do_split(const Slice& line, std::vector<Slice>* splitted_values);

private:
    std::shared_ptr<EncloseCsvLineReaderCtx> _text_line_reader_ctx;
};

class PlainCsvTextFieldSplitter : public BaseCsvTextFieldSplitter<PlainCsvTextFieldSplitter> {
public:
    explicit PlainCsvTextFieldSplitter(bool trim_tailing_space, bool trim_ends,
                                       std::string value_sep, size_t value_sep_len = 1,
                                       char trimming_char = 0)
            : BaseCsvTextFieldSplitter(trim_tailing_space, trim_ends, value_sep_len, trimming_char),
              _value_sep(std::move(value_sep)) {
        is_single_char_delim = (value_sep_len == 1);
    }

    void do_split(const Slice& line, std::vector<Slice>* splitted_values);

private:
    void _split_field_single_char(const Slice& line, std::vector<Slice>* splitted_values);
    void _split_field_multi_char(const Slice& line, std::vector<Slice>* splitted_values);

    bool is_single_char_delim;
    std::string _value_sep;
};

class CsvReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(CsvReader);

public:
    CsvReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
              const TFileScanRangeParams& params, const TFileRangeDesc& range,
              const std::vector<SlotDescriptor*>& file_slot_descs, io::IOContext* io_ctx);
    ~CsvReader() override = default;

    Status init_reader(bool is_load);
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    Status init_schema_reader() override;
    // get schema of csv file from first one line or first two lines.
    // if file format is FORMAT_CSV_DEFLATE and if
    // 1. header_type is empty, get schema from first line.
    // 2. header_type is CSV_WITH_NAMES, get schema from first line.
    // 3. header_type is CSV_WITH_NAMES_AND_TYPES, get schema from first two line.
    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<TypeDescriptor>* col_types) override;

    Status close() override;

protected:
    // init options for type serde
    virtual Status _init_options();
    virtual Status _create_line_reader();
    virtual Status _deserialize_one_cell(DataTypeSerDeSPtr serde, IColumn& column, Slice& slice);
    virtual Status _deserialize_nullable_string(IColumn& column, Slice& slice);
    // check the utf8 encoding of a line.
    // return error status to stop processing.
    // If return Status::OK but "success" is false, which means this is load request
    // and the line is skipped as unqualified row, and the process should continue.
    virtual Status _validate_line(const Slice& line, bool* success);

    RuntimeProfile* _profile = nullptr;
    const TFileScanRangeParams& _params;
    std::string _value_separator;
    size_t _value_separator_length;
    std::string _line_delimiter;
    size_t _line_delimiter_length;
    char _escape = 0;
    vectorized::DataTypeSerDeSPtrs _serdes;
    vectorized::DataTypeSerDe::FormatOptions _options;
    std::unique_ptr<LineFieldSplitterIf> _fields_splitter;
    int64_t _start_offset;
    int64_t _size;
    io::FileReaderSPtr _file_reader;
    std::unique_ptr<LineReader> _line_reader;
    std::unique_ptr<Decompressor> _decompressor;

private:
    Status _create_decompressor();
    Status _create_file_reader(bool need_schema);
    Status _fill_dest_columns(const Slice& line, Block* block,
                              std::vector<MutableColumnPtr>& columns, size_t* rows);
    Status _fill_empty_line(Block* block, std::vector<MutableColumnPtr>& columns, size_t* rows);
    Status _line_split_to_values(const Slice& line, bool* success);
    void _split_line(const Slice& line);
    void _init_system_properties();
    void _init_file_description();

    Status _parse_col_nums(size_t* col_nums);
    Status _parse_col_names(std::vector<std::string>* col_names);
    // TODO(ftw): parse type
    Status _parse_col_types(size_t col_nums, std::vector<TypeDescriptor>* col_types);

    // If the CSV file is an UTF8 encoding with BOM,
    // then remove the first 3 bytes at the beginning of this file
    // and set size = size - 3.
    const uint8_t* _remove_bom(const uint8_t* ptr, size_t& size);

    RuntimeState* _state = nullptr;
    ScannerCounter* _counter = nullptr;
    const TFileRangeDesc& _range;
    io::FileSystemProperties _system_properties;
    io::FileDescription _file_description;
    const std::vector<SlotDescriptor*>& _file_slot_descs;
    // Only for query task, save the file slot to columns in block map.
    // eg, there are 3 cols in "_file_slot_descs" named: k1, k2, k3
    // and this 3 columns in block are k2, k3, k1,
    // the _file_slot_idx_map will save: 2, 0, 1
    std::vector<int> _file_slot_idx_map;
    // Only for query task, save the columns' index which need to be read.
    // eg, there are 3 cols in "_file_slot_descs" named: k1, k2, k3
    // and the corresponding position in file is 0, 3, 5.
    // So the _col_idx will be: <0, 3, 5>
    std::vector<int> _col_idxs;
    // True if this is a load task
    bool _is_load = false;
    bool _line_reader_eof;
    // For schema reader
    size_t _read_line = 0;
    bool _is_parse_name = false;
    TFileFormatType::type _file_format_type;
    bool _is_proto_format;
    TFileCompressType::type _file_compress_type;

    // When we fetch range start from 0, header_type="csv_with_names" skip first line
    // When we fetch range start from 0, header_type="csv_with_names_and_types" skip first two line
    // When we fetch range doesn't start from 0 will always skip the first line
    int _skip_lines;
    char _enclose = 0;
    bool _trim_double_quotes = false;
    bool _trim_tailing_spaces = false;
    bool _keep_cr = false;

    io::IOContext* _io_ctx = nullptr;
    // save source text which have been splitted.
    std::vector<Slice> _split_values;
    std::vector<int> _use_nullable_string_opt;
};
} // namespace vectorized
#include "common/compile_check_end.h"
} // namespace doris
