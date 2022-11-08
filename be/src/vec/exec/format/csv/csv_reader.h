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

#include "vec/exec/format/generic_reader.h"
namespace doris {

class FileReader;
class LineReader;
class TextConverter;
class Decompressor;
class SlotDescriptor;

namespace vectorized {

struct ScannerCounter;
class CsvReader : public GenericReader {
public:
    CsvReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
              const TFileScanRangeParams& params, const TFileRangeDesc& range,
              const std::vector<SlotDescriptor*>& file_slot_descs);

    CsvReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
              const TFileRangeDesc& range, const std::vector<SlotDescriptor*>& file_slot_descs);
    ~CsvReader() override;

    Status init_reader(bool is_query);
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    // get schema of csv file from first one line or first two lines.
    // if file format is FORMAT_CSV_DEFLATE and if
    // 1. header_type is empty, get schema from first line.
    // 2. header_type is CSV_WITH_NAMES, get schema from first line.
    // 3. header_type is CSV_WITH_NAMES_AND_TYPES, get schema from first two line.
    Status get_parsered_schema(std::vector<std::string>* col_names,
                               std::vector<TypeDescriptor>* col_types) override;

private:
    // used for stream/broker load of csv file.
    Status _create_decompressor();
    Status _fill_dest_columns(const Slice& line, Block* block, size_t* rows);
    Status _line_split_to_values(const Slice& line, bool* success);
    void _split_line(const Slice& line);
    Status _check_array_format(std::vector<Slice>& split_values, bool* is_success);
    bool _is_null(const Slice& slice);
    bool _is_array(const Slice& slice);

    // used for parse table schema of csv file.
    Status _prepare_parse(size_t* read_line, bool* is_parse_name);
    Status _parse_col_nums(size_t* col_nums);
    Status _parse_col_names(std::vector<std::string>* col_names);
    // TODO(ftw): parse type
    Status _parse_col_types(size_t col_nums, std::vector<TypeDescriptor>* col_types);

private:
    RuntimeState* _state;
    RuntimeProfile* _profile;
    ScannerCounter* _counter;
    const TFileScanRangeParams& _params;
    const TFileRangeDesc& _range;
    const std::vector<SlotDescriptor*>& _file_slot_descs;
    // Only for query task, save the columns' index which need to be read.
    // eg, there are 3 cols in "_file_slot_descs" named: k1, k2, k3
    // and the corressponding position in file is 0, 3, 5.
    // So the _col_idx will be: <0, 3, 5>
    std::vector<int> _col_idxs;
    // True if this is a load task
    bool _is_load = false;

    // _file_reader_s is for stream load pipe reader,
    // and _file_reader is for other file reader.
    // TODO: refactor this to use only shared_ptr or unique_ptr
    std::unique_ptr<FileReader> _file_reader;
    std::shared_ptr<FileReader> _file_reader_s;
    std::unique_ptr<LineReader> _line_reader;
    bool _line_reader_eof;
    std::unique_ptr<TextConverter> _text_converter;
    std::unique_ptr<Decompressor> _decompressor;

    TFileFormatType::type _file_format_type;
    bool _is_proto_format;
    TFileCompressType::type _file_compress_type;
    int64_t _size;
    // When we fetch range start from 0, header_type="csv_with_names" skip first line
    // When we fetch range start from 0, header_type="csv_with_names_and_types" skip first two line
    // When we fetch range doesn't start from 0 will always skip the first line
    int _skip_lines;

    std::string _value_separator;
    std::string _line_delimiter;
    int _value_separator_length;
    int _line_delimiter_length;

    // save source text which have been splitted.
    std::vector<Slice> _split_values;
};
} // namespace vectorized
} // namespace doris
