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
              const std::vector<SlotDescriptor*>& file_slot_descs, FileReader* file_reader);
    ~CsvReader() override;

    Status init_reader();
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

private:
    Status _create_decompressor(TFileFormatType::type type);
    Status _fill_dest_columns(const Slice& line, std::vector<MutableColumnPtr>& columns);
    Status _line_split_to_values(const Slice& line, bool* success);
    void _split_line(const Slice& line);
    Status _check_array_format(std::vector<Slice>& split_values, bool* is_success);
    bool _is_null(const Slice& slice);
    bool _is_array(const Slice& slice);

private:
    RuntimeState* _state;
    RuntimeProfile* _profile;
    ScannerCounter* _counter;
    const TFileScanRangeParams& _params;
    const TFileRangeDesc& _range;
    const std::vector<SlotDescriptor*>& _file_slot_descs;

    FileReader* _file_reader;
    std::unique_ptr<LineReader> _line_reader;
    bool _line_reader_eof;
    std::unique_ptr<TextConverter> _text_converter;
    Decompressor* _decompressor;

    TFileFormatType::type _file_format_type;
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
