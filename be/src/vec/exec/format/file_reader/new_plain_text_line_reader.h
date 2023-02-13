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

#include "exec/line_reader.h"
#include "io/fs/file_reader.h"
#include "util/runtime_profile.h"

namespace doris {

class Decompressor;
class Status;

class NewPlainTextLineReader : public LineReader {
public:
    NewPlainTextLineReader(RuntimeProfile* profile, io::FileReaderSPtr file_reader,
                           Decompressor* decompressor, size_t length,
                           const std::string& line_delimiter, size_t line_delimiter_length,
                           size_t current_offset);

    NewPlainTextLineReader(RuntimeProfile* profile, io::FileReaderSPtr file_reader,
                           Decompressor* decompressor, size_t length,
                           const std::string& line_delimiter, size_t line_delimiter_length,
                           size_t current_offset, const std::string& value_delimiter,
                           size_t value_delimiter_length,
                           bool trim_tailing_spaces_for_external_table_query,
                           bool trim_double_quotes);

    ~NewPlainTextLineReader() override;

    Status read_line(const uint8_t** ptr, size_t* size, bool* eof) override;

    Status read_fields(const uint8_t** ptr, size_t* size, bool* eof,
                       std::vector<std::pair<int, int>>* fields) override;

    void close() override;

private:
    bool update_eof();

    size_t output_buf_read_remaining() const { return _output_buf_limit - _output_buf_pos; }

    size_t input_buf_read_remaining() const { return _input_buf_limit - _input_buf_pos; }

    bool done() { return _file_eof && output_buf_read_remaining() == 0; }

    // find line delimiter from 'start' to 'start' + len,
    // return line delimiter pos if found, otherwise return nullptr.
    uint8_t* _find_line_delimiter(const uint8_t* start, size_t len);
    // save positions of fields and return and line delimiter.
    uint8_t* _update_field_pos_and_find_line_delimiter(const uint8_t* start, size_t len,
                                                       std::vector<std::pair<int, int>>* fields);

    bool _read_more_data();
    void extend_input_buf();
    void extend_output_buf();

    uint8_t* line_start() { return _output_buf + _output_buf_pos; }

    RuntimeProfile* _profile;
    io::FileReaderSPtr _file_reader;
    Decompressor* _decompressor;
    // the min length that should be read.
    // -1 means endless(for stream load)
    // and only valid if the content is uncompressed
    size_t _min_length;
    size_t _total_read_bytes;
    std::string _line_delimiter;
    size_t _line_delimiter_length;

    // save the data read from file reader
    uint8_t* _input_buf;
    size_t _input_buf_size;
    size_t _input_buf_pos;
    size_t _input_buf_limit;

    // save the data decompressed from decompressor.
    uint8_t* _output_buf;
    size_t _output_buf_size;
    size_t _output_buf_pos;
    size_t _output_buf_limit;

    bool _file_eof;
    bool _eof;
    bool _stream_end;
    size_t _more_input_bytes;
    size_t _more_output_bytes;

    size_t _current_offset;

    std::string _value_delimiter;
    size_t _value_delimiter_length;
    bool _trim_tailing_spaces_for_external_table_query = false;
    bool _trim_double_quotes = false;

    // point to the start of next field.
    // this is an offset from line_start().
    size_t _field_start = 0;
    // point to the current position from line_start().
    // this is an offset from line_start() .
    size_t _line_cur_pos = 0;
    // point to the current pos of separator matching sequence.
    // this is an offset from the start of value_delimiter.
    size_t _value_delimiter_cur_pos = 0;
    // point to the current pos of separator matching sequence.
    // this is an offset from the start of line_delimiter.
    size_t _line_delimiter_cur_pos = 0;

    // Profile counters
    RuntimeProfile::Counter* _bytes_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _bytes_decompress_counter;
    RuntimeProfile::Counter* _decompress_timer;
};
} // namespace doris
