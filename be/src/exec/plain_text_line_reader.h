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
#include "util/runtime_profile.h"

namespace doris {

class FileReader;
class Decompressor;
class Status;

class PlainTextLineReader : public LineReader {
public:
    PlainTextLineReader(RuntimeProfile* profile, FileReader* file_reader,
                        Decompressor* decompressor, size_t length,
                        const std::string& line_delimiter, size_t line_delimiter_length);

    ~PlainTextLineReader() override;

    Status read_line(const uint8_t** ptr, size_t* size, bool* eof) override;

    void close() override;

private:
    bool update_eof();

    size_t output_buf_read_remaining() const { return _output_buf_limit - _output_buf_pos; }

    size_t input_buf_read_remaining() const { return _input_buf_limit - _input_buf_pos; }

    bool done() { return _file_eof && output_buf_read_remaining() == 0; }

    // find line delimiter from 'start' to 'start' + len,
    // return line delimiter pos if found, otherwise return nullptr.
    // TODO:
    //  save to positions of field separator
    uint8_t* update_field_pos_and_find_line_delimiter(const uint8_t* start, size_t len);

    void extend_input_buf();
    void extend_output_buf();

private:
    RuntimeProfile* _profile;
    FileReader* _file_reader;
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

    // Profile counters
    RuntimeProfile::Counter* _bytes_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _bytes_decompress_counter;
    RuntimeProfile::Counter* _decompress_timer;
};

} // namespace doris
