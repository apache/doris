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

#include <stdint.h>

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "exec/line_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "util/runtime_profile.h"

namespace doris {
namespace io {
class IOContext;
}

class Decompressor;
class Status;

enum class ReaderState { START, NORMAL, PRE_MATCH_ENCLOSE, MATCH_ENCLOSE };
struct ReaderStateWrapper {
    inline void forward_to(ReaderState state) {
        this->prev_state = this->curr_state;
        this->curr_state = state;
    }

    inline void reset() {
        this->curr_state = ReaderState::START;
        this->prev_state = ReaderState::START;
    }

    inline bool operator==(const ReaderState& state) const { return curr_state == state; }

    ReaderState curr_state = ReaderState::START;
    ReaderState prev_state = ReaderState::START;
};

class TextLineReaderContextIf {
public:
    virtual ~TextLineReaderContextIf() = default;

    /// @brief find line delimiter from 'start' to 'start' + len,
    // info about the current line may be record to the ctx, like column seprator pos.
    /// @return line delimiter pos if found, otherwise return nullptr.
    virtual const uint8_t* read_line(const uint8_t* start, const size_t len) = 0;

    /// @return length of line delimiter
    [[nodiscard]] virtual size_t line_delimiter_length() const = 0;

    /// @brief should be called when beginning to read a new line
    virtual void refresh() = 0;
};

class PlainTexLineReaderCtx : public TextLineReaderContextIf {
public:
    explicit PlainTexLineReaderCtx(const std::string& line_delimiter_,
                                   const size_t line_delimiter_len_)
            : line_delimiter(line_delimiter_), line_delimiter_len(line_delimiter_len_) {}

    inline const uint8_t* read_line(const uint8_t* start, const size_t len) override {
        return (uint8_t*)memmem(start, len, line_delimiter.c_str(), line_delimiter_len);
    }

    [[nodiscard]] inline size_t line_delimiter_length() const override {
        return line_delimiter_len;
    }

    inline void refresh() override {}

protected:
    const std::string line_delimiter;
    const size_t line_delimiter_len;
};

class CsvLineReaderContext final : public PlainTexLineReaderCtx {
public:
    explicit CsvLineReaderContext(const std::string& line_delimiter_,
                                  const size_t line_delimiter_len_, const std::string& column_sep_,
                                  const size_t column_sep_num, const size_t column_sep_len_,
                                  const char enclose, const char escape)
            : PlainTexLineReaderCtx(line_delimiter_, line_delimiter_len_),
              _enclose(enclose),
              _escape(escape),
              _column_sep(column_sep_),
              _column_sep_len(column_sep_len_) {
        _column_sep_positions.reserve(column_sep_num);
    }

    const uint8_t* read_line(const uint8_t* start, const size_t len) override;

    inline void refresh() override {
        _idx = 0;
        _delimiter_match_len = 0;
        _left_enclose_pos = 0;
        _state.reset();
        _result = nullptr;
        _column_sep_positions.clear();
    }

    [[nodiscard]] inline const std::vector<size_t>& column_sep_positions() const {
        return _column_sep_positions;
    }

protected:
    void on_start(const uint8_t* start, size_t len);
    void on_normal(const uint8_t* start, size_t len);
    void on_pre_match_enclose(const uint8_t* start, size_t len);
    void on_match_enclose(const uint8_t* start, size_t len);

private:
    bool _look_for_column_sep(const uint8_t* curr_start, size_t curr_len);
    bool _look_for_line_delim(const uint8_t* curr_start, size_t curr_len);

    const char _enclose;
    const char _escape;
    const std::string _column_sep;
    const size_t _column_sep_len;

    size_t _idx = 0;
    size_t _delimiter_match_len = 0;
    size_t _left_enclose_pos = 0;
    ReaderStateWrapper _state;
    const uint8_t* _result = nullptr;
    // record the start pos of each column sep
    std::vector<size_t> _column_sep_positions;
};

using TextLineReaderCtxPtr = std::shared_ptr<TextLineReaderContextIf>;

class NewPlainTextLineReader : public LineReader {
    ENABLE_FACTORY_CREATOR(NewPlainTextLineReader);

public:
    NewPlainTextLineReader(RuntimeProfile* profile, io::FileReaderSPtr file_reader,
                           Decompressor* decompressor, TextLineReaderCtxPtr line_reader_ctx,
                           size_t length, size_t current_offset);

    ~NewPlainTextLineReader() override;

    Status read_line(const uint8_t** ptr, size_t* size, bool* eof,
                     const io::IOContext* io_ctx) override;

    inline TextLineReaderCtxPtr text_line_reader_ctx() { return _line_reader_ctx; }

    void close() override;

private:
    bool update_eof();

    [[nodiscard]] size_t output_buf_read_remaining() const {
        return _output_buf_limit - _output_buf_pos;
    }

    [[nodiscard]] size_t input_buf_read_remaining() const {
        return _input_buf_limit - _input_buf_pos;
    }

    bool done() { return _file_eof && output_buf_read_remaining() == 0; }

    // find line delimiter from 'start' to 'start' + len,
    // return line delimiter pos if found, otherwise return nullptr.
    const uint8_t* update_field_pos_and_find_line_delimiter(const uint8_t* start, size_t len);

    void extend_input_buf();
    void extend_output_buf();

    RuntimeProfile* _profile;
    io::FileReaderSPtr _file_reader;
    Decompressor* _decompressor;
    // the min length that should be read.
    // -1 means endless(for stream load)
    // and only valid if the content is uncompressed
    size_t _min_length;
    size_t _total_read_bytes;

    TextLineReaderCtxPtr _line_reader_ctx;

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

    // Profile counters
    RuntimeProfile::Counter* _bytes_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _bytes_decompress_counter;
    RuntimeProfile::Counter* _decompress_timer;
};
} // namespace doris
