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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "exec/line_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "util/runtime_profile.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace io {
struct IOContext;
}

class Decompressor;
class Status;

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

template <typename Ctx>
class BaseTextLineReaderContext : public TextLineReaderContextIf {
    // using a function ptr to decrease the overhead (found very effective during test).
    using FindDelimiterFunc = const uint8_t* (*)(const uint8_t*, size_t, const char*, size_t);

public:
    explicit BaseTextLineReaderContext(std::string line_delimiter_,
                                       const size_t line_delimiter_len_, const bool keep_cr_)
            : line_delimiter(std::move(line_delimiter_)),
              line_delimiter_len(line_delimiter_len_),
              keep_cr(keep_cr_) {
        use_memmem = line_delimiter_len != 1 || line_delimiter != "\n" || keep_cr;
        if (use_memmem) {
            find_line_delimiter_func = &BaseTextLineReaderContext::find_multi_char_line_sep;
        } else {
            find_line_delimiter_func = &BaseTextLineReaderContext::find_lf_crlf_line_sep;
        }
    }

    inline const uint8_t* read_line(const uint8_t* start, const size_t len) final {
        return static_cast<Ctx*>(this)->read_line_impl(start, len);
    }

    [[nodiscard]] inline size_t line_delimiter_length() const final {
        return line_delimiter_len + line_crlf;
    }

    inline void refresh() final { return static_cast<Ctx*>(this)->refresh_impl(); };

    inline const uint8_t* find_multi_char_line_sep(const uint8_t* start, const size_t length) {
        return static_cast<uint8_t*>(
                memmem(start, length, line_delimiter.c_str(), line_delimiter_len));
    }

    const uint8_t* find_lf_crlf_line_sep(const uint8_t* start, const size_t length) {
        line_crlf = false;
        if (start == nullptr || length == 0) {
            return nullptr;
        }

        const auto* p = (const uint8_t*)memchr(start, '\n', length);
        if (p) {
            size_t i = p - start;
            if (i > 0 and start[i - 1] == '\r') {
                return p - 1;
            }
            return p;
        }
        return nullptr;
    }

    const uint8_t* call_find_line_sep(const uint8_t* start, const size_t length) {
        return (this->*find_line_delimiter_func)(start, length);
    }

protected:
    const std::string line_delimiter;
    const size_t line_delimiter_len;
    bool keep_cr = false;
    bool line_crlf = false;
    bool use_memmem = true;
    using FindLineDelimiterFunc = const uint8_t* (BaseTextLineReaderContext::*)(const uint8_t*,
                                                                                size_t);
    FindLineDelimiterFunc find_line_delimiter_func;
};
class PlainTextLineReaderCtx final : public BaseTextLineReaderContext<PlainTextLineReaderCtx> {
public:
    explicit PlainTextLineReaderCtx(const std::string& line_delimiter_,
                                    const size_t line_delimiter_len_, const bool keep_cr_)
            : BaseTextLineReaderContext(line_delimiter_, line_delimiter_len_, keep_cr_) {}

    inline const uint8_t* read_line_impl(const uint8_t* start, const size_t length) {
        return call_find_line_sep(start, length);
    }

    inline void refresh_impl() {}
};

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

class EncloseCsvLineReaderCtx final : public BaseTextLineReaderContext<EncloseCsvLineReaderCtx> {
    // using a function ptr to decrease the overhead (found very effective during test).
    using FindDelimiterFunc = const uint8_t* (*)(const uint8_t*, size_t, const char*, size_t);

public:
    explicit EncloseCsvLineReaderCtx(const std::string& line_delimiter_,
                                     const size_t line_delimiter_len_, std::string column_sep_,
                                     const size_t column_sep_len_, size_t col_sep_num,
                                     const char enclose, const char escape, const bool keep_cr_)
            : BaseTextLineReaderContext(line_delimiter_, line_delimiter_len_, keep_cr_),
              _enclose(enclose),
              _escape(escape),
              _column_sep_len(column_sep_len_),
              _column_sep(std::move(column_sep_)) {
        if (column_sep_len_ == 1) {
            find_col_sep_func = &EncloseCsvLineReaderCtx::look_for_column_sep_pos<true>;
        } else {
            find_col_sep_func = &EncloseCsvLineReaderCtx::look_for_column_sep_pos<false>;
        }
        _column_sep_positions.reserve(col_sep_num);
    }

    inline void refresh_impl() {
        _idx = 0;
        _should_escape = false;
        _quote_escape = false;
        _result = nullptr;
        _column_sep_positions.clear();
        _state.reset();
    }

    [[nodiscard]] inline std::vector<size_t> column_sep_positions() const {
        return _column_sep_positions;
    }

    const uint8_t* read_line_impl(const uint8_t* start, size_t length);

private:
    template <bool SingleChar>
    static const uint8_t* look_for_column_sep_pos(const uint8_t* curr_start, size_t curr_len,
                                                  const char* column_sep, size_t column_sep_len);

    size_t update_reading_bound(const uint8_t* start);
    void on_col_sep_found(const uint8_t* curr_start, const uint8_t* col_sep_pos);

    void _on_start(const uint8_t* start, size_t& len);
    void _on_normal(const uint8_t* start, size_t& len);
    void _on_pre_match_enclose(const uint8_t* start, size_t& len);
    void _on_match_enclose(const uint8_t* start, size_t& len);

    ReaderStateWrapper _state;
    const char _enclose;
    const char _escape;
    const uint8_t* _result = nullptr;

    size_t _total_len;
    const size_t _column_sep_len;

    size_t _idx = 0;
    bool _should_escape = false;
    // quote is specially escaped by quote in csv format
    bool _quote_escape = false;

    const std::string _column_sep;
    std::vector<size_t> _column_sep_positions;

    FindDelimiterFunc find_col_sep_func;
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

protected:
    void _collect_profile_before_close() override;

private:
    bool update_eof();

    [[nodiscard]] size_t output_buf_read_remaining() const {
        return _output_buf_limit - _output_buf_pos;
    }

    [[nodiscard]] size_t input_buf_read_remaining() const {
        return _input_buf_limit - _input_buf_pos;
    }

    bool done() { return _file_eof && output_buf_read_remaining() == 0; }

    void extend_input_buf();
    void extend_output_buf();

    RuntimeProfile* _profile = nullptr;
    io::FileReaderSPtr _file_reader;
    Decompressor* _decompressor = nullptr;
    // the min length that should be read.
    // -1 means endless(for stream load)
    // and only valid if the content is uncompressed
    size_t _min_length;
    size_t _total_read_bytes;

    TextLineReaderCtxPtr _line_reader_ctx;

    // save the data read from file reader
    uint8_t* _input_buf = nullptr;
    size_t _input_buf_size;
    size_t _input_buf_pos;
    size_t _input_buf_limit;

    // save the data decompressed from decompressor.
    uint8_t* _output_buf = nullptr;
    size_t _output_buf_size;
    size_t _output_buf_pos;
    size_t _output_buf_limit;

    bool _file_eof;
    bool _eof;
    size_t _more_input_bytes;
    size_t _more_output_bytes;

    size_t _current_offset;

    // Profile counters
    RuntimeProfile::Counter* _bytes_decompress_counter = nullptr;
    RuntimeProfile::Counter* _decompress_timer = nullptr;
};
#include "common/compile_check_end.h"
} // namespace doris
