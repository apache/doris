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
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "exec/line_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "util/runtime_profile.h"
#include "util/slice.h"

namespace doris {
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
    explicit BaseTextLineReaderContext(const std::string& line_delimiter_,
                                       const size_t line_delimiter_len_, const bool keep_cr_)
            : line_delimiter(line_delimiter_),
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
        size_t i = 0;
#ifdef __AVX2__
        // const uint8_t* end = start + length;
        const __m256i newline = _mm256_set1_epi8('\n');
        const __m256i carriage_return = _mm256_set1_epi8('\r');

        const size_t simd_width = 32;
        // Process 32 bytes at a time using AVX2
        for (; i + simd_width <= length; i += simd_width) {
            __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(start + i));

            // Compare with '\n' and '\r'
            __m256i cmp_newline = _mm256_cmpeq_epi8(data, newline);
            __m256i cmp_carriage_return = _mm256_cmpeq_epi8(data, carriage_return);

            // Check if there is a match
            int mask_newline = _mm256_movemask_epi8(cmp_newline);
            int mask_carriage_return = _mm256_movemask_epi8(cmp_carriage_return);

            if (mask_newline != 0 || mask_carriage_return != 0) {
                int pos_lf = (mask_newline != 0) ? i + __builtin_ctz(mask_newline) : INT32_MAX;
                int pos_cr = (mask_carriage_return != 0) ? i + __builtin_ctz(mask_carriage_return)
                                                         : INT32_MAX;
                if (pos_lf < pos_cr) {
                    return start + pos_lf;
                } else if (pos_cr < pos_lf) {
                    if (pos_lf != INT32_MAX) {
                        if (pos_lf - 1 >= 0 && start[pos_lf - 1] == '\r') {
                            //check   xxx\r\r\r\nxxx
                            line_crlf = true;
                            return start + pos_lf - 1;
                        }
                        // xxx\rxxxx\nxx
                        return start + pos_lf;
                    } else if (i + simd_width < length && start[i + simd_width - 1] == '\r' &&
                               start[i + simd_width] == '\n') {
                        //check [/r/r/r/r/r/r/rxxx/r]  [\nxxxx]
                        line_crlf = true;
                        return start + i + simd_width - 1;
                    }
                }
            }
        }

        // Process remaining bytes
#endif
        for (; i < length; ++i) {
            if (start[i] == '\n') {
                return &start[i];
            }
            if (start[i] == '\r' && (i + 1 < length) && start[i + 1] == '\n') {
                line_crlf = true;
                return &start[i];
            }
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

class EncloseCsvLineReaderContext final
        : public BaseTextLineReaderContext<EncloseCsvLineReaderContext> {
    // using a function ptr to decrease the overhead (found very effective during test).
    using FindDelimiterFunc = const uint8_t* (*)(const uint8_t*, size_t, const char*, size_t);

public:
    explicit EncloseCsvLineReaderContext(const std::string& line_delimiter_,
                                         const size_t line_delimiter_len_,
                                         const std::string& column_sep_,
                                         const size_t column_sep_len_, size_t col_sep_num,
                                         const char enclose, const char escape, const bool keep_cr_)
            : BaseTextLineReaderContext(line_delimiter_, line_delimiter_len_, keep_cr_),
              _enclose(enclose),
              _escape(escape),
              _column_sep_len(column_sep_len_),
              _column_sep(column_sep_) {
        if (column_sep_len_ == 1) {
            find_col_sep_func = &EncloseCsvLineReaderContext::look_for_column_sep_pos<true>;
        } else {
            find_col_sep_func = &EncloseCsvLineReaderContext::look_for_column_sep_pos<false>;
        }
        _column_sep_positions.reserve(col_sep_num);
    }

    inline void refresh_impl() {
        _idx = 0;
        _should_escape = false;
        _result = nullptr;
        _column_sep_positions.clear();
        _state.reset();
    }

    [[nodiscard]] inline const std::vector<size_t> column_sep_positions() const {
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
    RuntimeProfile::Counter* _bytes_read_counter = nullptr;
    RuntimeProfile::Counter* _read_timer = nullptr;
    RuntimeProfile::Counter* _bytes_decompress_counter = nullptr;
    RuntimeProfile::Counter* _decompress_timer = nullptr;
};
} // namespace doris
