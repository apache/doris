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

#include "vec/exec/format/file_reader/new_plain_text_line_reader.h"

#include <gen_cpp/Metrics_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <ostream>

#include "common/compiler_util.h"
#include "exec/decompressor.h"
#include "io/fs/file_reader.h"
#include "util/slice.h"

// INPUT_CHUNK must
//  larger than 15B for correct lz4 file decompressing
//  larger than 300B for correct lzo header decompressing
#define INPUT_CHUNK (2 * 1024 * 1024)
// #define INPUT_CHUNK  (34)
// align with prefetch buffer size
#define OUTPUT_CHUNK (4 * 1024 * 1024)
// #define OUTPUT_CHUNK (32)
// leave these 2 size small for debugging

namespace doris {

const uint8_t* CsvLineReaderContext::read_line(const uint8_t* start, const size_t len) {
    while (!(_idx == len || _result != nullptr)) {
        switch (_state.curr_state) {
        case ReaderState::START: {
            on_start(start, len);
            break;
        }
        case ReaderState::NORMAL: {
            on_normal(start, len);
            break;
        }
        case ReaderState::MATCH_COLUMN_SEP: {
            on_match_column_sep(start, len);
            break;
        }
        case ReaderState::PRE_MATCH_ENCLOSE: {
            on_pre_match_enclose(start, len);
            break;
        }
        case ReaderState::MATCH_ENCLOSE: {
            on_match_enclose(start, len);
            break;
        }
        case ReaderState::POST_ENCLOSE_LINE_DELIMITER: {
            on_post_match_enclose_line_delimiter(start, len);
            break;
        }
        case ReaderState::POST_ENCLOSE_COLUMN_SEP: {
            on_post_match_enclose_column_sep(start, len);
            break;
        }
        case ReaderState::MATCH_ESCAPE: {
            on_match_escape(start, len);
            break;
        }
        case ReaderState::FOUND_LINE: {
            on_found_line(start, len);
            break;
        }
        }
    }
    if (UNLIKELY(_state == ReaderState::FOUND_LINE && _delimiter_match_len == line_delimiter_len)) {
        // means it happens to be a complete line
        _result = start + (_idx - line_delimiter_len);
    }
    return _result;
}

void CsvLineReaderContext::on_start(const uint8_t* start, size_t len) {
    DCHECK_EQ(_delimiter_match_len, 0);
    if (start[_idx] == _enclose) {
        _state.forward_to(ReaderState::PRE_MATCH_ENCLOSE);
    } else if (UNLIKELY(start[_idx] == _column_sep[0])) {
        ++_delimiter_match_len;
        _state.forward_to(ReaderState::MATCH_COLUMN_SEP);
    } else if (UNLIKELY(start[_idx] == line_delimiter[0])) {
        ++_delimiter_match_len;
        _state.forward_to(ReaderState::FOUND_LINE);
    } else {
        _state.forward_to(ReaderState::NORMAL);
    }
    ++_idx;
}

void CsvLineReaderContext::on_normal(const uint8_t* start, size_t len) {
    DCHECK_EQ(_delimiter_match_len, 0);
    if (UNLIKELY(start[_idx] == _column_sep[0])) {
        ++_delimiter_match_len;
        _state.forward_to(ReaderState::MATCH_COLUMN_SEP);
    } else if (UNLIKELY(start[_idx] == line_delimiter[0])) {
        ++_delimiter_match_len;
        _state.forward_to(ReaderState::FOUND_LINE);
    }
    ++_idx;
}

void CsvLineReaderContext::on_match_column_sep(const uint8_t* start, size_t len) {
    DCHECK_NE(_delimiter_match_len, 0);
    if (_delimiter_match_len == _column_sep_len) {
        // found colum sep
        _column_sep_positions.push_back(_idx - _column_sep_len);
        _delimiter_match_len = 0;
        _state.forward_to(ReaderState::START);
    } else if (start[_idx] == _column_sep[_delimiter_match_len]) {
        // matching multi-char col sep
        DCHECK_GT(_column_sep_len, 1);
        ++_delimiter_match_len;
        ++_idx;
    } else {
        // not found
        _delimiter_match_len = 0;
        _state.forward_to(ReaderState::NORMAL);
    }
}

void CsvLineReaderContext::on_pre_match_enclose(const uint8_t* start, size_t len) {
    if (_state.prev_state == ReaderState::START) {
        DCHECK_GT(_idx, 0);
        _left_enclose_pos = _idx - 1;
        _state.forward_to(_state.curr_state);
        return;
    }
    if (start[_idx] == _escape) {
        _state.forward_to(ReaderState::MATCH_ESCAPE);
    } else if (start[_idx] == _enclose) {
        _state.forward_to(ReaderState::MATCH_ENCLOSE);
    }
    ++_idx;
}

void CsvLineReaderContext::on_match_enclose(const uint8_t* start, size_t len) {
    if (LIKELY(start[_idx] == _column_sep[0])) {
        _state.forward_to(ReaderState::POST_ENCLOSE_COLUMN_SEP);
        ++_delimiter_match_len;
    } else if (start[_idx] == line_delimiter[0]) {
        _state.forward_to(ReaderState::POST_ENCLOSE_LINE_DELIMITER);
        ++_delimiter_match_len;
    } else {
        // unfortunately, meet corner case(suppose `,` is delimiter and `"` is enclose): ,"part1"part2,
        // will reset to left enclose idx to do parse in nornal state
        _delimiter_match_len = 0;
        _state.forward_to(ReaderState::NORMAL);
        _idx = _left_enclose_pos;
        return;
    }
    ++_idx;
}

void CsvLineReaderContext::on_match_escape(const uint8_t* start, size_t len) {
    _state.forward_to(_state.prev_state);
    ++_idx;
}

void CsvLineReaderContext::on_post_match_enclose_column_sep(const uint8_t* start, size_t len) {
    if (_delimiter_match_len == _column_sep_len) {
        _column_sep_positions.push_back(_idx - _column_sep_len);
        _delimiter_match_len = 0;
        _state.forward_to(ReaderState::START);
    } else if (start[_idx] == _column_sep[_delimiter_match_len]) {
        DCHECK_GT(_column_sep_len, 1);
        ++_delimiter_match_len;
        ++_idx;
    } else {
        // unfortunately, meet corner case(suppose `,` is delimiter and `"` is enclose): ,"part1"part2,
        // will reset to left enclose idx to do parse in nornal state
        _delimiter_match_len = 0;
        _state.forward_to(ReaderState::NORMAL);
        _idx = _left_enclose_pos;
    }
}

void CsvLineReaderContext::on_post_match_enclose_line_delimiter(const uint8_t* start, size_t len) {
    if (_delimiter_match_len == line_delimiter_len) {
        _result = start + _idx - line_delimiter_len;
    } else if (start[_idx] == line_delimiter[_delimiter_match_len]) {
        DCHECK_GT(line_delimiter_len, 1);
        ++_delimiter_match_len;
        ++_idx;
    } else {
        // unfortunately, meet corner case(suppose `,` is delimiter and `"` is enclose): ,"part1"part2,
        // will reset to left enclose idx to do parse in nornal state
        _delimiter_match_len = 0;
        _state.forward_to(ReaderState::NORMAL);
        _idx = _left_enclose_pos;
    }
}

void CsvLineReaderContext::on_found_line(const uint8_t* start, size_t len) {
    DCHECK_NE(_delimiter_match_len, 0);
    if (_delimiter_match_len == line_delimiter_len) {
        // found line delimiter
        _result = start + _idx - line_delimiter_len;
        _delimiter_match_len = 0;
    } else if (start[_idx] == line_delimiter[_delimiter_match_len]) {
        // matching multi-char line delimiter
        DCHECK_GT(line_delimiter_len, 1);
        ++_delimiter_match_len;
        ++_idx;
    } else {
        _delimiter_match_len = 0;
        _state.forward_to(ReaderState::NORMAL);
    }
}

NewPlainTextLineReader::NewPlainTextLineReader(RuntimeProfile* profile,
                                               io::FileReaderSPtr file_reader,
                                               Decompressor* decompressor,
                                               TextLineReaderCtxPtr line_reader_ctx, size_t length,
                                               size_t current_offset)
        : _profile(profile),
          _file_reader(file_reader),
          _decompressor(decompressor),
          _min_length(length),
          _total_read_bytes(0),
          _line_reader_ctx(line_reader_ctx),
          _input_buf(new uint8_t[INPUT_CHUNK]),
          _input_buf_size(INPUT_CHUNK),
          _input_buf_pos(0),
          _input_buf_limit(0),
          _output_buf(new uint8_t[OUTPUT_CHUNK]),
          _output_buf_size(OUTPUT_CHUNK),
          _output_buf_pos(0),
          _output_buf_limit(0),
          _file_eof(false),
          _eof(false),
          _stream_end(true),
          _more_input_bytes(0),
          _more_output_bytes(0),
          _current_offset(current_offset),
          _bytes_read_counter(nullptr),
          _read_timer(nullptr),
          _bytes_decompress_counter(nullptr),
          _decompress_timer(nullptr) {
    _bytes_read_counter = ADD_COUNTER(_profile, "BytesRead", TUnit::BYTES);
    _read_timer = ADD_TIMER(_profile, "FileReadTime");
    _bytes_decompress_counter = ADD_COUNTER(_profile, "BytesDecompressed", TUnit::BYTES);
    _decompress_timer = ADD_TIMER(_profile, "DecompressTime");
}

NewPlainTextLineReader::~NewPlainTextLineReader() {
    close();
}

void NewPlainTextLineReader::close() {
    if (_input_buf != nullptr) {
        delete[] _input_buf;
        _input_buf = nullptr;
    }

    if (_output_buf != nullptr) {
        delete[] _output_buf;
        _output_buf = nullptr;
    }
}

inline bool NewPlainTextLineReader::update_eof() {
    if (done()) {
        _eof = true;
    } else if (_decompressor == nullptr && (_total_read_bytes >= _min_length)) {
        _eof = true;
    }
    return _eof;
}

const uint8_t* NewPlainTextLineReader::update_field_pos_and_find_line_delimiter(
        const uint8_t* start, size_t len) {
    return _line_reader_ctx->read_line(start, len);
}

// extend input buf if necessary only when _more_input_bytes > 0
void NewPlainTextLineReader::extend_input_buf() {
    DCHECK(_more_input_bytes > 0);

    // left capacity
    size_t capacity = _input_buf_size - _input_buf_limit;

    // we want at least _more_input_bytes capacity left
    do {
        if (capacity >= _more_input_bytes) {
            // enough
            break;
        }

        capacity = capacity + _input_buf_pos;
        if (capacity >= _more_input_bytes) {
            // move the read remaining to the beginning of the current input buf,
            memmove(_input_buf, _input_buf + _input_buf_pos, input_buf_read_remaining());
            _input_buf_limit -= _input_buf_pos;
            _input_buf_pos = 0;
            break;
        }

        while (_input_buf_size - input_buf_read_remaining() < _more_input_bytes) {
            _input_buf_size = _input_buf_size * 2;
        }

        uint8_t* new_input_buf = new uint8_t[_input_buf_size];
        memmove(new_input_buf, _input_buf + _input_buf_pos, input_buf_read_remaining());
        delete[] _input_buf;

        _input_buf = new_input_buf;
        _input_buf_limit -= _input_buf_pos;
        _input_buf_pos = 0;
    } while (false);
}

void NewPlainTextLineReader::extend_output_buf() {
    // left capacity
    size_t capacity = _output_buf_size - _output_buf_limit;
    // we want at least 1024 bytes capacity left
    size_t target = std::max<size_t>(1024, capacity + _more_output_bytes);

    do {
        // 1. if left capacity is enough, return;
        if (capacity >= target) {
            break;
        }

        // 2. try reuse buf
        capacity = capacity + _output_buf_pos;
        if (capacity >= target) {
            // move the read remaining to the beginning of the current output buf,
            memmove(_output_buf, _output_buf + _output_buf_pos, output_buf_read_remaining());
            _output_buf_limit -= _output_buf_pos;
            _output_buf_pos = 0;
            break;
        }

        // 3. extend buf size to meet the target
        while (_output_buf_size - output_buf_read_remaining() < target) {
            _output_buf_size = _output_buf_size * 2;
        }

        uint8_t* new_output_buf = new uint8_t[_output_buf_size];
        memmove(new_output_buf, _output_buf + _output_buf_pos, output_buf_read_remaining());
        delete[] _output_buf;

        _output_buf = new_output_buf;
        _output_buf_limit -= _output_buf_pos;
        _output_buf_pos = 0;
    } while (false);
}

Status NewPlainTextLineReader::read_line(const uint8_t** ptr, size_t* size, bool* eof,
                                         const io::IOContext* io_ctx) {
    if (_eof || update_eof()) {
        *size = 0;
        *eof = true;
        return Status::OK();
    }
    _line_reader_ctx->refresh();
    int found_line_delimiter = 0;
    size_t offset = 0;
    while (!done()) {
        // find line delimiter in current decompressed data
        uint8_t* cur_ptr = _output_buf + _output_buf_pos;
        const uint8_t* pos =
                update_field_pos_and_find_line_delimiter(cur_ptr, output_buf_read_remaining());

        if (pos == nullptr) {
            // didn't find line delimiter, read more data from decompressor
            // for multi bytes delimiter we cannot set offset to avoid incomplete
            // delimiter
            // read from file reader
            offset = output_buf_read_remaining();
            extend_output_buf();
            if ((_input_buf_limit > _input_buf_pos) && _more_input_bytes == 0) {
                // we still have data in input which is not decompressed.
                // and no more data is required for input
            } else {
                size_t read_len = 0;
                int64_t buffer_len = 0;
                uint8_t* file_buf;

                if (_decompressor == nullptr) {
                    // uncompressed file, read directly into output buf
                    file_buf = _output_buf + _output_buf_limit;
                    buffer_len = _output_buf_size - _output_buf_limit;
                } else {
                    // MARK
                    if (_more_input_bytes > 0) {
                        // we already extend input buf.
                        // current data in input buf should remain unchanged
                        file_buf = _input_buf + _input_buf_limit;
                        buffer_len = _input_buf_size - _input_buf_limit;
                        // leave input pos and limit unchanged
                    } else {
                        // here we are sure that all data in input buf has been consumed.
                        // which means input pos and limit should be reset.
                        file_buf = _input_buf;
                        buffer_len = _input_buf_size;
                        // reset input pos and limit
                        _input_buf_pos = 0;
                        _input_buf_limit = 0;
                    }
                }

                {
                    SCOPED_TIMER(_read_timer);
                    Slice file_slice(file_buf, buffer_len);
                    RETURN_IF_ERROR(
                            _file_reader->read_at(_current_offset, file_slice, &read_len, io_ctx));
                    _current_offset += read_len;
                    if (read_len == 0) {
                        _file_eof = true;
                    }
                    COUNTER_UPDATE(_bytes_read_counter, read_len);
                }
                if (_file_eof || read_len == 0) {
                    if (!_stream_end) {
                        return Status::InternalError(
                                "Compressed file has been truncated, which is not allowed");
                    } else {
                        // last loop we meet stream end,
                        // and now we finished reading file, so we are finished
                        // break this loop to see if there is data in buffer
                        break;
                    }
                }

                if (_decompressor == nullptr) {
                    _output_buf_limit += read_len;
                    _stream_end = true;
                } else {
                    // only update input limit.
                    // input pos is set at MARK step
                    _input_buf_limit += read_len;
                }

                if (read_len < _more_input_bytes) {
                    // we failed to read enough data, continue to read from file
                    _more_input_bytes = _more_input_bytes - read_len;
                    continue;
                }
            }

            if (_decompressor != nullptr) {
                SCOPED_TIMER(_decompress_timer);
                // decompress
                size_t input_read_bytes = 0;
                size_t decompressed_len = 0;
                _more_input_bytes = 0;
                _more_output_bytes = 0;
                RETURN_IF_ERROR(_decompressor->decompress(
                        _input_buf + _input_buf_pos,                        /* input */
                        _input_buf_limit - _input_buf_pos,                  /* input_len */
                        &input_read_bytes, _output_buf + _output_buf_limit, /* output */
                        _output_buf_size - _output_buf_limit,               /* output_max_len */
                        &decompressed_len, &_stream_end, &_more_input_bytes, &_more_output_bytes));

                // LOG(INFO) << "after decompress:"
                //           << " stream_end: " << _stream_end
                //           << " input_read_bytes: " << input_read_bytes
                //           << " decompressed_len: " << decompressed_len
                //           << " more_input_bytes: " << _more_input_bytes
                //           << " more_output_bytes: " << _more_output_bytes;

                // update pos and limit
                _input_buf_pos += input_read_bytes;
                _output_buf_limit += decompressed_len;
                COUNTER_UPDATE(_bytes_decompress_counter, decompressed_len);

                // TODO(cmy): watch this case
                if ((input_read_bytes == 0 /*decompressed_len == 0*/) && _more_input_bytes == 0 &&
                    _more_output_bytes == 0) {
                    // decompress made no progress, may be
                    // A. input data is not enough to decompress data to output
                    // B. output buf is too small to save decompressed output
                    // this is very unlikely to happen
                    // print the log and just go to next loop to read more data or extend output buf.

                    // (cmy), for now, return failed to avoid potential endless loop
                    std::stringstream ss;
                    ss << "decompress made no progress."
                       << " input_read_bytes: " << input_read_bytes
                       << " decompressed_len: " << decompressed_len;
                    LOG(WARNING) << ss.str();
                    return Status::InternalError(ss.str());
                }

                if (_more_input_bytes > 0) {
                    extend_input_buf();
                }
            }
        } else {
            // we found a complete line
            // ready to return
            offset = pos - cur_ptr;
            found_line_delimiter = _line_reader_ctx->line_delimiter_length();
            break;
        }
    } // while (!done())

    *ptr = _output_buf + _output_buf_pos;
    *size = offset;

    // Skip offset and _line_delimiter size;
    _output_buf_pos += offset + found_line_delimiter;
    if (offset == 0 && found_line_delimiter == 0) {
        *eof = true;
    } else {
        *eof = false;
    }

    // update total read bytes
    _total_read_bytes += *size + found_line_delimiter;

    return Status::OK();
}
} // namespace doris
