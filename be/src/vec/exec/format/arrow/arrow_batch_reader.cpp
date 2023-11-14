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

#include "arrow_batch_reader.h"

#include "arrow/array.h"
#include "arrow/io/buffered.h"
#include "arrow/io/stdio.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "common/logging.h"
#include "io/fs/stream_load_pipe.h"
#include "olap/wal_manager.h"
#include "runtime/runtime_state.h"

#define DEFAULT_READ_BUF_CAP (4 * 1024 * 1024)
#define BATCH_SIZE_LENGTH (4)

namespace doris::vectorized {

ArrowBatchReader::ArrowBatchReader(io::FileReaderSPtr file_reader)
        : _file_reader(file_reader),
          _read_buf(new uint8_t[DEFAULT_READ_BUF_CAP]),
          _read_buf_cap(DEFAULT_READ_BUF_CAP),
          _read_buf_pos(0),
          _read_buf_len(0),
          _batch_size(-1) {}

ArrowBatchReader::~ArrowBatchReader() = default;

Status ArrowBatchReader::get_one_batch(uint8_t** data, int* length) {
    _init();
    RETURN_IF_ERROR(_get_batch_size());
    RETURN_IF_ERROR(_get_batch_value());

    *data = &_read_buf[_read_buf_pos];
    if (_batch_size <= 0) {
        *length = 0;
    } else {
        *length = _read_buf_len;
    }
    return Status::OK();
}

void ArrowBatchReader::_init() {
    _read_buf_pos = _read_buf_pos + _read_buf_len;
    _batch_size = -1;
    _read_buf_len = 0;
}

Status ArrowBatchReader::_get_batch_size() {
    if (_batch_size >= 0) {
        return Status::OK();
    }

    if (_get_valid_cap() < BATCH_SIZE_LENGTH) {
        memmove(_read_buf, _read_buf + _read_buf_pos, _read_buf_len);
    }

    while (_read_buf_len < BATCH_SIZE_LENGTH) {
        Slice file_slice(_read_buf + _read_buf_pos + _read_buf_len, _get_valid_cap());
        size_t read_length = 0;
        RETURN_IF_ERROR(_file_reader->read_at(0, file_slice, &read_length, NULL));
        if (read_length == 0) {
            return Status::OK();
        }
        _read_buf_len += read_length;
    }

    _batch_size = _convert_to_length(_read_buf);

    if (_batch_size < 0) {
        return Status::InternalError("Invalid batch size");
    }

    _read_buf_pos += BATCH_SIZE_LENGTH;
    _read_buf_len -= BATCH_SIZE_LENGTH;

    if (_batch_size > _read_buf_cap) {
        while (_read_buf_cap < _batch_size) {
            _read_buf_cap *= 2;
        }

        uint8_t* new_read_buf = new uint8_t[_read_buf_cap];
        memmove(new_read_buf, _read_buf + _read_buf_pos, _read_buf_len);
        delete[] _read_buf;
        _read_buf = new_read_buf;
    }
    return Status::OK();
}

int ArrowBatchReader::_get_valid_cap() {
    return _read_buf_cap - _read_buf_pos - _read_buf_len;
}

uint32_t ArrowBatchReader::_convert_to_length(uint8_t* data) {
    uint32_t len = (((uint32_t)data[0]) << 24) & 0xff000000;
    len |= (((uint32_t)data[1]) << 16) & 0xff0000;
    len |= (((uint32_t)data[2]) << 8) & 0xff00;
    len |= ((uint32_t)data[3]) & 0xff;
    return len;
}

Status ArrowBatchReader::_get_batch_value() {
    if (_batch_size <= 0) {
        return Status::OK();
    }

    if (_get_valid_cap() < _batch_size) {
        memmove(_read_buf, _read_buf + _read_buf_pos, _read_buf_len);
    }

    while (_read_buf_len < _batch_size) {
        Slice file_slice(_read_buf + _read_buf_pos + _read_buf_len, _get_valid_cap());
        size_t read_length = 0;
        RETURN_IF_ERROR(_file_reader->read_at(0, file_slice, &read_length, NULL));
        _read_buf_len += read_length;
    }
    return Status::OK();
}

} // namespace doris::vectorized