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

#include "batch_with_length_reader.h"

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


// BatchWithLengthReader used in fixed format:  <len><value><len><value>...
// The <len> is of type int and represents the length of the <value>.
// We obtain data through the following process:
// 1. Get the buff length data from the _file_reader and fill data in the _read_buff.
// 2. Read the first 4 bytes and convert them to the length of the int type, and save it as _bash_size.
// 3. If _bash_size is greater than the capacity of the _read_buff, we need to expand the _read_buff.
// 4. Here, we get a complete value, return it to the application layer.
// 5. Continue parsing the _read_buff until the _read_buff is resolved.
// 6. If there is some incomplete data left in the _read_buff, 
// we copy some of the data that is currently parsed to the front end of the buff (because the data has been consumed by the upper-layer application), 
// so that _read_buff can have the rest of the space to continue fetching data from the _read_file.

namespace doris::vectorized {

BatchWithLengthReader::BatchWithLengthReader(io::FileReaderSPtr file_reader)
        : _file_reader(file_reader),
          _read_buf(new uint8_t[DEFAULT_READ_BUF_CAP]),
          _read_buf_cap(DEFAULT_READ_BUF_CAP),
          _read_buf_pos(0),
          _read_buf_len(0),
          _batch_size(-1) {}

BatchWithLengthReader::~BatchWithLengthReader() {
    delete[] _read_buf;
}

Status BatchWithLengthReader::get_one_batch(uint8_t** data, int* length) {
    RETURN_IF_ERROR(_get_batch_size());
    RETURN_IF_ERROR(_get_batch_value());

    *data = &_read_buf[_read_buf_pos];
    if (_batch_size <= 0) {
        *length = 0;
    } else {
        *length = _batch_size;
        _read_buf_pos += _batch_size;
        _read_buf_len -= _batch_size;
    }
    _batch_size = -1;
    return Status::OK();
}

Status BatchWithLengthReader::_get_batch_size() {
    if (_batch_size >= 0) {
        return Status::OK();
    }

    _ensure_cap(BATCH_SIZE_LENGTH);

    RETURN_IF_ERROR(_get_data_from_reader(BATCH_SIZE_LENGTH));
    if (_read_buf_len < BATCH_SIZE_LENGTH) {
        return Status::OK();
    }

    _batch_size = _convert_to_length(_read_buf + _read_buf_pos);

    if (_batch_size < 0) {
        return Status::InternalError("Invalid batch size");
    }

    _read_buf_pos += BATCH_SIZE_LENGTH;
    _read_buf_len -= BATCH_SIZE_LENGTH;

    if (_batch_size > _read_buf_cap) {
        while (_read_buf_cap < _batch_size) {
            _read_buf_cap *= 2;
        }

        // The current buff capacity is not enough and needs to be expanded
        auto* new_read_buf = new uint8_t[_read_buf_cap];
        memmove(new_read_buf, _read_buf + _read_buf_pos, _read_buf_len);
        delete[] _read_buf;
        _read_buf = new_read_buf;
        _read_buf_pos = 0;
    }
    return Status::OK();
}

Status BatchWithLengthReader::_get_data_from_reader(int req_size) {
    while (_read_buf_len < req_size) {
        Slice file_slice(_read_buf + _read_buf_pos + _read_buf_len,
                         _read_buf_cap - _read_buf_pos - _read_buf_len);
        size_t read_length = 0;
        RETURN_IF_ERROR(_file_reader->read_at(0, file_slice, &read_length, NULL));
        _read_buf_len += read_length;
        if (read_length == 0) {
            break;
        }
    }
    return Status::OK();
}

void BatchWithLengthReader::_ensure_cap(int req_size) {
    if (_read_buf_len < req_size && _read_buf_cap - _read_buf_pos < req_size) {
        // there is not enough space for data,
        // we need to move the data that has been used before and the useful data forward
        memmove(_read_buf, _read_buf + _read_buf_pos, _read_buf_len);
        _read_buf_pos = 0;
    }
}

uint32_t BatchWithLengthReader::_convert_to_length(const uint8_t* data) {
    uint32_t len = (((uint32_t)data[0]) << 24) & 0xff000000;
    len |= (((uint32_t)data[1]) << 16) & 0xff0000;
    len |= (((uint32_t)data[2]) << 8) & 0xff00;
    len |= ((uint32_t)data[3]) & 0xff;
    return len;
}

Status BatchWithLengthReader::_get_batch_value() {
    if (_batch_size <= 0) {
        return Status::OK();
    }

    _ensure_cap(_batch_size);

    RETURN_IF_ERROR(_get_data_from_reader(_batch_size));
    return Status::OK();
}

} // namespace doris::vectorized