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

#include "exec/buffered_reader.h"

#include <algorithm>
#include <sstream>

#include "common/config.h"
#include "common/logging.h"

namespace doris {

// buffered reader
BufferedReader::BufferedReader(RuntimeProfile* profile, FileReader* reader, int64_t buffer_size)
        : _profile(profile),
          _reader(reader),
          _buffer_size(buffer_size),
          _buffer_offset(0),
          _buffer_limit(0),
          _cur_offset(0) {
    if (_buffer_size == -1L) {
        _buffer_size = config::remote_storage_read_buffer_mb * 1024 * 1024;
    }
    _buffer = new char[_buffer_size];
    // set the _cur_offset of this reader as same as the inner reader's,
    // to make sure the buffer reader will start to read at right position.
    _reader->tell(&_cur_offset);
}

BufferedReader::~BufferedReader() {
    close();
}

Status BufferedReader::open() {
    if (!_reader) {
        std::stringstream ss;
        ss << "Open buffered reader failed, reader is null";
        return Status::InternalError(ss.str());
    }

    // the macro ADD_XXX is idempotent.
    // So although each scanner calls the ADD_XXX method, they all use the same counters.
    _read_timer = ADD_TIMER(_profile, "FileReadTime");
    _remote_read_timer = ADD_CHILD_TIMER(_profile, "FileRemoteReadTime", "FileReadTime");
    _read_counter = ADD_COUNTER(_profile, "FileReadCalls", TUnit::UNIT);
    _remote_read_counter = ADD_COUNTER(_profile, "FileRemoteReadCalls", TUnit::UNIT);

    RETURN_IF_ERROR(_reader->open());
    return Status::OK();
}

//not support
Status BufferedReader::read_one_message(std::unique_ptr<uint8_t[]>* buf, int64_t* length) {
    return Status::NotSupported("Not support");
}

Status BufferedReader::read(uint8_t* buf, int64_t buf_len, int64_t* bytes_read, bool* eof) {
    DCHECK_NE(buf_len, 0);
    RETURN_IF_ERROR(readat(_cur_offset, buf_len, bytes_read, buf));
    if (*bytes_read == 0) {
        *eof = true;
    } else {
        *eof = false;
    }
    return Status::OK();
}

Status BufferedReader::readat(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) {
    SCOPED_TIMER(_read_timer);
    if (nbytes <= 0) {
        *bytes_read = 0;
        return Status::OK();
    }
    RETURN_IF_ERROR(_read_once(position, nbytes, bytes_read, out));
    //EOF
    if (*bytes_read <= 0) {
        return Status::OK();
    }
    while (*bytes_read < nbytes) {
        int64_t len;
        RETURN_IF_ERROR(_read_once(position + *bytes_read, nbytes - *bytes_read, &len,
                                   reinterpret_cast<char*>(out) + *bytes_read));
        // EOF
        if (len <= 0) {
            break;
        }
        *bytes_read += len;
    }
    return Status::OK();
}

Status BufferedReader::_read_once(int64_t position, int64_t nbytes, int64_t* bytes_read,
                                  void* out) {
    _read_count++;
    // requested bytes missed the local buffer
    if (position >= _buffer_limit || position < _buffer_offset) {
        // if requested length is larger than the capacity of buffer, do not
        // need to copy the character into local buffer.
        if (nbytes > _buffer_size) {
            auto st = _reader->readat(position, nbytes, bytes_read, out);
            if (st.ok()) {
                _cur_offset = position + *bytes_read;
            }
            return st;
        }
        _buffer_offset = position;
        RETURN_IF_ERROR(_fill());
        if (position >= _buffer_limit) {
            *bytes_read = 0;
            return Status::OK();
        }
    }
    int64_t len = std::min(_buffer_limit - position, nbytes);
    int64_t off = position - _buffer_offset;
    memcpy(out, _buffer + off, len);
    *bytes_read = len;
    _cur_offset = position + *bytes_read;
    return Status::OK();
}

Status BufferedReader::_fill() {
    if (_buffer_offset >= 0) {
        int64_t bytes_read = 0;
        SCOPED_TIMER(_remote_read_timer);
        RETURN_IF_ERROR(_reader->readat(_buffer_offset, _buffer_size, &bytes_read, _buffer));
        _buffer_limit = _buffer_offset + bytes_read;
    }
    return Status::OK();
}

int64_t BufferedReader::size() {
    return _reader->size();
}

Status BufferedReader::seek(int64_t position) {
    _cur_offset = position;
    return Status::OK();
}

Status BufferedReader::tell(int64_t* position) {
    *position = _cur_offset;
    return Status::OK();
}

void BufferedReader::close() {
    _reader->close();
    SAFE_DELETE_ARRAY(_buffer);

    if (_read_counter != nullptr) {
        COUNTER_UPDATE(_read_counter, _read_count);
    }
    if (_remote_read_counter != nullptr) {
        COUNTER_UPDATE(_remote_read_counter, _remote_read_count);
    }
}

bool BufferedReader::closed() {
    return _reader->closed();
}

} // namespace doris
