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

#include "common/logging.h"

namespace doris {

// buffered reader
BufferedReader::BufferedReader(FileReader* reader, int64_t buffer_size)
        : _reader(reader),
          _buffer_size(buffer_size),
          _buffer_offset(0),
          _buffer_limit(0),
          _cur_offset(0) {
    _buffer = new char[_buffer_size];
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
    RETURN_IF_ERROR(_reader->open());
    RETURN_IF_ERROR(_fill());
    return Status::OK();
}

//not support
Status BufferedReader::read_one_message(std::unique_ptr<uint8_t[]>* buf, size_t* length) {
    return Status::NotSupported("Not support");

}

Status BufferedReader::read(uint8_t* buf, size_t* buf_len, bool* eof) {
    DCHECK_NE(*buf_len, 0);
    int64_t bytes_read;
    RETURN_IF_ERROR(readat(_cur_offset, (int64_t)*buf_len, &bytes_read, buf));
    if (bytes_read == 0) {
        *eof = true;
    } else {
        *eof = false;
    }
    return Status::OK();
}

Status BufferedReader::readat(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) {
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
    // requested bytes missed the local buffer
    if (position >= _buffer_limit || position < _buffer_offset) {
        // if requested length is larger than the capacity of buffer, do not
        // need to copy the character into local buffer.
        if (nbytes > _buffer_size) {
            return _reader->readat(position, nbytes, bytes_read, out);
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
        int64_t bytes_read;
        // retry for new content
        int retry_times = 1;
        do {
            // fill the buffer
            RETURN_IF_ERROR(_reader->readat(_buffer_offset, _buffer_size, &bytes_read, _buffer));
        } while (bytes_read == 0 && retry_times++ < 2);
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
}

bool BufferedReader::closed() {
    return _reader->closed();
}

} // namespace doris
