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

#include "filesystem/read_stream.h"

namespace doris {

class LocalReadStream : public ReadStream {
public:
    LocalReadStream(int fd, size_t file_size, size_t buffer_size);
    ~LocalReadStream() override;

    Status read(char* to, size_t n, size_t* read_n) override;

    Status seek(int64_t position) override;

    Status tell(int64_t* position) override;

    Status close() override;

private:
    // Fill the buffer.
    Status fill();

    bool eof() const { return _file_size == _offset; }

private:
    int _fd; // shared
    size_t _file_size;
    // file offset
    size_t _offset = 0;

    char* _buffer;
    size_t _buffer_size;
    // Buffered begin offset relative to file.
    size_t _buffer_begin = 0;
    // Buffered end offset relative to file.
    size_t _buffer_end = 0;
};

} // namespace doris
