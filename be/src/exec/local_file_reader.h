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

#define _FILE_OFFSET_BITS 64
#include <stdio.h>

#include "exec/file_reader.h"

namespace doris {

class LocalFileReader : public FileReader {
public:
    LocalFileReader(const std::string& path, int64_t start_offset);
    virtual ~LocalFileReader();

    Status open();

    // Read content to 'buf', 'buf_len' is the max size of this buffer.
    // Return ok when read success, and 'buf_len' is set to size of read content
    // If reach to end of file, the eof is set to true. meanwhile 'buf_len'
    // is set to zero.
    virtual Status read(uint8_t* buf, size_t* buf_len, bool* eof) override;

    virtual void close() override;
private:
    std::string _path;
    int64_t _start_offset;
    bool _eof;
    FILE* _fp;
};

}
