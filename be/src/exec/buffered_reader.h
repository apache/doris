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

#include <memory>

#include "common/status.h"
#include "exec/file_reader.h"
#include "olap/olap_define.h"
#include "util/runtime_profile.h"

namespace doris {

// Buffered Reader
// Add a cache layer between the caller and the file reader to reduce the
// times of calls to the read function to speed up.
class BufferedReader : public FileReader {
public:
    // If the reader need the file size, set it when construct FileReader.
    // There is no other way to set the file size.
    // buffered_reader will acquire reader
    // -1 means using config buffered_reader_buffer_size_bytes
    BufferedReader(RuntimeProfile* profile, FileReader* reader, int64_t = -1L);
    virtual ~BufferedReader();

    virtual Status open() override;

    // Read
    virtual Status read(uint8_t* buf, int64_t buf_len, int64_t* bytes_read, bool* eof) override;
    virtual Status readat(int64_t position, int64_t nbytes, int64_t* bytes_read,
                          void* out) override;
    virtual Status read_one_message(std::unique_ptr<uint8_t[]>* buf, int64_t* length) override;
    virtual int64_t size() override;
    virtual Status seek(int64_t position) override;
    virtual Status tell(int64_t* position) override;
    virtual void close() override;
    virtual bool closed() override;

private:
    Status _fill();
    Status _read_once(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out);

private:
    RuntimeProfile* _profile;
    std::unique_ptr<FileReader> _reader;
    char* _buffer;
    int64_t _buffer_size;
    int64_t _buffer_offset;
    int64_t _buffer_limit;
    int64_t _cur_offset;

    int64_t _read_count = 0;
    int64_t _remote_read_count = 0;

    // total time cost in this reader
    RuntimeProfile::Counter* _read_timer = nullptr;
    // time cost of "_reader", "remote" because "_reader" is always a remote reader
    RuntimeProfile::Counter* _remote_read_timer = nullptr;
    // counter of calling read()
    RuntimeProfile::Counter* _read_counter = nullptr;
    // counter of calling "remote read()"
    RuntimeProfile::Counter* _remote_read_counter = nullptr;
};

} // namespace doris
