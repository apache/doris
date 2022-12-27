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
#include "io/file_reader.h"
#include "io/fs/file_reader.h"
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
    int64_t _remote_bytes = 0;

    // total time cost in this reader
    RuntimeProfile::Counter* _read_timer = nullptr;
    // time cost of "_reader", "remote" because "_reader" is always a remote reader
    RuntimeProfile::Counter* _remote_read_timer = nullptr;
    // counter of calling read()
    RuntimeProfile::Counter* _read_counter = nullptr;
    // counter of calling "remote read()"
    RuntimeProfile::Counter* _remote_read_counter = nullptr;
    RuntimeProfile::Counter* _remote_read_bytes = nullptr;
    RuntimeProfile::Counter* _remote_read_rate = nullptr;
};

/**
 * Load all the needed data in underlying buffer, so the caller does not need to prepare the data container.
 */
class BufferedStreamReader {
public:
    struct Statistics {
        int64_t read_time = 0;
        int64_t read_calls = 0;
        int64_t read_bytes = 0;
    };

    /**
     * Return the address of underlying buffer that locates the start of data between [offset, offset + bytes_to_read)
     * @param buf the buffer address to save the start address of data
     * @param offset start offset ot read in stream
     * @param bytes_to_read bytes to read
     */
    virtual Status read_bytes(const uint8_t** buf, uint64_t offset, const size_t bytes_to_read) = 0;
    /**
     * Save the data address to slice.data, and the slice.size is the bytes to read.
     */
    virtual Status read_bytes(Slice& slice, uint64_t offset) = 0;
    Statistics& statistics() { return _statistics; }
    virtual ~BufferedStreamReader() = default;

protected:
    Statistics _statistics;
};

class BufferedFileStreamReader : public BufferedStreamReader {
public:
    BufferedFileStreamReader(io::FileReaderSPtr file, uint64_t offset, uint64_t length,
                             size_t max_buf_size);
    ~BufferedFileStreamReader() override = default;

    Status read_bytes(const uint8_t** buf, uint64_t offset, const size_t bytes_to_read) override;
    Status read_bytes(Slice& slice, uint64_t offset) override;

private:
    std::unique_ptr<uint8_t[]> _buf;
    io::FileReaderSPtr _file;
    uint64_t _file_start_offset;
    uint64_t _file_end_offset;

    uint64_t _buf_start_offset = 0;
    uint64_t _buf_end_offset = 0;
    size_t _buf_size = 0;
    size_t _max_buf_size;
};

} // namespace doris
