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

#include <condition_variable>
#include <memory>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "olap/olap_define.h"
#include "util/runtime_profile.h"

namespace doris {

class BufferedReader;
struct PrefetchBuffer : std::enable_shared_from_this<PrefetchBuffer> {
    enum class BufferStatus { RESET, PENDING, PREFETCHED, CLOSED };
    PrefetchBuffer() = default;
    PrefetchBuffer(size_t offset, size_t buffer_size, size_t whole_buffer_size,
                   io::FileReader* reader)
            : _offset(offset),
              _size(buffer_size),
              _whole_buffer_size(whole_buffer_size),
              _reader(reader),
              _buf(buffer_size, '0') {}
    PrefetchBuffer(PrefetchBuffer&& other)
            : _offset(other._offset),
              _size(other._size),
              _whole_buffer_size(other._whole_buffer_size),
              _reader(other._reader),
              _buf(std::move(other._buf)) {}
    ~PrefetchBuffer() = default;
    size_t _offset;
    size_t _size;
    size_t _len {0};
    size_t _whole_buffer_size;
    io::FileReader* _reader;
    std::string _buf;
    BufferStatus _buffer_status {BufferStatus::RESET};
    std::mutex _lock;
    std::condition_variable _prefetched;
    Status _prefetch_status {Status::OK()};
    // @brief: reset the start offset of this buffer to offset
    // @param: the new start offset for this buffer
    void reset_offset(size_t offset);
    // @brief: start to fetch the content between [_offset, _offset + _size)
    void prefetch_buffer();
    // @brief: used by BufferedReader to read the prefetched data
    // @param[off] read start address
    // @param[buf] buffer to put the actual content
    // @param[buf_len] maximum len trying to read
    // @param[bytes_read] actual bytes read
    Status read_buffer(size_t off, const char* buf, size_t buf_len, size_t* bytes_read);
    // @brief: shut down the buffer until the prior prefetching task is done
    void close();
    // @brief: to detect whether this buffer contains off
    // @param[off] detect offset
    bool inline contains(size_t off) const { return _offset <= off && off < _offset + _size; }
};

class BufferedReader : public io::FileReader {
public:
    BufferedReader(io::FileReaderSPtr reader, int64_t buffer_size = -1L);
    ~BufferedReader() override;

    Status close() override;

    Status read_at(size_t offset, Slice result, const IOContext& io_ctx,
                   size_t* bytes_read) override;

    const io::Path& path() const override { return _reader->path(); }

    size_t size() const override { return _reader->size(); }

    bool closed() const override { return _closed; }

    std::shared_ptr<io::FileSystem> fs() const override { return _reader->fs(); }

private:
    size_t get_buffer_pos(int64_t position) const {
        return (position % _whole_pre_buffer_size) / s_max_pre_buffer_size;
    }
    size_t get_buffer_offset(int64_t position) const {
        return (position / s_max_pre_buffer_size) * s_max_pre_buffer_size;
    }
    void resetAllBuffer(size_t position) {
        for (int64_t i = 0; i < _pre_buffers.size(); i++) {
            int64_t cur_pos = position + i * s_max_pre_buffer_size;
            int cur_buf_pos = get_buffer_pos(cur_pos);
            // reset would do all the prefetch work
            _pre_buffers[cur_buf_pos]->reset_offset(get_buffer_offset(cur_pos));
        }
    }

    io::FileReaderSPtr _reader;
    int64_t s_max_pre_buffer_size = config::prefetch_single_buffer_size_mb * 1024 * 1024;
    std::vector<std::shared_ptr<PrefetchBuffer>> _pre_buffers;
    int64_t _whole_pre_buffer_size;
    std::atomic_bool _closed {false};
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
