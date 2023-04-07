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

#include "io/fs/buffered_reader.h"

#include <algorithm>
#include <sstream>

#include "common/config.h"
#include "olap/iterators.h"
#include "olap/olap_define.h"

namespace doris {
namespace io {

// there exists occasions where the buffer is already closed but
// some prior tasks are still queued in thread pool, so we have to check whether
// the buffer is closed each time the condition variable is notified.
void PrefetchBuffer::reset_offset(size_t offset) {
    if (UNLIKELY(offset >= _end_offset)) {
        return;
    }
    {
        std::unique_lock lck {_lock};
        _prefetched.wait(lck, [this]() { return _buffer_status != BufferStatus::PENDING; });
        if (UNLIKELY(_buffer_status == BufferStatus::CLOSED)) {
            _prefetched.notify_all();
            return;
        }
        _buffer_status = BufferStatus::RESET;
        _offset = offset;
        _prefetched.notify_all();
    }
    ExecEnv::GetInstance()->buffered_reader_prefetch_thread_pool()->submit_func(
            [buffer_ptr = shared_from_this()]() { buffer_ptr->prefetch_buffer(); });
}

// only this function would run concurrently in another thread
void PrefetchBuffer::prefetch_buffer() {
    {
        std::unique_lock lck {_lock};
        _prefetched.wait(lck, [this]() {
            return _buffer_status == BufferStatus::RESET || _buffer_status == BufferStatus::CLOSED;
        });
        // in case buffer is already closed
        if (UNLIKELY(_buffer_status == BufferStatus::CLOSED)) {
            _prefetched.notify_all();
            return;
        }
        _buffer_status = BufferStatus::PENDING;
        _prefetched.notify_all();
    }
    _len = 0;
    Status s;

    size_t buf_size = _end_offset - _offset > _size ? _size : _end_offset - _offset;
    s = _reader->read_at(_offset, Slice {_buf.data(), buf_size}, &_len);
    std::unique_lock lck {_lock};
    _prefetched.wait(lck, [this]() { return _buffer_status == BufferStatus::PENDING; });
    if (!s.ok() && _offset < _reader->size()) {
        _prefetch_status = std::move(s);
    }
    _buffer_status = BufferStatus::PREFETCHED;
    _prefetched.notify_all();
    // eof would come up with len == 0, it would be handled by read_buffer
}

Status PrefetchBuffer::read_buffer(size_t off, const char* out, size_t buf_len,
                                   size_t* bytes_read) {
    if (UNLIKELY(off >= _end_offset)) {
        // Reader can read out of [_start_offset, _end_offset) by synchronous method.
        return _reader->read_at(off, Slice {out, buf_len}, bytes_read);
    }
    {
        std::unique_lock lck {_lock};
        // buffer must be prefetched or it's closed
        _prefetched.wait(lck, [this]() {
            return _buffer_status == BufferStatus::PREFETCHED ||
                   _buffer_status == BufferStatus::CLOSED;
        });
        if (UNLIKELY(BufferStatus::CLOSED == _buffer_status)) {
            return Status::OK();
        }
    }
    RETURN_IF_ERROR(_prefetch_status);
    // there is only parquet would do not sequence read
    // it would read the end of the file first
    if (UNLIKELY(!contains(off))) {
        reset_offset((off / _size) * _size);
        return read_buffer(off, out, buf_len, bytes_read);
    }
    if (UNLIKELY(0 == _len || _offset + _len < off)) {
        return Status::OK();
    }
    // [0]: maximum len trying to read, [1] maximum length buffer can provide, [2] actual len buffer has
    size_t read_len = std::min({buf_len, _offset + _size - off, _offset + _len - off});
    memcpy((void*)out, _buf.data() + (off - _offset), read_len);
    *bytes_read = read_len;
    if (off + *bytes_read == _offset + _len) {
        reset_offset(_offset + _whole_buffer_size);
    }
    return Status::OK();
}

void PrefetchBuffer::close() {
    std::unique_lock lck {_lock};
    // in case _reader still tries to write to the buf after we close the buffer
    _prefetched.wait(lck, [this]() { return _buffer_status != BufferStatus::PENDING; });
    _buffer_status = BufferStatus::CLOSED;
    _prefetched.notify_all();
}

// buffered reader
PrefetchBufferedReader::PrefetchBufferedReader(io::FileReaderSPtr reader, int64_t offset,
                                               int64_t length, int64_t buffer_size)
        : _reader(std::move(reader)), _start_offset(offset), _end_offset(offset + length) {
    if (buffer_size == -1L) {
        buffer_size = config::remote_storage_read_buffer_mb * 1024 * 1024;
    }
    _size = _reader->size();
    _whole_pre_buffer_size = buffer_size;
    int buffer_num = buffer_size > s_max_pre_buffer_size ? buffer_size / s_max_pre_buffer_size : 1;
    // set the _cur_offset of this reader as same as the inner reader's,
    // to make sure the buffer reader will start to read at right position.
    for (int i = 0; i < buffer_num; i++) {
        _pre_buffers.emplace_back(
                std::make_shared<PrefetchBuffer>(_start_offset, _end_offset, s_max_pre_buffer_size,
                                                 _whole_pre_buffer_size, _reader.get()));
    }
}

PrefetchBufferedReader::~PrefetchBufferedReader() {
    close();
    _closed = true;
}

Status PrefetchBufferedReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                            const IOContext* io_ctx) {
    if (!_initialized) {
        reset_all_buffer(offset);
        _initialized = true;
    }
    if (UNLIKELY(result.get_size() == 0 || offset >= size())) {
        *bytes_read = 0;
        return Status::OK();
    }
    size_t nbytes = result.get_size();
    int actual_bytes_read = 0;
    while (actual_bytes_read < nbytes && offset < size()) {
        size_t read_num = 0;
        auto buffer_pos = get_buffer_pos(offset);
        RETURN_IF_ERROR(
                _pre_buffers[buffer_pos]->read_buffer(offset, result.get_data() + actual_bytes_read,
                                                      nbytes - actual_bytes_read, &read_num));
        actual_bytes_read += read_num;
        offset += read_num;
    }
    *bytes_read = actual_bytes_read;
    return Status::OK();
}

Status PrefetchBufferedReader::close() {
    std::for_each(_pre_buffers.begin(), _pre_buffers.end(),
                  [](std::shared_ptr<PrefetchBuffer>& buffer) { buffer->close(); });
    _reader->close();
    _closed = true;

    return Status::OK();
}

BufferedFileStreamReader::BufferedFileStreamReader(io::FileReaderSPtr file, uint64_t offset,
                                                   uint64_t length, size_t max_buf_size)
        : _file(file),
          _file_start_offset(offset),
          _file_end_offset(offset + length),
          _max_buf_size(max_buf_size) {}

Status BufferedFileStreamReader::read_bytes(const uint8_t** buf, uint64_t offset,
                                            const size_t bytes_to_read, const IOContext* io_ctx) {
    if (offset < _file_start_offset || offset >= _file_end_offset) {
        return Status::IOError("Out-of-bounds Access");
    }
    int64_t end_offset = offset + bytes_to_read;
    if (_buf_start_offset <= offset && _buf_end_offset >= end_offset) {
        *buf = _buf.get() + offset - _buf_start_offset;
        return Status::OK();
    }
    size_t buf_size = std::max(_max_buf_size, bytes_to_read);
    if (_buf_size < buf_size) {
        std::unique_ptr<uint8_t[]> new_buf(new uint8_t[buf_size]);
        if (offset >= _buf_start_offset && offset < _buf_end_offset) {
            memcpy(new_buf.get(), _buf.get() + offset - _buf_start_offset,
                   _buf_end_offset - offset);
        }
        _buf = std::move(new_buf);
        _buf_size = buf_size;
    } else if (offset > _buf_start_offset && offset < _buf_end_offset) {
        memmove(_buf.get(), _buf.get() + offset - _buf_start_offset, _buf_end_offset - offset);
    }
    if (offset < _buf_start_offset || offset >= _buf_end_offset) {
        _buf_end_offset = offset;
    }
    _buf_start_offset = offset;
    int64_t buf_remaining = _buf_end_offset - _buf_start_offset;
    int64_t to_read = std::min(_buf_size - buf_remaining, _file_end_offset - _buf_end_offset);
    int64_t has_read = 0;
    SCOPED_RAW_TIMER(&_statistics.read_time);
    while (has_read < to_read) {
        size_t loop_read = 0;
        Slice result(_buf.get() + buf_remaining + has_read, to_read - has_read);
        RETURN_IF_ERROR(_file->read_at(_buf_end_offset + has_read, result, &loop_read, io_ctx));
        _statistics.read_calls++;
        if (loop_read == 0) {
            break;
        }
        has_read += loop_read;
    }
    if (has_read != to_read) {
        return Status::Corruption("Try to read {} bytes, but received {} bytes", to_read, has_read);
    }
    _statistics.read_bytes += to_read;
    _buf_end_offset += to_read;
    *buf = _buf.get();
    return Status::OK();
}

Status BufferedFileStreamReader::read_bytes(Slice& slice, uint64_t offset,
                                            const IOContext* io_ctx) {
    return read_bytes((const uint8_t**)&slice.data, offset, slice.size, io_ctx);
}

} // namespace io
} // namespace doris
