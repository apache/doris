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

#include "stream_load_pipe.h"

#include <gen_cpp/internal_service.pb.h>

#include "olap/iterators.h"
#include "runtime/thread_context.h"
#include "util/bit_util.h"

namespace doris {
namespace io {
StreamLoadPipe::StreamLoadPipe(size_t max_buffered_bytes, size_t min_chunk_size,
                               int64_t total_length, bool use_proto)
        : _buffered_bytes(0),
          _proto_buffered_bytes(0),
          _max_buffered_bytes(max_buffered_bytes),
          _min_chunk_size(min_chunk_size),
          _total_length(total_length),
          _use_proto(use_proto) {}

StreamLoadPipe::~StreamLoadPipe() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
    while (!_buf_queue.empty()) {
        _buf_queue.pop_front();
    }
}

Status StreamLoadPipe::read_at(size_t /*offset*/, Slice result, const IOContext& /*io_ctx*/,
                               size_t* bytes_read) {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
    *bytes_read = 0;
    size_t bytes_req = result.size;
    char* to = result.data;
    if (UNLIKELY(bytes_req == 0)) {
        return Status::OK();
    }
    while (*bytes_read < bytes_req) {
        std::unique_lock<std::mutex> l(_lock);
        while (!_cancelled && !_finished && _buf_queue.empty()) {
            _get_cond.wait(l);
        }
        // cancelled
        if (_cancelled) {
            return Status::InternalError("cancelled: {}", _cancelled_reason);
        }
        // finished
        if (_buf_queue.empty()) {
            DCHECK(_finished);
            // break the while loop
            bytes_req = *bytes_read;
            return Status::OK();
        }
        auto buf = _buf_queue.front();
        int64_t copy_size = std::min(bytes_req - *bytes_read, buf->remaining());
        buf->get_bytes(to + *bytes_read, copy_size);
        *bytes_read += copy_size;
        if (!buf->has_remaining()) {
            _buf_queue.pop_front();
            _buffered_bytes -= buf->limit;
            _put_cond.notify_one();
        }
    }
    DCHECK(*bytes_read == bytes_req)
            << "*bytes_read=" << *bytes_read << ", bytes_req=" << bytes_req;
    return Status::OK();
}

// If _total_length == -1, this should be a Kafka routine load task,
// just get the next buffer directly from the buffer queue, because one buffer contains a complete piece of data.
// Otherwise, this should be a stream load task that needs to read the specified amount of data.
Status StreamLoadPipe::read_one_message(std::unique_ptr<uint8_t[]>* data, size_t* length) {
    if (_total_length < -1) {
        return Status::InternalError("invalid, _total_length is: {}", _total_length);
    } else if (_total_length == 0) {
        // no data
        *length = 0;
        return Status::OK();
    }

    if (_total_length == -1) {
        return _read_next_buffer(data, length);
    }

    // _total_length > 0, read the entire data
    data->reset(new uint8_t[_total_length]);
    Slice result(data->get(), _total_length);
    IOContext io_ctx;
    Status st = read_at(0, result, io_ctx, length);
    return st;
}

Status StreamLoadPipe::append_and_flush(const char* data, size_t size, size_t proto_byte_size) {
    ByteBufferPtr buf = ByteBuffer::allocate(BitUtil::RoundUpToPowerOfTwo(size + 1));
    buf->put_bytes(data, size);
    buf->flip();
    return _append(buf, proto_byte_size);
}

Status StreamLoadPipe::append(const char* data, size_t size) {
    size_t pos = 0;
    if (_write_buf != nullptr) {
        if (size < _write_buf->remaining()) {
            _write_buf->put_bytes(data, size);
            return Status::OK();
        } else {
            pos = _write_buf->remaining();
            _write_buf->put_bytes(data, pos);

            _write_buf->flip();
            RETURN_IF_ERROR(_append(_write_buf));
            _write_buf.reset();
        }
    }
    // need to allocate a new chunk, min chunk is 64k
    size_t chunk_size = std::max(_min_chunk_size, size - pos);
    chunk_size = BitUtil::RoundUpToPowerOfTwo(chunk_size);
    _write_buf = ByteBuffer::allocate(chunk_size);
    _write_buf->put_bytes(data + pos, size - pos);
    return Status::OK();
}

Status StreamLoadPipe::append(const ByteBufferPtr& buf) {
    if (_write_buf != nullptr) {
        _write_buf->flip();
        RETURN_IF_ERROR(_append(_write_buf));
        _write_buf.reset();
    }
    return _append(buf);
}

// read the next buffer from _buf_queue
Status StreamLoadPipe::_read_next_buffer(std::unique_ptr<uint8_t[]>* data, size_t* length) {
    std::unique_lock<std::mutex> l(_lock);
    while (!_cancelled && !_finished && _buf_queue.empty()) {
        _get_cond.wait(l);
    }
    // cancelled
    if (_cancelled) {
        return Status::InternalError("cancelled: {}", _cancelled_reason);
    }
    // finished
    if (_buf_queue.empty()) {
        DCHECK(_finished);
        data->reset();
        *length = 0;
        return Status::OK();
    }
    auto buf = _buf_queue.front();
    *length = buf->remaining();
    data->reset(new uint8_t[*length]);
    buf->get_bytes((char*)(data->get()), *length);
    _buf_queue.pop_front();
    _buffered_bytes -= buf->limit;
    if (_use_proto) {
        PDataRow** ptr = reinterpret_cast<PDataRow**>(data->get());
        _proto_buffered_bytes -= (sizeof(PDataRow*) + (*ptr)->GetCachedSize());
    }
    _put_cond.notify_one();
    return Status::OK();
}

Status StreamLoadPipe::_append(const ByteBufferPtr& buf, size_t proto_byte_size) {
    {
        std::unique_lock<std::mutex> l(_lock);
        // if _buf_queue is empty, we append this buf without size check
        if (_use_proto) {
            while (!_cancelled && !_buf_queue.empty() &&
                   (_proto_buffered_bytes + proto_byte_size > _max_buffered_bytes)) {
                _put_cond.wait(l);
            }
        } else {
            while (!_cancelled && !_buf_queue.empty() &&
                   _buffered_bytes + buf->remaining() > _max_buffered_bytes) {
                _put_cond.wait(l);
            }
        }
        if (_cancelled) {
            return Status::InternalError("cancelled: {}", _cancelled_reason);
        }
        _buf_queue.push_back(buf);
        if (_use_proto) {
            _proto_buffered_bytes += proto_byte_size;
        } else {
            _buffered_bytes += buf->remaining();
        }
    }
    _get_cond.notify_one();
    return Status::OK();
}

// called when producer finished
Status StreamLoadPipe::finish() {
    if (_write_buf != nullptr) {
        _write_buf->flip();
        _append(_write_buf);
        _write_buf.reset();
    }
    {
        std::lock_guard<std::mutex> l(_lock);
        _finished = true;
    }
    _get_cond.notify_all();
    return Status::OK();
}

// called when producer/consumer failed
void StreamLoadPipe::cancel(const std::string& reason) {
    {
        std::lock_guard<std::mutex> l(_lock);
        _cancelled = true;
        _cancelled_reason = reason;
    }
    _get_cond.notify_all();
    _put_cond.notify_all();
}

} // namespace io
} // namespace doris
