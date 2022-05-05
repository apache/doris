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

#include <condition_variable>
#include <deque>
#include <mutex>

#include "exec/file_reader.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/message_body_sink.h"
#include "util/bit_util.h"
#include "util/byte_buffer.h"

namespace doris {

// StreamLoadPipe use to transfer data from producer to consumer
// Data in pip is stored in chunks.
class StreamLoadPipe : public MessageBodySink, public FileReader {
public:
    StreamLoadPipe(size_t max_buffered_bytes = 1024 * 1024, size_t min_chunk_size = 64 * 1024,
                   int64_t total_length = -1, bool use_proto = false)
            : _buffered_bytes(0),
              _proto_buffered_bytes(0),
              _max_buffered_bytes(max_buffered_bytes),
              _min_chunk_size(min_chunk_size),
              _total_length(total_length),
              _use_proto(use_proto) {}
    virtual ~StreamLoadPipe() {}

    Status open() override { return Status::OK(); }

    Status append_and_flush(const char* data, size_t size, size_t proto_byte_size = 0) {
        ByteBufferPtr buf = ByteBuffer::allocate(BitUtil::RoundUpToPowerOfTwo(size + 1));
        buf->put_bytes(data, size);
        buf->flip();
        return _append(buf, proto_byte_size);
    }

    Status append(const char* data, size_t size) override {
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

    Status append(const ByteBufferPtr& buf) override {
        if (_write_buf != nullptr) {
            _write_buf->flip();
            RETURN_IF_ERROR(_append(_write_buf));
            _write_buf.reset();
        }
        return _append(buf);
    }

    // If _total_length == -1, this should be a Kafka routine load task,
    // just get the next buffer directly from the buffer queue, because one buffer contains a complete piece of data.
    // Otherwise, this should be a stream load task that needs to read the specified amount of data.
    Status read_one_message(std::unique_ptr<uint8_t[]>* data, int64_t* length) override {
        if (_total_length < -1) {
            std::stringstream ss;
            ss << "invalid, _total_length is: " << _total_length;
            return Status::InternalError(ss.str());
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
        bool eof = false;
        Status st = read(data->get(), _total_length, length, &eof);
        if (eof) {
            *length = 0;
        }
        return st;
    }

    Status read(uint8_t* data, int64_t data_size, int64_t* bytes_read, bool* eof) override {
        *bytes_read = 0;
        while (*bytes_read < data_size) {
            std::unique_lock<std::mutex> l(_lock);
            while (!_cancelled && !_finished && _buf_queue.empty()) {
                _get_cond.wait(l);
            }
            // cancelled
            if (_cancelled) {
                return Status::InternalError("cancelled: " + _cancelled_reason);
            }
            // finished
            if (_buf_queue.empty()) {
                DCHECK(_finished);
                data_size = *bytes_read;
                *eof = (*bytes_read == 0);
                return Status::OK();
            }
            auto buf = _buf_queue.front();
            int64_t copy_size = std::min(data_size - *bytes_read, (int64_t)buf->remaining());
            buf->get_bytes((char*)data + *bytes_read, copy_size);
            *bytes_read += copy_size;
            if (!buf->has_remaining()) {
                _buf_queue.pop_front();
                _buffered_bytes -= buf->limit;
                _put_cond.notify_one();
            }
        }
        DCHECK(*bytes_read == data_size)
                << "*bytes_read=" << *bytes_read << ", data_size=" << data_size;
        *eof = false;
        return Status::OK();
    }

    Status readat(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) override {
        return Status::InternalError("Not implemented");
    }

    int64_t size() override { return 0; }

    Status seek(int64_t position) override { return Status::InternalError("Not implemented"); }

    Status tell(int64_t* position) override { return Status::InternalError("Not implemented"); }

    // called when consumer finished
    void close() override { cancel("closed"); }

    bool closed() override { return _cancelled; }

    // called when producer finished
    Status finish() override {
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
    void cancel(const std::string& reason) override {
        {
            std::lock_guard<std::mutex> l(_lock);
            _cancelled = true;
            _cancelled_reason = reason;
        }
        _get_cond.notify_all();
        _put_cond.notify_all();
    }

private:
    // read the next buffer from _buf_queue
    Status _read_next_buffer(std::unique_ptr<uint8_t[]>* data, int64_t* length) {
        std::unique_lock<std::mutex> l(_lock);
        while (!_cancelled && !_finished && _buf_queue.empty()) {
            _get_cond.wait(l);
        }
        // cancelled
        if (_cancelled) {
            return Status::InternalError("cancelled: " + _cancelled_reason);
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

    Status _append(const ByteBufferPtr& buf, size_t proto_byte_size = 0) {
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
                return Status::InternalError("cancelled: " + _cancelled_reason);
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

    // Blocking queue
    std::mutex _lock;
    size_t _buffered_bytes;
    size_t _proto_buffered_bytes;
    size_t _max_buffered_bytes;
    size_t _min_chunk_size;
    // The total amount of data expected to be read.
    // In some scenarios, such as loading json format data through stream load,
    // the data needs to be completely read before it can be parsed,
    // so the total size of the data needs to be known.
    // The default is -1, which means that the data arrives in a stream
    // and the length is unknown.
    // size_t is unsigned, so use int64_t
    int64_t _total_length = -1;
    bool _use_proto = false;
    std::deque<ByteBufferPtr> _buf_queue;
    std::condition_variable _put_cond;
    std::condition_variable _get_cond;

    ByteBufferPtr _write_buf;
};

} // namespace doris
