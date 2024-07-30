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

#include "s3_file_write_bufferpool.h"

#include <boost/core/noncopyable.hpp>
#include <chrono>
#include <cstring>

#include "common/config.h"
#include "common/logging.h"
#include "io/fs/s3_common.h"
#include "runtime/exec_env.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris {
namespace io {

bvar::Adder<uint64_t> s3_file_buffer_allocated("s3_file_buffer_allocated");

template <typename Allocator = Allocator<false, false, false>>
struct Memory : boost::noncopyable, Allocator {
    Memory() = default;
    explicit Memory(size_t size) : _size(size) {
        alloc(size);
        s3_file_buffer_allocated << 1;
    }
    ~Memory() {
        dealloc();
        s3_file_buffer_allocated << -1;
    }
    void alloc(size_t size) { _data = static_cast<char*>(Allocator::alloc(size, 0)); }
    void dealloc() {
        if (_data == nullptr) {
            return;
        }
        Allocator::free(_data, _size);
        _data = nullptr;
    }
    size_t _size;
    char* _data;
};

struct S3FileBuffer::PartData {
    Memory<> _memory;
    PartData() : _memory(config::s3_write_buffer_size) {}
    ~PartData() = default;
};

S3FileBuffer::S3FileBuffer(ThreadPool* pool)
        : _inner_data(std::make_unique<S3FileBuffer::PartData>()), _thread_pool(pool) {}

S3FileBuffer::~S3FileBuffer() = default;

void S3FileBuffer::on_finished() {
    if (nullptr == _inner_data) {
        return;
    }
    reset();
}

// when there is memory preserved, directly write data to buf
// TODO:(AlexYue): write to file cache otherwise, then we'll wait for free buffer
// and to rob it
Status S3FileBuffer::append_data(const Slice& data) {
    Defer defer {[&] { _size += data.get_size(); }};
    while (true) {
        // if buf is not empty, it means there is memory preserved for this buf
        if (_inner_data != nullptr) {
            memcpy(_inner_data->_memory._data + _size, data.get_data(), data.get_size());
            break;
        } else {
            return Status::InternalError("Failed to allocate s3 writer buffer");
        }
    }
    return Status::OK();
}

void S3FileBuffer::submit() {
    if (LIKELY(nullptr != _inner_data)) {
        _stream_ptr = std::make_shared<StringViewStream>(_inner_data->_memory._data, _size);
    }

    _thread_pool->submit_func([buf = this->shared_from_this()]() { buf->_on_upload(); });
}

void S3FileBufferPool::init(doris::ThreadPool* thread_pool) {
    _thread_pool = thread_pool;
}

Status S3FileBufferPool::allocate(std::shared_ptr<S3FileBuffer>* buf) {
    RETURN_IF_CATCH_EXCEPTION(*buf = std::make_shared<S3FileBuffer>(_thread_pool));
    return Status::OK();
}
} // namespace io
} // namespace doris
