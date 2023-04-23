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

#include <cstring>

#include "common/config.h"
#include "common/logging.h"
#include "io/fs/s3_common.h"
#include "runtime/exec_env.h"
#include "util/defer_op.h"

namespace doris {
namespace io {
void S3FileBuffer::on_finished() {
    if (_buf == nullptr) {
        return;
    }
    reset();
    S3FileBufferPool::GetInstance()->reclaim(shared_from_this());
}

// when there is memory preserved, directly write data to buf
// TODO:(AlexYue): write to file cache otherwise, then we'll wait for free buffer
// and to rob it
void S3FileBuffer::append_data(const Slice& data) {
    Defer defer {[&] { _size += data.get_size(); }};
    while (true) {
        // if buf is not empty, it means there is memory preserved for this buf
        if (_buf != nullptr) {
            memcpy(_buf->data() + _size, data.get_data(), data.get_size());
            break;
        } else {
            // wait allocate buffer pool
            auto tmp = S3FileBufferPool::GetInstance()->allocate(true);
            rob_buffer(tmp);
        }
    }
}

void S3FileBuffer::submit() {
    if (LIKELY(_buf != nullptr)) {
        _stream_ptr = std::make_shared<StringViewStream>(_buf->data(), _size);
    }

    ExecEnv::GetInstance()->buffered_reader_prefetch_thread_pool()->submit_func(
            [buf = this->shared_from_this()]() { buf->_on_upload(); });
}

S3FileBufferPool::S3FileBufferPool() {
    // the nums could be one configuration
    size_t buf_num = config::s3_write_buffer_whole_size / config::s3_write_buffer_size;
    DCHECK((config::s3_write_buffer_size >= 5 * 1024 * 1024) &&
           (config::s3_write_buffer_whole_size > config::s3_write_buffer_size));
    LOG_INFO("S3 file buffer pool with {} buffers", buf_num);
    for (size_t i = 0; i < buf_num; i++) {
        auto buf = std::make_shared<S3FileBuffer>();
        buf->reserve_buffer();
        _free_buffers.emplace_back(std::move(buf));
    }
}

std::shared_ptr<S3FileBuffer> S3FileBufferPool::allocate(bool reserve) {
    std::shared_ptr<S3FileBuffer> buf;
    // if need reserve then we must ensure return buf with memory preserved
    if (reserve) {
        {
            std::unique_lock<std::mutex> lck {_lock};
            _cv.wait(lck, [this]() { return !_free_buffers.empty(); });
            buf = std::move(_free_buffers.front());
            _free_buffers.pop_front();
        }
        return buf;
    }
    // try to get one memory reserved buffer
    {
        std::unique_lock<std::mutex> lck {_lock};
        if (!_free_buffers.empty()) {
            buf = std::move(_free_buffers.front());
            _free_buffers.pop_front();
        }
    }
    if (buf != nullptr) {
        return buf;
    }
    // if there is no free buffer and no need to reserve memory, we could return one empty buffer
    buf = std::make_shared<S3FileBuffer>();
    // if the buf has no memory reserved, it would try to write the data to file cache first
    // or it would try to rob buffer from other S3FileBuffer
    return buf;
}
} // namespace io
} // namespace doris
