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
#include <cstdint>
#include <fstream>
#include <functional>
#include <list>
#include <memory>
#include <mutex>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/s3_common.h"
#include "runtime/exec_env.h"
#include "util/slice.h"

namespace doris {
class ThreadPool;
namespace io {

// TODO(AlexYue): 1. support write into cache 2. unify write buffer and read buffer
struct S3FileBuffer : public std::enable_shared_from_this<S3FileBuffer> {
    using Callback = std::function<void()>;

    S3FileBuffer(ThreadPool* pool) { _thread_pool = pool; }
    ~S3FileBuffer() = default;

    void rob_buffer(std::shared_ptr<S3FileBuffer>& other) {
        _buf = other->_buf;
        // we should clear other's memory buffer in case it woule be reclaimed twice
        // when calling on_finished
        other->_buf.clear();
    }

    void reserve_buffer(Slice s) { _buf = s; }

    // append data into the memory buffer inside
    // or into the file cache if the buffer has no memory buffer
    void append_data(const Slice& data);
    // upload to S3 and file cache in async threadpool
    void submit();
    // set the callback to upload to S3 file
    void set_upload_remote_callback(Callback cb) { _upload_to_remote_callback = std::move(cb); }
    // set callback to do task sync for the caller
    void set_finish_upload(Callback cb) { _on_finish_upload = std::move(cb); }
    // set cancel callback to indicate if the whole task is cancelled or not
    void set_is_cancel(std::function<bool()> cb) { _is_cancelled = std::move(cb); }
    // set callback to notify all the tasks that the whole procedure could be cancelled
    // if this buffer's task failed
    void set_on_failed(std::function<void(Status)> cb) { _on_failed = std::move(cb); }
    // reclaim this buffer when task is done
    void on_finished();
    // set the status of the caller if task failed
    void set_status(Status s) { _status = std::move(s); }
    // get the size of the content already appendded
    size_t get_size() const { return _size; }
    // get the underlying stream containing
    const std::shared_ptr<std::iostream>& get_stream() const { return _stream_ptr; }
    // get file offset corresponding to the buffer
    size_t get_file_offset() const { return _offset; }
    // set the offset of the buffer
    void set_file_offset(size_t offset) { _offset = offset; }
    // reset this buffer to be reused
    void reset() {
        _upload_to_remote_callback = nullptr;
        _is_cancelled = nullptr;
        _on_failed = nullptr;
        _on_finish_upload = nullptr;
        _offset = 0;
        _size = 0;
    }

    Callback _upload_to_remote_callback = nullptr;
    // to control the callback control flow
    // 1. read from cache if the data is written to cache first
    // 2. upload content of buffer to S3
    // 3. upload content to file cache if necessary
    // 4. call the finish callback caller specified
    // 5. reclaim self
    void _on_upload() {
        _upload_to_remote_callback();
        _on_finish_upload();
        on_finished();
    };
    // the caller might be cancelled
    std::function<bool()> _is_cancelled = []() { return false; };
    // set the caller to be failed
    std::function<void(Status)> _on_failed = nullptr;
    // caller of this buf could use this callback to do syncronization
    Callback _on_finish_upload = nullptr;
    Status _status;
    size_t _offset {0};
    size_t _size {0};
    std::shared_ptr<std::iostream> _stream_ptr;
    // only served as one reserved buffer
    Slice _buf;
    size_t _append_offset {0};
    // not owned
    ThreadPool* _thread_pool = nullptr;
};

class S3FileBufferPool {
public:
    S3FileBufferPool() = default;
    ~S3FileBufferPool() = default;

    // should be called one and only once
    // at startup
    void init(int32_t s3_write_buffer_whole_size, int32_t s3_write_buffer_size,
              doris::ThreadPool* thread_pool);

    static S3FileBufferPool* GetInstance() {
        return ExecEnv::GetInstance()->get_s3_file_buffer_pool();
    }

    void reclaim(Slice buf) {
        std::unique_lock<std::mutex> lck {_lock};
        _free_raw_buffers.emplace_front(buf);
        _cv.notify_all();
    }

    std::shared_ptr<S3FileBuffer> allocate(bool reserve = false);

private:
    std::mutex _lock;
    std::condition_variable _cv;
    std::unique_ptr<char[]> _whole_mem_buffer;
    std::list<Slice> _free_raw_buffers;
    // not owned
    ThreadPool* _thread_pool = nullptr;
};
} // namespace io
} // namespace doris
