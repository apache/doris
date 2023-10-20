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

#include "common/status.h"
#include "io/cache/block/block_file_segment.h"
#include "runtime/exec_env.h"
#include "util/slice.h"
#include "util/threadpool.h"

namespace doris {
namespace io {
enum class BufferType { DOWNLOAD, UPLOAD };
using FileBlocksHolderPtr = std::unique_ptr<FileBlocksHolder>;
struct OperationState {
    OperationState(std::function<bool(Status)> sync_after_complete_task,
                   std::function<bool()> is_cancelled)
            : _sync_after_complete_task(std::move(sync_after_complete_task)),
              _is_cancelled(std::move(is_cancelled)) {}
    /**
    * set the val of this operation state which indicates it failed or succeeded
    *
    * @param S the execution result
    */
    void set_val(Status s = Status::OK()) {
        // make sure we wouldn't sync twice
        if (_value_set) [[unlikely]] {
            return;
        }
        if (nullptr != _sync_after_complete_task) {
            _fail_after_sync = _sync_after_complete_task(s);
        }
        _value_set = true;
    }

    /**
    * detect whether the execution task is done
    *
    * @return is the execution task is done
    */
    [[nodiscard]] bool is_cancelled() const {
        DCHECK(nullptr != _is_cancelled);
        // If _fail_after_sync is true then it means the sync task already returns
        // that the task failed and if the outside file writer might already be
        // destructed
        return _fail_after_sync ? true : _is_cancelled();
    }

    std::function<bool(Status)> _sync_after_complete_task;
    std::function<bool()> _is_cancelled;
    bool _value_set = false;
    bool _fail_after_sync = false;
};

struct FileBuffer : public std::enable_shared_from_this<FileBuffer> {
    FileBuffer(std::function<FileBlocksHolderPtr()> alloc_holder, size_t offset,
               OperationState state, bool reserve = false);
    virtual ~FileBuffer() { on_finish(); }
    /**
    * submit the correspoding task to async executor
    */
    virtual void submit() = 0;
    /**
    * append data to the inner memory buffer
    *
    * @param S the content to be appended
    */
    virtual Status append_data(const Slice& s) = 0;
    /**
    * call the reclaim callback when task is done 
    */
    void on_finish();
    /**
    * swap memory buffer
    *
    * @param other which has memory buffer allocated
    */
    void swap_buffer(Slice& other);
    /**
    * set the val of it's operation state
    *
    * @param S the execution result
    */
    void set_val(Status s) { _state.set_val(s); }
    /**
    * get the start offset of this file buffer
    *
    * @return start offset of this file buffer
    */
    size_t get_file_offset() const { return _offset; }
    /**
    * get the size of the buffered data
    *
    * @return the size of the buffered data
    */
    size_t get_size() const { return _size; }
    /**
    * detect whether the execution task is done
    *
    * @return is the execution task is done
    */
    bool is_cancelled() const { return _state.is_cancelled(); }

    std::function<FileBlocksHolderPtr()> _alloc_holder;
    Slice _buffer;
    size_t _offset;
    size_t _size;
    OperationState _state;
    size_t _capacity;
};

struct UploadFileBuffer final : public FileBuffer {
    UploadFileBuffer(std::function<void(UploadFileBuffer&)> upload_cb, OperationState state,
                     size_t offset, std::function<FileBlocksHolderPtr()> alloc_holder,
                     size_t index_offset)
            : FileBuffer(alloc_holder, offset, state),
              _upload_to_remote(std::move(upload_cb)),
              _index_offset(index_offset) {}
    ~UploadFileBuffer() override = default;
    void submit() override;
    /**
    * set the index offset
    *
    * @param offset the index offset
    */
    void set_index_offset(size_t offset);
    Status append_data(const Slice& s) override;
    /**
    * read the content from local file cache
    * because previously lack of  memory buffer
    */
    void read_from_cache();
    /**
    * write the content inside memory buffer into 
    * local file cache
    */
    void upload_to_local_file_cache(bool);
    /**
    * do the upload work
    * 1. read from cache if the data is written to cache first
    * 2. upload content of buffer to S3
    * 3. upload content to file cache if necessary
    * 4. call the finish callback caller specified
    * 5. reclaim self
    */
    void on_upload() {
        if (_buffer.empty()) {
            read_from_cache();
        }
        _upload_to_remote(*this);
        if (config::enable_flush_file_cache_async) {
            // If we call is_cancelled() after _state.set_val() then there might one situation where
            // s3 file writer is already destructed
            bool cancelled = is_cancelled();
            _state.set_val();
            // this control flow means the buf and the stream shares one memory
            // so we can directly use buf here
            upload_to_local_file_cache(cancelled);
        } else {
            upload_to_local_file_cache(is_cancelled());
            _state.set_val();
        }
        on_finish();
    }
    /**
    *
    * @return the stream representing the inner memory buffer
    */
    std::shared_ptr<std::iostream> get_stream() const { return _stream_ptr; }

    /**
    * Currently only used for small file to set callback
    */
    void set_upload_to_remote(std::function<void(UploadFileBuffer&)> cb) {
        _upload_to_remote = std::move(cb);
    }

private:
    std::function<void(UploadFileBuffer&)> _upload_to_remote = nullptr;
    std::shared_ptr<std::iostream> _stream_ptr; // point to _buffer.get_data()

    bool _is_cache_allocated {false};
    FileBlocksHolderPtr _holder;
    decltype(_holder->file_segments.begin()) _cur_file_segment;
    size_t _append_offset {0};
    size_t _index_offset {0};
};

struct FileBufferBuilder {
    FileBufferBuilder() = default;
    ~FileBufferBuilder() = default;
    /**
    * build one file buffer using previously set properties
    * @return the file buffer's base shared pointer
    */
    std::shared_ptr<FileBuffer> build();
    /**
    * set the file buffer type
    *
    * @param type enum class for buffer type
    */
    FileBufferBuilder& set_type(BufferType type);
    /**
    * set the download callback which would download the content on cloud into file buffer
    *
    * @param cb 
    */
    FileBufferBuilder& set_download_callback(std::function<Status(Slice&)> cb) {
        _download = std::move(cb);
        return *this;
    }
    /**
    * set the upload callback which would upload the content inside buffer into remote storage
    *
    * @param cb 
    */
    FileBufferBuilder& set_upload_callback(std::function<void(UploadFileBuffer& buf)> cb);
    /**
    * set the callback which would do task sync for the caller
    *
    * @param cb 
    */
    FileBufferBuilder& set_sync_after_complete_task(std::function<bool(Status)> cb);
    /**
    * set the callback which detect whether the task is done
    *
    * @param cb 
    */
    FileBufferBuilder& set_is_cancelled(std::function<bool()> cb) {
        _is_cancelled = std::move(cb);
        return *this;
    }
    /**
    * set the callback which allocate file cache segment holder
    * **Notice**: Because the load file cache workload coule be done
    * asynchronously so you must make sure all the dependencies of this
    * cb could last until this cb is invoked
    * @param cb 
    */
    FileBufferBuilder& set_allocate_file_segments_holder(std::function<FileBlocksHolderPtr()> cb);
    /**
    * set the file offset of the file buffer
    *
    * @param cb 
    */
    FileBufferBuilder& set_file_offset(size_t offset) {
        _offset = offset;
        return *this;
    }
    /**
    * set the index offset of the file buffer
    *
    * @param cb 
    */
    FileBufferBuilder& set_index_offset(size_t index_offset) {
        _index_offset = index_offset;
        return *this;
    }
    /**
    * set the callback which write the content into local file cache
    *
    * @param cb 
    */
    FileBufferBuilder& set_write_to_local_file_cache(
            std::function<void(FileBlocksHolderPtr, Slice)> cb) {
        _write_to_local_file_cache = std::move(cb);
        return *this;
    }
    /**
    * set the callback which would write the downloaded content into user's buffer
    *
    * @param cb 
    */
    FileBufferBuilder& set_write_to_use_buffer(std::function<void(Slice, size_t)> cb) {
        _write_to_use_buffer = std::move(cb);
        return *this;
    }

    BufferType _type;
    std::function<void(UploadFileBuffer& buf)> _upload_cb = nullptr;
    std::function<bool(Status)> _sync_after_complete_task = nullptr;
    std::function<FileBlocksHolderPtr()> _alloc_holder_cb = nullptr;
    std::function<bool()> _is_cancelled = nullptr;
    std::function<void(FileBlocksHolderPtr, Slice)> _write_to_local_file_cache;
    std::function<Status(Slice&)> _download;
    std::function<void(Slice, size_t)> _write_to_use_buffer;
    size_t _offset;
    size_t _index_offset;
};

class S3FileBufferPool {
public:
    S3FileBufferPool() = default;
    ~S3FileBufferPool() = default;

    // should be called one and only once
    // at startup
    void init(int32_t s3_write_buffer_whole_size, int32_t s3_write_buffer_size,
              doris::ThreadPool* thread_pool);

    /**
    *
    * @return singleton of the S3FileBufferPool
    */
    static S3FileBufferPool* GetInstance() {
        return ExecEnv::GetInstance()->get_s3_file_buffer_pool();
    }

    void reclaim(Slice buf) {
        std::unique_lock<std::mutex> lck {_lock};
        _free_raw_buffers.emplace_front(buf);
        _cv.notify_all();
    }

    /**
    *
    * @param reserve must return buffer with memory allocated
    * @return memory buffer
    */
    Slice allocate(bool reserve = false);

    ThreadPool* thread_pool() { return _thread_pool; }

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
