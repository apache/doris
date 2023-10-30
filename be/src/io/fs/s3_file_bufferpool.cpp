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

#include "s3_file_bufferpool.h"

#include "common/config.h"
#include "common/logging.h"
#include "io/cache/block/block_file_segment.h"
#include "io/fs/s3_common.h"
#include "runtime/exec_env.h"
#include "util/defer_op.h"
#include "util/slice.h"

namespace doris {
namespace io {

bvar::Adder<uint64_t> s3_file_buffer_allocated("s3_file_buffer_allocated");
bvar::Adder<uint64_t> s3_file_buffer_allocating("s3_file_buffer_allocating");

/**
 * 0. check if the inner memory buffer is empty or not
 * 1. relcaim the memory buffer if it's mot empty
 */
void FileBuffer::on_finish() {
    if (_buffer.empty()) {
        return;
    }
    S3FileBufferPool::GetInstance()->reclaim(Slice {_buffer.get_data(), _capacity});
    _buffer.clear();
}

/**
 * take other buffer's memory space and refresh capacity
 */
void FileBuffer::swap_buffer(Slice& other) {
    _buffer = other;
    _capacity = _buffer.get_size();
    other.clear();
}

FileBuffer::FileBuffer(std::function<FileBlocksHolderPtr()> alloc_holder, size_t offset,
                       OperationState state, bool reserve)
        : _alloc_holder(std::move(alloc_holder)),
          _buffer(S3FileBufferPool::GetInstance()->allocate(reserve)),
          _offset(offset),
          _size(0),
          _state(std::move(state)),
          _capacity(_buffer.get_size()) {}

/**
 * 0. check if file cache holder allocated
 * 1. update the cache's type to index cache
 */
void UploadFileBuffer::set_index_offset(size_t offset) {
    _index_offset = offset;
    if (_holder) {
        bool change_to_index_cache = false;
        for (auto iter = _holder->file_segments.begin(); iter != _holder->file_segments.end();
             ++iter) {
            if (iter == _cur_file_segment) {
                change_to_index_cache = true;
            }
            if (change_to_index_cache) {
                static_cast<void>((*iter)->change_cache_type_self(CacheType::INDEX));
            }
        }
    }
}

/**
 * 0. when there is memory preserved, directly write data to buf
 * 1. write to file cache otherwise, then we'll wait for free buffer and to rob it
 */
Status UploadFileBuffer::append_data(const Slice& data) {
    Defer defer {[&] { _size += data.get_size(); }};
    while (true) {
        // if buf is not empty, it means there is memory preserved for this buf
        if (!_buffer.empty()) {
            std::memcpy((void*)(_buffer.get_data() + _size), data.get_data(), data.get_size());
            break;
        }
        // if the buf has no memory reserved, then write to disk first
        if (!_is_cache_allocated && config::enable_file_cache && _alloc_holder != nullptr) {
            _holder = _alloc_holder();
            bool cache_is_not_enough = false;
            for (auto& segment : _holder->file_segments) {
                DCHECK(segment->state() == FileBlock::State::SKIP_CACHE ||
                       segment->state() == FileBlock::State::EMPTY);
                if (segment->state() == FileBlock::State::SKIP_CACHE) [[unlikely]] {
                    cache_is_not_enough = true;
                    break;
                }
                if (_index_offset != 0) {
                    RETURN_IF_ERROR(segment->change_cache_type_self(CacheType::INDEX));
                }
            }
            // if cache_is_not_enough, cannot use it !
            _cur_file_segment = _holder->file_segments.begin();
            _append_offset = (*_cur_file_segment)->range().left;
            _holder = cache_is_not_enough ? nullptr : std::move(_holder);
            if (_holder) {
                (*_cur_file_segment)->get_or_set_downloader();
            }
            _is_cache_allocated = true;
        }
        if (_holder) [[likely]] {
            size_t data_remain_size = data.get_size();
            size_t pos = 0;
            while (data_remain_size != 0) {
                auto range = (*_cur_file_segment)->range();
                size_t segment_remain_size = range.right - _append_offset + 1;
                size_t append_size = std::min(data_remain_size, segment_remain_size);
                Slice append_data(data.get_data() + pos, append_size);
                // When there is no available free memory buffer, the data will be written to the cache first
                // and then uploaded to S3 when there is an available free memory buffer.
                // However, if an error occurs during the write process to the local cache,
                // continuing to upload the dirty data from the cache to S3 will result in erroneous data(Bad segment).
                // Considering that local disk write failures are rare, a simple approach is chosen here,
                // which is to treat the import as a failure directly when a local write failure occurs
                RETURN_IF_ERROR((*_cur_file_segment)->append(append_data));
                if (segment_remain_size == append_size) {
                    RETURN_IF_ERROR((*_cur_file_segment)->finalize_write());
                    if (++_cur_file_segment != _holder->file_segments.end()) {
                        (*_cur_file_segment)->get_or_set_downloader();
                    }
                }
                data_remain_size -= append_size;
                _append_offset += append_size;
                pos += append_size;
            }
            break;
        } else {
            // wait allocate buffer pool
            auto tmp = S3FileBufferPool::GetInstance()->allocate(true);
            if (tmp.empty()) [[unlikely]] {
                return Status::InternalError("Failed to allocate S3 buffer for {} seconds",
                                             config::s3_writer_buffer_allocation_timeout);
            }
            swap_buffer(tmp);
        }
    }
    return Status::OK();
}

/**
 * 0. allocate one memory buffer
 * 1. read the content from the cache and then write
 * it into memory buffer
 */
void UploadFileBuffer::read_from_cache() {
    auto tmp = S3FileBufferPool::GetInstance()->allocate(true);
    if (tmp.empty()) [[unlikely]] {
        set_val(Status::InternalError("Failed to allocate S3 buffer for {} seconds",
                                      config::s3_writer_buffer_allocation_timeout));
        return;
    }
    swap_buffer(tmp);

    DCHECK(_holder != nullptr);
    DCHECK(_capacity >= _size);
    size_t pos = 0;
    for (auto& segment : _holder->file_segments) {
        if (pos == _size) {
            break;
        }
        if (auto s = segment->finalize_write(); !s.ok()) [[unlikely]] {
            set_val(std::move(s));
            return;
        }
        size_t segment_size = segment->range().size();
        Slice s(_buffer.get_data() + pos, segment_size);
        if (auto st = segment->read_at(s, 0); !st.ok()) [[unlikely]] {
            set_val(std::move(st));
            return;
        }
        pos += segment_size;
    }

    // the real lenght should be the buf.get_size() in this situation(consider it's the last part,
    // size of it could be less than 5MB)
    _stream_ptr = std::make_shared<StringViewStream>(_buffer.get_data(), _size);
}

/**
 * 0. constrcut the stream ptr if the buffer is not empty
 * 1. submit the on_upload() callback to executor
 */
void UploadFileBuffer::submit() {
    if (!_buffer.empty()) [[likely]] {
        _stream_ptr = std::make_shared<StringViewStream>(_buffer.get_data(), _size);
    }
    // If the data is written into file cache
    if (_holder && _cur_file_segment != _holder->file_segments.end()) {
        if (auto s = (*_cur_file_segment)->finalize_write(); !s.ok()) [[unlikely]] {
            set_val(std::move(s));
            return;
        }
    }
    static_cast<void>(S3FileBufferPool::GetInstance()->thread_pool()->submit_func(
            [buf = this->shared_from_this(), this]() {
                // to extend buf's lifetime
                // (void)buf;
                on_upload();
            }));
}

/**
 * write the content of the memory buffer to local file cache
 */
void UploadFileBuffer::upload_to_local_file_cache(bool is_cancelled) {
    if (!config::enable_file_cache || _alloc_holder == nullptr) {
        return;
    }
    if (_holder) {
        return;
    }
    if (is_cancelled) {
        return;
    }
    // the data is already written to S3 in this situation
    // so i didn't handle the file cache write error
    _holder = _alloc_holder();
    size_t pos = 0;
    size_t data_remain_size = _size;
    for (auto& segment : _holder->file_segments) {
        if (data_remain_size == 0) {
            break;
        }
        size_t segment_size = segment->range().size();
        size_t append_size = std::min(data_remain_size, segment_size);
        if (segment->state() == FileBlock::State::EMPTY) {
            if (_index_offset != 0 && segment->range().right >= _index_offset) {
                // segment->change_cache_type_self(CacheType::INDEX);
            }
            segment->get_or_set_downloader();
            // Another thread may have started downloading due to a query
            // Just skip putting to cache from UploadFileBuffer
            if (segment->is_downloader()) {
                Slice s(_buffer.get_data() + pos, append_size);
                if (auto st = segment->append(s); !st.ok()) [[unlikely]] {
                    LOG_WARNING("append data to cache segmetn failed due to {}", st);
                    return;
                }
                if (auto st = segment->finalize_write(); !st.ok()) [[unlikely]] {
                    LOG_WARNING("finalize write to cache segmetn failed due to {}", st);
                    return;
                }
            }
        }
        data_remain_size -= append_size;
        pos += append_size;
    }
}

FileBufferBuilder& FileBufferBuilder::set_type(BufferType type) {
    _type = type;
    return *this;
}
FileBufferBuilder& FileBufferBuilder::set_upload_callback(
        std::function<void(UploadFileBuffer& buf)> cb) {
    _upload_cb = std::move(cb);
    return *this;
}
// set callback to do task sync for the caller
FileBufferBuilder& FileBufferBuilder::set_sync_after_complete_task(std::function<bool(Status)> cb) {
    _sync_after_complete_task = std::move(cb);
    return *this;
}

FileBufferBuilder& FileBufferBuilder::set_allocate_file_segments_holder(
        std::function<FileBlocksHolderPtr()> cb) {
    _alloc_holder_cb = std::move(cb);
    return *this;
}

std::shared_ptr<FileBuffer> FileBufferBuilder::build() {
    OperationState state(_sync_after_complete_task, _is_cancelled);
    if (_type == BufferType::UPLOAD) {
        return std::make_shared<UploadFileBuffer>(std::move(_upload_cb), std::move(state), _offset,
                                                  std::move(_alloc_holder_cb), _index_offset);
    }
    // should never come here
    return nullptr;
}

void S3FileBufferPool::init(int32_t s3_write_buffer_whole_size, int32_t s3_write_buffer_size,
                            ThreadPool* thread_pool) {
    // the nums could be one configuration
    size_t buf_num = s3_write_buffer_whole_size / s3_write_buffer_size;
    DCHECK((s3_write_buffer_size >= 5 * 1024 * 1024) &&
           (s3_write_buffer_whole_size > s3_write_buffer_size))
            << "s3 write buffer size " << s3_write_buffer_size << " whole s3 write buffer size "
            << s3_write_buffer_whole_size;
    LOG_INFO("S3 file buffer pool with {} buffers, each with {}", buf_num, s3_write_buffer_size);
    _whole_mem_buffer = std::make_unique<char[]>(s3_write_buffer_whole_size);
    for (size_t i = 0; i < buf_num; i++) {
        Slice s {_whole_mem_buffer.get() + i * s3_write_buffer_size,
                 static_cast<size_t>(s3_write_buffer_size)};
        _free_raw_buffers.emplace_back(s);
    }
    _thread_pool = thread_pool;
}

Slice S3FileBufferPool::allocate(bool reserve) {
    Slice buf;
    Defer defer {[&]() {
        if (!buf.empty()) {
            s3_file_buffer_allocated << 1;
        }
        s3_file_buffer_allocating << -1;
    }};
    s3_file_buffer_allocating << 1;
    // if need reserve or no cache then we must ensure return buf with memory preserved
    if (reserve || !config::enable_file_cache) {
        {
            std::unique_lock<std::mutex> lck {_lock};
            _cv.wait_for(lck, std::chrono::seconds(config::s3_writer_buffer_allocation_timeout),
                         [this]() { return !_free_raw_buffers.empty(); });
            if (!_free_raw_buffers.empty()) {
                buf = _free_raw_buffers.front();
                _free_raw_buffers.pop_front();
            }
        }
        return buf;
    }
    // try to get one memory reserved buffer
    {
        std::unique_lock<std::mutex> lck {_lock};
        if (!_free_raw_buffers.empty()) {
            buf = _free_raw_buffers.front();
            _free_raw_buffers.pop_front();
        }
    }
    if (!buf.empty()) {
        return buf;
    }
    // if there is no free buffer and no need to reserve memory, we could return one empty buffer
    buf = Slice();
    // if the buf has no memory reserved, it would try to write the data to file cache first
    // or it would try to rob buffer from other S3FileBuffer
    return buf;
}
} // namespace io
} // namespace doris
