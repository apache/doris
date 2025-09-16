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

#include <bvar/bvar.h>

#include <chrono>
#include <memory>

#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/s3_common.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/slice.h"
#include "vec/common/arena.h"

namespace doris {
namespace io {

bvar::Adder<uint64_t> s3_file_buffer_allocated("s3_file_buffer_allocated");

template <typename Allocator = Allocator<false>>
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

struct FileBuffer::PartData {
    Memory<> _memory;
    PartData() : _memory(config::s3_write_buffer_size) {}
    ~PartData() = default;
    [[nodiscard]] Slice data() const { return Slice {_memory._data, _memory._size}; }
    [[nodiscard]] size_t size() const { return _memory._size; }
};

Slice FileBuffer::get_slice() const {
    return _inner_data->data();
}

FileBuffer::FileBuffer(BufferType type, std::function<FileBlocksHolderPtr()> alloc_holder,
                       size_t offset, OperationState state)
        : _type(type),
          _alloc_holder(std::move(alloc_holder)),
          _offset(offset),
          _size(0),
          _state(std::move(state)),
          _inner_data(std::make_unique<FileBuffer::PartData>()),
          _capacity(_inner_data->size()) {}

FileBuffer::~FileBuffer() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->s3_file_buffer_tracker());
    _inner_data.reset();
}

/**
 * 0. when there is memory preserved, directly write data to buf
 * 1. write to file cache otherwise, then we'll wait for free buffer and to rob it
 */
Status UploadFileBuffer::append_data(const Slice& data) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("UploadFileBuffer::append_data", Status::OK(), this,
                                      data.get_size());
    std::memcpy((void*)(_inner_data->data().get_data() + _size), data.get_data(), data.get_size());
    _size += data.get_size();
    _crc_value = crc32c::Extend(_crc_value, data.get_data(), data.get_size());
    return Status::OK();
}

/**
 * 0. constrcut the stream ptr if the buffer is not empty
 * 1. submit the on_upload() callback to executor
 */
static Status submit_upload_buffer(std::shared_ptr<FileBuffer> buffer) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("UploadFileBuffer::submit", Status::OK(), buffer.get());
    return ExecEnv::GetInstance()->s3_file_upload_thread_pool()->submit_func(
            [buf = std::move(buffer)]() { buf->execute_async(); });
}

std::ostream& operator<<(std::ostream& os, const BufferType& value) {
    switch (value) {
    case BufferType::UPLOAD:
        os << "upload";
        break;
    case BufferType::DOWNLOAD:
        os << "download";
        break;
    default:
        auto cast_value = static_cast<uint32_t>(value);
        os << cast_value;
    }
    return os;
}

Status FileBuffer::submit(std::shared_ptr<FileBuffer> buf) {
    switch (buf->_type) {
    case BufferType::UPLOAD:
        return submit_upload_buffer(std::move(buf));
        break;
    default:
        CHECK(false) << "should never come here, the illegal type is " << buf->_type;
    };
    return Status::InternalError("should never come here");
}

std::string_view FileBuffer::get_string_view_data() const {
    return {_inner_data->data().get_data(), _size};
}

void UploadFileBuffer::on_upload() {
    _stream_ptr = std::make_shared<StringViewStream>(_inner_data->data().get_data(), _size);
    if (_crc_value != crc32c::Value(_inner_data->data().get_data(), _size)) {
        DCHECK(false);
        set_status(Status::IOError("Buffer checksum not match"));
        return;
    }
    _upload_to_remote(*this);
    if (config::enable_flush_file_cache_async) {
        // If we call is_cancelled() after _state.set_status() then there might one situation where
        // s3 file writer is already destructed
        bool cancelled = is_cancelled();
        _state.set_status();
        // this control flow means the buf and the stream shares one memory
        // so we can directly use buf here
        upload_to_local_file_cache(cancelled);
    } else {
        upload_to_local_file_cache(is_cancelled());
        _state.set_status();
    }
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
    TEST_INJECTION_POINT_CALLBACK("UploadFileBuffer::upload_to_local_file_cache");
    // the data is already written to S3 in this situation
    // so i didn't handle the file cache write error
    _holder = _alloc_holder();
    size_t pos = 0;
    size_t data_remain_size = _size;
    for (auto& block : _holder->file_blocks) {
        if (data_remain_size == 0) {
            break;
        }
        size_t block_size = block->range().size();
        size_t append_size = std::min(data_remain_size, block_size);
        if (block->state() == FileBlock::State::EMPTY) {
            block->get_or_set_downloader();
            // Another thread may have started downloading due to a query
            // Just skip putting to cache from UploadFileBuffer
            if (block->is_downloader()) {
                Slice s(_inner_data->data().get_data() + pos, append_size);
                Status st = block->append(s);
                TEST_INJECTION_POINT_CALLBACK("UploadFileBuffer::upload_to_local_file_cache_inject",
                                              &st);
                if (st.ok()) {
                    st = block->finalize();
                }
                if (!st.ok()) {
                    {
                        [[maybe_unused]] bool ret = false;
                        TEST_SYNC_POINT_CALLBACK("UploadFileBuffer::upload_to_local_file_cache",
                                                 &ret);
                    }
                    LOG_WARNING("failed to append data to file cache").error(st);
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

FileBufferBuilder& FileBufferBuilder::set_allocate_file_blocks_holder(
        std::function<FileBlocksHolderPtr()> cb) {
    _alloc_holder_cb = std::move(cb);
    return *this;
}

Status FileBufferBuilder::build(std::shared_ptr<FileBuffer>* buf) {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->s3_file_buffer_tracker());
    OperationState state(_sync_after_complete_task, _is_cancelled);

    if (_type == BufferType::UPLOAD) {
        RETURN_IF_CATCH_EXCEPTION(*buf = std::make_shared<UploadFileBuffer>(
                                          std::move(_upload_cb), std::move(state), _offset,
                                          std::move(_alloc_holder_cb)));
        return Status::OK();
    }
    if (_type == BufferType::DOWNLOAD) {
        RETURN_IF_CATCH_EXCEPTION(*buf = std::make_shared<DownloadFileBuffer>(
                                          std::move(_download),
                                          std::move(_write_to_local_file_cache),
                                          std::move(_write_to_use_buffer), std::move(state),
                                          _offset, std::move(_alloc_holder_cb)));
        return Status::OK();
    }
    // should never come here
    return Status::InternalError("unsupport buffer type {}", _type);
}

/**
 * 0. check if we need to write into cache
 * 1. check if there is free space inside the file cache
 * 2. call the download callback
 * 3. write the downloaded content into user buffer if necessary
 */
void DownloadFileBuffer::on_download() {
    auto s = Status::OK();
    Defer def {[&]() { _state.set_status(std::move(s)); }};
    if (is_cancelled()) {
        return;
    }
    FileBlocksHolderPtr holder = nullptr;
    bool need_to_download_into_cache = false;
    if (_alloc_holder != nullptr) {
        holder = _alloc_holder();
        std::for_each(holder->file_blocks.begin(), holder->file_blocks.end(),
                      [&need_to_download_into_cache](FileBlockSPtr& file_block) {
                          if (file_block->state() == FileBlock::State::EMPTY) {
                              file_block->get_or_set_downloader();
                              if (file_block->is_downloader()) {
                                  need_to_download_into_cache = true;
                              }
                          }
                      });
        if (!need_to_download_into_cache && !_write_to_use_buffer) [[unlikely]] {
            LOG(INFO) << "Skipping download because that there is no space for catch data.";
        } else {
            Slice tmp = _inner_data->data();
            s = _download(tmp);
            if (s) {
                _size = tmp.get_size();
                if (_write_to_use_buffer != nullptr) {
                    _write_to_use_buffer({_inner_data->data().get_data(), get_size()},
                                         get_file_offset());
                }
                if (need_to_download_into_cache) {
                    _write_to_local_file_cache(std::move(holder),
                                               Slice {_inner_data->data().get_data(), _size});
                }
            } else {
                LOG(WARNING) << s;
            }
            _state.set_status(std::move(s));
        }
    } else {
        Slice tmp = _inner_data->data();
        s = _download(tmp);
        _size = tmp.get_size();
        if (s.ok() && _write_to_use_buffer != nullptr) {
            _write_to_use_buffer({_inner_data->data().get_data(), get_size()}, get_file_offset());
        }
    }
}

} // namespace io
} // namespace doris
