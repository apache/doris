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

#include "byte_buffer.h"

#include <sys/mman.h>

#include "olap/utils.h"
#include "runtime/thread_context.h"

namespace doris {

StorageByteBuffer::StorageByteBuffer()
        : _array(nullptr), _capacity(0), _limit(0), _position(0), _is_mmap(false) {}

StorageByteBuffer::BufDeleter::BufDeleter() : _is_mmap(false), _mmap_length(0) {}

void StorageByteBuffer::BufDeleter::set_mmap(size_t mmap_length) {
    _is_mmap = true;
    _mmap_length = mmap_length;
}

void StorageByteBuffer::BufDeleter::operator()(char* p) {
    if (nullptr == p) {
        return;
    }

    if (_is_mmap) {
        if (0 != munmap(p, _mmap_length)) {
            LOG(FATAL) << "fail to munmap: mem=" << p << ", len=" << _mmap_length
                       << ", errno=" << Errno::no() << ", errno_str=" << Errno::str();
        } else {
            RELEASE_THREAD_LOCAL_MEM_TRACKER(_mmap_length);
        }
    } else {
        delete[] p;
    }
}

// 创建ByteBuffer与array
StorageByteBuffer* StorageByteBuffer::create(uint64_t capacity) {
    char* memory = new (std::nothrow) char[capacity];
    StorageByteBuffer* buf = new (std::nothrow) StorageByteBuffer;

    if (buf != nullptr && memory != nullptr) {
        buf->_buf = std::shared_ptr<char>(memory, BufDeleter());
        buf->_array = buf->_buf.get();
        buf->_capacity = capacity;
        buf->_limit = capacity;
        return buf;
    }

    SAFE_DELETE(buf);
    SAFE_DELETE_ARRAY(memory);
    return nullptr;
}

StorageByteBuffer* StorageByteBuffer::reference_buffer(StorageByteBuffer* reference,
                                                       uint64_t offset, uint64_t length) {
    if (nullptr == reference || 0 == length) {
        return nullptr;
    }

    if (offset + length > reference->capacity()) {
        return nullptr;
    }

    StorageByteBuffer* buf = new (std::nothrow) StorageByteBuffer();

    if (nullptr == buf) {
        return nullptr;
    }

    buf->_buf = reference->_buf;
    buf->_array = &(reference->_array[offset]);
    buf->_capacity = length;
    buf->_limit = length;
    buf->_is_mmap = reference->_is_mmap;

    return buf;
}

StorageByteBuffer* StorageByteBuffer::mmap(void* start, uint64_t length, int prot, int flags,
                                           int fd, uint64_t offset) {
    CONSUME_THREAD_LOCAL_MEM_TRACKER(length);
    char* memory = (char*)::mmap(start, length, prot, flags, fd, offset);

    if (MAP_FAILED == memory) {
        OLAP_LOG_WARNING("fail to mmap. [errno='%d' errno_str='%s']", Errno::no(), Errno::str());
        RELEASE_THREAD_LOCAL_MEM_TRACKER(length);
        return nullptr;
    }

    BufDeleter deleter;
    deleter.set_mmap(length);

    StorageByteBuffer* buf = new (std::nothrow) StorageByteBuffer();

    if (nullptr == buf) {
        deleter(memory);
        OLAP_LOG_WARNING("fail to allocate StorageByteBuffer.");
        RELEASE_THREAD_LOCAL_MEM_TRACKER(length);
        return nullptr;
    }

    buf->_buf = std::shared_ptr<char>(memory, deleter);
    buf->_array = buf->_buf.get();
    buf->_capacity = length;
    buf->_limit = length;
    buf->_is_mmap = true;
    return buf;
}

StorageByteBuffer* StorageByteBuffer::mmap(FileHandler* handler, uint64_t offset, int prot,
                                           int flags) {
    if (nullptr == handler) {
        OLAP_LOG_WARNING("invalid file handler");
        return nullptr;
    }

    size_t length = handler->length();
    int fd = handler->fd();
    CONSUME_THREAD_LOCAL_MEM_TRACKER(length);
    char* memory = (char*)::mmap(nullptr, length, prot, flags, fd, offset);

    if (MAP_FAILED == memory) {
        OLAP_LOG_WARNING("fail to mmap. [errno='%d' errno_str='%s']", Errno::no(), Errno::str());
        RELEASE_THREAD_LOCAL_MEM_TRACKER(length);
        return nullptr;
    }

    BufDeleter deleter;
    deleter.set_mmap(length);

    StorageByteBuffer* buf = new (std::nothrow) StorageByteBuffer();

    if (nullptr == buf) {
        deleter(memory);
        OLAP_LOG_WARNING("fail to allocate StorageByteBuffer.");
        RELEASE_THREAD_LOCAL_MEM_TRACKER(length);
        return nullptr;
    }

    buf->_buf = std::shared_ptr<char>(memory, deleter);
    buf->_array = buf->_buf.get();
    buf->_capacity = length;
    buf->_limit = length;
    buf->_is_mmap = true;
    return buf;
}

Status StorageByteBuffer::put(char src) {
    if (_position < _limit) {
        _array[_position++] = src;
        return Status::OK();
    }

    return Status::OLAPInternalError(OLAP_ERR_BUFFER_OVERFLOW);
}

Status StorageByteBuffer::put(uint64_t index, char src) {
    if (index < _limit) {
        _array[index] = src;
        return Status::OK();
    }

    return Status::OLAPInternalError(OLAP_ERR_BUFFER_OVERFLOW);
}

Status StorageByteBuffer::put(const char* src, uint64_t src_size, uint64_t offset,
                              uint64_t length) {
    //没有足够的空间可以写
    if (length > remaining()) {
        return Status::OLAPInternalError(OLAP_ERR_BUFFER_OVERFLOW);
    }

    //src不够大
    if (offset + length > src_size) {
        return Status::OLAPInternalError(OLAP_ERR_OUT_OF_BOUND);
    }

    memory_copy(&_array[_position], &src[offset], length);
    _position += length;
    return Status::OK();
}

} // namespace doris
