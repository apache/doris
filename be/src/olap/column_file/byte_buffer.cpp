// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

namespace palo {
namespace column_file {

ByteBuffer::ByteBuffer() : 
        _array(NULL),
        _capacity(0),
        _limit(0),
        _position(0),
        _is_mmap(false) {}

ByteBuffer::BufDeleter::BufDeleter() : 
        _is_mmap(false),
        _mmap_length(0) {}

void ByteBuffer::BufDeleter::set_mmap(size_t mmap_length) {
    _is_mmap = true;
    _mmap_length = mmap_length;
}

void ByteBuffer::BufDeleter::operator()(char* p) {
    if (NULL == p) {
        return;
    }

    if (_is_mmap) {
        if (0 != munmap(p, _mmap_length)) {
            OLAP_LOG_FATAL("fail to munmap: [mem='%p', len='%lu', errno='%d' errno_str='%s']",
                    p, _mmap_length, Errno::no(), Errno::str());
        }
    } else {
        delete []p;
    }
}

// 创建ByteBuffer与array
ByteBuffer* ByteBuffer::create(uint64_t capacity) {
    char* memory = new(std::nothrow) char[capacity];
    ByteBuffer* buf = new(std::nothrow) ByteBuffer;

    if (buf != NULL && memory != NULL) {
        buf->_buf = boost::shared_ptr<char>(memory, BufDeleter());
        buf->_array = buf->_buf.get();
        buf->_capacity = capacity;
        buf->_limit = capacity;
        return buf;
    }

    SAFE_DELETE(buf);
    SAFE_DELETE(memory);
    return NULL;
}

ByteBuffer* ByteBuffer::reference_buffer(ByteBuffer* reference,
        uint64_t offset,
        uint64_t length) {
    if (NULL == reference || 0 == length) {
        return NULL;
    }

    if (offset + length > reference->capacity()) {
        return NULL;
    }

    ByteBuffer* buf = new(std::nothrow) ByteBuffer();

    if (NULL == buf) {
        return NULL;
    }

    buf->_buf = reference->_buf;
    buf->_array = &(reference->_array[offset]);
    buf->_capacity = length;
    buf->_limit = length;
    buf->_is_mmap = reference->_is_mmap;

    return buf;
}

ByteBuffer* ByteBuffer::mmap(void* start, uint64_t length, int prot, int flags,
        int fd, uint64_t offset) {
    char* memory = (char*)::mmap(start, length, prot, flags, fd, offset);

    if (MAP_FAILED == memory) {
        OLAP_LOG_WARNING("fail to mmap. [errno='%d' errno_str='%s']", Errno::no(), Errno::str());
        return NULL;
    }

    BufDeleter deleter;
    deleter.set_mmap(length);

    ByteBuffer* buf = new(std::nothrow) ByteBuffer();

    if (NULL == buf) {
        deleter(memory);
        OLAP_LOG_WARNING("fail to allocate ByteBuffer.");
        return NULL;
    }

    buf->_buf = boost::shared_ptr<char>(memory, deleter);
    buf->_array = buf->_buf.get();
    buf->_capacity = length;
    buf->_limit = length;
    buf->_is_mmap = true;
    return buf;
}

ByteBuffer* ByteBuffer::mmap(FileHandler* handler, uint64_t offset, int prot, int flags) {
    if (NULL == handler) {
        OLAP_LOG_WARNING("invalid file handler");
        return NULL;
    }

    size_t length = handler->length();
    int fd = handler->fd();
    char* memory = (char*)::mmap(NULL, length, prot, flags, fd, offset);

    if (MAP_FAILED == memory) {
        OLAP_LOG_WARNING("fail to mmap. [errno='%d' errno_str='%s']", Errno::no(), Errno::str());
        return NULL;
    }

    BufDeleter deleter;
    deleter.set_mmap(length);

    ByteBuffer* buf = new(std::nothrow) ByteBuffer();

    if (NULL == buf) {
        deleter(memory);
        OLAP_LOG_WARNING("fail to allocate ByteBuffer.");
        return NULL;
    }

    buf->_buf = boost::shared_ptr<char>(memory, deleter);
    buf->_array = buf->_buf.get();
    buf->_capacity = length;
    buf->_limit = length;
    buf->_is_mmap = true;
    return buf;
}

OLAPStatus ByteBuffer::set_position(uint64_t new_position) {
    if (new_position <= _limit) {
        _position = new_position;
        return OLAP_SUCCESS;
    } else {
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
}

OLAPStatus ByteBuffer::set_limit(uint64_t new_limit) {
    if (new_limit > _capacity) {
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    _limit = new_limit;

    if (_position > _limit) {
        _position = _limit;
    }

    return OLAP_SUCCESS;
}

void ByteBuffer::flip() {
    _limit = _position;
    _position = 0;
}

OLAPStatus ByteBuffer::put(char src) {
    if (_position < _limit) {
        _array[_position++] = src;
        return OLAP_SUCCESS;
    }

    return OLAP_ERR_BUFFER_OVERFLOW;
}

OLAPStatus ByteBuffer::put(uint64_t index, char src) {
    if (index < _limit) {
        _array[index] = src;
        return OLAP_SUCCESS;
    }

    return OLAP_ERR_BUFFER_OVERFLOW;
}

OLAPStatus ByteBuffer::put(const char* src, uint64_t src_size, uint64_t offset,
        uint64_t length) {
    //没有足够的空间可以写
    if (length > remaining()) {
        return OLAP_ERR_BUFFER_OVERFLOW;
    }

    //src不够大
    if (offset + length > src_size) {
        return OLAP_ERR_OUT_OF_BOUND;
    }

    memory_copy(&_array[_position], &src[offset], length);
    _position += length;
    return OLAP_SUCCESS;
}

}  // namespace column_file
}  // namespace palo
