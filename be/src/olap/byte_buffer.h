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

#include "olap/file_helper.h"
#include "olap/olap_define.h"
#include "util/mem_util.hpp"

namespace doris {

// ByteBuffer is a class used for data caching
// ByteBuffer maintains an internal char array for caching data;
// ByteBuffer maintains internal Pointers for reading and writing data;
//
// ByteBuffer has the following important usage concepts:
// capacity - the capacity of the buffer, set at initialization, is the size of the internal char array
// position - the current internal pointer position
// limit - maximum usage limit, this value is less than or equal to capacity, position is always less than limit
//
// ByteBuffer supports safe shallow copying of data directly using the copy constructor or = operator
class StorageByteBuffer {
public:
    // Create a StorageByteBuffer of capacity with the new method.
    // The position of the new buffer is 0, and the limit is capacity
    // The caller obtains the ownership of the newly created ByteBuffer, and needs to use delete method to delete the obtained StorageByteBuffer
    //
    // TODO. I think the use of create here should directly return the ByteBuffer itself instead of the smart pointer,
    // otherwise the smart pointer will not work,
    // and the current memory management is still manual.and need to think delete.
    static StorageByteBuffer* create(uint64_t capacity);

    // Create a new StorageByteBuffer by referencing another ByteBuffer's memory
    // The position of the new buffer is 0, and the limit is length
    // The caller obtains the ownership of the newly created ByteBuffer, and needs to use delete method to delete the obtained StorageByteBuffer
    // Inputs:
    //   - reference referenced memory
    //   - offset The position of the referenced Buffer in the original ByteBuffer, i.e.&reference->array()[offset]
    //   - length The length of the referenced Buffer
    // Notes:
    //   offset + length < reference->capacity
    //
    // TODO. same as create
    static StorageByteBuffer* reference_buffer(StorageByteBuffer* reference, uint64_t offset,
                                               uint64_t length);

    // Create a ByteBuffer through mmap, and the memory after successful mmap is managed by ByteBuffer
    // start, length, prot, flags, fd, offset are all parameters of mmap function
    // The caller obtains the ownership of the newly created ByteBuffer, and needs to use delete method to delete the obtained StorageByteBuffer
    static StorageByteBuffer* mmap(void* start, uint64_t length, int prot, int flags, int fd,
                                   uint64_t offset);

    // Since olap files are encapsulated with FileHandler, the interface is slightly modified
    // and the omitted parameters can be obtained in the handler.
    // The old interface is still preserved, maybe it will be used?
    static StorageByteBuffer* mmap(FileHandler* handler, uint64_t offset, int prot, int flags);

    uint64_t capacity() const { return _capacity; }

    uint64_t position() const { return _position; }
    // Set the position of the internal pointer
    // If the new position is greater than or equal to limit, return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR)
    Status set_position(uint64_t new_position) {
        if (new_position <= _limit) {
            _position = new_position;
            return Status::OK();
        } else {
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }
    }

    uint64_t limit() const { return _limit; }
    //set new limit
    //If limit is greater than capacity, return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR)
    //If position is greater than the new limit, set position equal to limit
    Status set_limit(uint64_t new_limit) {
        if (new_limit > _capacity) {
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }

        _limit = new_limit;

        if (_position > _limit) {
            _position = _limit;
        }

        return Status::OK();
    }

    uint64_t remaining() const { return _limit - _position; }

    // Set limit to current position
    // set position to 0
    // This function can be used to change the ByteBuffer from the write state to the read state,
    //  that is, call this function after some writes, and then read the ByteBuffer.
    void flip() {
        _limit = _position;
        _position = 0;
    }

    // The following three read functions are inline optimized

    // Read one byte of data, increase position after completion
    Status get(char* result) {
        if (OLAP_LIKELY(_position < _limit)) {
            *result = _array[_position++];
            return Status::OK();
        } else {
            return Status::OLAPInternalError(OLAP_ERR_OUT_OF_BOUND);
        }
    }

    // Read one byte of data at the specified location
    Status get(uint64_t index, char* result) {
        if (OLAP_LIKELY(index < _limit)) {
            *result = _array[index];
            return Status::OK();
        } else {
            return Status::OLAPInternalError(OLAP_ERR_OUT_OF_BOUND);
        }
    }

    // Read a piece of data of length length to dst, and increase the position after completion
    Status get(char* dst, uint64_t dst_size, uint64_t length) {
        // Not enough data to read
        if (OLAP_UNLIKELY(length > remaining())) {
            return Status::OLAPInternalError(OLAP_ERR_OUT_OF_BOUND);
        }

        // dst is not big enough
        if (OLAP_UNLIKELY(length > dst_size)) {
            return Status::OLAPInternalError(OLAP_ERR_BUFFER_OVERFLOW);
        }

        memory_copy(dst, &_array[_position], length);
        _position += length;
        return Status::OK();
    }

    // Read dst_size long data to dst
    Status get(char* dst, uint64_t dst_size) { return get(dst, dst_size, dst_size); }

    // Write a byte, increment position when done
    // If position >= limit before writing, return Status::OLAPInternalError(OLAP_ERR_BUFFER_OVERFLOW)
    Status put(char src);

    // Write data at the index position without changing the position
    // Returns:
    //   Status::OLAPInternalError(OLAP_ERR_BUFFER_OVERFLOW) : index >= limit
    Status put(uint64_t index, char src);

    // Read length bytes from &src[offset], write to buffer, and increase position after completion
    // Returns:
    //   Status::OLAPInternalError(OLAP_ERR_BUFFER_OVERFLOW): remaining() < length
    //   Status::OLAPInternalError(OLAP_ERR_OUT_OF_BOUND): offset + length > src_size
    Status put(const char* src, uint64_t src_size, uint64_t offset, uint64_t length);

    // write a set of data
    Status put(const char* src, uint64_t src_size) { return put(src, src_size, 0, src_size); }

    // Returns the char array inside the ByteBuffer
    const char* array() const { return _array; }
    const char* array(size_t position) const {
        return position >= _limit ? nullptr : &_array[position];
    }
    char* array() { return _array; }

private:
    // A custom destructor class that supports destructing the memory of new[] and mmap
    // Use delete to release by default
    class BufDeleter {
    public:
        BufDeleter();
        // Set to use mmap method
        void set_mmap(size_t mmap_length);
        void operator()(char* p);

    private:
        bool _is_mmap;       // whether to use mmap
        size_t _mmap_length; // If mmap is used, record the length of mmap
    };

private:
    // Direct creation of ByteBuffer is not supported, but created through the create method
    StorageByteBuffer();

private:
    std::shared_ptr<char> _buf; // managed memory
    char* _array;
    uint64_t _capacity;
    uint64_t _limit;
    uint64_t _position;
    bool _is_mmap;
};

} // namespace doris
