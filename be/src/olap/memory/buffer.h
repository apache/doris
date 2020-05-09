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

#include "olap/memory/common.h"

namespace doris {
namespace memory {

// A generic buffer holding column base and delta data
// It can be considered as an array of any primitive type, but it does not
// have compile time type information, user can use utility method as<T> to
// get typed array view.
class Buffer {
public:
    Buffer() = default;
    ~Buffer();

    // allocate memory for this buffer, with buffer byte size of bsize
    Status alloc(size_t bsize);

    // clear buffer, free memory
    void clear();

    // set all memory content to zero
    void set_zero();

    // return true if this buffer is not empty
    operator bool() const { return _data != nullptr; }

    // returns a direct pointer to the memory array
    const uint8_t* data() const { return _data; }

    // returns a direct pointer to the memory array
    uint8_t* data() { return _data; }

    // get byte size of the buffer
    size_t bsize() const { return _bsize; }

    // get typed array view
    template <class T>
    T* as() {
        return reinterpret_cast<T*>(_data);
    }

    // get typed array view
    template <class T>
    const T* as() const {
        return reinterpret_cast<const T*>(_data);
    }

private:
    size_t _bsize = 0;
    uint8_t* _data = nullptr;
    DISALLOW_COPY_AND_ASSIGN(Buffer);
};

} // namespace memory
} // namespace doris
