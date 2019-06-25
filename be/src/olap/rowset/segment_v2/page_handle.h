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

#include "gutil/macros.h" // for DISALLOW_COPY_AND_ASSIGN
#include "util/slice.h" // for Slice

namespace doris {
namespace segment_v2 {

// when page is read into memory, we use this to store it
// This class should delete memory
class PageHandle {
public:
    static PageHandle create_from_slice(const Slice& slice) {
        return PageHandle(slice);
    }

    PageHandle() : _data((const uint8_t*)nullptr, 0) { }

    // Move constructor
    PageHandle(PageHandle&& other) noexcept : _data(other._data) {
        other._data = Slice();
    }

    PageHandle& operator=(PageHandle&& other) noexcept {
        std::swap(_data, other._data);
        return *this;
    }

    ~PageHandle() {
        delete[] _data.data;
        _data = Slice();
    }

    Slice data() const {
        return _data;
    }

private:
    PageHandle(const Slice& data) : _data(data) {
    }

    Slice _data;

    // cause we 
    DISALLOW_COPY_AND_ASSIGN(PageHandle);
};

}
}
