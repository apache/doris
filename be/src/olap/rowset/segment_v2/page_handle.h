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
#include "olap/page_cache.h"
#include "runtime/exec_env.h"
#include "util/slice.h" // for Slice

namespace doris {

// After disable page cache, sometimes we need to know the percentage of data pages in query memory.
inline bvar::Adder<int64_t> g_page_no_cache_mem_bytes("doris_page_no_cache_mem_bytes");

namespace segment_v2 {

// When a column page is read into memory, we use this to store it.
// A page's data may be in cache, or may not in cache. We use this
// class to unify these two cases.
// If client use this struct to wrap data not in cache, this class
// will free data's memory when it is destroyed.
class PageHandle {
public:
    PageHandle() : _is_data_owner(false) {}

    // This class will take the ownership of input data's memory. It will
    // free it when deconstructs.
    PageHandle(DataPage* data) : _is_data_owner(true), _data(data) {
        g_page_no_cache_mem_bytes << _data->capacity();
    }

    // This class will take the content of cache data, and will make input
    // cache_data to a invalid cache handle.
    PageHandle(PageCacheHandle cache_data)
            : _is_data_owner(false), _cache_data(std::move(cache_data)) {}

    // Move constructor
    PageHandle(PageHandle&& other) noexcept : _cache_data(std::move(other._cache_data)) {
        // we can use std::exchange if we switch c++14 on
        std::swap(_is_data_owner, other._is_data_owner);
        std::swap(_data, other._data);
    }

    PageHandle& operator=(PageHandle&& other) noexcept {
        std::swap(_is_data_owner, other._is_data_owner);
        std::swap(_data, other._data);
        _cache_data = std::move(other._cache_data);
        return *this;
    }

    ~PageHandle() {
        if (_is_data_owner) {
            g_page_no_cache_mem_bytes << -_data->capacity();
            delete _data;
        } else {
            DCHECK(_data == nullptr);
        }
    }

    // the return slice contains uncompressed page body, page footer, and footer size
    Slice data() const {
        if (_is_data_owner) {
            return Slice(_data->data(), _data->size());
        } else {
            return _cache_data.data();
        }
    }

private:
    // when this is true, it means this struct own data and _data is valid.
    // otherwise _cache_data is valid, and data is belong to cache.
    bool _is_data_owner = false;
    DataPage* _data = nullptr;
    PageCacheHandle _cache_data;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(PageHandle);
};

} // namespace segment_v2
} // namespace doris
