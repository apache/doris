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

#include "gutil/macros.h"
#include "util/slice.h"

/**
 *  used for maintain slice' life cycle
 *  Attention!!!  the input slice must own a data（char*） which using new to allocate memory,or will cause error
 */

namespace doris {

/**
 *  originally make this class  pageBuilder',to maintain the life cycle of returned slice
 *  Attention!!!
 *      1. the input slice must own a data（char*） which using new to allocate memory,or will cause error
 *      2. don't use std::move for this class
 *
 *  use case:
 *      fastring buf;
 *      buf.append();
 *      size_t size = buf.size();
 *      Slice slice(buf.release(), size); // here is the key point, using buf.release;use buf.data() is error
 *
 *      OwnedSlice owned_slice(slice);
 */
class OwnedSlice {

public:
    OwnedSlice() : _slice((uint8_t*)nullptr, 0) {}

    explicit OwnedSlice(Slice slice) : _slice(slice) {}

    OwnedSlice(uint8_t* _data, size_t size) :_slice(_data, size) {}

    ~OwnedSlice(){
        delete[] _slice.data;
    }

    OwnedSlice(OwnedSlice&& src) {
        _slice.data = src._slice.data;
        _slice.size = src._slice.size;
        src._slice.data = nullptr;
        src._slice.size = 0;
    }

    OwnedSlice& operator = (OwnedSlice&& src) {
        if (this != &src) {
            std::swap(_slice, src._slice);
        }
        return *this;
    }

    const Slice& slice() const {
        return _slice;
    }

private:
    DISALLOW_COPY_AND_ASSIGN(OwnedSlice);
    Slice _slice;
};

}// namespace doris