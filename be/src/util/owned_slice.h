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

class OwnedSlice {

public:
    Slice slice;

    OwnedSlice() {slice.data = nullptr;}

    OwnedSlice(Slice _slice) : slice(_slice) {}

    ~OwnedSlice(){
        if (slice.data != nullptr) {
            delete[] slice.data;
        }
    }

    OwnedSlice(OwnedSlice&& src) {
        slice.data = src.slice.data;
        slice.size = src.slice.size;
        src.slice.data = nullptr;
        src.slice.size = 0;
    }

    OwnedSlice& operator = (OwnedSlice&& src) {
        if (this != &src) {
            if (slice.data != nullptr) {
                delete[] slice.data;
            }
            slice.data = src.slice.data;
            slice.size = src.slice.size;
            src.slice.data = nullptr;
            src.slice.size = 0;
        }
        return *this;
    }

private:
    DISALLOW_COPY_AND_ASSIGN(OwnedSlice);
};
} // namespace doris