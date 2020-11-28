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

#include "util/faststring.h"

#include <glog/logging.h>

#include <memory>

namespace doris {

void faststring::GrowToAtLeast(size_t newcapacity) {
    // Not enough space, need to reserve more.
    // Don't reserve exactly enough space for the new string -- that makes it
    // too easy to write perf bugs where you get O(n^2) append.
    // Instead, always expand by at least 50%.

    if (newcapacity < capacity_ * 3 / 2) {
        newcapacity = capacity_ * 3 / 2;
    }
    GrowArray(newcapacity);
}

void faststring::GrowArray(size_t newcapacity) {
    DCHECK_GE(newcapacity, capacity_);
    std::unique_ptr<uint8_t[]> newdata(new uint8_t[newcapacity]);
    if (len_ > 0) {
        memcpy(&newdata[0], &data_[0], len_);
    }
    capacity_ = newcapacity;
    if (data_ != initial_data_) {
        delete[] data_;
    } else {
        ASAN_POISON_MEMORY_REGION(initial_data_, arraysize(initial_data_));
    }

    data_ = newdata.release();
    ASAN_POISON_MEMORY_REGION(data_ + len_, capacity_ - len_);
}

void faststring::ShrinkToFitInternal() {
    DCHECK_NE(data_, initial_data_);
    if (len_ <= kInitialCapacity) {
        ASAN_UNPOISON_MEMORY_REGION(initial_data_, len_);
        memcpy(initial_data_, &data_[0], len_);
        delete[] data_;
        data_ = initial_data_;
        capacity_ = kInitialCapacity;
    } else {
        std::unique_ptr<uint8_t[]> newdata(new uint8_t[len_]);
        memcpy(&newdata[0], &data_[0], len_);
        delete[] data_;
        data_ = newdata.release();
        capacity_ = len_;
    }
}

} // namespace doris
