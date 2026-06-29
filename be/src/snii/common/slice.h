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

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace snii {

// Read-only byte view (does not own memory). Lifetime is managed by the underlying buffer.
class Slice {
public:
    Slice() = default;
    Slice(const uint8_t* d, size_t n) : data_(d), size_(n) {}
    explicit Slice(const std::vector<uint8_t>& v) : data_(v.data()), size_(v.size()) {}
    explicit Slice(std::string_view sv)
            : data_(reinterpret_cast<const uint8_t*>(sv.data())), size_(sv.size()) {}

    const uint8_t* data() const { return data_; }
    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

    uint8_t operator[](size_t i) const {
        assert(i < size_);
        return data_[i];
    }

    Slice subslice(size_t off, size_t n) const {
        assert(off + n <= size_);
        return Slice(data_ + off, n);
    }

private:
    const uint8_t* data_ = nullptr;
    size_t size_ = 0;
};

} // namespace snii
