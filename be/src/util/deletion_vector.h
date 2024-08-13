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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>

#include "common/status.h"
#include "roaring/roaring.hh"

namespace doris {
class DeletionVector {
public:
    const static uint32_t MAGIC_NUMBER = 1581511376;
    DeletionVector(roaring::Roaring roaring_bitmap) : _roaring_bitmap(std::move(roaring_bitmap)) {};
    ~DeletionVector() = default;

    bool checked_delete(uint32_t postition) { return _roaring_bitmap.addChecked(postition); }

    bool is_delete(uint32_t postition) const { return _roaring_bitmap.contains(postition); }

    bool is_empty() const { return _roaring_bitmap.isEmpty(); }

    uint32_t maximum() const { return _roaring_bitmap.maximum(); }

    uint32_t minimum() const { return _roaring_bitmap.minimum(); }

    static Result<DeletionVector> deserialize(const char* buf, size_t length) {
        uint32_t actual_length;
        std::memcpy(reinterpret_cast<char*>(&actual_length), buf, 4);
        // change byte order to big endian
        std::reverse(reinterpret_cast<char*>(&actual_length),
                     reinterpret_cast<char*>(&actual_length) + 4);
        buf += 4;
        if (actual_length != length - 4) {
            return ResultError(
                    Status::RuntimeError("DeletionVector deserialize error: length not match, "
                                         "actual length: {}, expect length: {}",
                                         actual_length, length - 4));
        }
        uint32_t magic_number;
        std::memcpy(reinterpret_cast<char*>(&magic_number), buf, 4);
        // change byte order to big endian
        std::reverse(reinterpret_cast<char*>(&magic_number),
                     reinterpret_cast<char*>(&magic_number) + 4);
        buf += 4;
        if (magic_number != MAGIC_NUMBER) {
            return ResultError(Status::RuntimeError(
                    "DeletionVector deserialize error: invalid magic number {}", magic_number));
        }
        roaring::Roaring roaring_bitmap;
        try {
            roaring_bitmap = roaring::Roaring::readSafe(buf, length);
        } catch (std::runtime_error&) {
            return ResultError(Status::RuntimeError(
                    "DeletionVector deserialize error: failed to deserialize roaring bitmap"));
        }
        return DeletionVector(roaring_bitmap);
    }

private:
    roaring::Roaring _roaring_bitmap;
};
} // namespace doris
