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

#include <roaring/roaring.hh>

namespace doris {

class DeletionVector {
public:
    const static uint32_t MAGIC_NUMBER = 1581511376;
    const static uint32_t MAX_VALUE = std::numeric_limits<uint32_t>::max();
    DeletionVector() = default;
    DeletionVector(roaring::Roaring roaring_bitmap) : _roaring_bitmap(std::move(roaring_bitmap)) {};
    ~DeletionVector() = default;

    bool checked_delete(uint32_t postition) { return _roaring_bitmap.addChecked(postition); }

    bool is_delete(uint32_t postition) const { return _roaring_bitmap.contains(postition); }

    bool is_empty() const { return _roaring_bitmap.isEmpty(); }

    static DeletionVector deserialize(const char* buf, size_t maxbytes) {
        return {roaring::Roaring::readSafe(buf, maxbytes)};
    }
    roaring::Roaring _roaring_bitmap;

    // private:
};

} // namespace doris