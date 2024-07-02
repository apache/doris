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

#include <gen_cpp/segment_v2.pb.h>

#include <cstdint>
#include <string>

#include "util/coding.h"
#include "util/faststring.h"

namespace doris {
namespace segment_v2 {

struct PagePointer {
    uint64_t offset;
    uint32_t size;

    PagePointer() : offset(0), size(0) {}
    PagePointer(uint64_t offset_, uint32_t size_) : offset(offset_), size(size_) {}
    PagePointer(const PagePointerPB& from) : offset(from.offset()), size(from.size()) {}

    void reset() {
        offset = 0;
        size = 0;
    }

    void to_proto(PagePointerPB* to) {
        to->set_offset(offset);
        to->set_size(size);
    }

    const uint8_t* decode_from(const uint8_t* data, const uint8_t* limit) {
        data = decode_varint64_ptr(data, limit, &offset);
        if (data == nullptr) {
            return nullptr;
        }
        return decode_varint32_ptr(data, limit, &size);
    }

    bool decode_from(Slice* input) {
        bool result = get_varint64(input, &offset);
        if (!result) {
            return false;
        }
        return get_varint32(input, &size);
    }

    void encode_to(faststring* dst) const { put_varint64_varint32(dst, offset, size); }

    void encode_to(std::string* dst) const { put_varint64_varint32(dst, offset, size); }

    bool operator==(const PagePointer& other) const {
        return offset == other.offset && size == other.size;
    }

    bool operator!=(const PagePointer& other) const { return !(*this == other); }
};

inline std::ostream& operator<<(std::ostream& os, const PagePointer& pp) {
    os << "PagePointer { offset=" << pp.offset << " size=" << pp.size << " }";
    return os;
}

} // namespace segment_v2
} // namespace doris
