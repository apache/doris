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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

namespace doris::segment_v2::inverted_index {

// A value read piece by piece into a buffer, for a tokenizer that scans a fixed-size window. The
// reads behave as they did on the CLucene reader that held the value before.
class TextCursor {
public:
    void reset(std::string_view text) {
        _text = text;
        _position = 0;
    }

    // Copies up to `length` bytes at the cursor to `out` and moves past them. Returns how many,
    // 0 when `length` is 0, or -1 once the whole value is read.
    int32_t read_copy(char* out, int32_t length) {
        if (length == 0) {
            return 0;
        }
        if (_position >= _text.size()) {
            return -1;
        }
        const size_t count = std::min(_text.size() - _position, static_cast<size_t>(length));
        std::memcpy(out, _text.data() + _position, count);
        _position += count;
        return static_cast<int32_t>(count);
    }

private:
    std::string_view _text;
    size_t _position = 0;
};

} // namespace doris::segment_v2::inverted_index
