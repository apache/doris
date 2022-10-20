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

#include <cstring>
#include <functional>
#include <vector>

#include "common/logging.h"
#include "runtime/string_value.h"
#include "vec/common/volnitsky.h"

namespace doris {

class StringSearch {
public:
    virtual ~StringSearch() {}
    StringSearch() : _pattern(nullptr) {}

    StringSearch(const StringValue* pattern) { set_pattern(pattern); }

    void set_pattern(const StringValue* pattern) {
        _pattern = pattern;
        _vol_searcher.reset(new Volnitsky(pattern->ptr, pattern->len));
    }

    void set_pattern(const StringRef* pattern) {
        _pattern = reinterpret_cast<const StringValue*>(pattern);
        _vol_searcher.reset(new Volnitsky(pattern->data, pattern->size));
    }

    // search for this pattern in str.
    //   Returns the offset into str if the pattern exists
    //   Returns -1 if the pattern is not found
    int search(const StringValue* str) const {
        auto it = search(str->ptr, str->len);
        if (it == str->ptr + str->len) {
            return -1;
        } else {
            return it - str->ptr;
        }
    }

    int search(const StringRef& str) const {
        auto it = search(const_cast<char*>(str.data), str.size);
        if (it == str.data + str.size) {
            return -1;
        } else {
            return it - str.data;
        }
    }

    // search for this pattern in str.
    //   Returns the offset into str if the pattern exists
    //   Returns str+len if the pattern is not found
    const char* search(char* str, size_t len) const {
        if (!str || !_pattern || _pattern->len == 0) {
            return str + len;
        }

        return _vol_searcher->search(str, len);
    }

    inline size_t get_pattern_length() { return _pattern ? _pattern->len : 0; }

private:
    const StringValue* _pattern;
    std::unique_ptr<Volnitsky> _vol_searcher;
};

} // namespace doris
