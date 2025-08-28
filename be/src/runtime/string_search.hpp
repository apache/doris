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

#include "common/cast_set.h"
#include "vec/common/string_ref.h"
#include "vec/common/string_searcher.h"

namespace doris {
#include "common/compile_check_begin.h"

class StringSearch {
public:
    virtual ~StringSearch() = default;
    StringSearch() : _pattern(nullptr) {}

    StringSearch(const StringRef* pattern) { set_pattern(pattern); }

    void set_pattern(const StringRef* pattern) {
        _pattern = pattern;
        _str_searcher.reset(new ASCIICaseSensitiveStringSearcher(pattern->data, pattern->size));
    }

    // search for this pattern in str.
    //   Returns the offset into str if the pattern exists
    //   Returns -1 if the pattern is not found
    int search(const StringRef* str) const {
        const auto* it = search(str->data, str->size);
        if (it == str->data + str->size) {
            return -1;
        } else {
            return cast_set<int>((it - str->data));
        }
    }

    int search(const StringRef& str) const {
        const auto* it = search(str.data, str.size);
        if (it == str.data + str.size) {
            return -1;
        } else {
            return cast_set<int>(it - str.data);
        }
    }

    // search for this pattern in str.
    //   Returns the offset into str if the pattern exists
    //   Returns str+len if the pattern is not found
    const char* search(const char* str, size_t len) const {
        if (!str || !_pattern || _pattern->size == 0) {
            return str + len;
        }

        return _str_searcher->search(str, len);
    }

    inline size_t get_pattern_length() { return _pattern ? _pattern->size : 0; }

private:
    const StringRef* _pattern;
    std::unique_ptr<ASCIICaseSensitiveStringSearcher> _str_searcher;
};
#include "common/compile_check_end.h"
} // namespace doris
