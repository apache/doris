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

namespace doris {

class StringSearch {
public:
    virtual ~StringSearch() {}
    StringSearch() : _pattern(nullptr) {}

    StringSearch(const StringValue* pattern) : _pattern(pattern) {}

    // search for this pattern in str.
    //   Returns the offset into str if the pattern exists
    //   Returns -1 if the pattern is not found
    int search(const StringValue* str) const {
        if (!str || !_pattern || _pattern->len == 0) {
            return -1;
        }

        auto it = std::search(str->ptr, str->ptr + str->len,
                              std::default_searcher(_pattern->ptr, _pattern->ptr + _pattern->len));
        if (it == str->ptr + str->len) {
            return -1;
        } else {
            return it - str->ptr;
        }
    }

private:
    const StringValue* _pattern;
};

} // namespace doris
