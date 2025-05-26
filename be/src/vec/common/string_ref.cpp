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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/base/base/StringRef.
// And modified by Doris

#include "string_ref.h"

#include "common/compiler_util.h" // IWYU pragma: keep

namespace doris {

StringRef StringRef::trim() const {
    // Remove leading and trailing spaces.
    int32_t begin = 0;

    while (begin < size && data[begin] == ' ') {
        ++begin;
    }

    int32_t end = size - 1;

    while (end > begin && data[end] == ' ') {
        --end;
    }

    return StringRef(data + begin, end - begin + 1);
}

// TODO: rewrite in AVX2
size_t StringRef::find_first_of(char c) const {
    const char* p = static_cast<const char*>(memchr(data, c, size));
    return p == nullptr ? -1 : p - data;
}

StringRef StringRef::min_string_val() {
    return StringRef((char*)(&StringRef::MIN_CHAR), 0);
}

StringRef StringRef::max_string_val() {
    return StringRef((char*)(&StringRef::MAX_CHAR), 1);
}

bool StringRef::start_with(char ch) const {
    if (UNLIKELY(size == 0)) {
        return false;
    }
    return data[0] == ch;
}
bool StringRef::end_with(char ch) const {
    if (UNLIKELY(size == 0)) {
        return false;
    }
    return data[size - 1] == ch;
}

bool StringRef::start_with(const StringRef& search_string) const {
    if (search_string.size == 0) {
        return true;
    }

    if (UNLIKELY(size < search_string.size)) {
        return false;
    }

#if defined(__SSE2__) || defined(__aarch64__)
    return memequalSSE2Wide(data, search_string.data, search_string.size);
#else
    return 0 == memcmp(data, search_string.data, search_string.size);
#endif
}
bool StringRef::end_with(const StringRef& search_string) const {
    DCHECK(size >= search_string.size);
    if (search_string.size == 0) {
        return true;
    }

#if defined(__SSE2__) || defined(__aarch64__)
    return memequalSSE2Wide(data + size - search_string.size, search_string.data,
                            search_string.size);
#else
    return 0 == memcmp(data + size - search_string.size, search_string.data, search_string.size);
#endif
}
} // namespace doris
