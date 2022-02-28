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

#ifndef DORIS_BE_SRC_QUERY_BE_RUNTIME_STRING_VALUE_INLINE_H
#define DORIS_BE_SRC_QUERY_BE_RUNTIME_STRING_VALUE_INLINE_H

#include <cstring>

#include "runtime/string_value.h"

namespace doris {

inline StringValue StringValue::substring(int start_pos) const {
    return StringValue(ptr + start_pos, len - start_pos);
}

inline StringValue StringValue::substring(int start_pos, int new_len) const {
    return StringValue(ptr + start_pos, (new_len < 0) ? (len - start_pos) : new_len);
}

inline StringValue StringValue::trim() const {
    // Remove leading and trailing spaces.
    int32_t begin = 0;

    while (begin < len && ptr[begin] == ' ') {
        ++begin;
    }

    int32_t end = len - 1;

    while (end > begin && ptr[end] == ' ') {
        --end;
    }

    return StringValue(ptr + begin, end - begin + 1);
}

} // namespace doris

#endif
