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

#include <cstddef>

namespace doris {

inline bool is_hive_text_separator_escaped(const char* data, size_t separator_pos,
                                           char escape_char) {
    size_t escape_count = 0;
    while (separator_pos > escape_count && data[separator_pos - escape_count - 1] == escape_char) {
        ++escape_count;
    }
    return escape_count % 2 == 1;
}

} // namespace doris
