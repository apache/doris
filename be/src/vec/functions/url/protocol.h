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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/URL/protocol.h
// and modified by Doris

#pragma once

#include "vec/common/string_utils/string_utils.h"
#include "vec/functions/url/functions_url.h"

namespace doris::vectorized {

/// Extracts scheme from given url.
inline StringRef get_url_scheme(const char* data, size_t size) {
    // scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
    const char* pos = data;
    const char* end = data + size;

    if (is_alpha_ascii(*pos)) {
        for (++pos; pos < end; ++pos) {
            if (!(is_alpha_numeric_ascii(*pos) || *pos == '+' || *pos == '-' || *pos == '.')) {
                break;
            }
        }

        return StringRef(data, pos - data);
    }

    return {};
}

struct ExtractProtocol {
    static size_t get_reserve_length_for_element() { return strlen("https") + 1; }

    static void execute(Pos data, size_t size, Pos& res_data, size_t& res_size) {
        res_data = data;
        res_size = 0;

        StringRef scheme = get_url_scheme(data, size);
        Pos pos = data + scheme.size;

        if (scheme.size == 0 || (data + size) - pos < 4) return;

        if (pos[0] == ':') res_size = pos - data;
    }
};

} // namespace doris::vectorized
