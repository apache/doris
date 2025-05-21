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

#include "util/slice.h"

namespace doris::vectorized {

struct ComplexTypeDeserializeUtil {
    struct SplitResult {
        Slice element;
        char delimiter = 0;
    };

    template <typename Func>
    static std::vector<SplitResult> split_by_delimiter(Slice& str, Func func) {
        char quote_char = 0;
        int last_pos = 0;
        int nested_level = 0;
        bool has_quote = false;
        char delimiter = 0;
        std::vector<SplitResult> elements;
        for (int pos = 0; pos < str.size; ++pos) {
            char c = str[pos];
            if (c == '"' || c == '\'') {
                if (!has_quote) {
                    quote_char = c;
                    has_quote = !has_quote;
                } else if (has_quote && quote_char == c) {
                    quote_char = 0;
                    has_quote = !has_quote;
                }
            } else if (!has_quote && (c == '[' || c == '{')) {
                ++nested_level;
            } else if (!has_quote && (c == ']' || c == '}')) {
                --nested_level;
            } else if (!has_quote && nested_level == 0 && func(c)) {
                delimiter = c;
                if (last_pos != pos) {
                    elements.push_back({Slice(str.data + last_pos, pos - last_pos), delimiter});
                }
                last_pos = pos + 1;
            }
        }

        if (last_pos != str.size) {
            elements.push_back({Slice(str.data + last_pos, str.size - last_pos), delimiter});
        }

        for (auto& e : elements) {
            e.element.trim();
        }
        return elements;
    }
};

} // namespace doris::vectorized