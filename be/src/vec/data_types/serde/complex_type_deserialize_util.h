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

#include "vec/common/string_ref.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris::vectorized {

struct ComplexTypeDeserializeUtil {
    struct SplitResult {
        StringRef element;
        char delimiter = 0;
    };

    template <typename Func>
    static std::vector<SplitResult> split_by_delimiter(StringRef& str, Func func) {
        char quote_char = 0;
        int last_pos = 0;
        int nested_level = 0;
        bool has_quote = false;
        char delimiter = 0;
        std::vector<SplitResult> elements;
        for (int pos = 0; pos < str.size; ++pos) {
            char c = str.data[pos];
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
                    elements.push_back({StringRef(str.data + last_pos, pos - last_pos), delimiter});
                }
                last_pos = pos + 1;
            }
        }

        elements.push_back({StringRef(str.data + last_pos, str.size - last_pos), delimiter});

        for (auto& e : elements) {
            e.element = e.element.trim_whitespace();
        }
        return elements;
    }

    static bool is_null_string(const StringRef& str) {
        if (str.size == 4) {
            // null
            return str.data[0] == 'n' && str.data[1] == 'u' && str.data[2] == 'l' &&
                   str.data[3] == 'l';
        }
        return false;
    }

    static Status process_column(const DataTypeSerDeSPtr& serde, IColumn& column, StringRef& str,
                                 const DataTypeSerDe::FormatOptions& options) {
        DCHECK(column.is_nullable()) << "Column must be nullable but got " << column.get_name();
        // if (ComplexTypeDeserializeUtil::is_null_string(str)) {
        //     // if the string is null, we should insert a null value to the column
        //     column.insert_default();
        // } else {
        //     auto slice = str.to_slice();
        //     RETURN_IF_ERROR(serde->deserialize_one_cell_from_json(column, slice, options));
        // }
        auto slice = str.to_slice();
        RETURN_IF_ERROR(serde->deserialize_one_cell_from_json(column, slice, options));

        return Status::OK();
    }
};

} // namespace doris::vectorized