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

#include "common/status.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris::vectorized {

struct ComplexTypeDeserializeUtil {
    // SplitResult is used to store the result of splitting a string by a delimiter.
    // It contains the element as a StringRef and the delimiter used for splitting.
    // For example, if the input string is "a,b,c" and the delimiter is ',',
    // the SplitResult will contain three elements: {"a", ','}, {"b", ','}, {"c", }.
    // If the input string is "a:b,c:d" and the delimiter is ';' or ','
    // the SplitResult will contain two elements: {"a", ':'}, {"b", ','}, {"c", ':'}, {"d", }.
    struct SplitResult {
        StringRef element;
        char delimiter = 0;
    };

    // Enhanced version with error handling
    template <typename Func>
    static Status split_by_delimiter(StringRef& str, char escape_char,
                                     Func func,
                                     std::vector<SplitResult>& elements) {
        char quote_char = 0;
        int last_pos = 0;
        int nested_level = 0;
        bool has_quote = false;  
        char delimiter = 0;
        elements.clear();     // 
        for (int pos = 0; pos < str.size; ++pos) {
            char c = str.data[pos];
            // Idea from simdjson to handle escape characters
            // Handle escape characters first
            if (c == '\\') {  
                // count the number of consecutive backslashes
                int backslash_count = 0;  
                while (pos < str.size && str.data[pos] == '\\') {  
                    backslash_count++;  
                    pos++;  
                }  
                  
                // if the number of backslashes is odd, the next character is escaped
                if (backslash_count % 2 == 1 && pos < str.size) {  
                    pos++;  // skip the escaped character
                }  
                pos--;  // backtrack, because the for loop will ++pos
                continue;  
            }  
            
            // Handle quotes
            if (c == '"' || c == '\'') {
                if (!has_quote) {
                    quote_char = c;
                    has_quote = true;
                } else if (has_quote && quote_char == c) {
                    quote_char = 0;
                    has_quote = false;
                }
            } else if (!has_quote && (c == '[' || c == '{')) {
                ++nested_level;
            } else if (!has_quote && (c == ']' || c == '}')) {
                --nested_level;
            } else if (!has_quote && nested_level == 0 && func(c)) {
                delimiter = c;
                if (last_pos != pos) {
                    elements.push_back({StringRef(str.data + last_pos, pos - last_pos), delimiter});
                } else {
                    /// TODO: It's best that our StringRef is a nullptr data here.
                    elements.push_back({StringRef(str.data + last_pos, 0), delimiter});
                }
                last_pos = pos + 1;
            }
        }

        // Validate final state
        if (has_quote) {
            return Status::InvalidArgument("Unclosed quote detected in string");
        }
        
        if (nested_level != 0) {
            return Status::InvalidArgument("Unmatched brackets detected in string");
        }

        // Add the last element with no delimiter (or empty delimiter)
        elements.push_back({StringRef(str.data + last_pos, str.size - last_pos), 0});

        for (auto& e : elements) {
            e.element = e.element.trim_whitespace();
        }
        return Status::OK();
    }


    static bool is_null_string(const StringRef& str) {
        if (str.size == 4) {
            // null
            return str.data[0] == 'n' && str.data[1] == 'u' && str.data[2] == 'l' &&
                   str.data[3] == 'l';
        }
        return false;
    }

    template <bool is_strict_mode>
    static Status process_column(const DataTypeSerDeSPtr& serde, IColumn& column, StringRef& str,
                                 const DataTypeSerDe::FormatOptions& options) {
        DCHECK(column.is_nullable()) << "Column must be nullable but got " << column.get_name();
        if (is_null_string(str)) {
            auto& column_nullable = assert_cast<ColumnNullable&>(column);
            column_nullable.insert_default();
            return Status::OK();
        }
        auto str_without_quote = str.trim_quote();
        if constexpr (is_strict_mode) {
            return serde->from_string_strict_mode(str_without_quote, column, options);
        } else {
            auto st = serde->from_string(str_without_quote, column, options);
            DCHECK(st.ok()) << "no strict mode, so should not return error";
            return st;
        }
    }
};

} // namespace doris::vectorized