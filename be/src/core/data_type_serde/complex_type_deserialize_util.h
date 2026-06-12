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

#include "core/data_type_serde/data_type_nullable_serde.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/string_ref.h"

namespace doris {

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
            if (c == '\\' && pos + 1 < static_cast<int>(str.size)) {
                ++pos; // skip escaped character
            } else if (c == '"' || c == '\'') {
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
                } else {
                    /// TODO: It's best that our StringRef is a nullptr data here.
                    elements.push_back({StringRef(str.data + last_pos, 0), delimiter});
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
            // SQL NULL literal is case-insensitive.
            return (str.data[0] == 'n' || str.data[0] == 'N') &&
                   (str.data[1] == 'u' || str.data[1] == 'U') &&
                   (str.data[2] == 'l' || str.data[2] == 'L') &&
                   (str.data[3] == 'l' || str.data[3] == 'L');
        }
        return false;
    }

    static Status process_field(const DataTypeSerDeSPtr& serde, StringRef str, Field& field) {
        str = str.trim_whitespace();
        auto nullable_serde = std::dynamic_pointer_cast<DataTypeNullableSerDe>(serde);
        if (is_null_string(str)) {
            if (nullable_serde == nullptr) {
                return Status::InvalidArgument(
                        "NULL default is not allowed for non-nullable complex field");
            }
            field = Field::create_field<TYPE_NULL>(Null {});
            return Status::OK();
        }
        auto str_without_quote = str.trim_quote();
        auto nested_serde = serde;
        if (nullable_serde != nullptr) {
            // Complex default values need strict nested conversion. DataTypeNullableSerDe
            // converts nested parse failures to NULL, which would silently accept
            // type-mismatched default literals.
            nested_serde = nullable_serde->get_nested_serde();
        }
        return nested_serde->from_fe_string(str_without_quote.to_string(), field);
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

} // namespace doris
