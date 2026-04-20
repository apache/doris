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

#include <glog/logging.h>

#include <cstdio>
#include <optional>
#include <regex>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/define_primitive_type.h"
#include "core/types.h"
#include "exprs/function/cast_type_to_either.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

// ---------------------------------------------------------------------------
// Fast-path helpers for format(const_pattern, string_col...)
//
// When the format string is a constant with only plain {} or {N} placeholders
// (no format spec such as {:>10} or {:.2f}), the result can be assembled by
// directly memcpy-ing literal substrings and column data into a pre-allocated
// output buffer — no per-row fmt::format call is needed.
// ---------------------------------------------------------------------------

// Parsed representation of a format pattern that qualifies for the fast path.
struct FastFormatPattern {
    // Literal substrings split by the placeholders.
    // substrings.size() == indices.size() + 1
    std::vector<std::string> substrings;
    // 0-based argument indices for each placeholder (into the arg columns,
    // not counting the format-string column itself).
    std::vector<size_t> indices;
};

// Try to parse `pattern` into a FastFormatPattern.
// `arg_count` is the number of non-format arguments.
// Returns nullopt when the pattern is not suitable for the fast path (e.g.
// it contains a format spec, mismatched braces, or out-of-range indices).
static std::optional<FastFormatPattern> parse_format_fast_pattern(std::string_view pattern,
                                                                  size_t arg_count) {
    FastFormatPattern result;
    size_t auto_idx = 0;
    int numbering = -1; // -1=undetermined, 0=auto, 1=manual
    std::string cur;

    for (size_t i = 0; i < pattern.size(); ++i) {
        const char c = pattern[i];
        if (c == '{') {
            // Escaped {{ → literal '{'
            if (i + 1 < pattern.size() && pattern[i + 1] == '{') {
                cur += '{';
                ++i;
                continue;
            }
            // Scan to closing '}', bail out if ':' (format spec) is found first.
            size_t j = i + 1;
            while (j < pattern.size() && pattern[j] != '}' && pattern[j] != ':') {
                ++j;
            }
            if (j >= pattern.size() || pattern[j] == ':') {
                return std::nullopt;
            }
            const std::string_view idx_str = pattern.substr(i + 1, j - i - 1);
            result.substrings.push_back(std::move(cur));
            cur.clear();

            size_t arg_idx = 0;
            if (idx_str.empty()) {
                // Auto-numbering: {}, {}, ...
                if (numbering == 1) {
                    return std::nullopt; // mixed auto + manual
                }
                numbering = 0;
                arg_idx = auto_idx++;
            } else {
                // Manual numbering: {0}, {1}, ...
                if (numbering == 0) {
                    return std::nullopt; // mixed auto + manual
                }
                numbering = 1;
                for (char ch : idx_str) {
                    if (ch < '0' || ch > '9') {
                        return std::nullopt; // non-numeric index
                    }
                    arg_idx = arg_idx * 10 + static_cast<size_t>(ch - '0');
                }
            }
            if (arg_idx >= arg_count) {
                return std::nullopt; // out-of-range index
            }
            result.indices.push_back(arg_idx);
            i = j;
        } else if (c == '}') {
            // Escaped }} → literal '}'
            if (i + 1 < pattern.size() && pattern[i + 1] == '}') {
                cur += '}';
                ++i;
                continue;
            }
            return std::nullopt; // unmatched '}'
        } else {
            cur += c;
        }
    }
    result.substrings.push_back(std::move(cur));
    return result;
}

// Execute the fast path: assemble output by copying literal substrings and
// column data directly into a pre-allocated buffer.
//
// arg_cols[i] is the (already unpacked) ColumnString for argument i.
// arg_is_consts[i] indicates whether argument i was a ColumnConst.
static void execute_fast_string(const FastFormatPattern& parsed,
                                const std::vector<const ColumnString*>& arg_cols,
                                const std::vector<bool>& arg_is_consts, ColumnString* result,
                                size_t input_rows_count) {
    auto& res_chars = result->get_chars();
    auto& res_offsets = result->get_offsets();
    res_offsets.resize(input_rows_count);

    // Total literal bytes written per output row (constant across rows).
    size_t total_substr_per_row = 0;
    for (const auto& s : parsed.substrings) {
        total_substr_per_row += s.size();
    }

    // Total argument bytes across all output rows.
    // Each entry in `indices` counts the bytes from its column once per row.
    size_t total_arg_size = 0;
    for (const size_t idx : parsed.indices) {
        if (arg_is_consts[idx]) {
            // Single-row const column: replicate that row's size input_rows_count times.
            total_arg_size +=
                    static_cast<size_t>(arg_cols[idx]->get_offsets()[0]) * input_rows_count;
        } else {
            // Full column: offsets.back() == total chars written.
            total_arg_size += arg_cols[idx]->get_offsets().back();
        }
    }

    const size_t total = input_rows_count * total_substr_per_row + total_arg_size;
    ColumnString::check_chars_length(total, 0);
    res_chars.resize(total);

    const auto& subs = parsed.substrings;
    const size_t n_phs = parsed.indices.size();
    uint32_t dst = 0;

    for (size_t i = 0; i < input_rows_count; ++i) {
        // First literal segment
        if (!subs[0].empty()) {
            memcpy(res_chars.data() + dst, subs[0].data(), subs[0].size());
            dst += static_cast<uint32_t>(subs[0].size());
        }
        for (size_t j = 0; j < n_phs; ++j) {
            const size_t idx = parsed.indices[j];
            const size_t row_idx = arg_is_consts[idx] ? 0 : i;
            const auto& col_offsets = arg_cols[idx]->get_offsets();
            // Accessing col_offsets[row_idx - 1]: when row_idx == 0, this evaluates to
            // col_offsets[-1] == 0 via the PODArray pre-element (operator[] takes ssize_t).
            const uint32_t arg_start = col_offsets[row_idx - 1];
            const uint32_t arg_size = col_offsets[row_idx] - arg_start;
            if (arg_size > 0) {
                memcpy(res_chars.data() + dst, arg_cols[idx]->get_chars().data() + arg_start,
                       arg_size);
                dst += arg_size;
            }
            // Literal segment after this placeholder
            if (!subs[j + 1].empty()) {
                memcpy(res_chars.data() + dst, subs[j + 1].data(), subs[j + 1].size());
                dst += static_cast<uint32_t>(subs[j + 1].size());
            }
        }
        res_offsets[i] = dst;
    }
}

class FunctionFormatNumber : public IFunction {
public:
    static constexpr auto name = "format_number";

    static constexpr const char* UNITS[6] = {"", "K", "M", "B", "T", "Q"};

    static FunctionPtr create() { return std::make_shared<FunctionFormatNumber>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool is_variadic() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto column = block.get_by_position(arguments[0]).column;
        const auto& column_data = assert_cast<const ColumnFloat64*>(column.get())->get_data();
        auto col_res = ColumnString::create();
        fmt::memory_buffer buffer;

        for (auto i = 0; i < input_rows_count; ++i) {
            auto res_data = format_number(buffer, column_data[i]);
            col_res->insert_data(res_data.data(), res_data.length());
        }
        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }

    std::string format_number(fmt::memory_buffer& buffer, double number) const {
        buffer.clear();
        double abs_number = std::abs(number);
        int unit_index = 0;
        while (abs_number >= 1000 && unit_index < 5) {
            abs_number /= 1000;
            ++unit_index;
        }
        if (number < 0) {
            fmt::format_to(buffer, "-");
        }
        if (abs_number == 1) {
            //eg: 1000 ---> 1K
            fmt::format_to(buffer, "{}", abs_number);
        } else if (abs_number < 10) {
            //eg: 1239 ---> 1.24K only want to show 2 decimal
            fmt::format_to(buffer, "{:.2f}", abs_number);
        } else if (abs_number < 100) {
            //eg: 12399999 ---> 12.4M only want to show 1 decimal
            fmt::format_to(buffer, "{:.1f}", abs_number);
        } else {
            // eg: 999999999999999 ---> 1000T only want to show 0 decimal
            fmt::format_to(buffer, "{:.0f}", abs_number);
        }
        fmt::format_to(buffer, UNITS[unit_index]);
        return fmt::to_string(buffer);
    }
};

class FunctionFormat : public IFunction {
public:
    static constexpr auto name = "format";

    static FunctionPtr create() { return std::make_shared<FunctionFormat>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_GE(arguments.size(), 2);
        bool valid =
                cast_type(block.get_by_position(arguments[1]).type.get(), [&](const auto& type) {
                    using DataType = std::decay_t<decltype(type)>;
                    using ColVecData =
                            std::conditional_t<is_number(DataType::PType),
                                               ColumnVector<DataType::PType>, ColumnString>;
                    if (auto col = check_and_get_column<ColVecData>(
                                           block.get_by_position(arguments[1]).column.get()) ||
                                   is_column_const(*block.get_by_position(arguments[1]).column)) {
                        execute_inner<ColVecData, DataType::PType>(block, arguments, result,
                                                                   input_rows_count);
                        return true;
                    }
                    return false;
                });
        if (!valid) {
            return Status::RuntimeError(
                    "{}'s argument does not match the expected data type, type: {}, column: {}",
                    get_name(), block.get_by_position(arguments[1]).type->get_name(),
                    block.get_by_position(arguments[1]).column->dump_structure());
        }
        return Status::OK();
    }

    template <typename F>
    static bool cast_type(const IDataType* type, F&& f) {
        return cast_type_to_either<DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
                                   DataTypeInt128, DataTypeFloat32, DataTypeFloat64,
                                   DataTypeString>(type, std::forward<F>(f));
    }

    template <typename ColVecData, PrimitiveType T>
    void execute_inner(Block& block, const ColumnNumbers& arguments, uint32_t result,
                       size_t input_rows_count) const {
        const size_t argument_size = arguments.size();
        std::vector<ColumnPtr> argument_columns(argument_size);
        auto result_column = ColumnString::create();

        // Fast path: constant format string + all-String arguments + no format specs.
        // Avoids per-row fmt::format by parsing the pattern once and directly
        // memcpy-ing literals and column data into a pre-allocated output buffer.
        if constexpr (is_string_type(T)) {
            if (is_column_const(*block.get_by_position(arguments[0]).column)) {
                const auto format_sv =
                        block.get_by_position(arguments[0]).column->get_data_at(0).to_string_view();
                auto parsed = parse_format_fast_pattern(format_sv, argument_size - 1);
                if (parsed.has_value()) {
                    std::vector<const ColumnString*> arg_cols(argument_size - 1);
                    std::vector<bool> arg_is_consts(argument_size - 1);
                    // Hold unpacked column pointers alive for the duration of the call.
                    std::vector<ColumnPtr> col_holders(argument_size - 1);
                    for (size_t i = 1; i < argument_size; ++i) {
                        auto [col, is_const] =
                                unpack_if_const(block.get_by_position(arguments[i]).column);
                        col_holders[i - 1] = col;
                        arg_cols[i - 1] = assert_cast<const ColumnString*>(col.get());
                        arg_is_consts[i - 1] = is_const;
                    }
                    execute_fast_string(*parsed, arg_cols, arg_is_consts,
                                        assert_cast<ColumnString*>(result_column.get()),
                                        input_rows_count);
                    block.replace_by_position(result, std::move(result_column));
                    return;
                }
            }
        }

        // Slow path — unchanged.
        if (argument_size == 2) {
            std::vector<uint8_t> is_consts(argument_size);
            std::tie(argument_columns[0], is_consts[0]) =
                    unpack_if_const(block.get_by_position(arguments[0]).column);
            std::tie(argument_columns[1], is_consts[1]) =
                    unpack_if_const(block.get_by_position(arguments[1]).column);
            execute_for_two_argument<ColVecData, T>(argument_columns, is_consts,
                                                    assert_cast<ColumnString*>(result_column.get()),
                                                    input_rows_count);
        } else {
            for (size_t i = 0; i < argument_size; ++i) {
                argument_columns[i] = block.get_by_position(arguments[i])
                                              .column->convert_to_full_column_if_const();
            }
            execute_for_others_arg<ColVecData, T>(argument_columns,
                                                  assert_cast<ColumnString*>(result_column.get()),
                                                  argument_size, input_rows_count);
        }

        block.replace_by_position(result, std::move(result_column));
    }

    template <typename ColVecData, PrimitiveType T>
    void execute_for_two_argument(std::vector<ColumnPtr>& argument_columns,
                                  std::vector<uint8_t>& is_consts, ColumnString* result_data_column,
                                  size_t input_rows_count) const {
        const auto& format_column = assert_cast<const ColumnString&>(*argument_columns[0].get());
        const auto& value_column = assert_cast<const ColVecData&>(*argument_columns[1].get());
        for (int i = 0; i < input_rows_count; ++i) {
            auto format =
                    format_column.get_data_at(index_check_const(i, is_consts[0])).to_string_view();
            std::string res;
            try {
                if constexpr (is_string_type(T)) {
                    auto value = value_column.get_data_at(index_check_const(i, is_consts[1]));
                    res = fmt::format(format, value);
                } else {
                    auto value = value_column.get_data()[index_check_const(i, is_consts[1])];
                    res = fmt::format(format, value);
                }
            } catch (const std::exception& e) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Invalid Input argument \"{}\" of function format, error: {}", format,
                        e.what());
            }
            result_data_column->insert_data(res.data(), res.length());
        }
    }

    template <typename ColVecData, PrimitiveType T>
    void execute_for_others_arg(std::vector<ColumnPtr>& argument_columns,
                                ColumnString* result_data_column, size_t argument_size,
                                size_t input_rows_count) const {
        const auto& format_column = assert_cast<const ColumnString&>(*argument_columns[0].get());
        for (int i = 0; i < input_rows_count; ++i) {
            auto format = format_column.get_data_at(i).to_string_view();
            std::string res;
            fmt::dynamic_format_arg_store<fmt::format_context> args;
            if constexpr (is_string_type(T)) {
                for (int col = 1; col < argument_size; ++col) {
                    const auto& arg_column_data =
                            assert_cast<const ColVecData&>(*argument_columns[col].get());
                    args.push_back(arg_column_data.get_data_at(i).to_string());
                }
            } else {
                for (int col = 1; col < argument_size; ++col) {
                    const auto& arg_column_data =
                            assert_cast<const ColVecData&>(*argument_columns[col].get()).get_data();
                    args.push_back(arg_column_data[i]);
                }
            }
            try {
                res = fmt::vformat(format, args);
            } catch (const std::exception& e) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Invalid Input argument \"{}\" of function format, error: {}", format,
                        e.what());
            }
            result_data_column->insert_data(res.data(), res.length());
        }
    }
};

void register_function_format(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionFormatNumber>();
    factory.register_function<FunctionFormat>();
}

} // namespace doris
