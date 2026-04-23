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

#include <fmt/format.h>

#include <algorithm>
#include <cstddef>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column_const.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_string.h"
#include "core/memcpy_small.h"
#include "core/string_ref.h"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"

namespace doris {
#include "common/compile_check_avoid_begin.h"

template <bool Reverse>
class FunctionMaskPartial;

class FunctionMask : public IFunction {
public:
    static constexpr auto name = "mask";
    static constexpr unsigned char DEFAULT_UPPER_MASK = 'X';
    static constexpr unsigned char DEFAULT_LOWER_MASK = 'x';
    static constexpr unsigned char DEFAULT_NUMBER_MASK = 'n';
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionMask>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    size_t get_number_of_arguments() const override { return 0; }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1, 2, 3}; }

    bool is_variadic() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_GE(arguments.size(), 1);
        DCHECK_LE(arguments.size(), 4);

        char upper = DEFAULT_UPPER_MASK, lower = DEFAULT_LOWER_MASK, number = DEFAULT_NUMBER_MASK;

        auto res = ColumnString::create();
        const auto& source_column =
                assert_cast<const ColumnString&>(*block.get_by_position(arguments[0]).column);

        if (arguments.size() > 1) {
            const auto& col = *block.get_by_position(arguments[1]).column;
            auto string_ref = col.get_data_at(0);
            if (string_ref.size > 0) {
                upper = *string_ref.data;
            }
        }

        if (arguments.size() > 2) {
            const auto& col = *block.get_by_position(arguments[2]).column;
            auto string_ref = col.get_data_at(0);
            if (string_ref.size > 0) {
                lower = *string_ref.data;
            }
        }

        if (arguments.size() > 3) {
            const auto& col = *block.get_by_position(arguments[3]).column;
            auto string_ref = col.get_data_at(0);
            if (string_ref.size > 0) {
                number = *string_ref.data;
            }
        }

        if (arguments.size() > 4) {
            return Status::InvalidArgument(
                    fmt::format("too many arguments for function {}", get_name()));
        }

        vector_mask(source_column, *res, upper, lower, number);

        block.get_by_position(result).column = std::move(res);

        return Status::OK();
    }
    friend class FunctionMaskPartial<true>;
    friend class FunctionMaskPartial<false>;

private:
    static void vector_mask(const ColumnString& source, ColumnString& result, const char upper,
                            const char lower, const char number) {
        result.get_chars().resize(source.get_chars().size());
        result.get_offsets().resize(source.get_offsets().size());
        memcpy_small_allow_read_write_overflow15(
                result.get_offsets().data(), source.get_offsets().data(),
                source.get_offsets().size() * sizeof(ColumnString::Offset));

        const unsigned char* src = source.get_chars().data();
        const size_t size = source.get_chars().size();
        unsigned char* res = result.get_chars().data();
        mask(src, size, upper, lower, number, res);
    }

    static void mask(const unsigned char* __restrict src, const size_t size,
                     const unsigned char upper, const unsigned char lower,
                     const unsigned char number, unsigned char* __restrict res) {
        for (size_t i = 0; i != size; ++i) {
            auto c = src[i];
            if (c >= 'A' && c <= 'Z') {
                res[i] = upper;
            } else if (c >= 'a' && c <= 'z') {
                res[i] = lower;
            } else if (c >= '0' && c <= '9') {
                res[i] = number;
            } else {
                res[i] = c;
            }
        }
    }
};

template <bool Reverse>
class FunctionMaskPartial : public IFunction {
public:
    static constexpr auto name = Reverse ? "mask_last_n" : "mask_first_n";
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionMaskPartial>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto res = ColumnString::create();
        auto col = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& source_column = assert_cast<const ColumnString&>(*col);

        if (arguments.size() == 1) { // no 2nd arg, just mask all
            FunctionMask::vector_mask(source_column, *res, FunctionMask::DEFAULT_UPPER_MASK,
                                      FunctionMask::DEFAULT_LOWER_MASK,
                                      FunctionMask::DEFAULT_NUMBER_MASK);
        } else {
            const auto& [col_2nd, is_const] =
                    unpack_if_const(block.get_by_position(arguments[1]).column);

            const auto& col_n = assert_cast<const ColumnInt32&>(*col_2nd);

            if (is_const) {
                RETURN_IF_ERROR(vector<true>(source_column, col_n, *res));
            } else {
                RETURN_IF_ERROR(vector<false>(source_column, col_n, *res));
            }
        }

        block.get_by_position(result).column = std::move(res);

        return Status::OK();
    }

private:
    template <bool is_const>
    static Status vector(const ColumnString& src, const ColumnInt32& col_n, ColumnString& result) {
        const auto num_rows = src.size();
        const auto* chars = src.get_chars().data();
        const auto* offsets = src.get_offsets().data();
        result.get_chars().resize(src.get_chars().size());
        result.get_offsets().resize(src.get_offsets().size());
        memcpy_small_allow_read_write_overflow15(
                result.get_offsets().data(), src.get_offsets().data(),
                src.get_offsets().size() * sizeof(ColumnString::Offset));
        auto* res = result.get_chars().data();

        const auto& col_n_data = col_n.get_data();

        for (ssize_t i = 0; i != num_rows; ++i) {
            auto offset = offsets[i - 1];
            int len = offsets[i] - offset;
            const int n = col_n_data[index_check_const<is_const>(i)];

            if (n < 0) [[unlikely]] {
                return Status::InvalidArgument(
                        "function {} only accept non-negative input for 2nd argument but got {}",
                        name, n);
            }

            if constexpr (Reverse) {
                auto start = std::max(len - n, 0);
                if (start > 0) {
                    memcpy(&res[offset], &chars[offset], start);
                }
                offset += start;
            } else {
                if (n < len) {
                    memcpy(&res[offset + n], &chars[offset + n], len - n);
                }
            }

            len = std::min(n, len);
            FunctionMask::mask(&chars[offset], len, FunctionMask::DEFAULT_UPPER_MASK,
                               FunctionMask::DEFAULT_LOWER_MASK, FunctionMask::DEFAULT_NUMBER_MASK,
                               &res[offset]);
        }

        return Status::OK();
    }
};

void register_function_string_mask(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMask>();
    factory.register_function<FunctionMaskPartial<true>>();
    factory.register_function<FunctionMaskPartial<false>>();
}

#include "common/compile_check_avoid_end.h"
} // namespace doris
