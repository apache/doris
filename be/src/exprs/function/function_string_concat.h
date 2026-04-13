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

#include <fmt/format.h>

#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/memcpy_small.h"
#include "core/string_ref.h"
#include "exec/common/stringop_substring.h"
#include "exec/common/template_helpers.hpp"
#include "exec/common/util.hpp"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function_context.h"
#include "util/simd/vstring_function.h"

namespace doris {
#include "common/compile_check_avoid_begin.h"

class FunctionStringConcat : public IFunction {
public:
    struct ConcatState {
        bool use_state = false;
        std::string tail;
    };

    static constexpr auto name = "concat";
    static FunctionPtr create() { return std::make_shared<FunctionStringConcat>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        std::shared_ptr<ConcatState> state = std::make_shared<ConcatState>();

        context->set_function_state(scope, state);

        state->use_state = true;

        // Optimize function calls like this:
        // concat(col, "123", "abc", "456") -> tail = "123abc456"
        for (size_t i = 1; i < context->get_num_args(); i++) {
            const auto* column_string = context->get_constant_col(i);
            if (column_string == nullptr) {
                state->use_state = false;
                return IFunction::open(context, scope);
            }
            auto string_vale = column_string->column_ptr->get_data_at(0);
            if (string_vale.data == nullptr) {
                // For concat(col, null), it is handled by default_implementation_for_nulls
                state->use_state = false;
                return IFunction::open(context, scope);
            }

            state->tail.append(string_vale.begin(), string_vale.size);
        }

        // The reserve is used here to allow the usage of memcpy_small_allow_read_write_overflow15 below.
        state->tail.reserve(state->tail.size() + 16);

        return IFunction::open(context, scope);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_GE(arguments.size(), 1);

        if (arguments.size() == 1) {
            block.get_by_position(result).column = block.get_by_position(arguments[0]).column;
            return Status::OK();
        }
        auto* concat_state = reinterpret_cast<ConcatState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (!concat_state) {
            return Status::RuntimeError("funciton context for function '{}' must have ConcatState;",
                                        get_name());
        }
        if (concat_state->use_state) {
            const auto& [col, is_const] =
                    unpack_if_const(block.get_by_position(arguments[0]).column);
            const auto* col_str = assert_cast<const ColumnString*>(col.get());
            if (is_const) {
                return execute_const<true>(concat_state, block, col_str, result, input_rows_count);
            } else {
                return execute_const<false>(concat_state, block, col_str, result, input_rows_count);
            }

        } else {
            return execute_vecotr(block, arguments, result, input_rows_count);
        }
    }

    Status execute_vecotr(Block& block, const ColumnNumbers& arguments, uint32_t result,
                          size_t input_rows_count) const {
        int argument_size = arguments.size();
        std::vector<ColumnPtr> argument_columns(argument_size);

        std::vector<const ColumnString::Offsets*> offsets_list(argument_size);
        std::vector<const ColumnString::Chars*> chars_list(argument_size);
        std::vector<bool> is_const_args(argument_size);

        for (int i = 0; i < argument_size; ++i) {
            const auto& [col, is_const] =
                    unpack_if_const(block.get_by_position(arguments[i]).column);

            const auto* col_str = assert_cast<const ColumnString*>(col.get());
            offsets_list[i] = &col_str->get_offsets();
            chars_list[i] = &col_str->get_chars();
            is_const_args[i] = is_const;
        }

        auto res = ColumnString::create();
        auto& res_data = res->get_chars();
        auto& res_offset = res->get_offsets();

        res_offset.resize(input_rows_count);
        size_t res_reserve_size = 0;
        for (size_t i = 0; i < argument_size; ++i) {
            if (is_const_args[i]) {
                res_reserve_size += (*offsets_list[i])[0] * input_rows_count;
            } else {
                res_reserve_size += (*offsets_list[i])[input_rows_count - 1];
            }
        }

        ColumnString::check_chars_length(res_reserve_size, 0);

        res_data.resize(res_reserve_size);

        auto* data = res_data.data();
        size_t dst_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i) {
            for (size_t j = 0; j < argument_size; ++j) {
                const auto& current_offsets = *offsets_list[j];
                const auto& current_chars = *chars_list[j];
                auto idx = index_check_const(i, is_const_args[j]);
                const auto size = current_offsets[idx] - current_offsets[idx - 1];
                if (size > 0) {
                    memcpy_small_allow_read_write_overflow15(
                            data + dst_offset, current_chars.data() + current_offsets[idx - 1],
                            size);
                    dst_offset += size;
                }
            }
            res_offset[i] = dst_offset;
        }

        block.get_by_position(result).column = std::move(res);
        return Status::OK();
    }

    template <bool is_const>
    Status execute_const(ConcatState* concat_state, Block& block, const ColumnString* col_str,
                         uint32_t result, size_t input_rows_count) const {
        // using tail optimize

        auto res = ColumnString::create();
        auto& res_data = res->get_chars();
        auto& res_offset = res->get_offsets();
        res_offset.resize(input_rows_count);

        size_t res_reserve_size = 0;
        if constexpr (is_const) {
            res_reserve_size = col_str->get_offsets()[0] * input_rows_count;
        } else {
            res_reserve_size = col_str->get_offsets()[input_rows_count - 1];
        }
        res_reserve_size += concat_state->tail.size() * input_rows_count;

        ColumnString::check_chars_length(res_reserve_size, 0);
        res_data.resize(res_reserve_size);

        const auto& tail = concat_state->tail;
        auto* data = res_data.data();
        size_t dst_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i) {
            const auto idx = index_check_const<is_const>(i);
            StringRef str_val = col_str->get_data_at(idx);
            // copy column
            memcpy_small_allow_read_write_overflow15(data + dst_offset, str_val.data, str_val.size);
            dst_offset += str_val.size;
            // copy tail
            memcpy_small_allow_read_write_overflow15(data + dst_offset, tail.data(), tail.size());
            dst_offset += tail.size();
            res_offset[i] = dst_offset;
        }
        block.get_by_position(result).column = std::move(res);
        return Status::OK();
    }
};

class FunctionStringElt : public IFunction {
public:
    static constexpr auto name = "elt";
    static FunctionPtr create() { return std::make_shared<FunctionStringElt>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }
    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        int arguent_size = arguments.size();
        int num_children = arguent_size - 1;
        auto res = ColumnString::create();

        if (auto const_column = check_and_get_column<ColumnConst>(
                    *block.get_by_position(arguments[0]).column)) {
            auto data = const_column->get_data_at(0);
            // return NULL, pos is null or pos < 0 or pos > num_children
            auto is_null = data.data == nullptr;
            auto pos = is_null ? 0 : *(Int32*)data.data;
            is_null = pos <= 0 || pos > num_children;

            auto null_map = ColumnUInt8::create(input_rows_count, is_null);
            if (is_null) {
                res->insert_many_defaults(input_rows_count);
            } else {
                auto& target_column = block.get_by_position(arguments[pos]).column;
                if (auto target_const_column = check_and_get_column<ColumnConst>(*target_column)) {
                    auto target_data = target_const_column->get_data_at(0);
                    // return NULL, no target data
                    if (target_data.data == nullptr) {
                        null_map = ColumnUInt8::create(input_rows_count, true);
                        res->insert_many_defaults(input_rows_count);
                    } else {
                        res->insert_data_repeatedly(target_data.data, target_data.size,
                                                    input_rows_count);
                    }
                } else if (auto target_nullable_column =
                                   check_and_get_column<ColumnNullable>(*target_column)) {
                    auto& target_null_map = target_nullable_column->get_null_map_data();
                    VectorizedUtils::update_null_map(
                            assert_cast<ColumnUInt8&>(*null_map).get_data(), target_null_map);

                    auto& target_str_column = assert_cast<const ColumnString&>(
                            target_nullable_column->get_nested_column());
                    res->get_chars().assign(target_str_column.get_chars().begin(),
                                            target_str_column.get_chars().end());
                    res->get_offsets().assign(target_str_column.get_offsets().begin(),
                                              target_str_column.get_offsets().end());
                } else {
                    auto& target_str_column = assert_cast<const ColumnString&>(*target_column);
                    res->get_chars().assign(target_str_column.get_chars().begin(),
                                            target_str_column.get_chars().end());
                    res->get_offsets().assign(target_str_column.get_offsets().begin(),
                                              target_str_column.get_offsets().end());
                }
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(res), std::move(null_map));
        } else if (auto pos_null_column = check_and_get_column<ColumnNullable>(
                           *block.get_by_position(arguments[0]).column)) {
            auto& pos_column =
                    assert_cast<const ColumnInt32&>(pos_null_column->get_nested_column());
            auto& pos_null_map = pos_null_column->get_null_map_data();
            auto null_map = ColumnUInt8::create(input_rows_count, false);
            auto& res_null_map = assert_cast<ColumnUInt8&>(*null_map).get_data();

            for (size_t i = 0; i < input_rows_count; ++i) {
                auto pos = pos_column.get_element(i);
                res_null_map[i] =
                        pos_null_map[i] || pos <= 0 || pos > num_children ||
                        block.get_by_position(arguments[pos]).column->get_data_at(i).data ==
                                nullptr;
                if (res_null_map[i]) {
                    res->insert_default();
                } else {
                    auto insert_data = block.get_by_position(arguments[pos]).column->get_data_at(i);
                    res->insert_data(insert_data.data, insert_data.size);
                }
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(res), std::move(null_map));
        } else {
            auto& pos_column =
                    assert_cast<const ColumnInt32&>(*block.get_by_position(arguments[0]).column);
            auto null_map = ColumnUInt8::create(input_rows_count, false);
            auto& res_null_map = assert_cast<ColumnUInt8&>(*null_map).get_data();

            for (size_t i = 0; i < input_rows_count; ++i) {
                auto pos = pos_column.get_element(i);
                res_null_map[i] =
                        pos <= 0 || pos > num_children ||
                        block.get_by_position(arguments[pos]).column->get_data_at(i).data ==
                                nullptr;
                if (res_null_map[i]) {
                    res->insert_default();
                } else {
                    auto insert_data = block.get_by_position(arguments[pos]).column->get_data_at(i);
                    res->insert_data(insert_data.data, insert_data.size);
                }
            }
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(res), std::move(null_map));
        }
        return Status::OK();
    }
};

// concat_ws (string,string....) or (string, Array)
// TODO: avoid use fmtlib
class FunctionStringConcatWs : public IFunction {
public:
    using Chars = ColumnString::Chars;
    using Offsets = ColumnString::Offsets;

    static constexpr auto name = "concat_ws";
    static FunctionPtr create() { return std::make_shared<FunctionStringConcatWs>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        const IDataType* first_type = arguments[0].get();
        if (first_type->is_nullable()) {
            return make_nullable(std::make_shared<DataTypeString>());
        } else {
            return std::make_shared<DataTypeString>();
        }
    }
    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_GE(arguments.size(), 2);
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        // we create a zero column to simply implement
        auto const_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res = ColumnString::create();
        bool is_null_type = block.get_by_position(arguments[0]).type.get()->is_nullable();
        size_t argument_size = arguments.size();
        std::vector<const Offsets*> offsets_list(argument_size);
        std::vector<const Chars*> chars_list(argument_size);
        std::vector<const ColumnUInt8::Container*> null_list(argument_size);

        std::vector<ColumnPtr> argument_columns(argument_size);
        std::vector<ColumnPtr> argument_null_columns(argument_size);

        for (size_t i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (const auto* nullable =
                        check_and_get_column<const ColumnNullable>(*argument_columns[i])) {
                // Danger: Here must dispose the null map data first! Because
                // argument_columns[i]=nullable->get_nested_column_ptr(); will release the mem
                // of column nullable mem of null map
                null_list[i] = &nullable->get_null_map_data();
                argument_null_columns[i] = nullable->get_null_map_column_ptr();
                argument_columns[i] = nullable->get_nested_column_ptr();
            } else {
                null_list[i] = &const_null_map->get_data();
            }

            if (is_column<ColumnArray>(argument_columns[i].get())) {
                continue;
            }

            const auto* col_str = assert_cast<const ColumnString*>(argument_columns[i].get());
            offsets_list[i] = &col_str->get_offsets();
            chars_list[i] = &col_str->get_chars();
        }

        auto& res_data = res->get_chars();
        auto& res_offset = res->get_offsets();
        res_offset.resize(input_rows_count);

        VectorizedUtils::update_null_map(null_map->get_data(), *null_list[0]);
        fmt::memory_buffer buffer;
        std::vector<std::string_view> views;

        if (is_column<ColumnArray>(argument_columns[1].get())) {
            // Determine if the nested type of the array is String
            const auto& array_column = reinterpret_cast<const ColumnArray&>(*argument_columns[1]);
            if (!array_column.get_data().is_column_string()) {
                return Status::NotSupported(
                        fmt::format("unsupported nested array of type {} for function {}",
                                    is_column_nullable(array_column.get_data())
                                            ? array_column.get_data().get_name()
                                            : array_column.get_data().get_name(),
                                    get_name()));
            }
            // Concat string in array
            _execute_array(input_rows_count, array_column, buffer, views, offsets_list, chars_list,
                           null_list, res_data, res_offset);

        } else {
            // Concat string
            _execute_string(input_rows_count, argument_size, buffer, views, offsets_list,
                            chars_list, null_list, res_data, res_offset);
        }
        if (is_null_type) {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(res), std::move(null_map));
        } else {
            block.get_by_position(result).column = std::move(res);
        }
        return Status::OK();
    }

private:
    void _execute_array(const size_t& input_rows_count, const ColumnArray& array_column,
                        fmt::memory_buffer& buffer, std::vector<std::string_view>& views,
                        const std::vector<const Offsets*>& offsets_list,
                        const std::vector<const Chars*>& chars_list,
                        const std::vector<const ColumnUInt8::Container*>& null_list,
                        Chars& res_data, Offsets& res_offset) const {
        // Get array nested column
        const UInt8* array_nested_null_map = nullptr;
        ColumnPtr array_nested_column = nullptr;

        if (is_column_nullable(array_column.get_data())) {
            const auto& array_nested_null_column =
                    reinterpret_cast<const ColumnNullable&>(array_column.get_data());
            // String's null map in array
            array_nested_null_map =
                    array_nested_null_column.get_null_map_column().get_data().data();
            array_nested_column = array_nested_null_column.get_nested_column_ptr();
        } else {
            array_nested_column = array_column.get_data_ptr();
        }

        const auto& string_column = reinterpret_cast<const ColumnString&>(*array_nested_column);
        const Chars& string_src_chars = string_column.get_chars();
        const auto& src_string_offsets = string_column.get_offsets();
        const auto& src_array_offsets = array_column.get_offsets();
        size_t current_src_array_offset = 0;

        // Concat string in array
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto& sep_offsets = *offsets_list[0];
            auto& sep_chars = *chars_list[0];
            auto& sep_nullmap = *null_list[0];

            if (sep_nullmap[i]) {
                res_offset[i] = res_data.size();
                current_src_array_offset += src_array_offsets[i] - src_array_offsets[i - 1];
                continue;
            }

            int sep_size = sep_offsets[i] - sep_offsets[i - 1];
            const char* sep_data = reinterpret_cast<const char*>(&sep_chars[sep_offsets[i - 1]]);

            std::string_view sep(sep_data, sep_size);
            buffer.clear();
            views.clear();

            for (auto next_src_array_offset = src_array_offsets[i];
                 current_src_array_offset < next_src_array_offset; ++current_src_array_offset) {
                const auto current_src_string_offset =
                        current_src_array_offset ? src_string_offsets[current_src_array_offset - 1]
                                                 : 0;
                size_t bytes_to_copy =
                        src_string_offsets[current_src_array_offset] - current_src_string_offset;
                const char* ptr =
                        reinterpret_cast<const char*>(&string_src_chars[current_src_string_offset]);

                if (array_nested_null_map == nullptr ||
                    !array_nested_null_map[current_src_array_offset]) {
                    views.emplace_back(ptr, bytes_to_copy);
                }
            }

            fmt::format_to(buffer, "{}", fmt::join(views, sep));

            StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i, res_data,
                                        res_offset);
        }
    }

    void _execute_string(const size_t& input_rows_count, const size_t& argument_size,
                         fmt::memory_buffer& buffer, std::vector<std::string_view>& views,
                         const std::vector<const Offsets*>& offsets_list,
                         const std::vector<const Chars*>& chars_list,
                         const std::vector<const ColumnUInt8::Container*>& null_list,
                         Chars& res_data, Offsets& res_offset) const {
        // Concat string
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto& sep_offsets = *offsets_list[0];
            auto& sep_chars = *chars_list[0];
            auto& sep_nullmap = *null_list[0];
            if (sep_nullmap[i]) {
                res_offset[i] = res_data.size();
                continue;
            }

            int sep_size = sep_offsets[i] - sep_offsets[i - 1];
            const char* sep_data = reinterpret_cast<const char*>(&sep_chars[sep_offsets[i - 1]]);

            std::string_view sep(sep_data, sep_size);
            buffer.clear();
            views.clear();
            for (size_t j = 1; j < argument_size; ++j) {
                auto& current_offsets = *offsets_list[j];
                auto& current_chars = *chars_list[j];
                auto& current_nullmap = *null_list[j];
                int size = current_offsets[i] - current_offsets[i - 1];
                const char* ptr =
                        reinterpret_cast<const char*>(&current_chars[current_offsets[i - 1]]);
                if (!current_nullmap[i]) {
                    views.emplace_back(ptr, size);
                }
            }
            fmt::format_to(buffer, "{}", fmt::join(views, sep));
            StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i, res_data,
                                        res_offset);
        }
    }
};

class FunctionStringRepeat : public IFunction {
public:
    static constexpr auto name = "repeat";
    static FunctionPtr create() { return std::make_shared<FunctionStringRepeat>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    // should set NULL value of nested data to default,
    // as iff it's not inited and invalid, the repeat result of length is so large cause overflow
    bool need_replace_null_data_to_default() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 2);
        auto res = ColumnString::create();
        auto null_map = ColumnUInt8::create();

        ColumnPtr argument_ptr[2];
        argument_ptr[0] =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        argument_ptr[1] = block.get_by_position(arguments[1]).column;

        if (const auto* col1 = check_and_get_column<ColumnString>(*argument_ptr[0])) {
            if (const auto* col2 = check_and_get_column<ColumnInt32>(*argument_ptr[1])) {
                RETURN_IF_ERROR(vector_vector(col1->get_chars(), col1->get_offsets(),
                                              col2->get_data(), res->get_chars(),
                                              res->get_offsets(), null_map->get_data()));
                block.replace_by_position(
                        result, ColumnNullable::create(std::move(res), std::move(null_map)));
                return Status::OK();
            } else if (const auto* col2_const =
                               check_and_get_column<ColumnConst>(*argument_ptr[1])) {
                DCHECK(check_and_get_column<ColumnInt32>(col2_const->get_data_column()));
                int repeat = col2_const->get_int(0);
                if (repeat <= 0) {
                    null_map->get_data().resize_fill(input_rows_count, 0);
                    res->insert_many_defaults(input_rows_count);
                } else {
                    vector_const(col1->get_chars(), col1->get_offsets(), repeat, res->get_chars(),
                                 res->get_offsets(), null_map->get_data());
                }
                block.replace_by_position(
                        result, ColumnNullable::create(std::move(res), std::move(null_map)));
                return Status::OK();
            }
        }

        return Status::RuntimeError("repeat function get error param: {}, {}",
                                    argument_ptr[0]->get_name(), argument_ptr[1]->get_name());
    }

    Status vector_vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         const ColumnInt32::Container& repeats, ColumnString::Chars& res_data,
                         ColumnString::Offsets& res_offsets,
                         ColumnUInt8::Container& null_map) const {
        size_t input_row_size = offsets.size();

        fmt::memory_buffer buffer;
        res_offsets.resize(input_row_size);
        null_map.resize_fill(input_row_size, 0);
        for (ssize_t i = 0; i < input_row_size; ++i) {
            buffer.clear();
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            size_t size = offsets[i] - offsets[i - 1];
            int repeat = repeats[i];
            if (repeat <= 0) {
                StringOP::push_empty_string(i, res_data, res_offsets);
            } else {
                ColumnString::check_chars_length(repeat * size + res_data.size(), 0);
                for (int j = 0; j < repeat; ++j) {
                    buffer.append(raw_str, raw_str + size);
                }
                StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i,
                                            res_data, res_offsets);
            }
        }
        return Status::OK();
    }

    // TODO: 1. use pmr::vector<char> replace fmt_buffer may speed up the code
    //       2. abstract the `vector_vector` and `vector_const`
    //       3. rethink we should use `DEFAULT_MAX_STRING_SIZE` to bigger here
    void vector_const(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                      int repeat, ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets,
                      ColumnUInt8::Container& null_map) const {
        size_t input_row_size = offsets.size();

        fmt::memory_buffer buffer;
        res_offsets.resize(input_row_size);
        null_map.resize_fill(input_row_size, 0);
        for (ssize_t i = 0; i < input_row_size; ++i) {
            buffer.clear();
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            size_t size = offsets[i] - offsets[i - 1];
            ColumnString::check_chars_length(repeat * size + res_data.size(), 0);

            for (int j = 0; j < repeat; ++j) {
                buffer.append(raw_str, raw_str + size);
            }
            StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i, res_data,
                                        res_offsets);
        }
    }
};

/// PaddingChars pre-processes the pad string for efficient padding.
/// When is_utf8=false, character count equals byte count — no UTF-8 decoding needed.
/// When is_utf8=true, we build a byte-offset table for code points.
/// In both cases, the pad string is pre-expanded (doubled) until it has >= 16 characters,
/// so that each memcpy in append_to copies at least 16 bytes at a time.
template <bool is_utf8>
struct PaddingChars {
    std::string pad_string;
    /// utf8_byte_offsets[i] = byte offset of i-th code point in pad_string.
    /// utf8_byte_offsets has (num_chars + 1) entries, with [0]=0 and [num_chars]=pad_string.size().
    std::vector<size_t> utf8_byte_offsets;

    explicit PaddingChars(const uint8_t* data, size_t len)
            : pad_string(reinterpret_cast<const char*>(data), len) {
        init();
    }

    size_t num_chars() const {
        if constexpr (is_utf8) {
            return utf8_byte_offsets.size() - 1;
        } else {
            return pad_string.size();
        }
    }

    size_t chars_to_bytes(size_t n) const {
        if constexpr (is_utf8) {
            return utf8_byte_offsets[n];
        } else {
            return n;
        }
    }

    /// Append `num_chars_to_pad` padding characters to dst, return bytes written.
    size_t append_to(uint8_t* dst, size_t num_chars_to_pad) const {
        if (num_chars_to_pad == 0) {
            return 0;
        }
        const auto* src = reinterpret_cast<const uint8_t*>(pad_string.data());
        const size_t step = num_chars();
        uint8_t* dst_start = dst;
        while (num_chars_to_pad > step) {
            size_t bytes = chars_to_bytes(step);
            memcpy(dst, src, bytes);
            dst += bytes;
            num_chars_to_pad -= step;
        }
        size_t bytes = chars_to_bytes(num_chars_to_pad);
        memcpy(dst, src, bytes);
        dst += bytes;
        return dst - dst_start;
    }

private:
    void init() {
        if (pad_string.empty()) {
            return;
        }

        if constexpr (is_utf8) {
            // Build byte-offset table for each code point.
            size_t offset = 0;
            utf8_byte_offsets.reserve(pad_string.size() + 1);
            while (offset < pad_string.size()) {
                utf8_byte_offsets.push_back(offset);
                offset += get_utf8_byte_length(static_cast<uint8_t>(pad_string[offset]));
                offset = std::min(offset, pad_string.size());
            }
            utf8_byte_offsets.push_back(pad_string.size());
        }

        // Pre-expand pad_string until it has >= 16 characters.
        // This ensures append_to() copies at least 16 bytes per iteration.
        while (num_chars() < 16) {
            if constexpr (is_utf8) {
                size_t old_count = utf8_byte_offsets.size();
                size_t base = utf8_byte_offsets.back();
                for (size_t i = 1; i < old_count; ++i) {
                    utf8_byte_offsets.push_back(utf8_byte_offsets[i] + base);
                }
            }
            pad_string += pad_string;
        }
    }
};

template <typename Impl>
class FunctionStringPad : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionStringPad>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_GE(arguments.size(), 3);
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res = ColumnString::create();

        ColumnPtr col[3];
        bool col_const[3];
        for (size_t i = 0; i < 3; ++i) {
            std::tie(col[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
        }
        auto& null_map_data = null_map->get_data();
        auto& res_offsets = res->get_offsets();
        auto& res_chars = res->get_chars();
        res_offsets.resize(input_rows_count);

        const auto* strcol = assert_cast<const ColumnString*>(col[0].get());
        const auto* col_len = assert_cast<const ColumnInt32*>(col[1].get());
        const auto& col_len_data = col_len->get_data();

        const auto* padcol = assert_cast<const ColumnString*>(col[2].get());

        if (col_const[1] && col_const[2]) {
            auto pad = padcol->get_data_at(0);
            const bool pad_all_ascii =
                    simd::VStringFunctions::is_ascii({pad.data, static_cast<size_t>(pad.size)});
            const bool all_ascii = pad_all_ascii && strcol->is_ascii();
            std::visit(
                    [&](auto str_const) {
                        if (all_ascii) {
                            execute_const_len_const_pad<true, str_const>(
                                    *strcol, col_len_data, *padcol, res_offsets, res_chars,
                                    null_map_data, input_rows_count);
                        } else {
                            execute_const_len_const_pad<false, str_const>(
                                    *strcol, col_len_data, *padcol, res_offsets, res_chars,
                                    null_map_data, input_rows_count);
                        }
                    },
                    make_bool_variant(col_const[0]));
        } else {
            std::visit(
                    [&](auto str_const) {
                        execute_general<str_const>(*strcol, col_len_data, col_const[1], *padcol,
                                                   col_const[2], res_offsets, res_chars,
                                                   null_map_data, input_rows_count);
                    },
                    make_bool_variant(col_const[0]));
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
    }

private:
    template <bool is_utf8>
    static size_t get_char_length(const uint8_t* str_data, size_t str_byte_len) {
        if constexpr (is_utf8) {
            return simd::VStringFunctions::get_char_len(reinterpret_cast<const char*>(str_data),
                                                        str_byte_len);
        }
        return str_byte_len;
    }

    template <bool is_utf8>
    static size_t get_truncated_byte_length(const uint8_t* str_data, size_t str_byte_len,
                                            size_t str_char_len, size_t target_len) {
        if constexpr (!is_utf8) {
            return target_len;
        }
        if (str_char_len == target_len) {
            return str_byte_len;
        }
        auto [byte_len, _] = simd::VStringFunctions::iterate_utf8_with_limit_length(
                reinterpret_cast<const char*>(str_data),
                reinterpret_cast<const char*>(str_data) + str_byte_len, target_len);
        return byte_len;
    }

    static void ensure_capacity(ColumnString::Chars& res_chars, size_t needed, size_t row) {
        if (needed <= res_chars.size()) {
            return;
        }
        ColumnString::check_chars_length(needed, row);
        res_chars.resize(std::max(needed, res_chars.size() * 3 / 2));
    }

    template <bool is_utf8>
    static size_t estimate_const_output_bytes(const ColumnString::Chars& strcol_chars,
                                              int target_len, size_t input_rows_count,
                                              const PaddingChars<is_utf8>* padding) {
        if (target_len <= 0) {
            return 0;
        }
        if constexpr (!is_utf8) {
            return static_cast<size_t>(target_len) * input_rows_count;
        }
        if (padding != nullptr && padding->num_chars() > 0) {
            size_t pad_bytes_per_char =
                    (padding->pad_string.size() + padding->num_chars() - 1) / padding->num_chars();
            return strcol_chars.size() +
                   static_cast<size_t>(target_len) * pad_bytes_per_char * input_rows_count;
        }
        return strcol_chars.size();
    }

    template <bool is_utf8>
    static void append_result_row(const uint8_t* str_data, size_t str_byte_len, int target_len,
                                  const PaddingChars<is_utf8>* padding,
                                  ColumnString::Chars& res_chars,
                                  ColumnString::Offsets& res_offsets,
                                  ColumnUInt8::Container& null_map_data, size_t row,
                                  size_t& dst_offset) {
        if (target_len < 0) {
            null_map_data[row] = true;
            res_offsets[row] = dst_offset;
            return;
        }

        const size_t str_char_len = get_char_length<is_utf8>(str_data, str_byte_len);
        const size_t target_char_len = static_cast<size_t>(target_len);
        if (str_char_len >= target_char_len) {
            const size_t truncated_byte_len = get_truncated_byte_length<is_utf8>(
                    str_data, str_byte_len, str_char_len, target_char_len);
            const size_t needed = dst_offset + truncated_byte_len;
            ensure_capacity(res_chars, needed, row);
            memcpy(res_chars.data() + dst_offset, str_data, truncated_byte_len);
            dst_offset += truncated_byte_len;
            res_offsets[row] = dst_offset;
            return;
        }

        if (padding == nullptr || padding->num_chars() == 0) {
            res_offsets[row] = dst_offset;
            return;
        }

        const size_t pad_char_count = target_char_len - str_char_len;
        const size_t full_cycles = pad_char_count / padding->num_chars();
        const size_t remainder_chars = pad_char_count % padding->num_chars();
        const size_t pad_bytes =
                full_cycles * padding->pad_string.size() + padding->chars_to_bytes(remainder_chars);
        const size_t needed = dst_offset + str_byte_len + pad_bytes;
        ensure_capacity(res_chars, needed, row);

        if constexpr (Impl::is_lpad) {
            dst_offset += padding->append_to(res_chars.data() + dst_offset, pad_char_count);
            memcpy(res_chars.data() + dst_offset, str_data, str_byte_len);
            dst_offset += str_byte_len;
        } else {
            memcpy(res_chars.data() + dst_offset, str_data, str_byte_len);
            dst_offset += str_byte_len;
            dst_offset += padding->append_to(res_chars.data() + dst_offset, pad_char_count);
        }
        res_offsets[row] = dst_offset;
    }

    template <bool all_ascii, bool str_const>
    static void execute_const_len_const_pad(const ColumnString& strcol,
                                            const ColumnInt32::Container& col_len_data,
                                            const ColumnString& padcol,
                                            ColumnString::Offsets& res_offsets,
                                            ColumnString::Chars& res_chars,
                                            ColumnUInt8::Container& null_map_data,
                                            size_t input_rows_count) {
        constexpr bool is_utf8 = !all_ascii;
        using PadChars = PaddingChars<is_utf8>;

        const int target_len = col_len_data[0];
        std::optional<PadChars> padding;
        const auto pad = padcol.get_data_at(0);
        if (!pad.empty()) {
            padding.emplace(reinterpret_cast<const uint8_t*>(pad.data), pad.size);
        }

        const PadChars* padding_ptr = padding ? &*padding : nullptr;
        const size_t estimated_total = estimate_const_output_bytes<is_utf8>(
                strcol.get_chars(), target_len, input_rows_count, padding_ptr);
        if (estimated_total > 0) {
            ColumnString::check_chars_length(estimated_total, 0, input_rows_count);
        }
        res_chars.resize(estimated_total);

        size_t dst_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto str = strcol.get_data_at(index_check_const<str_const>(i));
            append_result_row<is_utf8>(reinterpret_cast<const uint8_t*>(str.data), str.size,
                                       target_len, padding_ptr, res_chars, res_offsets,
                                       null_map_data, i, dst_offset);
        }
        res_chars.resize(dst_offset);
    }

    template <bool str_const>
    static void execute_general(const ColumnString& strcol,
                                const ColumnInt32::Container& col_len_data, bool len_const,
                                const ColumnString& padcol, bool pad_const,
                                ColumnString::Offsets& res_offsets, ColumnString::Chars& res_chars,
                                ColumnUInt8::Container& null_map_data, size_t input_rows_count) {
        using PadChars = PaddingChars<true>;
        std::optional<PadChars> const_padding;
        const PadChars* const_padding_ptr = nullptr;
        if (pad_const) {
            auto pad = padcol.get_data_at(0);
            if (!pad.empty()) {
                const_padding.emplace(reinterpret_cast<const uint8_t*>(pad.data), pad.size);
                const_padding_ptr = &*const_padding;
            }
        }

        res_chars.resize(strcol.get_chars().size());
        size_t dst_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto str = strcol.get_data_at(index_check_const<str_const>(i));
            const int target_len = col_len_data[len_const ? 0 : i];

            const PadChars* padding_ptr = const_padding_ptr;
            std::optional<PadChars> row_padding;
            if (!pad_const) {
                auto pad = padcol.get_data_at(i);
                if (!pad.empty()) {
                    row_padding.emplace(reinterpret_cast<const uint8_t*>(pad.data), pad.size);
                    padding_ptr = &*row_padding;
                } else {
                    padding_ptr = nullptr;
                }
            }

            append_result_row<true>(reinterpret_cast<const uint8_t*>(str.data), str.size,
                                    target_len, padding_ptr, res_chars, res_offsets, null_map_data,
                                    i, dst_offset);
        }
        res_chars.resize(dst_offset);
    }
};

#include "common/compile_check_avoid_end.h"
} // namespace doris
