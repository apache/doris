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

#include <cstdlib>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "CLucene/util/stringUtil.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/jsonb_value.h"
#include "runtime/primitive_type.h"
#include "udf/udf.h"
#include "util/jsonb_document.h"
#include "util/jsonb_parser_simd.h"
#include "util/jsonb_stream.h"
#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/custom_allocator.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/like.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/stringop_substring.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

enum class NullalbeMode { NULLABLE = 0, FOLLOW_INPUT };

enum class JsonbParseErrorMode { FAIL = 0, RETURN_NULL, RETURN_VALUE };

// func(string,string) -> json
template <NullalbeMode nullable_mode, JsonbParseErrorMode parse_error_handle_mode>
class FunctionJsonbParseBase : public IFunction {
private:
    struct FunctionJsonbParseState {
        StringRef default_value;
        JsonBinaryValue default_value_parser;
        bool has_const_default_value = false;
        bool default_is_null = false;
    };

public:
    static constexpr auto name = "json_parse";
    static constexpr auto alias = "jsonb_parse";
    static FunctionPtr create() { return std::make_shared<FunctionJsonbParseBase>(); }

    String get_name() const override {
        String error_mode;
        switch (parse_error_handle_mode) {
        case JsonbParseErrorMode::FAIL:
            break;
        case JsonbParseErrorMode::RETURN_NULL:
            error_mode = "_error_to_null";
            break;
        case JsonbParseErrorMode::RETURN_VALUE:
            error_mode = "_error_to_value";
            break;
        }

        return name + error_mode;
    }

    bool is_variadic() const override {
        return parse_error_handle_mode == JsonbParseErrorMode::RETURN_VALUE;
    }

    size_t get_number_of_arguments() const override {
        switch (parse_error_handle_mode) {
        case JsonbParseErrorMode::FAIL:
            return 1;
        case JsonbParseErrorMode::RETURN_NULL:
            return 1;
        case JsonbParseErrorMode::RETURN_VALUE:
            return 0;
        }
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        bool is_nullable = false;
        switch (nullable_mode) {
        case NullalbeMode::NULLABLE:
            is_nullable = true;
            break;
        case NullalbeMode::FOLLOW_INPUT: {
            for (auto arg : arguments) {
                is_nullable |= arg->is_nullable();
            }
            break;
        }
        }

        return is_nullable ? make_nullable(std::make_shared<DataTypeJsonb>())
                           : std::make_shared<DataTypeJsonb>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::FunctionStateScope::FRAGMENT_LOCAL) {
            std::shared_ptr<FunctionJsonbParseState> state =
                    std::make_shared<FunctionJsonbParseState>();
            context->set_function_state(FunctionContext::FRAGMENT_LOCAL, state);
        }
        if constexpr (parse_error_handle_mode == JsonbParseErrorMode::RETURN_VALUE) {
            if (scope == FunctionContext::FunctionStateScope::FRAGMENT_LOCAL) {
                auto* state = reinterpret_cast<FunctionJsonbParseState*>(
                        context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
                if (state) {
                    if (context->get_num_args() == 2) {
                        if (context->is_col_constant(1)) {
                            const auto default_value_col = context->get_constant_col(1)->column_ptr;
                            if (default_value_col->is_null_at(0)) {
                                state->default_is_null = true;
                            } else {
                                const auto& default_value = default_value_col->get_data_at(0);

                                state->default_value = default_value;
                                state->has_const_default_value = true;
                            }
                        }
                    } else if (context->get_num_args() == 1) {
                        RETURN_IF_ERROR(
                                state->default_value_parser.from_json_string(std::string("{}")));
                        state->default_value = StringRef(state->default_value_parser.value(),
                                                         state->default_value_parser.size());
                        state->has_const_default_value = true;
                    }
                }
            }

            if (context->get_num_args() != 1 && context->get_num_args() != 2) {
                return Status::InvalidArgument(
                        "jsonb_parse_error_to_value function should have 1 or 2 arguments, "
                        "but got {}",
                        context->get_num_args());
            }
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto&& [col_from, col_from_is_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);

        if (col_from_is_const) {
            if (col_from->is_null_at(0)) {
                auto col_str = ColumnString::create();
                col_str->insert_default();
                auto null_map = ColumnUInt8::create(1, 1);
                auto nullable_col = ColumnNullable::create(std::move(col_str), std::move(null_map));
                if (input_rows_count > 1) {
                    block.get_by_position(result).column =
                            ColumnConst::create(std::move(nullable_col), input_rows_count);
                } else {
                    block.get_by_position(result).column = std::move(nullable_col);
                }
            }
            return Status::OK();
        }

        auto null_map = ColumnUInt8::create(0, 0);
        bool is_nullable = false;

        switch (nullable_mode) {
        case NullalbeMode::NULLABLE: {
            is_nullable = true;
            break;
        }
        case NullalbeMode::FOLLOW_INPUT: {
            for (auto arg : arguments) {
                is_nullable |= block.get_by_position(arg).type->is_nullable();
            }
            break;
        }
        }

        if (is_nullable) {
            null_map = ColumnUInt8::create(input_rows_count, 0);
        }

        const ColumnString* col_from_string = nullptr;
        if (col_from->is_nullable()) {
            const auto& nullable_col = assert_cast<const ColumnNullable&>(*col_from);

            VectorizedUtils::update_null_map(null_map->get_data(),
                                             nullable_col.get_null_map_data());
            col_from_string =
                    assert_cast<const ColumnString*>(nullable_col.get_nested_column_ptr().get());
        } else {
            col_from_string = assert_cast<const ColumnString*>(col_from.get());
        }

        StringRef constant_default_value;
        bool default_value_const = false;
        bool default_value_null_const = false;
        ColumnPtr default_value_col;
        JsonBinaryValue default_jsonb_value_parser;
        const ColumnString* default_value_str_col = nullptr;
        const NullMap* default_value_nullmap = nullptr;
        if constexpr (parse_error_handle_mode == JsonbParseErrorMode::RETURN_VALUE) {
            auto* state = reinterpret_cast<FunctionJsonbParseState*>(
                    context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
            if (state && state->has_const_default_value) {
                constant_default_value = state->default_value;
                default_value_null_const = state->default_is_null;
                default_value_const = true;
            } else if (arguments.size() > 1) {
                if (block.get_by_position(arguments[1]).type->get_primitive_type() !=
                    PrimitiveType::TYPE_JSONB) {
                    return Status::InvalidArgument(
                            "jsonb_parse second argument should be jsonb type, but got {}",
                            block.get_by_position(arguments[1]).type->get_name());
                }
                std::tie(default_value_col, default_value_const) =
                        unpack_if_const(block.get_by_position(arguments[1]).column);
                if (default_value_const) {
                    JsonbDocument* default_value_doc = nullptr;
                    if (default_value_col->is_null_at(0)) {
                        default_value_null_const = true;
                    } else {
                        auto data = default_value_col->get_data_at(0);
                        RETURN_IF_ERROR(JsonbDocument::checkAndCreateDocument(data.data, data.size,
                                                                              &default_value_doc));
                        constant_default_value = data;
                    }
                } else {
                    if (default_value_col->is_nullable()) {
                        const auto& nullable_col =
                                assert_cast<const ColumnNullable&>(*default_value_col);
                        default_value_str_col = assert_cast<const ColumnString*>(
                                nullable_col.get_nested_column_ptr().get());
                        default_value_nullmap = &(nullable_col.get_null_map_data());
                    } else {
                        default_value_str_col =
                                assert_cast<const ColumnString*>(default_value_col.get());
                    }
                }
            } else if (arguments.size() == 1) {
                auto st = default_jsonb_value_parser.from_json_string(std::string("{}"));
                if (!st.ok()) {
                    return Status::InvalidArgument(
                            "jsonb_parse_error_to_value parse default value failed: {}",
                            st.to_string());
                }
                default_value_const = true;
                constant_default_value.data = default_jsonb_value_parser.value();
                constant_default_value.size = default_jsonb_value_parser.size();
            }
        }

        auto col_to = ColumnString::create();

        col_to->reserve(input_rows_count);

        auto& null_map_data = null_map->get_data();

        // parser can be reused for performance
        JsonBinaryValue jsonb_value;

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (is_nullable && null_map_data[i]) {
                col_to->insert_default();
                continue;
            }

            const auto& val = col_from_string->get_data_at(i);
            auto st = jsonb_value.from_json_string(val.data, val.size);
            if (st.ok()) {
                // insert jsonb format data
                col_to->insert_data(jsonb_value.value(), jsonb_value.size());
            } else {
                if constexpr (parse_error_handle_mode == JsonbParseErrorMode::FAIL) {
                    return Status::InvalidArgument(
                            "Parse json document failed at row {}, error: {}", i, st.to_string());
                } else if constexpr (parse_error_handle_mode == JsonbParseErrorMode::RETURN_NULL) {
                    null_map_data[i] = 1;
                    col_to->insert_default();
                } else {
                    if (default_value_const) {
                        if (default_value_null_const) {
                            null_map_data[i] = 1;
                            col_to->insert_default();
                        } else {
                            col_to->insert_data(constant_default_value.data,
                                                constant_default_value.size);
                        }
                    } else {
                        if (default_value_nullmap && (*default_value_nullmap)[i]) {
                            null_map_data[i] = 1;
                            col_to->insert_default();
                            continue;
                        }
                        auto value = default_value_str_col->get_data_at(i);
                        col_to->insert_data(value.data, value.size);
                    }
                }
            }
        }

        if (is_nullable) {
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_to), std::move(null_map)));
        } else {
            block.replace_by_position(result, std::move(col_to));
        }

        return Status::OK();
    }
};

// jsonb_parse return type nullable as input
using FunctionJsonbParse =
        FunctionJsonbParseBase<NullalbeMode::FOLLOW_INPUT, JsonbParseErrorMode::FAIL>;
using FunctionJsonbParseErrorNull =
        FunctionJsonbParseBase<NullalbeMode::NULLABLE, JsonbParseErrorMode::RETURN_NULL>;
using FunctionJsonbParseErrorValue =
        FunctionJsonbParseBase<NullalbeMode::FOLLOW_INPUT, JsonbParseErrorMode::RETURN_VALUE>;

// func(jsonb, [varchar, varchar, ...]) -> nullable(type)
template <typename Impl>
class FunctionJsonbExtract : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static constexpr auto alias = Impl::alias;
    static FunctionPtr create() { return std::make_shared<FunctionJsonbExtract>(); }
    String get_name() const override { return name; }
    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }
    bool use_default_implementation_for_nulls() const override { return false; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<typename Impl::ReturnType>());
    }
    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (vectorized::HasGetVariadicArgumentTypesImpl<Impl>) {
            return Impl::get_variadic_argument_types_impl();
        } else {
            return {};
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_GE(arguments.size(), 2);

        ColumnPtr jsonb_data_column;
        bool jsonb_data_const = false;
        const NullMap* data_null_map = nullptr;

        if (block.get_by_position(arguments[0]).type->get_primitive_type() !=
            PrimitiveType::TYPE_JSONB) {
            return Status::InvalidArgument(
                    "jsonb_extract first argument should be json type, but got {}",
                    block.get_by_position(arguments[0]).type->get_name());
        }

        // prepare jsonb data column
        std::tie(jsonb_data_column, jsonb_data_const) =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        if (jsonb_data_column->is_nullable()) {
            const auto& nullable_column = assert_cast<const ColumnNullable&>(*jsonb_data_column);
            jsonb_data_column = nullable_column.get_nested_column_ptr();
            data_null_map = &nullable_column.get_null_map_data();
        }
        const auto& ldata = assert_cast<const ColumnString*>(jsonb_data_column.get())->get_chars();
        const auto& loffsets =
                assert_cast<const ColumnString*>(jsonb_data_column.get())->get_offsets();

        // prepare parse path column prepare
        std::vector<const ColumnString*> jsonb_path_columns;
        std::vector<bool> path_const(arguments.size() - 1);
        std::vector<const NullMap*> path_null_maps(arguments.size() - 1, nullptr);
        for (int i = 0; i < arguments.size() - 1; ++i) {
            ColumnPtr path_column;
            bool is_const = false;
            std::tie(path_column, is_const) =
                    unpack_if_const(block.get_by_position(arguments[i + 1]).column);
            path_const[i] = is_const;
            if (path_column->is_nullable()) {
                const auto& nullable_column = assert_cast<const ColumnNullable&>(*path_column);
                path_column = nullable_column.get_nested_column_ptr();
                path_null_maps[i] = &nullable_column.get_null_map_data();
            }
            jsonb_path_columns.push_back(assert_cast<const ColumnString*>(path_column.get()));
        }

        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res = Impl::ColumnType::create();

        // execute Impl
        if constexpr (std::is_same_v<typename Impl::ReturnType, DataTypeString> ||
                      std::is_same_v<typename Impl::ReturnType, DataTypeJsonb>) {
            auto& res_data = res->get_chars();
            auto& res_offsets = res->get_offsets();
            RETURN_IF_ERROR(Impl::vector_vector_v2(
                    context, ldata, loffsets, data_null_map, jsonb_data_const, jsonb_path_columns,
                    path_null_maps, path_const, res_data, res_offsets, null_map->get_data()));
        } else {
            // not support other extract type for now (e.g. int, double, ...)
            DCHECK_EQ(jsonb_path_columns.size(), 1);
            const auto& rdata = jsonb_path_columns[0]->get_chars();
            const auto& roffsets = jsonb_path_columns[0]->get_offsets();

            auto create_all_null_result = [&]() {
                res = Impl::ColumnType::create();
                res->insert_default();
                auto nullable_column =
                        ColumnNullable::create(std::move(res), ColumnUInt8::create(1, 1));
                auto const_column =
                        ColumnConst::create(std::move(nullable_column), input_rows_count);
                block.get_by_position(result).column = std::move(const_column);
                return Status::OK();
            };

            if (jsonb_data_const) {
                if (data_null_map && (*data_null_map)[0]) {
                    return create_all_null_result();
                }

                RETURN_IF_ERROR(Impl::scalar_vector(context, jsonb_data_column->get_data_at(0),
                                                    rdata, roffsets, path_null_maps[0],
                                                    res->get_data(), null_map->get_data()));
            } else if (path_const[0]) {
                if (path_null_maps[0] && (*path_null_maps[0])[0]) {
                    return create_all_null_result();
                }
                RETURN_IF_ERROR(Impl::vector_scalar(context, ldata, loffsets, data_null_map,
                                                    jsonb_path_columns[0]->get_data_at(0),
                                                    res->get_data(), null_map->get_data()));
            } else {
                RETURN_IF_ERROR(Impl::vector_vector(context, ldata, loffsets, data_null_map, rdata,
                                                    roffsets, path_null_maps[0], res->get_data(),
                                                    null_map->get_data()));
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
    }
};

class FunctionJsonbKeys : public IFunction {
public:
    static constexpr auto name = "json_keys";
    static constexpr auto alias = "jsonb_keys";
    static FunctionPtr create() { return std::make_shared<FunctionJsonbKeys>(); }
    String get_name() const override { return name; }
    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(
                std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>())));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_GE(arguments.size(), 1);
        if (arguments.size() != 1 && arguments.size() != 2) {
            // here has argument param error
            return Status::InvalidArgument("json_keys should have 1 or 2 arguments");
        }

        ColumnPtr jsonb_data_column = nullptr;
        const NullMap* data_null_map = nullptr;
        // prepare jsonb data column
        jsonb_data_column = unpack_if_const(block.get_by_position(arguments[0]).column).first;
        if (block.get_by_position(arguments[0]).column->is_nullable()) {
            const auto* nullable = check_and_get_column<ColumnNullable>(jsonb_data_column.get());
            jsonb_data_column = nullable->get_nested_column_ptr();
            data_null_map = &nullable->get_null_map_data();
        }
        const ColumnString* col_from_string =
                check_and_get_column<ColumnString>(jsonb_data_column.get());

        // prepare parse path column prepare, maybe we do not have path column
        ColumnPtr jsonb_path_column = nullptr;
        const ColumnString* jsonb_path_col = nullptr;
        bool path_const = false;
        const NullMap* path_null_map = nullptr;
        if (arguments.size() == 2) {
            // we have should have a ColumnString for path
            std::tie(jsonb_path_column, path_const) =
                    unpack_if_const(block.get_by_position(arguments[1]).column);
            if (block.get_by_position(arguments[1]).column->is_nullable()) {
                const auto* nullable =
                        check_and_get_column<ColumnNullable>(jsonb_path_column.get());
                jsonb_path_column = nullable->get_nested_column_ptr();
                path_null_map = &nullable->get_null_map_data();
            }
            jsonb_path_col = check_and_get_column<ColumnString>(jsonb_path_column.get());
        }

        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        NullMap& res_null_map = null_map->get_data();

        auto dst_arr = ColumnArray::create(
                ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()),
                ColumnArray::ColumnOffsets::create());
        ColumnNullable& dst_nested_column = assert_cast<ColumnNullable&>(dst_arr->get_data());

        Status st;
        if (jsonb_path_column) {
            if (path_const) {
                st = inner_loop_impl<true, true>(input_rows_count, *dst_arr, dst_nested_column,
                                                 res_null_map, *col_from_string, data_null_map,
                                                 jsonb_path_col, path_null_map);
            } else {
                st = inner_loop_impl<true, false>(input_rows_count, *dst_arr, dst_nested_column,
                                                  res_null_map, *col_from_string, data_null_map,
                                                  jsonb_path_col, path_null_map);
            }
        } else {
            st = inner_loop_impl<false, false>(input_rows_count, *dst_arr, dst_nested_column,
                                               res_null_map, *col_from_string, data_null_map,
                                               jsonb_path_col, path_null_map);
        }
        if (!st.ok()) {
            return st;
        }
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(dst_arr), std::move(null_map));
        return st;
    }

private:
    template <bool JSONB_PATH_PARAM, bool JSON_PATH_CONST>
    static ALWAYS_INLINE Status inner_loop_impl(size_t input_rows_count, ColumnArray& dst_arr,
                                                ColumnNullable& dst_nested_column,
                                                NullMap& res_null_map,
                                                const ColumnString& col_from_string,
                                                const NullMap* jsonb_data_nullmap,
                                                const ColumnString* jsonb_path_column,
                                                const NullMap* path_null_map) {
        // if path is const, we just need to parse it once
        JsonbPath const_path;
        if constexpr (JSONB_PATH_PARAM && JSON_PATH_CONST) {
            StringRef r_raw_ref = jsonb_path_column->get_data_at(0);
            if (!const_path.seek(r_raw_ref.data, r_raw_ref.size)) {
                return Status::InvalidArgument("Json path error: Invalid Json Path for value: {}",
                                               r_raw_ref.to_string());
            }

            if (const_path.is_wildcard()) {
                return Status::InvalidJsonPath(
                        "In this situation, path expressions may not contain the * and ** tokens "
                        "or an array range.");
            }
        }
        const auto& ldata = col_from_string.get_chars();
        const auto& loffsets = col_from_string.get_offsets();
        for (size_t i = 0; i < input_rows_count; ++i) {
            // if jsonb data is null or path column is null , we should return null
            if (jsonb_data_nullmap && (&jsonb_data_nullmap)[i]) {
                res_null_map[i] = 1;
                dst_arr.insert_default();
                continue;
            }
            if constexpr (JSONB_PATH_PARAM && !JSON_PATH_CONST) {
                if (path_null_map && (&path_null_map)[i]) {
                    res_null_map[i] = 1;
                    dst_arr.insert_default();
                    continue;
                }
            }
            // extract jsonb keys
            size_t l_off = loffsets[i - 1];
            size_t l_size = loffsets[i] - l_off;
            if (l_size == 0) {
                res_null_map[i] = 1;
                dst_arr.insert_default();
                continue;
            }
            const char* l_raw = reinterpret_cast<const char*>(&ldata[l_off]);
            JsonbDocument* doc = nullptr;
            auto st = JsonbDocument::checkAndCreateDocument(l_raw, l_size, &doc);
            if (!st.ok() || !doc || !doc->getValue()) [[unlikely]] {
                dst_arr.clear();
                return Status::InvalidArgument("jsonb data is invalid");
            }
            const JsonbValue* obj_val;
            JsonbFindResult find_result;
            if constexpr (JSONB_PATH_PARAM) {
                if constexpr (!JSON_PATH_CONST) {
                    const ColumnString::Chars& rdata = jsonb_path_column->get_chars();
                    const ColumnString::Offsets& roffsets = jsonb_path_column->get_offsets();
                    size_t r_off = roffsets[i - 1];
                    size_t r_size = roffsets[i] - r_off;
                    const char* r_raw = reinterpret_cast<const char*>(&rdata[r_off]);
                    JsonbPath path;
                    if (!path.seek(r_raw, r_size)) {
                        return Status::InvalidArgument(
                                "Json path error: Invalid Json Path for value: {}",
                                std::string_view(reinterpret_cast<const char*>(rdata.data()),
                                                 rdata.size()));
                    }

                    if (path.is_wildcard()) {
                        return Status::InvalidJsonPath(
                                "In this situation, path expressions may not contain the * and ** "
                                "tokens "
                                "or an array range.");
                    }
                    find_result = doc->getValue()->findValue(path);
                } else {
                    find_result = doc->getValue()->findValue(const_path);
                }
                obj_val = find_result.value;
            } else {
                obj_val = doc->getValue();
            }

            if (!obj_val || !obj_val->isObject()) {
                // if jsonb data is not object we should return null
                res_null_map[i] = 1;
                dst_arr.insert_default();
                continue;
            }
            const auto* obj = obj_val->unpack<ObjectVal>();
            for (const auto& it : *obj) {
                dst_nested_column.insert_data(it.getKeyStr(), it.klen());
            }
            dst_arr.get_offsets().push_back(dst_nested_column.size());
        } //for
        return Status::OK();
    }
};

class FunctionJsonbExtractPath : public IFunction {
public:
    static constexpr auto name = "json_exists_path";
    static constexpr auto alias = "jsonb_exists_path";
    using ColumnType = ColumnUInt8;
    using Container = typename ColumnType::Container;
    static FunctionPtr create() { return std::make_shared<FunctionJsonbExtractPath>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        // it only needs to indicate existence and does not need to return nullable values.
        return std::make_shared<DataTypeUInt8>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        ColumnPtr jsonb_data_column;
        bool jsonb_data_const = false;
        // prepare jsonb data column
        std::tie(jsonb_data_column, jsonb_data_const) =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& ldata = assert_cast<const ColumnString*>(jsonb_data_column.get())->get_chars();
        const auto& loffsets =
                assert_cast<const ColumnString*>(jsonb_data_column.get())->get_offsets();

        // prepare parse path column prepare
        ColumnPtr path_column;
        bool path_const = false;
        std::tie(path_column, path_const) =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        const auto* jsonb_path_column = assert_cast<const ColumnString*>(path_column.get());

        auto res = ColumnType::create();

        bool is_invalid_json_path = false;

        const auto& rdata = jsonb_path_column->get_chars();
        const auto& roffsets = jsonb_path_column->get_offsets();
        if (jsonb_data_const) {
            scalar_vector(context, jsonb_data_column->get_data_at(0), rdata, roffsets,
                          res->get_data(), is_invalid_json_path);
        } else if (path_const) {
            vector_scalar(context, ldata, loffsets, jsonb_path_column->get_data_at(0),
                          res->get_data(), is_invalid_json_path);
        } else {
            vector_vector(context, ldata, loffsets, rdata, roffsets, res->get_data(),
                          is_invalid_json_path);
        }
        if (is_invalid_json_path) {
            return Status::InvalidArgument(
                    "Json path error: Invalid Json Path for value: {}",
                    std::string_view(reinterpret_cast<const char*>(rdata.data()), rdata.size()));
        }

        block.get_by_position(result).column = std::move(res);
        return Status::OK();
    }

private:
    static ALWAYS_INLINE void inner_loop_impl(size_t i, Container& res, const char* l_raw_str,
                                              size_t l_str_size, JsonbPath& path) {
        // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
        JsonbDocument* doc = nullptr;
        auto st = JsonbDocument::checkAndCreateDocument(l_raw_str, l_str_size, &doc);
        if (!st.ok() || !doc || !doc->getValue()) [[unlikely]] {
            return;
        }

        // value is NOT necessary to be deleted since JsonbValue will not allocate memory
        auto result = doc->getValue()->findValue(path);

        if (result.value) {
            res[i] = 1;
        }
    }
    static void vector_vector(FunctionContext* context, const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, Container& res,
                              bool& is_invalid_json_path) {
        const size_t size = loffsets.size();
        res.resize_fill(size, 0);

        for (size_t i = 0; i < size; i++) {
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1];

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1];

            JsonbPath path;
            if (!path.seek(r_raw_str, r_str_size)) {
                is_invalid_json_path = true;
                return;
            }

            inner_loop_impl(i, res, l_raw_str, l_str_size, path);
        }
    }
    static void scalar_vector(FunctionContext* context, const StringRef& ldata,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, Container& res,
                              bool& is_invalid_json_path) {
        const size_t size = roffsets.size();
        res.resize_fill(size, 0);

        for (size_t i = 0; i < size; i++) {
            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1];

            JsonbPath path;
            if (!path.seek(r_raw_str, r_str_size)) {
                is_invalid_json_path = true;
                return;
            }

            inner_loop_impl(i, res, ldata.data, ldata.size, path);
        }
    }
    static void vector_scalar(FunctionContext* context, const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets, const StringRef& rdata,
                              Container& res, bool& is_invalid_json_path) {
        const size_t size = loffsets.size();
        res.resize_fill(size, 0);

        JsonbPath path;
        if (!path.seek(rdata.data, rdata.size)) {
            is_invalid_json_path = true;
            return;
        }

        for (size_t i = 0; i < size; i++) {
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1];

            inner_loop_impl(i, res, l_raw_str, l_str_size, path);
        }
    }
};

template <typename ValueType>
struct JsonbExtractStringImpl {
    using ReturnType = typename ValueType::ReturnType;
    using ColumnType = typename ValueType::ColumnType;
    static const bool only_check_exists = ValueType::only_check_exists;

private:
    static ALWAYS_INLINE void inner_loop_impl(size_t i, ColumnString::Chars& res_data,
                                              ColumnString::Offsets& res_offsets, NullMap& null_map,
                                              std::unique_ptr<JsonbToJson>& formater,
                                              const char* l_raw, size_t l_size, JsonbPath& path) {
        // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
        JsonbDocument* doc = nullptr;
        auto st = JsonbDocument::checkAndCreateDocument(l_raw, l_size, &doc);
        if (!st.ok() || !doc || !doc->getValue()) [[unlikely]] {
            StringOP::push_null_string(i, res_data, res_offsets, null_map);
            return;
        }

        // value is NOT necessary to be deleted since JsonbValue will not allocate memory
        auto find_result = doc->getValue()->findValue(path);

        if (UNLIKELY(!find_result.value)) {
            StringOP::push_null_string(i, res_data, res_offsets, null_map);
            return;
        }

        if constexpr (ValueType::only_get_type) {
            StringOP::push_value_string(std::string_view(find_result.value->typeName()), i,
                                        res_data, res_offsets);
            return;
        }

        if constexpr (std::is_same_v<DataTypeJsonb, ReturnType>) {
            JsonbWriter writer;

            if constexpr (ValueType::no_quotes) {
                if (find_result.value->isString()) {
                    const auto* str_value = find_result.value->unpack<JsonbStringVal>();
                    const auto* blob = str_value->getBlob();
                    if (str_value->length() > 1 && blob[0] == '"' &&
                        blob[str_value->length() - 1] == '"') {
                        writer.writeStartString();
                        writer.writeString(blob + 1, str_value->length() - 2);
                        writer.writeEndString();
                        StringOP::push_value_string(
                                std::string_view(writer.getOutput()->getBuffer(),
                                                 writer.getOutput()->getSize()),
                                i, res_data, res_offsets);
                        return;
                    }
                }
            }

            writer.writeValue(find_result.value);
            StringOP::push_value_string(std::string_view(writer.getOutput()->getBuffer(),
                                                         writer.getOutput()->getSize()),
                                        i, res_data, res_offsets);
        } else {
            if (LIKELY(find_result.value->isString())) {
                const auto* str_value = find_result.value->unpack<JsonbStringVal>();
                StringOP::push_value_string(
                        std::string_view(str_value->getBlob(), str_value->length()), i, res_data,
                        res_offsets);
            } else if (find_result.value->isNull()) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
            } else if (find_result.value->isTrue()) {
                StringOP::push_value_string("true", i, res_data, res_offsets);
            } else if (find_result.value->isFalse()) {
                StringOP::push_value_string("false", i, res_data, res_offsets);
            } else if (find_result.value->isInt8()) {
                StringOP::push_value_string(
                        std::to_string(find_result.value->unpack<JsonbInt8Val>()->val()), i,
                        res_data, res_offsets);
            } else if (find_result.value->isInt16()) {
                StringOP::push_value_string(
                        std::to_string(find_result.value->unpack<JsonbInt16Val>()->val()), i,
                        res_data, res_offsets);
            } else if (find_result.value->isInt32()) {
                StringOP::push_value_string(
                        std::to_string(find_result.value->unpack<JsonbInt32Val>()->val()), i,
                        res_data, res_offsets);
            } else if (find_result.value->isInt64()) {
                StringOP::push_value_string(
                        std::to_string(find_result.value->unpack<JsonbInt64Val>()->val()), i,
                        res_data, res_offsets);
            } else {
                if (!formater) {
                    formater.reset(new JsonbToJson());
                }
                StringOP::push_value_string(formater->to_json_string(find_result.value), i,
                                            res_data, res_offsets);
            }
        }
    }

public:
    // for jsonb_extract_string
    static Status vector_vector_v2(
            FunctionContext* context, const ColumnString::Chars& ldata,
            const ColumnString::Offsets& loffsets, const NullMap* l_null_map,
            const bool& json_data_const,
            const std::vector<const ColumnString*>& rdata_columns, // here we can support more paths
            const std::vector<const NullMap*>& r_null_maps, const std::vector<bool>& path_const,
            ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets, NullMap& null_map) {
        const size_t input_rows_count = null_map.size();
        res_offsets.resize(input_rows_count);

        auto writer = std::make_unique<JsonbWriter>();
        std::unique_ptr<JsonbToJson> formater;

        // reuseable json path list, espacially for const path
        std::vector<JsonbPath> json_path_list;
        json_path_list.resize(rdata_columns.size());

        // lambda function to parse json path for row i and path pi
        auto parse_json_path = [&](size_t i, size_t pi) -> Status {
            const auto index = index_check_const(i, path_const[pi]);

            const ColumnString* path_col = rdata_columns[pi];
            const ColumnString::Chars& rdata = path_col->get_chars();
            const ColumnString::Offsets& roffsets = path_col->get_offsets();
            size_t r_off = roffsets[index - 1];
            size_t r_size = roffsets[index] - r_off;
            const char* r_raw = reinterpret_cast<const char*>(&rdata[r_off]);

            JsonbPath path;
            if (!path.seek(r_raw, r_size)) {
                return Status::InvalidArgument("Json path error: Invalid Json Path for value: {}",
                                               std::string_view(r_raw, r_size));
            }

            json_path_list[pi] = std::move(path);

            return Status::OK();
        };

        for (size_t pi = 0; pi < rdata_columns.size(); pi++) {
            if (path_const[pi]) {
                if (r_null_maps[pi] && (*r_null_maps[pi])[0]) {
                    continue;
                }
                RETURN_IF_ERROR(parse_json_path(0, pi));
            }
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                continue;
            }

            const auto data_index = index_check_const(i, json_data_const);
            if (l_null_map && (*l_null_map)[data_index]) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                continue;
            }

            size_t l_off = loffsets[data_index - 1];
            size_t l_size = loffsets[data_index] - l_off;
            const char* l_raw = reinterpret_cast<const char*>(&ldata[l_off]);
            if (rdata_columns.size() == 1) { // just return origin value
                const auto path_index = index_check_const(i, path_const[0]);
                if (r_null_maps[0] && (*r_null_maps[0])[path_index]) {
                    StringOP::push_null_string(i, res_data, res_offsets, null_map);
                    continue;
                }

                if (!path_const[0]) {
                    RETURN_IF_ERROR(parse_json_path(i, 0));
                }
                inner_loop_impl(i, res_data, res_offsets, null_map, formater, l_raw, l_size,
                                json_path_list[0]);
            } else { // will make array string to user
                writer->reset();
                bool has_value = false;

                // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
                JsonbDocument* doc = nullptr;
                auto st = JsonbDocument::checkAndCreateDocument(l_raw, l_size, &doc);

                for (size_t pi = 0; pi < rdata_columns.size(); ++pi) {
                    if (!st.ok() || !doc || !doc->getValue()) [[unlikely]] {
                        continue;
                    }

                    const auto path_index = index_check_const(i, path_const[pi]);
                    if (r_null_maps[pi] && (*r_null_maps[pi])[path_index]) {
                        StringOP::push_null_string(i, res_data, res_offsets, null_map);
                        break;
                    }

                    if (!path_const[pi]) {
                        RETURN_IF_ERROR(parse_json_path(i, pi));
                    }

                    auto find_result = doc->getValue()->findValue(json_path_list[pi]);

                    if (find_result.value) {
                        if (!has_value) {
                            has_value = true;
                            writer->writeStartArray();
                        }
                        if (find_result.value->isArray() && find_result.is_wildcard) {
                            // To avoid getting results of nested array like [[1, 2, 3], [4, 5, 6]],
                            // if value is array, we should write all items in array, instead of write the array itself.
                            // finaly we will get results like [1, 2, 3, 4, 5, 6]
                            for (const auto& item : *find_result.value->unpack<ArrayVal>()) {
                                writer->writeValue(&item);
                            }
                        } else {
                            writer->writeValue(find_result.value);
                        }
                    }
                }
                if (has_value) {
                    writer->writeEndArray();
                    StringOP::push_value_string(std::string_view(writer->getOutput()->getBuffer(),
                                                                 writer->getOutput()->getSize()),
                                                i, res_data, res_offsets);
                } else {
                    StringOP::push_null_string(i, res_data, res_offsets, null_map);
                }
            }
        } //for
        return Status::OK();
    }

    static Status vector_vector(FunctionContext* context, const ColumnString::Chars& ldata,
                                const ColumnString::Offsets& loffsets, const NullMap* l_null_map,
                                const ColumnString::Chars& rdata,
                                const ColumnString::Offsets& roffsets, const NullMap* r_null_map,
                                ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets,
                                NullMap& null_map) {
        size_t input_rows_count = loffsets.size();
        res_offsets.resize(input_rows_count);

        std::unique_ptr<JsonbToJson> formater;

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (l_null_map && (*l_null_map)[i]) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                continue;
            }

            if (r_null_map && (*r_null_map)[i]) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                continue;
            }

            int l_size = loffsets[i] - loffsets[i - 1];
            const char* l_raw = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);

            int r_size = roffsets[i] - roffsets[i - 1];
            const char* r_raw = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);

            JsonbPath path;
            if (!path.seek(r_raw, r_size)) {
                return Status::InvalidArgument(
                        "Json path error: Invalid Json Path for value: {} at row: {}",
                        std::string_view(r_raw, r_size), i);
            }

            inner_loop_impl(i, res_data, res_offsets, null_map, formater, l_raw, l_size, path);
        } //for
        return Status::OK();
    } //function

    static Status vector_scalar(FunctionContext* context, const ColumnString::Chars& ldata,
                                const ColumnString::Offsets& loffsets, const NullMap* l_null_map,
                                const StringRef& rdata, ColumnString::Chars& res_data,
                                ColumnString::Offsets& res_offsets, NullMap& null_map) {
        size_t input_rows_count = loffsets.size();
        res_offsets.resize(input_rows_count);

        std::unique_ptr<JsonbToJson> formater;

        JsonbPath path;
        if (!path.seek(rdata.data, rdata.size)) {
            return Status::InvalidArgument("Json path error: Invalid Json Path for value: {}",
                                           std::string_view(rdata.data, rdata.size));
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (l_null_map && (*l_null_map)[i]) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                continue;
            }

            int l_size = loffsets[i] - loffsets[i - 1];
            const char* l_raw = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);

            inner_loop_impl(i, res_data, res_offsets, null_map, formater, l_raw, l_size, path);
        } //for
        return Status::OK();
    } //function

    static Status scalar_vector(FunctionContext* context, const StringRef& ldata,
                                const ColumnString::Chars& rdata,
                                const ColumnString::Offsets& roffsets, const NullMap* r_null_map,
                                ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets,
                                NullMap& null_map) {
        size_t input_rows_count = roffsets.size();
        res_offsets.resize(input_rows_count);

        std::unique_ptr<JsonbToJson> formater;

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (r_null_map && (*r_null_map)[i]) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                continue;
            }

            int r_size = roffsets[i] - roffsets[i - 1];
            const char* r_raw = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);

            JsonbPath path;
            if (!path.seek(r_raw, r_size)) {
                return Status::InvalidArgument(
                        "Json path error: Invalid Json Path for value: {} at row: {}",
                        std::string_view(r_raw, r_size), i);
            }

            inner_loop_impl(i, res_data, res_offsets, null_map, formater, ldata.data, ldata.size,
                            path);
        } //for
        return Status::OK();
    } //function
};

struct JsonbExtractIsnull {
    static constexpr auto name = "json_extract_isnull";
    static constexpr auto alias = "jsonb_extract_isnull";

    using ReturnType = DataTypeUInt8;
    using ColumnType = ColumnUInt8;
    using Container = typename ColumnType::Container;

private:
    static ALWAYS_INLINE void inner_loop_impl(size_t i, Container& res, NullMap& null_map,
                                              const char* l_raw_str, size_t l_str_size,
                                              JsonbPath& path) {
        if (null_map[i]) {
            res[i] = 0;
            return;
        }

        // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
        JsonbDocument* doc = nullptr;
        auto st = JsonbDocument::checkAndCreateDocument(l_raw_str, l_str_size, &doc);
        if (!st.ok() || !doc || !doc->getValue()) [[unlikely]] {
            null_map[i] = 1;
            res[i] = 0;
            return;
        }

        // value is NOT necessary to be deleted since JsonbValue will not allocate memory
        auto find_result = doc->getValue()->findValue(path);
        const auto* value = find_result.value;

        if (UNLIKELY(!value)) {
            null_map[i] = 1;
            res[i] = 0;
            return;
        }

        res[i] = value->isNull();
    }

public:
    // for jsonb_extract_int/int64/double
    static Status vector_vector(FunctionContext* context, const ColumnString::Chars& ldata,
                                const ColumnString::Offsets& loffsets, const NullMap* l_null_map,
                                const ColumnString::Chars& rdata,
                                const ColumnString::Offsets& roffsets, const NullMap* r_null_map,
                                Container& res, NullMap& null_map) {
        size_t size = loffsets.size();
        res.resize(size);

        for (size_t i = 0; i < loffsets.size(); i++) {
            if ((l_null_map && (*l_null_map)[i]) || (r_null_map && (*r_null_map)[i])) {
                res[i] = 0;
                null_map[i] = 1;
                continue;
            }

            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1];

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1];

            JsonbPath path;
            if (!path.seek(r_raw_str, r_str_size)) {
                return Status::InvalidArgument(
                        "Json path error: Invalid Json Path for value: {} at row: {}",
                        std::string_view(r_raw_str, r_str_size), i);
            }

            inner_loop_impl(i, res, null_map, l_raw_str, l_str_size, path);
        } //for
        return Status::OK();
    } //function

    static Status scalar_vector(FunctionContext* context, const StringRef& ldata,
                                const ColumnString::Chars& rdata,
                                const ColumnString::Offsets& roffsets, const NullMap* r_null_map,
                                Container& res, NullMap& null_map) {
        size_t size = roffsets.size();
        res.resize(size);

        for (size_t i = 0; i < size; i++) {
            if (r_null_map && (*r_null_map)[i]) {
                res[i] = 0;
                null_map[i] = 1;
                continue;
            }

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1];

            JsonbPath path;
            if (!path.seek(r_raw_str, r_str_size)) {
                return Status::InvalidArgument(
                        "Json path error: Invalid Json Path for value: {} at row: {}",
                        std::string_view(r_raw_str, r_str_size), i);
            }

            inner_loop_impl(i, res, null_map, ldata.data, ldata.size, path);
        } //for
        return Status::OK();
    } //function

    static Status vector_scalar(FunctionContext* context, const ColumnString::Chars& ldata,
                                const ColumnString::Offsets& loffsets, const NullMap* l_null_map,
                                const StringRef& rdata, Container& res, NullMap& null_map) {
        size_t size = loffsets.size();
        res.resize(size);

        JsonbPath path;
        if (!path.seek(rdata.data, rdata.size)) {
            return Status::InvalidArgument("Json path error: Invalid Json Path for value: {}",
                                           std::string_view(rdata.data, rdata.size));
        }

        for (size_t i = 0; i < loffsets.size(); i++) {
            if (l_null_map && (*l_null_map)[i]) {
                res[i] = 0;
                null_map[i] = 1;
                continue;
            }

            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1];

            inner_loop_impl(i, res, null_map, l_raw_str, l_str_size, path);
        } //for
        return Status::OK();
    } //function
};

struct JsonbTypeExists {
    using T = uint8_t;
    using ReturnType = DataTypeUInt8;
    using ColumnType = ColumnUInt8;
    static const bool only_check_exists = true;
};

struct JsonbTypeJson {
    using T = std::string;
    using ReturnType = DataTypeJsonb;
    using ColumnType = ColumnString;
    static const bool only_check_exists = false;
    static const bool only_get_type = false;
    static const bool no_quotes = false;
};

struct JsonbTypeJsonNoQuotes {
    using T = std::string;
    using ReturnType = DataTypeJsonb;
    using ColumnType = ColumnString;
    static const bool only_check_exists = false;
    static const bool only_get_type = false;
    static const bool no_quotes = true;
};

struct JsonbTypeType {
    using T = std::string;
    using ReturnType = DataTypeString;
    using ColumnType = ColumnString;
    static const bool only_check_exists = false;
    static const bool only_get_type = true;
    static const bool no_quotes = false;
};

struct JsonbExtractJsonb : public JsonbExtractStringImpl<JsonbTypeJson> {
    static constexpr auto name = "jsonb_extract";
    static constexpr auto alias = "json_extract";
};

struct JsonbExtractJsonbNoQuotes : public JsonbExtractStringImpl<JsonbTypeJsonNoQuotes> {
    static constexpr auto name = "jsonb_extract_no_quotes";
    static constexpr auto alias = "json_extract_no_quotes";
};

struct JsonbType : public JsonbExtractStringImpl<JsonbTypeType> {
    static constexpr auto name = "json_type";
    static constexpr auto alias = "jsonb_type";
};

using FunctionJsonbExists = FunctionJsonbExtractPath;
using FunctionJsonbType = FunctionJsonbExtract<JsonbType>;

using FunctionJsonbExtractIsnull = FunctionJsonbExtract<JsonbExtractIsnull>;
using FunctionJsonbExtractJsonb = FunctionJsonbExtract<JsonbExtractJsonb>;
using FunctionJsonbExtractJsonbNoQuotes = FunctionJsonbExtract<JsonbExtractJsonbNoQuotes>;

template <typename Impl>
class FunctionJsonbLength : public IFunction {
public:
    static constexpr auto name = "json_length";
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionJsonbLength<Impl>>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeInt32>());
    }
    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }
    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

struct JsonbLengthUtil {
    static Status jsonb_length_execute(FunctionContext* context, Block& block,
                                       const ColumnNumbers& arguments, uint32_t result,
                                       size_t input_rows_count) {
        DCHECK_GE(arguments.size(), 2);
        ColumnPtr jsonb_data_column;
        bool jsonb_data_const = false;
        // prepare jsonb data column
        std::tie(jsonb_data_column, jsonb_data_const) =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        ColumnPtr path_column;
        bool is_const = false;
        std::tie(path_column, is_const) =
                unpack_if_const(block.get_by_position(arguments[1]).column);

        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        JsonbPath path;
        if (is_const) {
            if (path_column->is_null_at(0)) {
                for (size_t i = 0; i < input_rows_count; ++i) {
                    null_map->get_data()[i] = 1;
                    res->insert_data(nullptr, 0);
                }

                block.replace_by_position(
                        result, ColumnNullable::create(std::move(res), std::move(null_map)));
                return Status::OK();
            }

            auto path_value = path_column->get_data_at(0);
            if (!path.seek(path_value.data, path_value.size)) {
                return Status::InvalidArgument("Json path error: Invalid Json Path for value: {}",
                                               std::string_view(path_value.data, path_value.size));
            }
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (jsonb_data_column->is_null_at(i) || path_column->is_null_at(i) ||
                (jsonb_data_column->get_data_at(i).size == 0)) {
                null_map->get_data()[i] = 1;
                res->insert_data(nullptr, 0);
                continue;
            }
            if (!is_const) {
                auto path_value = path_column->get_data_at(i);
                path.clean();
                if (!path.seek(path_value.data, path_value.size)) {
                    return Status::InvalidArgument(
                            "Json path error: Invalid Json Path for value: {}",
                            std::string_view(reinterpret_cast<const char*>(path_value.data),
                                             path_value.size));
                }
            }
            auto jsonb_value = jsonb_data_column->get_data_at(i);
            // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
            JsonbDocument* doc = nullptr;
            RETURN_IF_ERROR(JsonbDocument::checkAndCreateDocument(jsonb_value.data,
                                                                  jsonb_value.size, &doc));
            auto find_result = doc->getValue()->findValue(path);
            const auto* value = find_result.value;
            if (UNLIKELY(!value)) {
                null_map->get_data()[i] = 1;
                res->insert_data(nullptr, 0);
                continue;
            }
            auto length = value->numElements();
            res->insert_data(const_cast<const char*>((char*)&length), 0);
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

struct JsonbLengthImpl {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeJsonb>()}; }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto path = ColumnString::create();
        std::string root_path = "$";

        for (int i = 0; i < input_rows_count; i++) {
            reinterpret_cast<ColumnString*>(path.get())
                    ->insert_data(root_path.data(), root_path.size());
        }

        block.insert({std::move(path), std::make_shared<DataTypeString>(), "path"});
        ColumnNumbers temp_arguments = {arguments[0], block.columns() - 1};

        return JsonbLengthUtil::jsonb_length_execute(context, block, temp_arguments, result,
                                                     input_rows_count);
    }
};

struct JsonbLengthAndPathImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeJsonb>(), std::make_shared<DataTypeString>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        return JsonbLengthUtil::jsonb_length_execute(context, block, arguments, result,
                                                     input_rows_count);
    }
};

template <typename Impl>
class FunctionJsonbContains : public IFunction {
public:
    static constexpr auto name = "json_contains";
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionJsonbContains<Impl>>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeUInt8>());
    }
    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }
    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

struct JsonbContainsUtil {
    static Status jsonb_contains_execute(FunctionContext* context, Block& block,
                                         const ColumnNumbers& arguments, uint32_t result,
                                         size_t input_rows_count) {
        DCHECK_GE(arguments.size(), 3);

        auto jsonb_data1_column = block.get_by_position(arguments[0]).column;
        auto jsonb_data2_column = block.get_by_position(arguments[1]).column;

        ColumnPtr path_column;
        bool is_const = false;
        std::tie(path_column, is_const) =
                unpack_if_const(block.get_by_position(arguments[2]).column);

        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        JsonbPath path;
        if (is_const) {
            if (path_column->is_null_at(0)) {
                for (size_t i = 0; i < input_rows_count; ++i) {
                    null_map->get_data()[i] = 1;
                    res->insert_data(nullptr, 0);
                }

                block.replace_by_position(
                        result, ColumnNullable::create(std::move(res), std::move(null_map)));
                return Status::OK();
            }

            auto path_value = path_column->get_data_at(0);
            if (!path.seek(path_value.data, path_value.size)) {
                return Status::InvalidArgument("Json path error: Invalid Json Path for value: {}",
                                               std::string_view(path_value.data, path_value.size));
            }
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (jsonb_data1_column->is_null_at(i) || jsonb_data2_column->is_null_at(i) ||
                path_column->is_null_at(i)) {
                null_map->get_data()[i] = 1;
                res->insert_data(nullptr, 0);
                continue;
            }

            if (!is_const) {
                auto path_value = path_column->get_data_at(i);
                path.clean();
                if (!path.seek(path_value.data, path_value.size)) {
                    return Status::InvalidArgument(
                            "Json path error: Invalid Json Path for value: {}",
                            std::string_view(path_value.data, path_value.size));
                }
            }

            auto jsonb_value1 = jsonb_data1_column->get_data_at(i);
            auto jsonb_value2 = jsonb_data2_column->get_data_at(i);

            if (jsonb_value1.size == 0 || jsonb_value2.size == 0) {
                null_map->get_data()[i] = 1;
                res->insert_data(nullptr, 0);
                continue;
            }
            // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
            JsonbDocument* doc1 = nullptr;
            RETURN_IF_ERROR(JsonbDocument::checkAndCreateDocument(jsonb_value1.data,
                                                                  jsonb_value1.size, &doc1));
            JsonbDocument* doc2 = nullptr;
            RETURN_IF_ERROR(JsonbDocument::checkAndCreateDocument(jsonb_value2.data,
                                                                  jsonb_value2.size, &doc2));

            auto find_result = doc1->getValue()->findValue(path);
            const auto* value1 = find_result.value;
            JsonbValue* value2 = doc2->getValue();
            if (!value1 || !value2) {
                null_map->get_data()[i] = 1;
                res->insert_data(nullptr, 0);
                continue;
            }
            auto contains_value = value1->contains(value2);
            res->insert_data(const_cast<const char*>((char*)&contains_value), 0);
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res), std::move(null_map)));
        return Status::OK();
    }
};

template <bool ignore_null>
class FunctionJsonbArray : public IFunction {
public:
    static constexpr auto name = "json_array";
    static constexpr auto alias = "jsonb_array";

    static FunctionPtr create() { return std::make_shared<FunctionJsonbArray>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeJsonb>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto return_data_type = std::make_shared<DataTypeJsonb>();
        DorisVector<JsonbWriter> writers(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i) {
            writers[i].writeStartArray();
        }

        for (auto argument : arguments) {
            auto&& [arg_column, is_const] = unpack_if_const(block.get_by_position(argument).column);

            auto& data_type = block.get_by_position(argument).type;
            auto serde = data_type->get_serde();

            if (arg_column->is_nullable()) {
                const auto& nullable_column = assert_cast<const ColumnNullable&>(*arg_column);
                const auto& null_map = nullable_column.get_null_map_data();
                const auto& nested_column = nullable_column.get_nested_column();
                const auto& jsonb_column = assert_cast<const ColumnString&>(nested_column);

                for (size_t i = 0; i < input_rows_count; ++i) {
                    auto index = index_check_const(i, is_const);
                    if (null_map[index]) {
                        if constexpr (ignore_null) {
                            continue;
                        } else {
                            writers[i].writeNull();
                        }
                    } else {
                        auto jsonb_binary = jsonb_column.get_data_at(index);
                        JsonbDocument* doc = nullptr;
                        auto st = JsonbDocument::checkAndCreateDocument(jsonb_binary.data,
                                                                        jsonb_binary.size, &doc);
                        if (!st.ok() || !doc || !doc->getValue()) [[unlikely]] {
                            if constexpr (ignore_null) {
                                continue;
                            } else {
                                writers[i].writeNull();
                            }
                        } else {
                            writers[i].writeValue(doc->getValue());
                        }
                    }
                }
            } else {
                const auto& jsonb_column = assert_cast<const ColumnString&>(*arg_column);

                for (size_t i = 0; i < input_rows_count; ++i) {
                    auto index = index_check_const(i, is_const);
                    auto jsonb_binary = jsonb_column.get_data_at(index);
                    JsonbDocument* doc = nullptr;
                    auto st = JsonbDocument::checkAndCreateDocument(jsonb_binary.data,
                                                                    jsonb_binary.size, &doc);
                    if (!st.ok() || !doc || !doc->getValue()) [[unlikely]] {
                        if constexpr (ignore_null) {
                            continue;
                        } else {
                            writers[i].writeNull();
                        }
                    } else {
                        writers[i].writeValue(doc->getValue());
                    }
                }
            }
        }

        auto column = return_data_type->create_column();
        column->reserve(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i) {
            writers[i].writeEndArray();
            column->insert_data(writers[i].getOutput()->getBuffer(),
                                writers[i].getOutput()->getSize());
        }

        block.get_by_position(result).column = std::move(column);
        return Status::OK();
    }
};

class FunctionJsonbObject : public IFunction {
public:
    static constexpr auto name = "json_object";
    static constexpr auto alias = "jsonb_object";

    static FunctionPtr create() { return std::make_shared<FunctionJsonbObject>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeJsonb>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        if (arguments.size() % 2 != 0) {
            return Status::InvalidArgument(
                    "JSON object must have an even number of arguments, but got: {}",
                    arguments.size());
        }

        auto return_data_type = std::make_shared<DataTypeJsonb>();
        DorisVector<JsonbWriter> writers(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i) {
            writers[i].writeStartObject();
        }

        auto write_keys = [&writers](const ColumnString& key_col, const bool is_const,
                                     const NullMap* null_map, const size_t rows,
                                     const size_t arg_index) {
            for (size_t row_idx = 0; row_idx != rows; ++row_idx) {
                auto index = index_check_const(row_idx, is_const);
                if (null_map && (*null_map)[index]) {
                    return Status::InvalidArgument(
                            "JSON documents may not contain NULL member name(argument "
                            "index:  "
                            "{}, row index: {})",
                            row_idx, arg_index);
                }

                auto key_string = key_col.get_data_at(index);
                if (key_string.size > 255) {
                    return Status::InvalidArgument(
                            "JSON object keys(argument index: {}) must be less than 256 "
                            "bytes, but got size: {}",
                            arg_index, key_string.size);
                }
                writers[row_idx].writeKey(key_string.data, static_cast<uint8_t>(key_string.size));
            }
            return Status::OK();
        };

        auto write_values = [&writers](const ColumnString& value_col, const bool is_const,
                                       const NullMap* null_map, const size_t rows,
                                       const size_t arg_index) {
            for (size_t row_idx = 0; row_idx != rows; ++row_idx) {
                auto index = index_check_const(row_idx, is_const);
                if (null_map && (*null_map)[index]) {
                    writers[row_idx].writeNull();
                    continue;
                }

                auto value_string = value_col.get_data_at(index);
                JsonbDocument* doc = nullptr;
                RETURN_IF_ERROR(JsonbDocument::checkAndCreateDocument(value_string.data,
                                                                      value_string.size, &doc));
                writers[row_idx].writeValue(doc->getValue());
            }
            return Status::OK();
        };

        for (size_t i = 0; i < arguments.size(); i += 2) {
            auto key_argument = arguments[i];
            auto value_argument = arguments[i + 1];
            auto&& [key_column, key_const] =
                    unpack_if_const(block.get_by_position(key_argument).column);
            auto&& [value_column, value_const] =
                    unpack_if_const(block.get_by_position(value_argument).column);

            auto& key_data_type = block.get_by_position(key_argument).type;
            auto& value_data_type = block.get_by_position(value_argument).type;
            if (!is_string_type(key_data_type->get_primitive_type())) {
                return Status::InvalidArgument(
                        "JSON object key(argument index: {}) must be String, but got type: "
                        "{}(primitive type: {})",
                        i, key_data_type->get_name(),
                        static_cast<int>(key_data_type->get_primitive_type()));
            }

            if (value_data_type->get_primitive_type() != PrimitiveType::TYPE_JSONB) {
                return Status::InvalidArgument(
                        "JSON object value(argument index: {}) must be JSON, but got type: {}", i,
                        value_data_type->get_name());
            }

            if (key_column->is_nullable()) {
                const auto& nullable_column = assert_cast<const ColumnNullable&>(*key_column);
                const auto& null_map = nullable_column.get_null_map_data();
                const auto& nested_column = nullable_column.get_nested_column();
                const auto& key_arg_column = assert_cast<const ColumnString&>(nested_column);

                RETURN_IF_ERROR(
                        write_keys(key_arg_column, key_const, &null_map, input_rows_count, i));
            } else {
                const auto& key_arg_column = assert_cast<const ColumnString&>(*key_column);
                RETURN_IF_ERROR(
                        write_keys(key_arg_column, key_const, nullptr, input_rows_count, i));
            }

            if (value_column->is_nullable()) {
                const auto& nullable_column = assert_cast<const ColumnNullable&>(*value_column);
                const auto& null_map = nullable_column.get_null_map_data();
                const auto& nested_column = nullable_column.get_nested_column();
                const auto& value_arg_column = assert_cast<const ColumnString&>(nested_column);

                RETURN_IF_ERROR(write_values(value_arg_column, value_const, &null_map,
                                             input_rows_count, i + 1));
            } else {
                const auto& value_arg_column = assert_cast<const ColumnString&>(*value_column);
                RETURN_IF_ERROR(write_values(value_arg_column, value_const, nullptr,
                                             input_rows_count, i + 1));
            }
        }

        auto column = return_data_type->create_column();
        column->reserve(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i) {
            writers[i].writeEndObject();
            column->insert_data(writers[i].getOutput()->getBuffer(),
                                writers[i].getOutput()->getSize());
        }

        block.get_by_position(result).column = std::move(column);
        return Status::OK();
    }
};

enum class JsonbModifyType { Insert, Set, Replace };

template <JsonbModifyType modify_type>
struct JsonbModifyName {
    static constexpr auto name = "jsonb_modify";
    static constexpr auto alias = "json_modify";
};

template <>
struct JsonbModifyName<JsonbModifyType::Insert> {
    static constexpr auto name = "jsonb_insert";
    static constexpr auto alias = "json_insert";
};
template <>
struct JsonbModifyName<JsonbModifyType::Set> {
    static constexpr auto name = "jsonb_set";
    static constexpr auto alias = "json_set";
};
template <>
struct JsonbModifyName<JsonbModifyType::Replace> {
    static constexpr auto name = "jsonb_replace";
    static constexpr auto alias = "json_replace";
};

template <JsonbModifyType modify_type>
class FunctionJsonbModify : public IFunction {
public:
    static constexpr auto name = JsonbModifyName<modify_type>::name;
    static constexpr auto alias = JsonbModifyName<modify_type>::alias;

    static FunctionPtr create() { return std::make_shared<FunctionJsonbModify<modify_type>>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeJsonb>());
    }

    Status create_all_null_result(const DataTypePtr& return_data_type, Block& block,
                                  uint32_t result, size_t input_rows_count) const {
        auto result_column = return_data_type->create_column();
        result_column->insert_default();
        auto const_column = ColumnConst::create(std::move(result_column), input_rows_count);
        block.get_by_position(result).column = std::move(const_column);
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        if (arguments.size() % 2 != 1 || arguments.size() < 3) {
            return Status::InvalidArgument(
                    "Function {} must have an odd number of arguments and more than 2 arguments, "
                    "but got: {}",
                    name, arguments.size());
        }

        const size_t keys_count = (arguments.size() - 1) / 2;

        auto return_data_type = make_nullable(std::make_shared<DataTypeJsonb>());
        DorisVector<JsonbWriter> writers(input_rows_count);

        auto result_column = return_data_type->create_column();
        auto& result_nullable_col = assert_cast<ColumnNullable&>(*result_column);
        auto& null_map = result_nullable_col.get_null_map_data();
        auto& res_string_column =
                assert_cast<ColumnString&>(result_nullable_col.get_nested_column());
        auto& res_chars = res_string_column.get_chars();
        auto& res_offsets = res_string_column.get_offsets();

        null_map.resize_fill(input_rows_count, 0);
        res_offsets.resize(input_rows_count);
        auto&& [json_data_arg_column, json_data_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);

        if (json_data_const) {
            if (json_data_arg_column->is_null_at(0)) {
                return create_all_null_result(return_data_type, block, result, input_rows_count);
            }
        }

        std::vector<const ColumnString*> json_path_columns(keys_count);
        std::vector<bool> json_path_constant(keys_count);
        std::vector<const NullMap*> json_path_null_maps(keys_count, nullptr);

        std::vector<const ColumnString*> json_value_columns(keys_count);
        std::vector<bool> json_value_constant(keys_count);
        std::vector<const NullMap*> json_value_null_maps(keys_count, nullptr);

        const NullMap* json_data_null_map = nullptr;
        const ColumnString* json_data_column;
        if (json_data_arg_column->is_nullable()) {
            const auto& nullable_column = assert_cast<const ColumnNullable&>(*json_data_arg_column);
            json_data_null_map = &nullable_column.get_null_map_data();
            const auto& nested_column = nullable_column.get_nested_column();
            json_data_column = assert_cast<const ColumnString*>(&nested_column);
        } else {
            json_data_column = assert_cast<const ColumnString*>(json_data_arg_column.get());
        }

        for (size_t i = 1; i < arguments.size(); i += 2) {
            auto&& [path_column, path_const] =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
            auto&& [value_column, value_const] =
                    unpack_if_const(block.get_by_position(arguments[i + 1]).column);

            if (path_const) {
                if (path_column->is_null_at(0)) {
                    return create_all_null_result(return_data_type, block, result,
                                                  input_rows_count);
                }
            }

            json_path_constant[i / 2] = path_const;
            if (path_column->is_nullable()) {
                const auto& nullable_column = assert_cast<const ColumnNullable&>(*path_column);
                json_path_null_maps[i / 2] = &nullable_column.get_null_map_data();
                const auto& nested_column = nullable_column.get_nested_column();
                json_path_columns[i / 2] = assert_cast<const ColumnString*>(&nested_column);
            } else {
                json_path_columns[i / 2] = assert_cast<const ColumnString*>(path_column.get());
            }

            json_value_constant[i / 2] = value_const;
            if (value_column->is_nullable()) {
                const auto& nullable_column = assert_cast<const ColumnNullable&>(*value_column);
                json_value_null_maps[i / 2] = &nullable_column.get_null_map_data();
                const auto& nested_column = nullable_column.get_nested_column();
                json_value_columns[i / 2] = assert_cast<const ColumnString*>(&nested_column);
            } else {
                json_value_columns[i / 2] = assert_cast<const ColumnString*>(value_column.get());
            }
        }

        DorisVector<JsonbDocument*> json_documents(input_rows_count);
        if (json_data_const) {
            auto json_data_string = json_data_column->get_data_at(0);
            JsonbDocument* doc = nullptr;
            RETURN_IF_ERROR(JsonbDocument::checkAndCreateDocument(json_data_string.data,
                                                                  json_data_string.size, &doc));
            if (!doc || !doc->getValue()) [[unlikely]] {
                return create_all_null_result(return_data_type, block, result, input_rows_count);
            }
            for (size_t i = 0; i != input_rows_count; ++i) {
                json_documents[i] = doc;
            }
        } else {
            for (size_t i = 0; i != input_rows_count; ++i) {
                if (json_data_null_map && (*json_data_null_map)[i]) {
                    null_map[i] = 1;
                    json_documents[i] = nullptr;
                    continue;
                }

                auto json_data_string = json_data_column->get_data_at(i);
                JsonbDocument* doc = nullptr;
                RETURN_IF_ERROR(JsonbDocument::checkAndCreateDocument(json_data_string.data,
                                                                      json_data_string.size, &doc));
                if (!doc || !doc->getValue()) [[unlikely]] {
                    null_map[i] = 1;
                    continue;
                }
                json_documents[i] = doc;
            }
        }

        DorisVector<DorisVector<JsonbPath>> json_paths(keys_count);
        DorisVector<DorisVector<JsonbValue*>> json_values(keys_count);
        DorisVector<JsonbWriter> writer_holders(input_rows_count);

        RETURN_IF_ERROR(parse_paths_and_values(json_paths, json_values, arguments, input_rows_count,
                                               json_path_columns, json_path_constant,
                                               json_path_null_maps, json_value_columns,
                                               json_value_constant, json_value_null_maps));

        for (size_t i = 1; i < arguments.size(); i += 2) {
            const size_t index = i / 2;
            auto& json_path = json_paths[index];
            auto& json_value = json_values[index];

            for (size_t row_idx = 0; row_idx != input_rows_count; ++row_idx) {
                const auto path_index = index_check_const(row_idx, json_path_constant[index]);
                const auto value_index = index_check_const(row_idx, json_value_constant[index]);

                if (null_map[row_idx]) {
                    continue;
                }

                if (json_documents[row_idx] == nullptr) {
                    null_map[row_idx] = 1;
                    continue;
                }

                if (json_path_null_maps[index] && (*json_path_null_maps[index])[path_index]) {
                    null_map[row_idx] = 1;
                    continue;
                }

                auto find_result =
                        json_documents[row_idx]->getValue()->findValue(json_path[path_index]);

                if (find_result.is_wildcard) {
                    return Status::InvalidArgument(
                            " In this situation, path expressions may not contain the * and ** "
                            "tokens or an array range, argument index: {}, row index: {}",
                            i, row_idx);
                }

                if constexpr (modify_type == JsonbModifyType::Insert) {
                    if (find_result.value) {
                        continue;
                    }
                } else if constexpr (modify_type == JsonbModifyType::Replace) {
                    if (!find_result.value) {
                        continue;
                    }
                }

                std::vector<const JsonbValue*> parents;
                JsonbWriter writer;

                bool replace = false;
                parents.emplace_back(json_documents[row_idx]->getValue());
                if (find_result.value) {
                    // find target path, replace it with the new value.
                    replace = true;
                    if (!build_parents_by_path(json_documents[row_idx]->getValue(),
                                               json_path[path_index], parents)) {
                        DCHECK(false);
                        continue;
                    }
                } else {
                    // does not find target path, insert the new value.
                    JsonbPath new_path;
                    for (size_t j = 0; j < json_path[path_index].get_leg_vector_size() - 1; ++j) {
                        auto* current_leg = json_path[path_index].get_leg_from_leg_vector(j);
                        std::unique_ptr<leg_info> leg = std::make_unique<leg_info>(
                                current_leg->leg_ptr, current_leg->leg_len,
                                current_leg->array_index, current_leg->type);
                        new_path.add_leg_to_leg_vector(std::move(leg));
                    }

                    if (!build_parents_by_path(json_documents[row_idx]->getValue(), new_path,
                                               parents)) {
                        DCHECK(false);
                        continue;
                    }
                }

                const auto legs_count = json_path[path_index].get_leg_vector_size();
                leg_info* last_leg =
                        legs_count > 0
                                ? json_path[path_index].get_leg_from_leg_vector(legs_count - 1)
                                : nullptr;
                RETURN_IF_ERROR(write_json_value(json_documents[row_idx]->getValue(), parents, 0,
                                                 json_value[value_index], replace, last_leg,
                                                 writer));

                json_documents[row_idx] = writer.getDocument();
                writer_holders[row_idx] = std::move(writer);
            }
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (!null_map[i]) {
                const auto* jsonb_document = json_documents[i];
                const auto size = jsonb_document->numPackedBytes();
                res_chars.insert(reinterpret_cast<const char*>(jsonb_document),
                                 reinterpret_cast<const char*>(jsonb_document) + size);
            }

            res_offsets[i] = static_cast<uint32_t>(res_chars.size());

            if (!null_map[i]) {
                auto* ptr = res_chars.data() + res_offsets[i - 1];
                auto size = res_offsets[i] - res_offsets[i - 1];
                JsonbDocument* doc = nullptr;
                THROW_IF_ERROR(JsonbDocument::checkAndCreateDocument(
                        reinterpret_cast<const char*>(ptr), size,
                        &doc)); // doc is NOT necessary to be deleted since
                                // JsonbDocument will not allocate memory
            }
        }

        block.get_by_position(result).column = std::move(result_column);
        return Status::OK();
    }

    bool build_parents_by_path(const JsonbValue* root, const JsonbPath& path,
                               std::vector<const JsonbValue*>& parents) const {
        const size_t index = parents.size() - 1;
        if (index == path.get_leg_vector_size()) {
            return true;
        }

        JsonbPath current;
        auto* current_leg = path.get_leg_from_leg_vector(index);
        std::unique_ptr<leg_info> leg =
                std::make_unique<leg_info>(current_leg->leg_ptr, current_leg->leg_len,
                                           current_leg->array_index, current_leg->type);
        current.add_leg_to_leg_vector(std::move(leg));

        auto find_result = root->findValue(current);
        if (!find_result.value) {
            std::string path_string;
            current.to_string(&path_string);
            return false;
        } else if (find_result.value == root) {
            return true;
        } else {
            parents.emplace_back(find_result.value);
        }

        return build_parents_by_path(find_result.value, path, parents);
    }

    Status write_json_value(const JsonbValue* root, const std::vector<const JsonbValue*>& parents,
                            const size_t parent_index, const JsonbValue* value, const bool replace,
                            const leg_info* last_leg, JsonbWriter& writer) const {
        if (parent_index >= parents.size()) {
            return Status::InvalidArgument(
                    "JsonbModify: parent_index {} is out of bounds for parents size {}",
                    parent_index, parents.size());
        }

        if (parents[parent_index] != root) {
            return Status::InvalidArgument(
                    "JsonbModify: parent value does not match root value, parent_index: {}, "
                    "parents size: {}",
                    parent_index, parents.size());
        }

        if (parent_index == parents.size() - 1 && replace) {
            // We are at the last parent, write the value directly
            if (value == nullptr) {
                writer.writeNull();
            } else {
                writer.writeValue(value);
            }
            return Status::OK();
        }

        bool value_written = false;
        bool is_last_parent = (parent_index == parents.size() - 1);
        const auto* next_parent = is_last_parent ? nullptr : parents[parent_index + 1];
        if (root->isArray()) {
            writer.writeStartArray();
            const auto* array_val = root->unpack<ArrayVal>();
            for (int i = 0; i != array_val->numElem(); ++i) {
                auto* it = array_val->get(i);

                if (is_last_parent && last_leg->array_index == i) {
                    value_written = true;
                    writer.writeValue(value);
                } else if (it == next_parent) {
                    value_written = true;
                    RETURN_IF_ERROR(write_json_value(it, parents, parent_index + 1, value, replace,
                                                     last_leg, writer));
                } else {
                    writer.writeValue(it);
                }
            }
            if (is_last_parent && !value_written) {
                value_written = true;
                writer.writeValue(value);
            }

            writer.writeEndArray();

        } else {
            /**
                Because even for a non-array object, `$[0]` can still point to that object:
                ```
                select json_extract('{"key": "value"}', '$[0]');
                +------------------------------------------+
                | json_extract('{"key": "value"}', '$[0]') |
                +------------------------------------------+
                | {"key": "value"}                         |
                +------------------------------------------+
                ```
                So when inserting an element into `$[1]`, even if '$' does not represent an array, 
                it should be converted to an array before insertion:
                ```
                select json_insert('123','$[1]', null);
                +---------------------------------+
                | json_insert('123','$[1]', null) |
                +---------------------------------+
                | [123, null]                     |
                +---------------------------------+
                ```
             */
            if (is_last_parent && last_leg && last_leg->type == ARRAY_CODE) {
                writer.writeStartArray();
                writer.writeValue(root);
                writer.writeValue(value);
                writer.writeEndArray();
                return Status::OK();
            } else if (root->isObject()) {
                writer.writeStartObject();
                const auto* object_val = root->unpack<ObjectVal>();
                for (const auto& it : *object_val) {
                    writer.writeKey(it.getKeyStr(), it.klen());
                    if (it.value() == next_parent) {
                        value_written = true;
                        RETURN_IF_ERROR(write_json_value(it.value(), parents, parent_index + 1,
                                                         value, replace, last_leg, writer));
                    } else {
                        writer.writeValue(it.value());
                    }
                }

                if (is_last_parent && !value_written) {
                    value_written = true;
                    writer.writeStartObject();
                    writer.writeKey(last_leg->leg_ptr, static_cast<uint8_t>(last_leg->leg_len));
                    writer.writeValue(value);
                    writer.writeEndObject();
                }
                writer.writeEndObject();

            } else {
                return Status::InvalidArgument("Cannot insert value into this type");
            }
        }

        if (!value_written) {
            return Status::InvalidArgument(
                    "JsonbModify: value not written, parent_index: {}, parents size: {}",
                    parent_index, parents.size());
        }

        return Status::OK();
    }

    Status parse_paths_and_values(DorisVector<DorisVector<JsonbPath>>& json_paths,
                                  DorisVector<DorisVector<JsonbValue*>>& json_values,
                                  const ColumnNumbers& arguments, const size_t input_rows_count,
                                  const std::vector<const ColumnString*>& json_path_columns,
                                  const std::vector<bool>& json_path_constant,
                                  const std::vector<const NullMap*>& json_path_null_maps,
                                  const std::vector<const ColumnString*>& json_value_columns,
                                  const std::vector<bool>& json_value_constant,
                                  const std::vector<const NullMap*>& json_value_null_maps) const {
        for (size_t i = 1; i < arguments.size(); i += 2) {
            const size_t index = i / 2;
            const auto* json_path_column = json_path_columns[index];
            const auto* value_column = json_value_columns[index];

            json_paths[index].resize(json_path_constant[index] ? 1 : input_rows_count);
            json_values[index].resize(json_value_constant[index] ? 1 : input_rows_count, nullptr);

            for (size_t row_idx = 0; row_idx != json_paths[index].size(); ++row_idx) {
                if (json_path_null_maps[index] && (*json_path_null_maps[index])[row_idx]) {
                    continue;
                }

                auto path_string = json_path_column->get_data_at(row_idx);
                if (!json_paths[index][row_idx].seek(path_string.data, path_string.size)) {
                    return Status::InvalidArgument(
                            "Json path error: Invalid Json Path for value: {}, "
                            "argument "
                            "index: {}, row index: {}",
                            std::string_view(path_string.data, path_string.size), i, row_idx);
                }

                if (json_paths[index][row_idx].is_wildcard()) {
                    return Status::InvalidArgument(
                            "In this situation, path expressions may not contain the * and ** "
                            "tokens, argument index: {}, row index: {}",
                            i, row_idx);
                }
            }

            for (size_t row_idx = 0; row_idx != json_values[index].size(); ++row_idx) {
                if (json_value_null_maps[index] && (*json_value_null_maps[index])[row_idx]) {
                    continue;
                }

                auto value_string = value_column->get_data_at(row_idx);
                JsonbDocument* doc = nullptr;
                RETURN_IF_ERROR(JsonbDocument::checkAndCreateDocument(value_string.data,
                                                                      value_string.size, &doc));
                if (doc) {
                    json_values[index][row_idx] = doc->getValue();
                }
            }
        }

        return Status::OK();
    }
};

struct JsonbContainsImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeJsonb>(), std::make_shared<DataTypeJsonb>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto path = ColumnString::create();
        std::string root_path = "$";

        for (int i = 0; i < input_rows_count; i++) {
            reinterpret_cast<ColumnString*>(path.get())
                    ->insert_data(root_path.data(), root_path.size());
        }

        block.insert({std::move(path), std::make_shared<DataTypeString>(), "path"});
        ColumnNumbers temp_arguments = {arguments[0], arguments[1], block.columns() - 1};

        return JsonbContainsUtil::jsonb_contains_execute(context, block, temp_arguments, result,
                                                         input_rows_count);
    }
};

struct JsonbContainsAndPathImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeJsonb>(), std::make_shared<DataTypeJsonb>(),
                std::make_shared<DataTypeString>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        return JsonbContainsUtil::jsonb_contains_execute(context, block, arguments, result,
                                                         input_rows_count);
    }
};

class FunctionJsonSearch : public IFunction {
private:
    using OneFun = std::function<Status(size_t, bool*)>;
    static Status always_one(size_t i, bool* res) {
        *res = true;
        return Status::OK();
    }
    static Status always_all(size_t i, bool* res) {
        *res = false;
        return Status::OK();
    }

    using CheckNullFun = std::function<bool(size_t)>;
    static bool always_not_null(size_t) { return false; }

    using GetJsonStringRefFun = std::function<StringRef(size_t)>;

    Status matched(const std::string_view& str, LikeState* state, unsigned char* res) const {
        StringRef pattern; // not used
        StringRef value_val(str.data(), str.size());
        return (state->scalar_function)(&state->search_state, value_val, pattern, res);
    }

    /**
     * Recursive search for matching string, if found, the result will be added to a vector
     * @param element json element
     * @param one_match
     * @param search_str
     * @param cur_path
     * @param matches The path that has already been matched
     * @return true if matched else false
     */
    bool find_matches(const JsonbValue* element, const bool& one_match, LikeState* state,
                      JsonbPath* cur_path, std::unordered_set<std::string>* matches) const {
        if (element->isString()) {
            const auto* json_string = element->unpack<JsonbStringVal>();
            const std::string_view element_str(json_string->getBlob(), json_string->length());
            unsigned char res;
            RETURN_IF_ERROR(matched(element_str, state, &res));
            if (res) {
                std::string str;
                auto valid = cur_path->to_string(&str);
                if (!valid) {
                    return false;
                }
                return matches->insert(str).second;
            } else {
                return false;
            }
        } else if (element->isObject()) {
            const auto* object = element->unpack<ObjectVal>();
            bool find = false;
            for (const auto& item : *object) {
                const std::string_view key(item.getKeyStr(), item.klen());
                const auto* child_element = item.value();
                // construct an object member path leg.
                auto leg = std::make_unique<leg_info>(const_cast<char*>(key.data()), key.size(), 0,
                                                      MEMBER_CODE);
                cur_path->add_leg_to_leg_vector(std::move(leg));
                find |= find_matches(child_element, one_match, state, cur_path, matches);
                cur_path->pop_leg_from_leg_vector();
                if (one_match && find) {
                    return true;
                }
            }
            return find;
        } else if (element->isArray()) {
            const auto* array = element->unpack<ArrayVal>();
            bool find = false;
            for (int i = 0; i < array->numElem(); ++i) {
                auto leg = std::make_unique<leg_info>(nullptr, 0, i, ARRAY_CODE);
                cur_path->add_leg_to_leg_vector(std::move(leg));
                const auto* child_element = array->get(i);
                // construct an array cell path leg.
                find |= find_matches(child_element, one_match, state, cur_path, matches);
                cur_path->pop_leg_from_leg_vector();
                if (one_match && find) {
                    return true;
                }
            }
            return find;
        } else {
            return false;
        }
    }

    void make_result_str(std::unordered_set<std::string>& matches, ColumnString* result_col) const {
        JsonbWriter writer;
        if (matches.size() == 1) {
            for (const auto& str_ref : matches) {
                writer.writeStartString();
                writer.writeString(str_ref);
                writer.writeEndString();
            }
        } else {
            writer.writeStartArray();
            for (const auto& str_ref : matches) {
                writer.writeStartString();
                writer.writeString(str_ref);
                writer.writeEndString();
            }
            writer.writeEndArray();
        }

        result_col->insert_data(writer.getOutput()->getBuffer(),
                                (size_t)writer.getOutput()->getSize());
    }

    template <bool search_is_const>
    Status execute_vector(Block& block, size_t input_rows_count, CheckNullFun json_null_check,
                          GetJsonStringRefFun col_json_string, CheckNullFun one_null_check,
                          OneFun one_check, CheckNullFun search_null_check,
                          const ColumnString* col_search_string, FunctionContext* context,
                          size_t result) const {
        auto result_col = ColumnString::create();
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        std::shared_ptr<LikeState> state_ptr;
        LikeState* state = nullptr;
        if (search_is_const) {
            state = reinterpret_cast<LikeState*>(
                    context->get_function_state(FunctionContext::THREAD_LOCAL));
        }

        bool is_one = false;

        for (size_t i = 0; i < input_rows_count; ++i) {
            // an error occurs if the json_doc argument is not a valid json document.
            if (json_null_check(i)) {
                null_map->get_data()[i] = 1;
                result_col->insert_data("", 0);
                continue;
            }
            const auto& json_doc_str = col_json_string(i);
            JsonbDocument* json_doc = nullptr;
            auto st = JsonbDocument::checkAndCreateDocument(json_doc_str.data, json_doc_str.size,
                                                            &json_doc);
            if (!st.ok()) {
                return Status::InvalidArgument(
                        "the json_doc argument at row {} is not a valid json document: {}", i,
                        st.to_string());
            }

            if (!one_null_check(i)) {
                RETURN_IF_ERROR(one_check(i, &is_one));
            }

            if (one_null_check(i) || search_null_check(i)) {
                null_map->get_data()[i] = 1;
                result_col->insert_data("", 0);
                continue;
            }

            // an error occurs if any path argument is not a valid path expression.
            std::string root_path_str = "$";
            JsonbPath root_path;
            root_path.seek(root_path_str.c_str(), root_path_str.size());
            std::vector<JsonbPath*> paths;
            paths.push_back(&root_path);

            if (!search_is_const) {
                state_ptr = std::make_shared<LikeState>();
                state_ptr->is_like_pattern = true;
                const auto& search_str = col_search_string->get_data_at(i);
                RETURN_IF_ERROR(FunctionLike::construct_like_const_state(context, search_str,
                                                                         state_ptr, false));
                state = state_ptr.get();
            }

            // maintain a hashset to deduplicate matches.
            std::unordered_set<std::string> matches;
            for (const auto& item : paths) {
                auto* cur_path = item;
                auto find = find_matches(json_doc->getValue(), is_one, state, cur_path, &matches);
                if (is_one && find) {
                    break;
                }
            }
            if (matches.empty()) {
                // returns NULL if the search_str is not found in the document.
                null_map->get_data()[i] = 1;
                result_col->insert_data("", 0);
                continue;
            }
            make_result_str(matches, result_col.get());
        }
        auto result_col_nullable =
                ColumnNullable::create(std::move(result_col), std::move(null_map));
        block.replace_by_position(result, std::move(result_col_nullable));
        return Status::OK();
    }

    static constexpr auto one = "one";
    static constexpr auto all = "all";

public:
    static constexpr auto name = "json_search";
    static FunctionPtr create() { return std::make_shared<FunctionJsonSearch>(); }

    String get_name() const override { return name; }
    bool is_variadic() const override { return false; }
    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeJsonb>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope != FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        if (context->is_col_constant(2)) {
            std::shared_ptr<LikeState> state = std::make_shared<LikeState>();
            state->is_like_pattern = true;
            const auto pattern_col = context->get_constant_col(2)->column_ptr;
            const auto& pattern = pattern_col->get_data_at(0);
            RETURN_IF_ERROR(
                    FunctionLike::construct_like_const_state(context, pattern, state, false));
            context->set_function_state(scope, state);
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        // the json_doc, one_or_all, and search_str must be given.
        // and we require the positions are static.
        if (arguments.size() < 3) {
            return Status::InvalidArgument("too few arguments for function {}", name);
        }
        if (arguments.size() > 3) {
            return Status::NotSupported("escape and path params are not support now");
        }

        CheckNullFun json_null_check = always_not_null;
        GetJsonStringRefFun get_json_fun;
        // prepare jsonb data column
        auto&& [col_json, json_is_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto* col_json_string = check_and_get_column<ColumnString>(col_json.get());
        if (const auto* nullable = check_and_get_column<ColumnNullable>(col_json.get())) {
            col_json_string =
                    check_and_get_column<ColumnString>(nullable->get_nested_column_ptr().get());
        }

        if (!col_json_string) {
            return Status::RuntimeError("Illegal arg json {} should be ColumnString",
                                        col_json->get_name());
        }

        auto create_all_null_result = [&]() {
            auto res_str = ColumnString::create();
            res_str->insert_default();
            auto res = ColumnNullable::create(std::move(res_str), ColumnUInt8::create(1, 1));
            if (input_rows_count > 1) {
                block.get_by_position(result).column =
                        ColumnConst::create(std::move(res), input_rows_count);
            } else {
                block.get_by_position(result).column = std::move(res);
            }
            return Status::OK();
        };

        if (json_is_const) {
            if (col_json->is_null_at(0)) {
                return create_all_null_result();
            } else {
                const auto& json_str = col_json_string->get_data_at(0);
                get_json_fun = [json_str](size_t i) { return json_str; };
            }
        } else {
            json_null_check = [col_json](size_t i) { return col_json->is_null_at(i); };
            get_json_fun = [col_json_string](size_t i) { return col_json_string->get_data_at(i); };
        }

        // one_or_all
        CheckNullFun one_null_check = always_not_null;
        OneFun one_check = always_one;
        auto&& [col_one, one_is_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        one_is_const |= input_rows_count == 1;
        const auto* col_one_string = check_and_get_column<ColumnString>(col_one.get());
        if (const auto* nullable = check_and_get_column<ColumnNullable>(col_one.get())) {
            col_one_string = check_and_get_column<ColumnString>(*nullable->get_nested_column_ptr());
        }
        if (!col_one_string) {
            return Status::RuntimeError("Illegal arg one {} should be ColumnString",
                                        col_one->get_name());
        }
        if (one_is_const) {
            if (col_one->is_null_at(0)) {
                return create_all_null_result();
            } else {
                const auto& one_or_all = col_one_string->get_data_at(0);
                std::string one_or_all_str = one_or_all.to_string();
                if (strcasecmp(one_or_all_str.c_str(), all) == 0) {
                    one_check = always_all;
                } else if (strcasecmp(one_or_all_str.c_str(), one) == 0) {
                    // nothing
                } else {
                    // an error occurs if the one_or_all argument is not 'one' nor 'all'.
                    return Status::InvalidArgument(
                            "the one_or_all argument {} is not 'one' not 'all'", one_or_all_str);
                }
            }
        } else {
            one_null_check = [col_one](size_t i) { return col_one->is_null_at(i); };
            one_check = [col_one_string](size_t i, bool* is_one) {
                const auto& one_or_all = col_one_string->get_data_at(i);
                std::string one_or_all_str = one_or_all.to_string();
                if (strcasecmp(one_or_all_str.c_str(), all) == 0) {
                    *is_one = false;
                } else if (strcasecmp(one_or_all_str.c_str(), one) == 0) {
                    *is_one = true;
                } else {
                    // an error occurs if the one_or_all argument is not 'one' nor 'all'.
                    return Status::InvalidArgument(
                            "the one_or_all argument {} is not 'one' not 'all'", one_or_all_str);
                }
                return Status::OK();
            };
        }

        // search_str
        auto&& [col_search, search_is_const] =
                unpack_if_const(block.get_by_position(arguments[2]).column);

        const auto* col_search_string = check_and_get_column<ColumnString>(col_search.get());
        if (const auto* nullable = check_and_get_column<ColumnNullable>(col_search.get())) {
            col_search_string =
                    check_and_get_column<ColumnString>(*nullable->get_nested_column_ptr());
        }
        if (!col_search_string) {
            return Status::RuntimeError("Illegal arg pattern {} should be ColumnString",
                                        col_search->get_name());
        }
        if (search_is_const) {
            CheckNullFun search_null_check = always_not_null;
            if (col_search->is_null_at(0)) {
                return create_all_null_result();
            }
            RETURN_IF_ERROR(execute_vector<true>(
                    block, input_rows_count, json_null_check, get_json_fun, one_null_check,
                    one_check, search_null_check, col_search_string, context, result));
        } else {
            CheckNullFun search_null_check = [col_search](size_t i) {
                return col_search->is_null_at(i);
            };
            RETURN_IF_ERROR(execute_vector<false>(
                    block, input_rows_count, json_null_check, get_json_fun, one_null_check,
                    one_check, search_null_check, col_search_string, context, result));
        }
        return Status::OK();
    }
};

void register_function_jsonb(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionJsonbParse>(FunctionJsonbParse::name);
    factory.register_alias(FunctionJsonbParse::name, FunctionJsonbParse::alias);
    factory.register_function<FunctionJsonbParseErrorNull>("json_parse_error_to_null");
    factory.register_alias("json_parse_error_to_null", "jsonb_parse_error_to_null");
    factory.register_function<FunctionJsonbParseErrorValue>("json_parse_error_to_value");
    factory.register_alias("json_parse_error_to_value", "jsonb_parse_error_to_value");

    factory.register_function<FunctionJsonbExists>();
    factory.register_alias(FunctionJsonbExists::name, FunctionJsonbExists::alias);
    factory.register_function<FunctionJsonbType>();
    factory.register_alias(FunctionJsonbType::name, FunctionJsonbType::alias);

    factory.register_function<FunctionJsonbKeys>();
    factory.register_alias(FunctionJsonbKeys::name, FunctionJsonbKeys::alias);

    factory.register_function<FunctionJsonbExtractIsnull>();
    factory.register_alias(FunctionJsonbExtractIsnull::name, FunctionJsonbExtractIsnull::alias);

    factory.register_function<FunctionJsonbExtractJsonb>();
    factory.register_function<FunctionJsonbExtractJsonbNoQuotes>();
    factory.register_alias(FunctionJsonbExtractJsonbNoQuotes::name,
                           FunctionJsonbExtractJsonbNoQuotes::alias);

    factory.register_function<FunctionJsonbLength<JsonbLengthImpl>>();
    factory.register_function<FunctionJsonbLength<JsonbLengthAndPathImpl>>();
    factory.register_function<FunctionJsonbContains<JsonbContainsImpl>>();
    factory.register_function<FunctionJsonbContains<JsonbContainsAndPathImpl>>();

    factory.register_function<FunctionJsonSearch>();

    factory.register_function<FunctionJsonbArray<false>>();
    factory.register_alias(FunctionJsonbArray<false>::name, FunctionJsonbArray<false>::alias);

    factory.register_function<FunctionJsonbArray<true>>("json_array_ignore_null");
    factory.register_alias("json_array_ignore_null", "jsonb_array_ignore_null");

    factory.register_function<FunctionJsonbObject>();
    factory.register_alias(FunctionJsonbObject::name, FunctionJsonbObject::alias);

    factory.register_function<FunctionJsonbModify<JsonbModifyType::Insert>>();
    factory.register_alias(FunctionJsonbModify<JsonbModifyType::Insert>::name,
                           FunctionJsonbModify<JsonbModifyType::Insert>::alias);
    factory.register_function<FunctionJsonbModify<JsonbModifyType::Set>>();
    factory.register_alias(FunctionJsonbModify<JsonbModifyType::Set>::name,
                           FunctionJsonbModify<JsonbModifyType::Set>::alias);
    factory.register_function<FunctionJsonbModify<JsonbModifyType::Replace>>();
    factory.register_alias(FunctionJsonbModify<JsonbModifyType::Replace>::name,
                           FunctionJsonbModify<JsonbModifyType::Replace>::alias);
}

} // namespace doris::vectorized
