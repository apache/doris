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
#include <simdjson/simdjson.h> // IWYU pragma: keep
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "udf/udf.h"
#include "util/jsonb_document.h"
#include "util/jsonb_error.h"
#ifdef __AVX2__
#include "util/jsonb_parser_simd.h"
#else
#include "util/jsonb_parser.h"
#endif
#include "util/jsonb_stream.h"
#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_string.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

enum class NullalbeMode {
    NULLABLE = 0,
    NOT_NULL,
    FOLLOW_INPUT,
};

enum class JsonbParseErrorMode { FAIL = 0, RETURN_NULL, RETURN_VALUE, RETURN_INVALID };

// func(string,string) -> json
template <NullalbeMode nullable_mode, JsonbParseErrorMode parse_error_handle_mode>
class FunctionJsonbParseBase : public IFunction {
private:
    struct FunctionJsonbParseState {
        JsonbParser default_value_parser;
        bool has_const_default_value = false;
    };

public:
    static constexpr auto name = "json_parse";
    static constexpr auto alias = "jsonb_parse";
    static FunctionPtr create() { return std::make_shared<FunctionJsonbParseBase>(); }

    String get_name() const override {
        String nullable;
        switch (nullable_mode) {
        case NullalbeMode::NULLABLE:
            nullable = "_nullable";
            break;
        case NullalbeMode::NOT_NULL:
            nullable = "_notnull";
            break;
        case NullalbeMode::FOLLOW_INPUT:
            nullable = "";
            break;
        }

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
        case JsonbParseErrorMode::RETURN_INVALID:
            error_mode = "_error_to_invalid";
            break;
        }

        return name + nullable + error_mode;
    }

    size_t get_number_of_arguments() const override {
        switch (parse_error_handle_mode) {
        case JsonbParseErrorMode::FAIL:
            return 1;
        case JsonbParseErrorMode::RETURN_NULL:
            return 1;
        case JsonbParseErrorMode::RETURN_VALUE:
            return 2;
        case JsonbParseErrorMode::RETURN_INVALID:
            return 1;
        }
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        bool is_nullable = true;
        switch (nullable_mode) {
        case NullalbeMode::NULLABLE:
            is_nullable = true;
            break;
        case NullalbeMode::NOT_NULL:
            is_nullable = false;
            break;
        case NullalbeMode::FOLLOW_INPUT:
            is_nullable = arguments[0]->is_nullable();
            break;
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
            if (context->is_col_constant(1)) {
                const auto default_value_col = context->get_constant_col(1)->column_ptr;
                const auto& default_value = default_value_col->get_data_at(0);

                JsonbErrType error = JsonbErrType::E_NONE;
                if (scope == FunctionContext::FunctionStateScope::FRAGMENT_LOCAL) {
                    FunctionJsonbParseState* state = reinterpret_cast<FunctionJsonbParseState*>(
                            context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

                    if (!state->default_value_parser.parse(default_value.data,
                                                           default_value.size)) {
                        error = state->default_value_parser.getErrorCode();
                        return Status::InvalidArgument(
                                "invalid default json value: {} , error: {}",
                                std::string_view(default_value.data, default_value.size),
                                JsonbErrMsg::getErrMsg(error));
                    }
                    state->has_const_default_value = true;
                }
            }
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const IColumn& col_from = *(block.get_by_position(arguments[0]).column);

        auto null_map = ColumnUInt8::create(0, 0);
        bool is_nullable = false;
        switch (nullable_mode) {
        case NullalbeMode::NULLABLE: {
            is_nullable = true;
            null_map = ColumnUInt8::create(input_rows_count, 0);
            break;
        }
        case NullalbeMode::NOT_NULL:
            is_nullable = false;
            break;
        case NullalbeMode::FOLLOW_INPUT: {
            auto argument_column = col_from.convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_column)) {
                is_nullable = true;
                null_map = ColumnUInt8::create(input_rows_count, 0);
                // Danger: Here must dispose the null map data first! Because
                // argument_columns[i]=nullable->get_nested_column_ptr(); will release the mem
                // of column nullable mem of null map
                VectorizedUtils::update_null_map(null_map->get_data(),
                                                 nullable->get_null_map_data());
                argument_column = nullable->get_nested_column_ptr();
            }
            break;
        }
        }

        // const auto& col_with_type_and_name = block.get_by_position(arguments[0]);

        // const IColumn& col_from = *col_with_type_and_name.column;

        const ColumnString* col_from_string = check_and_get_column<ColumnString>(col_from);
        if (auto* nullable = check_and_get_column<ColumnNullable>(col_from)) {
            col_from_string =
                    check_and_get_column<ColumnString>(*nullable->get_nested_column_ptr());
        }

        if (!col_from_string) {
            return Status::RuntimeError("Illegal column {} should be ColumnString",
                                        col_from.get_name());
        }

        auto col_to = ColumnString::create();

        //IColumn & col_to = *res;
        size_t size = col_from.size();
        col_to->reserve(size);

        // parser can be reused for performance
        JsonbParser parser;
        JsonbErrType error = JsonbErrType::E_NONE;

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (col_from.is_null_at(i)) {
                null_map->get_data()[i] = 1;
                col_to->insert_data("", 0);
                continue;
            }

            const auto& val = col_from_string->get_data_at(i);
            if (parser.parse(val.data, val.size)) {
                // insert jsonb format data
                col_to->insert_data(parser.getWriter().getOutput()->getBuffer(),
                                    (size_t)parser.getWriter().getOutput()->getSize());
            } else {
                error = parser.getErrorCode();
                LOG(WARNING) << "json parse error: " << JsonbErrMsg::getErrMsg(error)
                             << " for value: " << std::string_view(val.data, val.size);

                switch (parse_error_handle_mode) {
                case JsonbParseErrorMode::FAIL:
                    return Status::InvalidArgument("json parse error: {} for value: {}",
                                                   JsonbErrMsg::getErrMsg(error),
                                                   std::string_view(val.data, val.size));
                case JsonbParseErrorMode::RETURN_NULL: {
                    if (is_nullable) {
                        null_map->get_data()[i] = 1;
                    }
                    col_to->insert_data("", 0);
                    continue;
                }
                case JsonbParseErrorMode::RETURN_VALUE: {
                    FunctionJsonbParseState* state = reinterpret_cast<FunctionJsonbParseState*>(
                            context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
                    if (state->has_const_default_value) {
                        col_to->insert_data(
                                state->default_value_parser.getWriter().getOutput()->getBuffer(),
                                (size_t)state->default_value_parser.getWriter()
                                        .getOutput()
                                        ->getSize());
                    } else {
                        auto val = block.get_by_position(arguments[1]).column->get_data_at(i);
                        if (parser.parse(val.data, val.size)) {
                            // insert jsonb format data
                            col_to->insert_data(parser.getWriter().getOutput()->getBuffer(),
                                                (size_t)parser.getWriter().getOutput()->getSize());
                        } else {
                            return Status::InvalidArgument(
                                    "json parse error: {} for default value: {}",
                                    JsonbErrMsg::getErrMsg(error),
                                    std::string_view(val.data, val.size));
                        }
                    }
                    continue;
                }
                case JsonbParseErrorMode::RETURN_INVALID:
                    col_to->insert_data("", 0);
                    continue;
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
using FunctionJsonbParseErrorInvalid =
        FunctionJsonbParseBase<NullalbeMode::FOLLOW_INPUT, JsonbParseErrorMode::RETURN_INVALID>;

// jsonb_parse return type is nullable
using FunctionJsonbParseNullable =
        FunctionJsonbParseBase<NullalbeMode::NULLABLE, JsonbParseErrorMode::FAIL>;
using FunctionJsonbParseNullableErrorNull =
        FunctionJsonbParseBase<NullalbeMode::NULLABLE, JsonbParseErrorMode::RETURN_NULL>;
using FunctionJsonbParseNullableErrorValue =
        FunctionJsonbParseBase<NullalbeMode::NULLABLE, JsonbParseErrorMode::RETURN_VALUE>;
using FunctionJsonbParseNullableErrorInvalid =
        FunctionJsonbParseBase<NullalbeMode::NULLABLE, JsonbParseErrorMode::RETURN_INVALID>;

// jsonb_parse return type is not nullable
using FunctionJsonbParseNotnull =
        FunctionJsonbParseBase<NullalbeMode::NOT_NULL, JsonbParseErrorMode::FAIL>;
using FunctionJsonbParseNotnullErrorValue =
        FunctionJsonbParseBase<NullalbeMode::NOT_NULL, JsonbParseErrorMode::RETURN_VALUE>;
using FunctionJsonbParseNotnullErrorInvalid =
        FunctionJsonbParseBase<NullalbeMode::NOT_NULL, JsonbParseErrorMode::RETURN_INVALID>;

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
                        size_t result, size_t input_rows_count) const override {
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        DCHECK_GE(arguments.size(), 2);

        ColumnPtr jsonb_data_column;
        bool jsonb_data_const = false;
        // prepare jsonb data column
        std::tie(jsonb_data_column, jsonb_data_const) =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        check_set_nullable(jsonb_data_column, null_map, jsonb_data_const);
        auto& ldata = assert_cast<const ColumnString*>(jsonb_data_column.get())->get_chars();
        auto& loffsets = assert_cast<const ColumnString*>(jsonb_data_column.get())->get_offsets();

        // prepare parse path column prepare
        std::vector<const ColumnString*> jsonb_path_columns;
        std::vector<bool> path_const(arguments.size() - 1);
        for (int i = 0; i < arguments.size() - 1; ++i) {
            ColumnPtr path_column;
            bool is_const = false;
            std::tie(path_column, is_const) =
                    unpack_if_const(block.get_by_position(arguments[i + 1]).column);
            path_const[i] = is_const;
            check_set_nullable(path_column, null_map, path_const[i]);
            jsonb_path_columns.push_back(assert_cast<const ColumnString*>(path_column.get()));
        }

        auto res = Impl::ColumnType::create();

        bool is_invalid_json_path = false;

        // execute Impl
        if constexpr (std::is_same_v<typename Impl::ReturnType, DataTypeString> ||
                      std::is_same_v<typename Impl::ReturnType, DataTypeJsonb>) {
            auto& res_data = res->get_chars();
            auto& res_offsets = res->get_offsets();
            Status st = Impl::vector_vector_v2(
                    context, ldata, loffsets, jsonb_data_const, jsonb_path_columns, path_const,
                    res_data, res_offsets, null_map->get_data(), is_invalid_json_path);
            if (!st.ok()) {
                return st;
            }
        } else {
            // not support other extract type for now (e.g. int, double, ...)
            DCHECK_EQ(jsonb_path_columns.size(), 1);
            auto& rdata = jsonb_path_columns[0]->get_chars();
            auto& roffsets = jsonb_path_columns[0]->get_offsets();
            if (jsonb_data_const) {
                Impl::scalar_vector(context, jsonb_data_column->get_data_at(0), rdata, roffsets,
                                    res->get_data(), null_map->get_data(), is_invalid_json_path);
            } else if (path_const[0]) {
                Impl::vector_scalar(context, ldata, loffsets, jsonb_path_columns[0]->get_data_at(0),
                                    res->get_data(), null_map->get_data(), is_invalid_json_path);
            } else {
                Impl::vector_vector(context, ldata, loffsets, rdata, roffsets, res->get_data(),
                                    null_map->get_data(), is_invalid_json_path);
            }
            if (is_invalid_json_path) {
                return Status::InvalidArgument(
                        "Json path error: {} for value: {}",
                        JsonbErrMsg::getErrMsg(JsonbErrType::E_INVALID_JSON_PATH),
                        std::string_view(reinterpret_cast<const char*>(rdata.data()),
                                         rdata.size()));
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
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
                                              const std::unique_ptr<JsonbWriter>& writer,
                                              std::unique_ptr<JsonbToJson>& formater,
                                              const char* l_raw, int l_size, JsonbPath& path) {
        if (null_map[i]) {
            StringOP::push_null_string(i, res_data, res_offsets, null_map);
            return;
        }

        // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
        JsonbDocument* doc = JsonbDocument::createDocument(l_raw, l_size);
        if (UNLIKELY(!doc || !doc->getValue())) {
            StringOP::push_null_string(i, res_data, res_offsets, null_map);
            return;
        }

        // value is NOT necessary to be deleted since JsonbValue will not allocate memory
        JsonbValue* value = doc->getValue()->findValue(path, nullptr);

        if (UNLIKELY(!value)) {
            StringOP::push_null_string(i, res_data, res_offsets, null_map);
            return;
        }

        if constexpr (ValueType::only_get_type) {
            StringOP::push_value_string(std::string_view(value->typeName()), i, res_data,
                                        res_offsets);
            return;
        }

        if constexpr (std::is_same_v<DataTypeJsonb, ReturnType>) {
            writer->reset();
            writer->writeValue(value);
            StringOP::push_value_string(std::string_view(writer->getOutput()->getBuffer(),
                                                         writer->getOutput()->getSize()),
                                        i, res_data, res_offsets);
        } else {
            if (LIKELY(value->isString())) {
                auto str_value = (JsonbStringVal*)value;
                StringOP::push_value_string(
                        std::string_view(str_value->getBlob(), str_value->length()), i, res_data,
                        res_offsets);
            } else if (value->isNull()) {
                StringOP::push_value_string("null", i, res_data, res_offsets);
            } else if (value->isTrue()) {
                StringOP::push_value_string("true", i, res_data, res_offsets);
            } else if (value->isFalse()) {
                StringOP::push_value_string("false", i, res_data, res_offsets);
            } else if (value->isInt8()) {
                StringOP::push_value_string(std::to_string(((const JsonbInt8Val*)value)->val()), i,
                                            res_data, res_offsets);
            } else if (value->isInt16()) {
                StringOP::push_value_string(std::to_string(((const JsonbInt16Val*)value)->val()), i,
                                            res_data, res_offsets);
            } else if (value->isInt32()) {
                StringOP::push_value_string(std::to_string(((const JsonbInt32Val*)value)->val()), i,
                                            res_data, res_offsets);
            } else if (value->isInt64()) {
                StringOP::push_value_string(std::to_string(((const JsonbInt64Val*)value)->val()), i,
                                            res_data, res_offsets);
            } else {
                if (!formater) {
                    formater.reset(new JsonbToJson());
                }
                StringOP::push_value_string(formater->to_json_string(value), i, res_data,
                                            res_offsets);
            }
        }
    }

public:
    // for jsonb_extract_string
    static Status vector_vector_v2(
            FunctionContext* context, const ColumnString::Chars& ldata,
            const ColumnString::Offsets& loffsets, const bool& json_data_const,
            const std::vector<const ColumnString*>& rdata_columns, // here we can support more paths
            const std::vector<bool>& path_const, ColumnString::Chars& res_data,
            ColumnString::Offsets& res_offsets, NullMap& null_map, bool& is_invalid_json_path) {
        size_t input_rows_count = json_data_const ? rdata_columns.size() : loffsets.size();
        res_offsets.resize(input_rows_count);

        auto writer = std::make_unique<JsonbWriter>();
        std::unique_ptr<JsonbToJson> formater;

        // reuseable json path list, espacially for const path
        std::vector<JsonbPath> json_path_list;
        json_path_list.resize(rdata_columns.size());

        // lambda function to parse json path for row i and path pi
        auto parse_json_path = [&](size_t i, size_t pi) -> Status {
            const ColumnString* path_col = rdata_columns[pi];
            const ColumnString::Chars& rdata = path_col->get_chars();
            const ColumnString::Offsets& roffsets = path_col->get_offsets();
            size_t r_off = roffsets[index_check_const(i, path_const[pi]) - 1];
            size_t r_size = roffsets[index_check_const(i, path_const[pi])] - r_off;
            const char* r_raw = reinterpret_cast<const char*>(&rdata[r_off]);

            JsonbPath path;
            if (!path.seek(r_raw, r_size)) {
                return Status::InvalidArgument(
                        "Json path error: {} for value: {}",
                        JsonbErrMsg::getErrMsg(JsonbErrType::E_INVALID_JSON_PATH),
                        std::string_view(reinterpret_cast<const char*>(rdata.data()),
                                         rdata.size()));
            }

            json_path_list[pi] = std::move(path);

            return Status::OK();
        };

        for (size_t pi = 0; pi < rdata_columns.size(); pi++) {
            if (path_const[pi]) {
                RETURN_IF_ERROR(parse_json_path(0, pi));
            }
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                continue;
            }
            size_t l_off = loffsets[index_check_const(i, json_data_const) - 1];
            size_t l_size = loffsets[index_check_const(i, json_data_const)] - l_off;
            const char* l_raw = reinterpret_cast<const char*>(&ldata[l_off]);
            if (rdata_columns.size() == 1) { // just return origin value
                if (!path_const[0]) {
                    RETURN_IF_ERROR(parse_json_path(i, 0));
                }
                inner_loop_impl(i, res_data, res_offsets, null_map, writer, formater, l_raw, l_size,
                                json_path_list[0]);
            } else { // will make array string to user
                writer->reset();
                writer->writeStartArray();

                // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
                JsonbDocument* doc = JsonbDocument::createDocument(l_raw, l_size);

                for (size_t pi = 0; pi < rdata_columns.size(); ++pi) {
                    if (UNLIKELY(!doc || !doc->getValue())) {
                        writer->writeNull();
                        continue;
                    }

                    if (!path_const[pi]) {
                        RETURN_IF_ERROR(parse_json_path(i, pi));
                    }

                    // value is NOT necessary to be deleted since JsonbValue will not allocate memory
                    JsonbValue* value = doc->getValue()->findValue(json_path_list[pi], nullptr);

                    if (UNLIKELY(!value)) {
                        writer->writeNull();
                    } else {
                        writer->writeValue(value);
                    }
                }
                writer->writeEndArray();
                StringOP::push_value_string(std::string_view(writer->getOutput()->getBuffer(),
                                                             writer->getOutput()->getSize()),
                                            i, res_data, res_offsets);
            }
        } //for
        return Status::OK();
    }

    static void vector_vector(FunctionContext* context, const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, ColumnString::Chars& res_data,
                              ColumnString::Offsets& res_offsets, NullMap& null_map,
                              bool& is_invalid_json_path) {
        size_t input_rows_count = loffsets.size();
        res_offsets.resize(input_rows_count);

        std::unique_ptr<JsonbWriter> writer;
        if constexpr (std::is_same_v<DataTypeJsonb, ReturnType>) {
            writer.reset(new JsonbWriter());
        }

        std::unique_ptr<JsonbToJson> formater;

        for (size_t i = 0; i < input_rows_count; ++i) {
            int l_size = loffsets[i] - loffsets[i - 1];
            const char* l_raw = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);

            int r_size = roffsets[i] - roffsets[i - 1];
            const char* r_raw = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);

            JsonbPath path;
            if (!path.seek(r_raw, r_size)) {
                is_invalid_json_path = true;
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                return;
            }

            inner_loop_impl(i, res_data, res_offsets, null_map, writer, formater, l_raw, l_size,
                            path);
        } //for
    }     //function
    static void vector_scalar(FunctionContext* context, const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets, const StringRef& rdata,
                              ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets,
                              NullMap& null_map, bool& is_invalid_json_path) {
        size_t input_rows_count = loffsets.size();
        res_offsets.resize(input_rows_count);

        std::unique_ptr<JsonbWriter> writer;
        if constexpr (std::is_same_v<DataTypeJsonb, ReturnType>) {
            writer.reset(new JsonbWriter());
        }

        std::unique_ptr<JsonbToJson> formater;

        JsonbPath path;
        if (!path.seek(rdata.data, rdata.size)) {
            is_invalid_json_path = true;
            return;
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            int l_size = loffsets[i] - loffsets[i - 1];
            const char* l_raw = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);

            inner_loop_impl(i, res_data, res_offsets, null_map, writer, formater, l_raw, l_size,
                            path);
        } //for
    }     //function
    static void scalar_vector(FunctionContext* context, const StringRef& ldata,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, ColumnString::Chars& res_data,
                              ColumnString::Offsets& res_offsets, NullMap& null_map,
                              bool& is_invalid_json_path) {
        size_t input_rows_count = roffsets.size();
        res_offsets.resize(input_rows_count);

        std::unique_ptr<JsonbWriter> writer;
        if constexpr (std::is_same_v<DataTypeJsonb, ReturnType>) {
            writer.reset(new JsonbWriter());
        }

        std::unique_ptr<JsonbToJson> formater;

        for (size_t i = 0; i < input_rows_count; ++i) {
            int r_size = roffsets[i] - roffsets[i - 1];
            const char* r_raw = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);

            JsonbPath path;
            if (!path.seek(r_raw, r_size)) {
                is_invalid_json_path = true;
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                return;
            }

            inner_loop_impl(i, res_data, res_offsets, null_map, writer, formater, ldata.data,
                            ldata.size, path);
        } //for
    }     //function
};

template <typename ValueType>
struct JsonbExtractImpl {
    using ReturnType = typename ValueType::ReturnType;
    using ColumnType = typename ValueType::ColumnType;
    using Container = typename ColumnType::Container;
    static const bool only_check_exists = ValueType::only_check_exists;

private:
    static ALWAYS_INLINE void inner_loop_impl(size_t i, Container& res, NullMap& null_map,
                                              const char* l_raw_str, int l_str_size,
                                              JsonbPath& path) {
        if (null_map[i]) {
            res[i] = 0;
            return;
        }

        // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
        JsonbDocument* doc = JsonbDocument::createDocument(l_raw_str, l_str_size);
        if (UNLIKELY(!doc || !doc->getValue())) {
            null_map[i] = 1;
            res[i] = 0;
            return;
        }

        // value is NOT necessary to be deleted since JsonbValue will not allocate memory
        JsonbValue* value = doc->getValue()->findValue(path, nullptr);

        if (UNLIKELY(!value)) {
            if constexpr (!only_check_exists) {
                null_map[i] = 1;
            }
            res[i] = 0;
            return;
        }

        // if only check path exists, it's true here and skip check value
        if constexpr (only_check_exists) {
            res[i] = 1;
            return;
        }

        if constexpr (std::is_same_v<void, typename ValueType::T>) {
            if (value->isNull()) {
                res[i] = 1;
            } else {
                res[i] = 0;
            }
        } else if constexpr (std::is_same_v<bool, typename ValueType::T>) {
            if (value->isTrue()) {
                res[i] = 1;
            } else if (value->isFalse()) {
                res[i] = 0;
            } else {
                null_map[i] = 1;
                res[i] = 0;
            }
        } else if constexpr (std::is_same_v<int32_t, typename ValueType::T>) {
            if (value->isInt8() || value->isInt16() || value->isInt32()) {
                res[i] = (int32_t)((const JsonbIntVal*)value)->val();
            } else {
                null_map[i] = 1;
                res[i] = 0;
            }
        } else if constexpr (std::is_same_v<int64_t, typename ValueType::T>) {
            if (value->isInt8() || value->isInt16() || value->isInt32() || value->isInt64()) {
                res[i] = (int64_t)((const JsonbIntVal*)value)->val();
            } else {
                null_map[i] = 1;
                res[i] = 0;
            }
        } else if constexpr (std::is_same_v<int128_t, typename ValueType::T>) {
            if (value->isInt8() || value->isInt16() || value->isInt32() || value->isInt64() ||
                value->isInt128()) {
                res[i] = (int128_t)((const JsonbIntVal*)value)->val();
            } else {
                null_map[i] = 1;
                res[i] = 0;
            }
        } else if constexpr (std::is_same_v<double, typename ValueType::T>) {
            if (value->isDouble()) {
                res[i] = ((const JsonbDoubleVal*)value)->val();
            } else if (value->isInt8() || value->isInt16() || value->isInt32() ||
                       value->isInt64()) {
                res[i] = ((const JsonbIntVal*)value)->val();
            } else {
                null_map[i] = 1;
                res[i] = 0;
            }
        } else {
            LOG(FATAL) << "unexpected type ";
        }
    }

public:
    // for jsonb_extract_int/int64/double
    static void vector_vector(FunctionContext* context, const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, Container& res,
                              NullMap& null_map, bool& is_invalid_json_path) {
        size_t size = loffsets.size();
        res.resize(size);

        for (size_t i = 0; i < loffsets.size(); i++) {
            if constexpr (only_check_exists) {
                res[i] = 0;
            }

            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1];

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1];

            JsonbPath path;
            if (!path.seek(r_raw_str, r_str_size)) {
                is_invalid_json_path = true;
                res[i] = 0;
                return;
            }

            inner_loop_impl(i, res, null_map, l_raw_str, l_str_size, path);
        } //for
    }     //function
    static void scalar_vector(FunctionContext* context, const StringRef& ldata,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, Container& res,
                              NullMap& null_map, bool& is_invalid_json_path) {
        size_t size = roffsets.size();
        res.resize(size);

        for (size_t i = 0; i < size; i++) {
            if constexpr (only_check_exists) {
                res[i] = 0;
            }

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1];

            JsonbPath path;
            if (!path.seek(r_raw_str, r_str_size)) {
                is_invalid_json_path = true;
                res[i] = 0;
                return;
            }

            inner_loop_impl(i, res, null_map, ldata.data, ldata.size, path);
        } //for
    }     //function
    static void vector_scalar(FunctionContext* context, const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets, const StringRef& rdata,
                              Container& res, NullMap& null_map, bool& is_invalid_json_path) {
        size_t size = loffsets.size();
        res.resize(size);

        JsonbPath path;
        if (!path.seek(rdata.data, rdata.size)) {
            is_invalid_json_path = true;
            return;
        }

        for (size_t i = 0; i < loffsets.size(); i++) {
            if constexpr (only_check_exists) {
                res[i] = 0;
            }

            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1];

            inner_loop_impl(i, res, null_map, l_raw_str, l_str_size, path);
        } //for
    }     //function
};

struct JsonbTypeExists {
    using T = uint8_t;
    using ReturnType = DataTypeUInt8;
    using ColumnType = ColumnVector<T>;
    static const bool only_check_exists = true;
};

struct JsonbTypeNull {
    using T = void;
    using ReturnType = DataTypeUInt8;
    using ColumnType = ColumnVector<uint8_t>;
    static const bool only_check_exists = false;
};

struct JsonbTypeBool {
    using T = bool;
    using ReturnType = DataTypeUInt8;
    using ColumnType = ColumnVector<uint8_t>;
    static const bool only_check_exists = false;
};

struct JsonbTypeInt {
    using T = int32_t;
    using ReturnType = DataTypeInt32;
    using ColumnType = ColumnVector<T>;
    static const bool only_check_exists = false;
};

struct JsonbTypeInt64 {
    using T = int64_t;
    using ReturnType = DataTypeInt64;
    using ColumnType = ColumnVector<T>;
    static const bool only_check_exists = false;
};

struct JsonbTypeInt128 {
    using T = int128_t;
    using ReturnType = DataTypeInt128;
    using ColumnType = ColumnVector<T>;
    static const bool only_check_exists = false;
};

struct JsonbTypeDouble {
    using T = double;
    using ReturnType = DataTypeFloat64;
    using ColumnType = ColumnVector<T>;
    static const bool only_check_exists = false;
};

struct JsonbTypeString {
    using T = std::string;
    using ReturnType = DataTypeString;
    using ColumnType = ColumnString;
    static const bool only_check_exists = false;
    static const bool only_get_type = false;
};

struct JsonbTypeJson {
    using T = std::string;
    using ReturnType = DataTypeJsonb;
    using ColumnType = ColumnString;
    static const bool only_check_exists = false;
    static const bool only_get_type = false;
};

struct JsonbTypeType {
    using T = std::string;
    using ReturnType = DataTypeString;
    using ColumnType = ColumnString;
    static const bool only_check_exists = false;
    static const bool only_get_type = true;
};

struct JsonbExists : public JsonbExtractImpl<JsonbTypeExists> {
    static constexpr auto name = "json_exists_path";
    static constexpr auto alias = "jsonb_exists_path";
};

struct JsonbExtractIsnull : public JsonbExtractImpl<JsonbTypeNull> {
    static constexpr auto name = "json_extract_isnull";
    static constexpr auto alias = "jsonb_extract_isnull";
};

struct JsonbExtractBool : public JsonbExtractImpl<JsonbTypeBool> {
    static constexpr auto name = "json_extract_bool";
    static constexpr auto alias = "jsonb_extract_bool";
};

struct JsonbExtractInt : public JsonbExtractImpl<JsonbTypeInt> {
    static constexpr auto name = "json_extract_int";
    static constexpr auto alias = "jsonb_extract_int";
    static constexpr auto name2 = "get_json_int";
    static DataTypes get_variadic_argument_types_impl() {
        return {std::make_shared<DataTypeJsonb>(), std::make_shared<DataTypeString>()};
    }
};

struct JsonbExtractBigInt : public JsonbExtractImpl<JsonbTypeInt64> {
    static constexpr auto name = "json_extract_bigint";
    static constexpr auto alias = "jsonb_extract_bigint";
    static constexpr auto name2 = "get_json_bigint";
    static DataTypes get_variadic_argument_types_impl() {
        return {std::make_shared<DataTypeJsonb>(), std::make_shared<DataTypeString>()};
    }
};

struct JsonbExtractLargeInt : public JsonbExtractImpl<JsonbTypeInt128> {
    static constexpr auto name = "json_extract_largeint";
    static constexpr auto alias = "jsonb_extract_largeint";
};

struct JsonbExtractDouble : public JsonbExtractImpl<JsonbTypeDouble> {
    static constexpr auto name = "json_extract_double";
    static constexpr auto alias = "jsonb_extract_double";
    static constexpr auto name2 = "get_json_double";
    static DataTypes get_variadic_argument_types_impl() {
        return {std::make_shared<DataTypeJsonb>(), std::make_shared<DataTypeString>()};
    }
};

struct JsonbExtractString : public JsonbExtractStringImpl<JsonbTypeString> {
    static constexpr auto name = "json_extract_string";
    static constexpr auto alias = "jsonb_extract_string";
    static constexpr auto name2 = "get_json_string";
    static DataTypes get_variadic_argument_types_impl() {
        return {std::make_shared<DataTypeJsonb>(), std::make_shared<DataTypeString>()};
    }
};

struct JsonbExtractJsonb : public JsonbExtractStringImpl<JsonbTypeJson> {
    static constexpr auto name = "jsonb_extract";
    static constexpr auto alias = "jsonb_extract";
};

struct JsonbType : public JsonbExtractStringImpl<JsonbTypeType> {
    static constexpr auto name = "json_type";
    static constexpr auto alias = "jsonb_type";
};

using FunctionJsonbExists = FunctionJsonbExtract<JsonbExists>;
using FunctionJsonbType = FunctionJsonbExtract<JsonbType>;

using FunctionJsonbExtractIsnull = FunctionJsonbExtract<JsonbExtractIsnull>;
using FunctionJsonbExtractBool = FunctionJsonbExtract<JsonbExtractBool>;
using FunctionJsonbExtractInt = FunctionJsonbExtract<JsonbExtractInt>;
using FunctionJsonbExtractBigInt = FunctionJsonbExtract<JsonbExtractBigInt>;
using FunctionJsonbExtractLargeInt = FunctionJsonbExtract<JsonbExtractLargeInt>;
using FunctionJsonbExtractDouble = FunctionJsonbExtract<JsonbExtractDouble>;
using FunctionJsonbExtractString = FunctionJsonbExtract<JsonbExtractString>;
using FunctionJsonbExtractJsonb = FunctionJsonbExtract<JsonbExtractJsonb>;

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
                        size_t result, size_t input_rows_count) const override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

struct JsonbLengthUtil {
    static Status jsonb_length_execute(FunctionContext* context, Block& block,
                                       const ColumnNumbers& arguments, size_t result,
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
        JsonbPath path;
        if (is_const) {
            auto path_value = path_column->get_data_at(0);
            if (!path.seek(path_value.data, path_value.size)) {
                return Status::InvalidArgument(
                        "Json path error: {} for value: {}",
                        JsonbErrMsg::getErrMsg(JsonbErrType::E_INVALID_JSON_PATH),
                        std::string_view(reinterpret_cast<const char*>(path_value.data),
                                         path_value.size));
            }
        }
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (jsonb_data_column->is_null_at(i) || path_column->is_null_at(i)) {
                null_map->get_data()[i] = 1;
                res->insert_data(nullptr, 0);
                continue;
            }
            if (!is_const) {
                auto path_value = path_column->get_data_at(i);
                path.clean();
                if (!path.seek(path_value.data, path_value.size)) {
                    return Status::InvalidArgument(
                            "Json path error: {} for value: {}",
                            JsonbErrMsg::getErrMsg(JsonbErrType::E_INVALID_JSON_PATH),
                            std::string_view(reinterpret_cast<const char*>(path_value.data),
                                             path_value.size));
                }
            }
            auto jsonb_value = jsonb_data_column->get_data_at(i);
            // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
            JsonbDocument* doc = JsonbDocument::createDocument(jsonb_value.data, jsonb_value.size);
            JsonbValue* value = doc->getValue()->findValue(path, nullptr);
            if (UNLIKELY(jsonb_value.size == 0 || !value)) {
                null_map->get_data()[i] = 1;
                res->insert_data(nullptr, 0);
                continue;
            }
            auto length = value->length();
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
                               const ColumnNumbers& arguments, size_t result,
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
                               const ColumnNumbers& arguments, size_t result,
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
                        size_t result, size_t input_rows_count) const override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

struct JsonbContainsUtil {
    static Status jsonb_contains_execute(FunctionContext* context, Block& block,
                                         const ColumnNumbers& arguments, size_t result,
                                         size_t input_rows_count) {
        DCHECK_GE(arguments.size(), 3);

        auto jsonb_data1_column = block.get_by_position(arguments[0]).column;
        auto jsonb_data2_column = block.get_by_position(arguments[1]).column;

        ColumnPtr path_column;
        bool is_const = false;
        std::tie(path_column, is_const) =
                unpack_if_const(block.get_by_position(arguments[2]).column);

        JsonbPath path;
        if (is_const) {
            auto path_value = path_column->get_data_at(0);
            if (!path.seek(path_value.data, path_value.size)) {
                return Status::InvalidArgument(
                        "Json path error: {} for value: {}",
                        JsonbErrMsg::getErrMsg(JsonbErrType::E_INVALID_JSON_PATH),
                        std::string_view(reinterpret_cast<const char*>(path_value.data),
                                         path_value.size));
            }
        }
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        auto return_type = block.get_data_type(result);
        MutableColumnPtr res = return_type->create_column();

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
                            "Json path error: {} for value: {}",
                            JsonbErrMsg::getErrMsg(JsonbErrType::E_INVALID_JSON_PATH),
                            std::string_view(reinterpret_cast<const char*>(path_value.data),
                                             path_value.size));
                }
            }

            auto jsonb_value1 = jsonb_data1_column->get_data_at(i);
            auto jsonb_value2 = jsonb_data2_column->get_data_at(i);

            // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
            JsonbDocument* doc1 =
                    JsonbDocument::createDocument(jsonb_value1.data, jsonb_value1.size);
            JsonbDocument* doc2 =
                    JsonbDocument::createDocument(jsonb_value2.data, jsonb_value2.size);

            JsonbValue* value1 = doc1->getValue()->findValue(path, nullptr);
            JsonbValue* value2 = doc2->getValue();
            if (UNLIKELY(jsonb_value1.size == 0 || jsonb_value2.size == 0 || !value1 || !value2)) {
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

struct JsonbContainsImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeJsonb>(), std::make_shared<DataTypeJsonb>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
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
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        return JsonbContainsUtil::jsonb_contains_execute(context, block, arguments, result,
                                                         input_rows_count);
    }
};

void register_function_jsonb(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionJsonbParse>(FunctionJsonbParse::name);
    factory.register_alias(FunctionJsonbParse::name, FunctionJsonbParse::alias);
    factory.register_function<FunctionJsonbParseErrorNull>("json_parse_error_to_null");
    factory.register_alias("json_parse_error_to_null", "jsonb_parse_error_to_null");
    factory.register_function<FunctionJsonbParseErrorValue>("json_parse_error_to_value");
    factory.register_alias("json_parse_error_to_value", "jsonb_parse_error_to_value");
    factory.register_function<FunctionJsonbParseErrorInvalid>("json_parse_error_to_invalid");
    factory.register_alias("json_parse_error_to_invalid", "jsonb_parse_error_to_invalid");

    factory.register_function<FunctionJsonbParseNullable>("json_parse_nullable");
    factory.register_alias("json_parse_nullable", "jsonb_parse_nullable");
    factory.register_function<FunctionJsonbParseNullableErrorNull>(
            "json_parse_nullable_error_to_null");
    factory.register_alias("json_parse_nullable_error_to_null",
                           "jsonb_parse_nullable_error_to_null");
    factory.register_function<FunctionJsonbParseNullableErrorValue>(
            "json_parse_nullable_error_to_value");
    factory.register_alias("json_parse_nullable_error_to_value",
                           "jsonb_parse_nullable_error_to_value");
    factory.register_function<FunctionJsonbParseNullableErrorInvalid>(
            "json_parse_nullable_error_to_invalid");
    factory.register_alias("json_parse_nullable_error_to_invalid",
                           "json_parse_nullable_error_to_invalid");

    factory.register_function<FunctionJsonbParseNotnull>("json_parse_notnull");
    factory.register_alias("json_parse_notnull", "jsonb_parse_notnull");
    factory.register_function<FunctionJsonbParseNotnullErrorValue>(
            "json_parse_notnull_error_to_value");
    factory.register_alias("json_parse_notnull", "jsonb_parse_notnull");
    factory.register_function<FunctionJsonbParseNotnullErrorInvalid>(
            "json_parse_notnull_error_to_invalid");
    factory.register_alias("json_parse_notnull_error_to_invalid",
                           "jsonb_parse_notnull_error_to_invalid");

    factory.register_function<FunctionJsonbExists>();
    factory.register_alias(FunctionJsonbExists::name, FunctionJsonbExists::alias);
    factory.register_function<FunctionJsonbType>();
    factory.register_alias(FunctionJsonbType::name, FunctionJsonbType::alias);

    factory.register_function<FunctionJsonbExtractIsnull>();
    factory.register_alias(FunctionJsonbExtractIsnull::name, FunctionJsonbExtractIsnull::alias);
    factory.register_function<FunctionJsonbExtractBool>();
    factory.register_alias(FunctionJsonbExtractBool::name, FunctionJsonbExtractBool::alias);
    factory.register_function<FunctionJsonbExtractInt>();
    factory.register_alias(FunctionJsonbExtractInt::name, FunctionJsonbExtractInt::alias);
    factory.register_function<FunctionJsonbExtractInt>(JsonbExtractInt::name2);
    factory.register_function<FunctionJsonbExtractBigInt>();
    factory.register_alias(FunctionJsonbExtractBigInt::name, FunctionJsonbExtractBigInt::alias);
    factory.register_function<FunctionJsonbExtractBigInt>(JsonbExtractBigInt::name2);
    factory.register_function<FunctionJsonbExtractLargeInt>();
    factory.register_alias(FunctionJsonbExtractLargeInt::name, FunctionJsonbExtractLargeInt::alias);
    factory.register_function<FunctionJsonbExtractDouble>();
    factory.register_alias(FunctionJsonbExtractDouble::name, FunctionJsonbExtractDouble::alias);
    factory.register_function<FunctionJsonbExtractDouble>(JsonbExtractDouble::name2);
    factory.register_function<FunctionJsonbExtractString>();
    factory.register_alias(FunctionJsonbExtractString::name, FunctionJsonbExtractString::alias);
    factory.register_function<FunctionJsonbExtractString>(JsonbExtractString::name2);
    factory.register_function<FunctionJsonbExtractJsonb>();
    // factory.register_alias(FunctionJsonbExtractJsonb::name, FunctionJsonbExtractJsonb::alias);

    factory.register_function<FunctionJsonbLength<JsonbLengthImpl>>();
    factory.register_function<FunctionJsonbLength<JsonbLengthAndPathImpl>>();
    factory.register_function<FunctionJsonbContains<JsonbContainsImpl>>();
    factory.register_function<FunctionJsonbContains<JsonbContainsAndPathImpl>>();
}

} // namespace doris::vectorized
