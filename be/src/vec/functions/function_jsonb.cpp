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

#include <boost/token_functions.hpp>
#include <vector>

#include "util/string_parser.hpp"
#include "util/string_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_jsonb.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function_string.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/template_helpers.hpp"

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
    JsonbParser default_value_parser;
    bool has_const_default_value = false;

public:
    static constexpr auto name = "jsonb_parse";
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

    bool use_default_implementation_for_constants() const override { return true; }

    Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if constexpr (parse_error_handle_mode == JsonbParseErrorMode::RETURN_VALUE) {
            if (context->is_col_constant(1)) {
                const auto default_value_col = context->get_constant_col(1)->column_ptr;
                const auto& default_value = default_value_col->get_data_at(0);

                JsonbErrType error = JsonbErrType::E_NONE;
                if (!default_value_parser.parse(default_value.data, default_value.size)) {
                    error = default_value_parser.getErrorCode();
                    return Status::InvalidArgument(
                            "invalid default json value: {} , error: {}",
                            std::string_view(default_value.data, default_value.size),
                            JsonbErrMsg::getErrMsg(error));
                }
                has_const_default_value = true;
            }
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
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

        auto col_to = ColumnJsonb::create();

        //IColumn & col_to = *res;
        size_t size = col_from.size();
        col_to->reserve(size);

        for (size_t i = 0; i < input_rows_count; ++i) {
            if (col_from.is_null_at(i)) {
                null_map->get_data()[i] = 1;
                col_to->insert_data("", 0);
                continue;
            }

            const auto& val = col_from_string->get_data_at(i);
            JsonbParser parser;
            JsonbErrType error = JsonbErrType::E_NONE;
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
                    if (is_nullable) null_map->get_data()[i] = 1;
                    col_to->insert_data("", 0);
                    continue;
                }
                case JsonbParseErrorMode::RETURN_VALUE: {
                    if (has_const_default_value) {
                        col_to->insert_data(
                                default_value_parser.getWriter().getOutput()->getBuffer(),
                                (size_t)default_value_parser.getWriter().getOutput()->getSize());
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

// func(json,string) -> nullable(type)
template <typename Impl>
class FunctionJsonbExtract : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionJsonbExtract>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<typename Impl::ReturnType>());
    }

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        DCHECK_EQ(arguments.size(), 2);
        ColumnPtr argument_columns[2];
        for (int i = 0; i < 2; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[i])) {
                // Danger: Here must dispose the null map data first! Because
                // argument_columns[i]=nullable->get_nested_column_ptr(); will release the mem
                // of column nullable mem of null map
                VectorizedUtils::update_null_map(null_map->get_data(),
                                                 nullable->get_null_map_data());
                argument_columns[i] = nullable->get_nested_column_ptr();
            }
        }

        auto res = Impl::ColumnType::create();

        auto jsonb_data_column = assert_cast<const ColumnJsonb*>(argument_columns[0].get());
        auto jsonb_path_column = assert_cast<const ColumnString*>(argument_columns[1].get());

        auto& ldata = jsonb_data_column->get_chars();
        auto& loffsets = jsonb_data_column->get_offsets();

        auto& rdata = jsonb_path_column->get_chars();
        auto& roffsets = jsonb_path_column->get_offsets();

        // execute Impl
        if constexpr (std::is_same_v<typename Impl::ReturnType, DataTypeString> ||
                      std::is_same_v<typename Impl::ReturnType, DataTypeJsonb>) {
            auto& res_data = res->get_chars();
            auto& res_offsets = res->get_offsets();
            Impl::vector_vector(context, ldata, loffsets, rdata, roffsets, res_data, res_offsets,
                                null_map->get_data());
        } else {
            Impl::vector_vector(context, ldata, loffsets, rdata, roffsets, res->get_data(),
                                null_map->get_data());
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

    // for jsonb_extract_string
    static void vector_vector(FunctionContext* context, const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, ColumnString::Chars& res_data,
                              ColumnString::Offsets& res_offsets, NullMap& null_map) {
        size_t input_rows_count = loffsets.size();
        res_offsets.resize(input_rows_count);

        std::unique_ptr<JsonbWriter> writer;
        if constexpr (std::is_same_v<DataTypeJsonb, ReturnType>) {
            writer.reset(new JsonbWriter());
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            int l_size = loffsets[i] - loffsets[i - 1] - 1;
            const auto l_raw = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);

            int r_size = roffsets[i] - roffsets[i - 1];
            const auto r_raw = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            String path(r_raw, r_size);

            if (null_map[i]) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                continue;
            }

            // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
            JsonbDocument* doc = JsonbDocument::createDocument(l_raw, l_size);
            if (UNLIKELY(!doc || !doc->getValue())) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                continue;
            }

            // value is NOT necessary to be deleted since JsonbValue will not allocate memory
            JsonbValue* value = doc->getValue()->findPath(r_raw, r_size, ".", nullptr);
            if (UNLIKELY(!value)) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map);
                continue;
            }

            if constexpr (ValueType::only_get_type) {
                StringOP::push_value_string(std::string_view(value->typeName()), i, res_data,
                                            res_offsets);
                continue;
            }

            if constexpr (std::is_same_v<DataTypeJsonb, ReturnType>) {
                writer->reset();
                writer->writeValue(value);
                // StringOP::push_value_string(
                //     std::string_view(writer->getOutput()->getBuffer(), writer->getOutput()->getSize()),
                //     i, res_data, res_offsets);
                res_data.insert(writer->getOutput()->getBuffer(),
                                writer->getOutput()->getBuffer() + writer->getOutput()->getSize());
                res_data.push_back('\0');
                res_offsets[i] = res_data.size();
            } else {
                if (LIKELY(value->isString())) {
                    auto str_value = (JsonbStringVal*)value;
                    StringOP::push_value_string(
                            std::string_view(str_value->getBlob(), str_value->length()), i,
                            res_data, res_offsets);
                } else {
                    StringOP::push_null_string(i, res_data, res_offsets, null_map);
                    continue;
                }
            }
        }
    }
};

template <typename ValueType>
struct JsonbExtractImpl {
    using ReturnType = typename ValueType::ReturnType;
    using ColumnType = typename ValueType::ColumnType;
    using Container = typename ColumnType::Container;
    static const bool only_check_exists = ValueType::only_check_exists;

    // for jsonb_extract_int/int64/double
    static void vector_vector(FunctionContext* context, const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, Container& res,
                              NullMap& null_map) {
        size_t size = loffsets.size();
        res.resize(size);

        for (size_t i = 0; i < loffsets.size(); i++) {
            if constexpr (only_check_exists) {
                res[i] = 0;
            }

            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1] - 1;

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1];

            if (null_map[i]) {
                res[i] = 0;
                continue;
            }

            // doc is NOT necessary to be deleted since JsonbDocument will not allocate memory
            JsonbDocument* doc = JsonbDocument::createDocument(l_raw_str, l_str_size);
            if (UNLIKELY(!doc || !doc->getValue())) {
                null_map[i] = 1;
                res[i] = 0;
                continue;
            }

            // value is NOT necessary to be deleted since JsonbValue will not allocate memory
            JsonbValue* value = doc->getValue()->findPath(r_raw_str, r_str_size, ".", nullptr);
            if (UNLIKELY(!value)) {
                null_map[i] = 1;
                res[i] = 0;
                continue;
            }

            // if only check path exists, it's true here and skip check value
            if constexpr (only_check_exists) {
                res[i] = 1;
                continue;
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
                    res[i] = ((const JsonbIntVal*)value)->val();
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
    }
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
    using ColumnType = ColumnJsonb;
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
    static constexpr auto name = "jsonb_exists_path";
};

struct JsonbExtractIsnull : public JsonbExtractImpl<JsonbTypeNull> {
    static constexpr auto name = "jsonb_extract_isnull";
};

struct JsonbExtractBool : public JsonbExtractImpl<JsonbTypeBool> {
    static constexpr auto name = "jsonb_extract_bool";
};

struct JsonbExtractInt : public JsonbExtractImpl<JsonbTypeInt> {
    static constexpr auto name = "jsonb_extract_int";
};

struct JsonbExtractBigInt : public JsonbExtractImpl<JsonbTypeInt64> {
    static constexpr auto name = "jsonb_extract_bigint";
};

struct JsonbExtractDouble : public JsonbExtractImpl<JsonbTypeDouble> {
    static constexpr auto name = "jsonb_extract_double";
};

struct JsonbExtractString : public JsonbExtractStringImpl<JsonbTypeString> {
    static constexpr auto name = "jsonb_extract_string";
};

struct JsonbExtractJsonb : public JsonbExtractStringImpl<JsonbTypeJson> {
    static constexpr auto name = "jsonb_extract";
};

struct JsonbType : public JsonbExtractStringImpl<JsonbTypeType> {
    static constexpr auto name = "jsonb_type";
};

using FunctionJsonbExists = FunctionJsonbExtract<JsonbExists>;
using FunctionJsonbType = FunctionJsonbExtract<JsonbType>;

using FunctionJsonbExtractIsnull = FunctionJsonbExtract<JsonbExtractIsnull>;
using FunctionJsonbExtractBool = FunctionJsonbExtract<JsonbExtractBool>;
using FunctionJsonbExtractInt = FunctionJsonbExtract<JsonbExtractInt>;
using FunctionJsonbExtractBigInt = FunctionJsonbExtract<JsonbExtractBigInt>;
using FunctionJsonbExtractDouble = FunctionJsonbExtract<JsonbExtractDouble>;
using FunctionJsonbExtractString = FunctionJsonbExtract<JsonbExtractString>;
using FunctionJsonbExtractJsonb = FunctionJsonbExtract<JsonbExtractJsonb>;

void register_function_jsonb(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionJsonbParse>("jsonb_parse");
    factory.register_function<FunctionJsonbParseErrorNull>("jsonb_parse_error_to_null");
    factory.register_function<FunctionJsonbParseErrorValue>("jsonb_parse_error_to_value");
    factory.register_function<FunctionJsonbParseErrorInvalid>("jsonb_parse_error_to_invalid");

    factory.register_function<FunctionJsonbParseNullable>("jsonb_parse_nullable");
    factory.register_function<FunctionJsonbParseNullableErrorNull>(
            "jsonb_parse_nullable_error_to_null");
    factory.register_function<FunctionJsonbParseNullableErrorValue>(
            "jsonb_parse_nullable_error_to_value");
    factory.register_function<FunctionJsonbParseNullableErrorInvalid>(
            "jsonb_parse_nullable_error_to_invalid");

    factory.register_function<FunctionJsonbParseNotnull>("jsonb_parse_notnull");
    factory.register_function<FunctionJsonbParseNotnullErrorValue>(
            "jsonb_parse_notnull_error_to_value");
    factory.register_function<FunctionJsonbParseNotnullErrorInvalid>(
            "jsonb_parse_notnull_error_to_invalid");

    factory.register_function<FunctionJsonbExists>();
    factory.register_function<FunctionJsonbType>();

    factory.register_function<FunctionJsonbExtractIsnull>();
    factory.register_function<FunctionJsonbExtractBool>();
    factory.register_function<FunctionJsonbExtractInt>();
    factory.register_function<FunctionJsonbExtractBigInt>();
    factory.register_function<FunctionJsonbExtractDouble>();
    factory.register_function<FunctionJsonbExtractString>();
    factory.register_function<FunctionJsonbExtractJsonb>();
}

} // namespace doris::vectorized
