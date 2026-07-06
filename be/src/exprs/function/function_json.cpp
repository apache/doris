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
#include <rapidjson/allocators.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/value/jsonb_value.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {
struct FunctionJsonQuoteImpl {
    static constexpr auto name = "json_quote";

    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        if (!arguments.empty() && arguments[0] && arguments[0]->is_nullable()) {
            return make_nullable(std::make_shared<DataTypeString>());
        }
        return std::make_shared<DataTypeString>();
    }
    static void execute(const std::vector<const ColumnString*>& data_columns,
                        ColumnString& result_column, size_t input_rows_count) {
        rapidjson::Document document;
        rapidjson::Document::AllocatorType& allocator = document.GetAllocator();

        rapidjson::Value value;

        rapidjson::StringBuffer buf;

        for (int i = 0; i < input_rows_count; i++) {
            StringRef data = data_columns[0]->get_data_at(i);
            value.SetString(data.data, cast_set<rapidjson::SizeType>(data.size), allocator);

            buf.Clear();
            rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
            value.Accept(writer);
            result_column.insert_data(buf.GetString(), buf.GetSize());
        }
    }
};

template <typename Impl>
class FunctionJson : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionJson<Impl>>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return Impl::get_return_type_impl(arguments);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto result_column = ColumnString::create();

        std::vector<ColumnPtr> column_ptrs; // prevent converted column destruct
        std::vector<const ColumnString*> data_columns;
        for (unsigned int argument : arguments) {
            column_ptrs.push_back(
                    block.get_by_position(argument).column->convert_to_full_column_if_const());
            data_columns.push_back(assert_cast<const ColumnString*>(column_ptrs.back().get()));
        }

        Impl::execute(data_columns, *result_column.get(), input_rows_count);
        block.get_by_position(result).column = std::move(result_column);
        return Status::OK();
    }
};

class FunctionJsonValid : public IFunction {
public:
    static constexpr auto name = "json_valid";
    static FunctionPtr create() { return std::make_shared<FunctionJsonValid>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeInt32>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const IColumn& col_from = *(block.get_by_position(arguments[0]).column);

        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        NullMapView input_null_map;
        bool has_input_null_map = false;
        const ColumnString* col_from_string = nullptr;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(col_from)) {
            input_null_map = nullable->get_null_map_data();
            has_input_null_map = true;
            col_from_string =
                    check_and_get_column<ColumnString>(*nullable->get_nested_column_ptr());
        } else {
            col_from_string = check_and_get_column<ColumnString>(col_from);
        }

        if (!col_from_string) {
            return Status::RuntimeError("Illegal column {} should be ColumnString",
                                        col_from.get_name());
        }

        auto col_to = ColumnInt32::create();
        auto& vec_to = col_to->get_data_mutable();
        size_t size = col_from.size();
        vec_to.resize(size);
        auto& null_map_data = null_map->get_data_mutable();

        // parser can be reused for performance

        auto input_type = block.get_by_position(arguments[0]).type->get_primitive_type();

        if (input_type == PrimitiveType::TYPE_VARCHAR || input_type == PrimitiveType::TYPE_CHAR ||
            input_type == PrimitiveType::TYPE_STRING) {
            JsonBinaryValue jsonb_value;
            for (size_t i = 0; i < input_rows_count; ++i) {
                if (has_input_null_map && input_null_map[i]) {
                    null_map_data[i] = 1;
                    vec_to[i] = 0;
                    continue;
                }

                const auto& val = col_from_string->get_data_at(i);
                if (jsonb_value.from_json_string(val.data, cast_set<unsigned int>(val.size)).ok()) {
                    vec_to[i] = 1;
                } else {
                    vec_to[i] = 0;
                }
            }

        } else {
            DCHECK(input_type == PrimitiveType::TYPE_JSONB);
            for (size_t i = 0; i < input_rows_count; ++i) {
                if (has_input_null_map && input_null_map[i]) {
                    null_map_data[i] = 1;
                    vec_to[i] = 0;
                    continue;
                }
                const auto& val = col_from_string->get_data_at(i);
                if (val.size == 0) {
                    vec_to[i] = 0;
                    continue;
                }
                const JsonbDocument* doc = nullptr;
                auto st = JsonbDocument::checkAndCreateDocument(val.data, val.size, &doc);
                if (!st.ok() || !doc || !doc->getValue()) [[unlikely]] {
                    vec_to[i] = 0;
                    continue;
                }
                const JsonbValue* value = doc->getValue();
                if (UNLIKELY(!value)) {
                    vec_to[i] = 0;
                    continue;
                }
                vec_to[i] = 1;
            }
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(col_to), std::move(null_map)));

        return Status::OK();
    }
};
class FunctionJsonUnquote : public IFunction {
public:
    static constexpr auto name = "json_unquote";
    static FunctionPtr create() { return std::make_shared<FunctionJsonUnquote>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const IColumn& col_from = *(block.get_by_position(arguments[0]).column);

        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        const auto* col_from_string = check_and_get_column<ColumnString>(col_from);
        if (const auto* nullable = check_and_get_column<ColumnNullable>(col_from)) {
            col_from_string =
                    check_and_get_column<ColumnString>(*nullable->get_nested_column_ptr());
        }

        if (!col_from_string) {
            return Status::RuntimeError("Illegal column {} should be ColumnString",
                                        col_from.get_name());
        }

        auto col_to = ColumnString::create();
        col_to->reserve(input_rows_count);
        auto& null_map_data = null_map->get_data_mutable();

        // parser can be reused for performance
        rapidjson::Document document;
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (col_from.is_null_at(i)) {
                null_map_data[i] = 1;
                col_to->insert_data(nullptr, 0);
                continue;
            }

            const auto& json_str = col_from_string->get_data_at(i);
            if (json_str.size < 2 || json_str.data[0] != '"' ||
                json_str.data[json_str.size - 1] != '"') {
                // non-quoted string
                col_to->insert_data(json_str.data, json_str.size);
            } else {
                document.Parse(json_str.data, json_str.size);
                if (document.HasParseError() || !document.IsString()) {
                    return Status::RuntimeError(
                            fmt::format("Invalid JSON text in argument 1 to function {}: {}", name,
                                        std::string_view(json_str.data, json_str.size)));
                }
                col_to->insert_data(document.GetString(), document.GetStringLength());
            }
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(col_to), std::move(null_map)));

        return Status::OK();
    }
};

void register_function_json(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionJsonUnquote>();

    factory.register_function<FunctionJson<FunctionJsonQuoteImpl>>();

    factory.register_function<FunctionJsonValid>();
}

} // namespace doris
