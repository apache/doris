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
// KIND, either explicit or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "vec/functions/date_format_spark_legacy.h"
#include "vec/functions/function_datetime_string_to_string.h"

namespace doris::vectorized {

template <PrimitiveType PType>
struct DateFormatSparkImpl {
    using DateType = typename PrimitiveTypeTraits<PType>::CppType;
    using ArgType = typename PrimitiveTypeTraits<PType>::CppType;
    using LegacyEngine = detail::SparkLegacyDateFormatEngine<PType>;
    using CompiledPattern = typename LegacyEngine::CompiledPattern;
    using SharedRuntimeContext = typename LegacyEngine::SharedRuntimeContext;

    static constexpr PrimitiveType FromPType = PType;
    static constexpr auto name = "date_format_spark";

    static bool compile_format_string(StringRef format_raw, CompiledPattern& compiled_pattern) {
        return LegacyEngine::compile_format_string(format_raw, compiled_pattern);
    }

    static std::unique_ptr<SharedRuntimeContext> create_shared_runtime_context(
            const CompiledPattern& compiled_pattern, const cctz::time_zone& time_zone) {
        return LegacyEngine::create_shared_runtime_context(compiled_pattern, time_zone);
    }

    static bool execute(const DateType& dt, const CompiledPattern& compiled_pattern,
                        ColumnString::Chars& res_data, size_t& offset,
                        SharedRuntimeContext& runtime_context) {
        return LegacyEngine::execute(dt, compiled_pattern, res_data, offset, runtime_context);
    }
};

// date_format_spark keeps its own open/execute path so the primary
// FunctionDateTimeStringToString template stays free of Spark-only traits.
template <PrimitiveType P>
class FunctionDateTimeStringToString<DateFormatSparkImpl<P>> : public IFunction {
public:
    static constexpr auto name = DateFormatSparkImpl<P>::name;
    using Transform = DateFormatSparkImpl<P>;
    using ColumnType = typename PrimitiveTypeTraits<Transform::FromPType>::ColumnType;
    static_assert(!is_decimal(Transform::FromPType),
                  "date_format_spark only supports date/datetime inputs");

    static FunctionPtr create() { return std::make_shared<FunctionDateTimeStringToString>(); }
    String get_name() const override { return name; }
    bool is_variadic() const override { return false; }
    size_t get_number_of_arguments() const override { return 2; }

    struct FormatState {
        std::string format_str;
        typename Transform::CompiledPattern compiled_pattern;
        bool is_valid = true;
    };

    // Spark-compatible Java datetime patterns can be much longer than Doris/MySQL style `%` formats,
    // especially with repeated fields and quoted literals, so we keep a larger hard limit here.
    static constexpr size_t MAX_SPARK_PATTERN_LENGTH = 512;

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        auto state = std::make_shared<FormatState>();
        DCHECK(context->get_num_args() == 2);
        context->set_function_state(scope, state);

        const auto* column_string = context->get_constant_col(1);
        if (column_string == nullptr) {
            return Status::InvalidArgument(
                    "The second parameter of the function {} must be a constant.", get_name());
        }

        auto string_value = column_string->column_ptr->get_data_at(0);
        if (string_value.data == nullptr) {
            state->is_valid = false;
            return IFunction::open(context, scope);
        }

        string_value = string_value.trim();
        auto format_str = StringRef(string_value.data, string_value.size);
        if (format_str.size > MAX_SPARK_PATTERN_LENGTH) {
            state->is_valid = false;
            return IFunction::open(context, scope);
        }

        state->format_str.assign(format_str.data, format_str.size);
        if (!Transform::compile_format_string(format_str, state->compiled_pattern)) {
            state->is_valid = false;
        }

        return IFunction::open(context, scope);
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    // This compatibility function has both DATEV2 and DATETIMEV2 implementations.
    // Exposing the argument families here lets SimpleFunctionFactory register and resolve
    // them as two overloads without changing Doris's shared function factory.
    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<typename PrimitiveTypeTraits<P>::DataType>(),
                std::make_shared<DataTypeString>()};
    }

    bool need_replace_null_data_to_default() const override { return true; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& sources =
                assert_cast<const ColumnType&>(*block.get_by_position(arguments[0]).column);

        auto col_res = ColumnString::create();
        RETURN_IF_ERROR(
                vector_constant(context, sources, col_res->get_chars(), col_res->get_offsets()));
        block.get_by_position(result).column = std::move(col_res);
        return Status::OK();
    }

    Status vector_constant(FunctionContext* context, const ColumnType& col,
                           ColumnString::Chars& res_data,
                           ColumnString::Offsets& res_offsets) const {
        auto* format_state = reinterpret_cast<FormatState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (!format_state) {
            return Status::RuntimeError("function context for function '{}' must have FormatState;",
                                        get_name());
        }
        if (!format_state->is_valid) {
            throw_invalid_string(name, "invalid or oversized format");
        }

        const auto& pod_array = col.get_data();
        const auto len = pod_array.size();
        res_offsets.resize(len);
        res_data.reserve(len * (format_state->format_str.size() + 16));

        auto runtime_context = Transform::create_shared_runtime_context(
                format_state->compiled_pattern, context->state()->timezone_obj());
        if (!runtime_context) {
            return Status::RuntimeError("failed to initialize Spark formatter runtime context");
        }

        size_t offset = 0;
        for (int i = 0; i < len; ++i) {
            bool invalid = Transform::execute(pod_array[i], format_state->compiled_pattern,
                                              res_data, offset, *runtime_context);
            if (invalid) [[unlikely]] {
                char buf[64];
                pod_array[i].to_string(buf);
                throw_invalid_strings(name, buf, format_state->format_str);
            }
            res_offsets[i] = cast_set<uint32_t>(offset);
        }
        res_data.resize(offset);
        return Status::OK();
    }
};

} // namespace doris::vectorized
