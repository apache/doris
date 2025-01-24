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

#include <memory>

#include "common/logging.h"
#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/functions/dictionary.h"
#include "vec/functions/dictionary_factory.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
namespace doris::vectorized {

struct DictGetState {
    std::shared_ptr<const IDictionary> dict;
};

template <typename Impl>
class FunctionDictGetWithType : public IFunction {
public:
    static constexpr auto name = Impl::name;
    using ReturnType = Impl::Type;
    static FunctionPtr create() { return std::make_shared<FunctionDictGetWithType>(); }
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }
    size_t get_number_of_arguments() const override { return 3; }

    DataTypes get_variadic_argument_types_impl() const override { return {}; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<ReturnType>();
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        std::shared_ptr<DictGetState> state = std::make_shared<DictGetState>();
        context->set_function_state(scope, state);
        DCHECK(context->get_num_args() == 3);

        std::string dict_name =
                context->get_constant_col(0)->column_ptr->get_data_at(0).to_string();
        auto dict = ExecEnv::GetInstance()->dict_factory()->get(dict_name);
        if (!dict) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "can not find dict {} , current_all_dict {} ", dict_name,
                                   ExecEnv::GetInstance()->dict_factory()->current_all_dict());
        }
        state->dict = dict;
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto* dict_state = reinterpret_cast<DictGetState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (!dict_state) {
            return Status::RuntimeError("funciton context for function '{}' must have dict_state;",
                                        get_name());
        }

        // dict get(name, att_name, key_value) -> att value
        const std::string attribute_name =
                block.get_by_position(arguments[1]).column->get_data_at(0).to_string();
        const DataTypePtr attribute_type = block.get_by_position(result).type;

        const ColumnPtr key_column =
                block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();
        const DataTypePtr key_type = block.get_by_position(arguments[2]).type;

        const auto dict = dict_state->dict;

        // check attribute
        if (!dict->has_attribute(attribute_name)) {
            throw doris::Exception(
                    ErrorCode::INVALID_ARGUMENT,
                    "No corresponding attribute_name. The current attribute_name is: {}",
                    attribute_name);
        }
        if (!dict->get_attribute_type(attribute_name)->equals(*attribute_type)) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Attribute type mismatch. Attribute name: {}   Expected type: "
                                   "{}  Input type: {}",
                                   attribute_name,
                                   dict->get_attribute_type(attribute_name)->get_name(),
                                   attribute_type->get_name());
        }

        // check key in dict::getColumn
        auto res = dict->getColumn(attribute_name, attribute_type, key_column, key_type);

        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

struct Int8Impl {
    constexpr static auto name = "dict_get_int8";
    using Type = DataTypeInt8;
};

struct Int16Impl {
    constexpr static auto name = "dict_get_int16";
    using Type = DataTypeInt16;
};

struct Int32Impl {
    constexpr static auto name = "dict_get_int32";
    using Type = DataTypeInt32;
};

struct Int64Impl {
    constexpr static auto name = "dict_get_int64";
    using Type = DataTypeInt64;
};

struct Int128Impl {
    constexpr static auto name = "dict_get_int128";
    using Type = DataTypeInt64;
};

struct UInt8Impl {
    constexpr static auto name = "dict_get_bool";
    using Type = DataTypeInt8;
};

struct Float32Impl {
    constexpr static auto name = "dict_get_float32";
    using Type = DataTypeFloat32;
};

struct Float64Impl {
    constexpr static auto name = "dict_get_float64";
    using Type = DataTypeFloat64;
};

struct DecimalImpl {
    constexpr static auto name = "dict_get_decimal";
    using Type = DataTypeDecimal<Decimal32>;
};

struct DateImpl {
    constexpr static auto name = "dict_get_date";
    using Type = DataTypeDateV2;
};

struct DateTimeImpl {
    constexpr static auto name = "dict_get_datetime";
    using Type = DataTypeDateTimeV2;
};

struct Ipv4Impl {
    constexpr static auto name = "dict_get_ipv4";
    using Type = DataTypeIPv4;
};

struct Ipv6Impl {
    constexpr static auto name = "dict_get_ipv6";
    using Type = DataTypeIPv6;
};

struct StringImpl {
    constexpr static auto name = "dict_get_string";
    using Type = DataTypeString;
};

void register_function_dict_get(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDictGetWithType<Int8Impl>>();
    factory.register_function<FunctionDictGetWithType<Int16Impl>>();
    factory.register_function<FunctionDictGetWithType<Int32Impl>>();
    factory.register_function<FunctionDictGetWithType<Int64Impl>>();
    factory.register_function<FunctionDictGetWithType<Int128Impl>>();

    factory.register_function<FunctionDictGetWithType<UInt8Impl>>();

    factory.register_function<FunctionDictGetWithType<Float32Impl>>();
    factory.register_function<FunctionDictGetWithType<Float64Impl>>();

    factory.register_function<FunctionDictGetWithType<DateImpl>>();
    factory.register_function<FunctionDictGetWithType<DateTimeImpl>>();

    factory.register_function<FunctionDictGetWithType<Ipv4Impl>>();
    factory.register_function<FunctionDictGetWithType<Ipv6Impl>>();

    factory.register_function<FunctionDictGetWithType<StringImpl>>();
}

} // namespace doris::vectorized