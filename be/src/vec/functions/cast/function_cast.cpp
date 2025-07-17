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

#include "function_cast.h"

#include <utility>

#include "cast_to_array.h"
#include "cast_to_boolean.h"
#include "cast_to_date.h"
#include "cast_to_decimal.h"
#include "cast_to_float.h"
#include "cast_to_int.h"
#include "cast_to_jsonb.h"
#include "cast_to_map.h"
#include "cast_to_string.h"
#include "cast_to_struct.h"
#include "cast_to_variant.h"
#include "vec/data_types/data_type_agg_state.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h" // IWYU pragma: keep
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

namespace CastWrapper {
template <typename ToDataType>
WrapperType create_wrapper(const DataTypePtr& from_type, const ToDataType* const,
                           bool requested_result_is_nullable) {
    if (requested_result_is_nullable && check_and_get_data_type<DataTypeString>(from_type.get())) {
        /// In case when converting to Nullable type, we apply different parsing rule,
        /// that will not throw an exception but return NULL in case of malformed input.
        auto function = FunctionConvertFromString<ToDataType>::create();
        return [function](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count,
                          const NullMap::value_type* null_map = nullptr) {
            return function->execute(context, block, arguments, result, input_rows_count);
        };
    } else if (requested_result_is_nullable &&
               (IsDatelikeV1Types<ToDataType> || IsDatelikeV2Types<ToDataType>)&&!(
                       check_and_get_data_type<DataTypeDateTime>(from_type.get()) ||
                       check_and_get_data_type<DataTypeDate>(from_type.get()) ||
                       check_and_get_data_type<DataTypeDateV2>(from_type.get()) ||
                       check_and_get_data_type<DataTypeDateTimeV2>(from_type.get()))) {
        FunctionPtr function;
        function = FunctionConvertToTimeType<ToDataType, NameCast>::create();
        return [function](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count,
                          const NullMap::value_type* null_map = nullptr) {
            return function->execute(context, block, arguments, result, input_rows_count);
        };
    } else {
        return [](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                  uint32_t result, size_t input_rows_count,
                  const NullMap::value_type* null_map = nullptr) {
            FunctionConvert<ToDataType> function;
            return function.execute_impl(context, block, arguments, result, input_rows_count);
        };
    }
}

WrapperType create_hll_wrapper(FunctionContext* context, const DataTypePtr& from_type_untyped,
                               const DataTypeHLL& to_type) {
    /// Conversion from String through parsing.
    if (check_and_get_data_type<DataTypeString>(from_type_untyped.get())) {
        return cast_from_string_to_generic;
    }

    //TODO: if from is not string, it must be HLL?
    const auto* from_type = check_and_get_data_type<DataTypeHLL>(from_type_untyped.get());

    if (!from_type) {
        return CastWrapper::create_unsupport_wrapper(
                "CAST AS HLL can only be performed between HLL, String "
                "types");
    }

    return nullptr;
}

WrapperType create_bitmap_wrapper(FunctionContext* context, const DataTypePtr& from_type_untyped,
                                  const DataTypeBitMap& to_type) {
    /// Conversion from String through parsing.
    if (check_and_get_data_type<DataTypeString>(from_type_untyped.get())) {
        return cast_from_string_to_generic;
    }

    //TODO: if from is not string, it must be BITMAP?
    const auto* from_type = check_and_get_data_type<DataTypeBitMap>(from_type_untyped.get());

    if (!from_type) {
        return CastWrapper::create_unsupport_wrapper(
                "CAST AS BITMAP can only be performed between BITMAP, String "
                "types");
    }

    return nullptr;
}

WrapperType prepare_unpack_dictionaries(FunctionContext* context, const DataTypePtr& from_type,
                                        const DataTypePtr& to_type) {
    const auto& from_nested = from_type;
    const auto& to_nested = to_type;

    if (from_type->is_null_literal()) {
        if (!to_nested->is_nullable()) {
            return CastWrapper::create_unsupport_wrapper(
                    "Cannot convert NULL to a non-nullable type");
        }

        return [](FunctionContext* context, Block& block, const ColumnNumbers&, uint32_t result,
                  size_t input_rows_count, const NullMap::value_type* null_map = nullptr) {
            auto& res = block.get_by_position(result);
            res.column = res.type->create_column_const_with_default_value(input_rows_count)
                                 ->convert_to_full_column_if_const();
            return Status::OK();
        };
    }

    bool skip_not_null_check = false;
    auto wrapper = prepare_remove_nullable(context, from_nested, to_nested, skip_not_null_check);

    return wrapper;
}

bool need_replace_null_data_to_default(FunctionContext* context, const DataTypePtr& from_type,
                                       const DataTypePtr& to_type) {
    if (from_type->equals(*to_type)) {
        return false;
    }

    auto make_default_wrapper = [&](const auto& types) -> bool {
        using Types = std::decay_t<decltype(types)>;
        using ToDataType = typename Types::LeftType;

        if constexpr (!(IsDataTypeDecimalOrNumber<ToDataType> || IsDatelikeV1Types<ToDataType> ||
                        IsDatelikeV2Types<ToDataType> ||
                        std::is_same_v<ToDataType, DataTypeTimeV2>)) {
            return false;
        }
        return call_on_index_and_data_type<
                ToDataType>(from_type->get_primitive_type(), [&](const auto& types2) -> bool {
            using Types2 = std::decay_t<decltype(types2)>;
            using FromDataType = typename Types2::LeftType;
            if constexpr (!(IsDataTypeDecimalOrNumber<FromDataType> ||
                            IsDatelikeV1Types<FromDataType> || IsDatelikeV2Types<FromDataType> ||
                            std::is_same_v<FromDataType, DataTypeTimeV2>)) {
                return false;
            }
            if constexpr (IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>) {
                using FromFieldType = typename FromDataType::FieldType;
                using ToFieldType = typename ToDataType::FieldType;
                UInt32 from_precision = NumberTraits::max_ascii_len<FromFieldType>();
                UInt32 from_scale = 0;

                if constexpr (IsDataTypeDecimal<FromDataType>) {
                    const auto* from_decimal_type =
                            check_and_get_data_type<FromDataType>(from_type.get());
                    from_precision =
                            NumberTraits::max_ascii_len<typename FromFieldType::NativeType>();
                    from_scale = from_decimal_type->get_scale();
                }

                UInt32 to_max_digits = 0;
                UInt32 to_precision = 0;
                UInt32 to_scale = 0;

                ToFieldType max_result {0};
                ToFieldType min_result {0};
                if constexpr (IsDataTypeDecimal<ToDataType>) {
                    to_max_digits = NumberTraits::max_ascii_len<typename ToFieldType::NativeType>();

                    const auto* to_decimal_type =
                            check_and_get_data_type<ToDataType>(to_type.get());
                    to_precision = to_decimal_type->get_precision();
                    ToDataType::check_type_precision(to_precision);

                    to_scale = to_decimal_type->get_scale();
                    ToDataType::check_type_scale(to_scale);

                    max_result = ToDataType::get_max_digits_number(to_precision);
                    min_result = -max_result;
                }
                if constexpr (IsIntegralV<ToFieldType> || std::is_floating_point_v<ToFieldType>) {
                    max_result = type_limit<ToFieldType>::max();
                    min_result = type_limit<ToFieldType>::min();
                    to_max_digits = NumberTraits::max_ascii_len<ToFieldType>();
                    to_precision = to_max_digits;
                }

                bool narrow_integral = context->check_overflow_for_decimal() &&
                                       (to_precision - to_scale) <= (from_precision - from_scale);

                bool multiply_may_overflow = context->check_overflow_for_decimal();
                if (to_scale > from_scale) {
                    multiply_may_overflow &=
                            (from_precision + to_scale - from_scale) >= to_max_digits;
                }
                return narrow_integral || multiply_may_overflow;
            }
            return false;
        });
    };

    return call_on_index_and_data_type<void>(to_type->get_primitive_type(), make_default_wrapper);
}

WrapperType prepare_remove_nullable(FunctionContext* context, const DataTypePtr& from_type,
                                    const DataTypePtr& to_type, bool skip_not_null_check) {
    /// Determine whether pre-processing and/or post-processing must take place during conversion.
    bool result_is_nullable = to_type->is_nullable();

    if (result_is_nullable) {
        return [from_type, to_type](FunctionContext* context, Block& block,
                                    const ColumnNumbers& arguments, uint32_t result,
                                    size_t input_rows_count,
                                    const NullMap::value_type* null_map = nullptr) {
            auto from_type_not_nullable = remove_nullable(from_type);
            auto to_type_not_nullable = remove_nullable(to_type);

            bool replace_null_data_to_default = need_replace_null_data_to_default(
                    context, from_type_not_nullable, to_type_not_nullable);

            auto nested_result_index = block.columns();
            block.insert(block.get_by_position(result).get_nested());
            auto nested_source_index = block.columns();
            block.insert(
                    block.get_by_position(arguments[0]).get_nested(replace_null_data_to_default));

            const auto& arg_col = block.get_by_position(arguments[0]);
            const NullMap::value_type* arg_null_map = nullptr;
            if (const auto* nullable = check_and_get_column<ColumnNullable>(*arg_col.column)) {
                arg_null_map = nullable->get_null_map_data().data();
            }
            RETURN_IF_ERROR(prepare_impl(context, from_type_not_nullable, to_type_not_nullable,
                                         true)(context, block, {nested_source_index},
                                               nested_result_index, input_rows_count,
                                               arg_null_map));

            block.get_by_position(result).column =
                    wrap_in_nullable(block.get_by_position(nested_result_index).column, block,
                                     arguments, result, input_rows_count);

            block.erase(nested_source_index);
            block.erase(nested_result_index);
            return Status::OK();
        };
    } else {
        return prepare_impl(context, from_type, to_type, false);
    }
}

/// 'from_type' and 'to_type' are nested types in case of Nullable.
/// 'requested_result_is_nullable' is true if CAST to Nullable type is requested.
WrapperType prepare_impl(FunctionContext* context, const DataTypePtr& origin_from_type,
                         const DataTypePtr& origin_to_type, bool requested_result_is_nullable) {
    auto to_type = get_serialized_type(origin_to_type);
    auto from_type = get_serialized_type(origin_from_type);
    if (from_type->equals(*to_type)) {
        return create_identity_wrapper(from_type);
    }

    // variant needs to be judged first
    if (to_type->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
        return create_cast_to_variant_wrapper(from_type,
                                              static_cast<const DataTypeVariant&>(*to_type));
    }
    if (from_type->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
        return create_cast_from_variant_wrapper(static_cast<const DataTypeVariant&>(*from_type),
                                                to_type);
    }

    switch (from_type->get_primitive_type()) {
    case PrimitiveType::INVALID_TYPE:
        return create_nothing_wrapper(to_type.get());
    case PrimitiveType::TYPE_JSONB:
        return create_cast_from_jsonb_wrapper(static_cast<const DataTypeJsonb&>(*from_type),
                                              to_type,
                                              context ? context->jsonb_string_as_string() : false);
    default:
        break;
    }

    WrapperType ret;

    auto make_default_wrapper = [&](const auto& types) -> bool {
        using Types = std::decay_t<decltype(types)>;
        using ToDataType = typename Types::LeftType;
        if constexpr (std::is_same_v<ToDataType, DataTypeUInt8>) {
            ret = create_boolean_wrapper(context, from_type);
            return true;
        } else if constexpr (IsDataTypeInt<ToDataType>) {
            ret = create_int_wrapper<ToDataType>(context, from_type);
            return true;
        } else if constexpr (IsDataTypeFloat<ToDataType>) {
            ret = create_float_wrapper<ToDataType>(context, from_type);
            return true;
        } else if constexpr (IsDatelikeTypes<ToDataType>) {
            ret = create_datelike_wrapper<ToDataType>(context, from_type);
            return true;
        } else if constexpr (std::is_same_v<ToDataType, DataTypeIPv4> ||
                             std::is_same_v<ToDataType, DataTypeIPv6>) {
            ret = create_wrapper(from_type, check_and_get_data_type<ToDataType>(to_type.get()),
                                 requested_result_is_nullable);
            return true;
        }

        if constexpr (IsDataTypeDecimal<ToDataType>) {
            ret = create_decimal_wrapper<ToDataType>(context, from_type);
            return true;
        }

        return false;
    };

    if (call_on_index_and_data_type<void>(to_type->get_primitive_type(), make_default_wrapper)) {
        return ret;
    }

    switch (to_type->get_primitive_type()) {
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING:
        return create_string_wrapper(from_type);
    case PrimitiveType::TYPE_ARRAY:
        return create_array_wrapper(context, from_type,
                                    static_cast<const DataTypeArray&>(*to_type));
    case PrimitiveType::TYPE_STRUCT:
        return create_struct_wrapper(context, from_type,
                                     static_cast<const DataTypeStruct&>(*to_type));
    case PrimitiveType::TYPE_MAP:
        return create_map_wrapper(context, from_type, static_cast<const DataTypeMap&>(*to_type));
    case PrimitiveType::TYPE_HLL:
        return create_hll_wrapper(context, from_type, static_cast<const DataTypeHLL&>(*to_type));
    case PrimitiveType::TYPE_BITMAP:
        return create_bitmap_wrapper(context, from_type,
                                     static_cast<const DataTypeBitMap&>(*to_type));
    case PrimitiveType::TYPE_JSONB:
        return create_cast_to_jsonb_wrapper(from_type, static_cast<const DataTypeJsonb&>(*to_type),
                                            context ? context->string_as_jsonb_string() : false);
    default:
        break;
    }

    return create_unsupport_wrapper(from_type->get_name(), to_type->get_name());
}

} // namespace CastWrapper

class PreparedFunctionCast : public PreparedFunctionImpl {
public:
    explicit PreparedFunctionCast(CastWrapper::WrapperType&& wrapper_function_, const char* name_)
            : wrapper_function(std::move(wrapper_function_)), name(name_) {}

    String get_name() const override { return name; }

protected:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return wrapper_function(context, block, arguments, result, input_rows_count, nullptr);
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

private:
    CastWrapper::WrapperType wrapper_function;
    const char* name;
};

class FunctionCast final : public IFunctionBase {
public:
    FunctionCast(const char* name_, DataTypes argument_types_, DataTypePtr return_type_)
            : name(name_),
              argument_types(std::move(argument_types_)),
              return_type(std::move(return_type_)) {}

    const DataTypes& get_argument_types() const override { return argument_types; }
    const DataTypePtr& get_return_type() const override { return return_type; }

    PreparedFunctionPtr prepare(FunctionContext* context, const Block& /*sample_block*/,
                                const ColumnNumbers& /*arguments*/,
                                uint32_t /*result*/) const override {
        return std::make_shared<PreparedFunctionCast>(
                CastWrapper::prepare_unpack_dictionaries(context, get_argument_types()[0],
                                                         get_return_type()),
                name);
    }

    String get_name() const override { return name; }

    bool is_use_default_implementation_for_constants() const override { return true; }

private:
    const char* name = nullptr;

    DataTypes argument_types;
    DataTypePtr return_type;
};

class FunctionBuilderCast : public FunctionBuilderImpl {
public:
    static constexpr auto name = "CAST";
    static FunctionBuilderPtr create() { return std::make_shared<FunctionBuilderCast>(); }

    FunctionBuilderCast() = default;

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

protected:
    FunctionBasePtr build_impl(const ColumnsWithTypeAndName& arguments,
                               const DataTypePtr& return_type) const override {
        DataTypes data_types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i) {
            data_types[i] = arguments[i].type;
        }

        return std::make_shared<FunctionCast>(name, data_types, return_type);
    }

    bool skip_return_type_check() const override { return true; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        DataTypePtr type = arguments[1].type;
        DCHECK(type != nullptr);
        bool need_to_be_nullable = false;
        // 1. from_type is nullable
        need_to_be_nullable |= arguments[0].type->is_nullable();
        // 2. from_type is string, to_type is not string
        need_to_be_nullable |= (is_string_type(arguments[0].type->get_primitive_type())) &&
                               (!is_string_type(type->get_primitive_type()));
        // 3. from_type is not DateTime/Date, to_type is DateTime/Date
        need_to_be_nullable |=
                (arguments[0].type->get_primitive_type() != PrimitiveType::TYPE_DATE &&
                 arguments[0].type->get_primitive_type() != PrimitiveType::TYPE_DATETIME) &&
                (type->get_primitive_type() == PrimitiveType::TYPE_DATE ||
                 type->get_primitive_type() == PrimitiveType::TYPE_DATETIME);
        // 4. from_type is not DateTimeV2/DateV2, to_type is DateTimeV2/DateV2
        need_to_be_nullable |=
                (arguments[0].type->get_primitive_type() != PrimitiveType::TYPE_DATEV2 &&
                 arguments[0].type->get_primitive_type() != PrimitiveType::TYPE_DATETIMEV2) &&
                (type->get_primitive_type() == PrimitiveType::TYPE_DATEV2 ||
                 type->get_primitive_type() == PrimitiveType::TYPE_DATETIMEV2);
        if (need_to_be_nullable && !type->is_nullable()) {
            return make_nullable(type);
        }

        return type;
    }

    bool use_default_implementation_for_nulls() const override { return false; }
};

void register_function_cast(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBuilderCast>();
}
} // namespace doris::vectorized
