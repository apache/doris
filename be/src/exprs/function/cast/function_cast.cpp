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

#include <utility>

#include "core/column/column_const.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h" // IWYU pragma: keep
#include "core/data_type/data_type_quantilestate.h"
#include "core/data_type/data_type_timestamp_ns.h" // IWYU pragma: keep
#include "core/data_type/data_type_uuid.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type/primitive_type.h"
#include "exprs/function/cast/cast_to_array.h"
#include "exprs/function/cast/cast_to_jsonb.h"
#include "exprs/function/cast/cast_to_map.h"
#include "exprs/function/cast/cast_to_struct.h"
#include "exprs/function/cast/cast_wrapper_decls.h"
#include "exprs/function/cast/variant_v2/cast_variant_v2.h"
#include "exprs/function/geo/spatial_wkb_validation.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

namespace CastWrapper {

WrapperType create_hll_wrapper(const CastOptions& /*options*/, const DataTypePtr& from_type_untyped,
                               const DataTypeHLL& to_type) {
    /// Conversion from String through parsing.
    if (check_and_get_data_type<DataTypeString>(from_type_untyped.get())) {
        return cast_from_string_to_generic;
    }

    return CastWrapper::create_unsupport_wrapper("Cast to HLL only support from String type");
}

WrapperType create_bitmap_wrapper(const CastOptions& /*options*/,
                                  const DataTypePtr& from_type_untyped,
                                  const DataTypeBitMap& to_type) {
    /// Conversion from String through parsing.
    if (check_and_get_data_type<DataTypeString>(from_type_untyped.get())) {
        return cast_from_string_to_generic;
    }

    return CastWrapper::create_unsupport_wrapper("Cast to BitMap only support from String type");
}

WrapperType create_quantile_state_wrapper(const CastOptions& /*options*/,
                                          const DataTypePtr& from_type_untyped,
                                          const DataTypeQuantileState& to_type) {
    /// Conversion from String through parsing.
    if (check_and_get_data_type<DataTypeString>(from_type_untyped.get())) {
        return cast_from_string_to_generic;
    }

    return CastWrapper::create_unsupport_wrapper(
            "Cast to QuantileState only support from String type");
}

WrapperType create_varbinary_wrapper(const DataTypePtr& from_type_untyped) {
    /// Conversion from String through parsing.
    if (check_and_get_data_type<DataTypeString>(from_type_untyped.get())) {
        return cast_from_string_to_generic;
    }

    return CastWrapper::create_unsupport_wrapper("Cast to Varbinary only support from String type");
}

WrapperType create_spatial_wrapper(const DataTypePtr& from_type) {
    if (from_type->get_primitive_type() != TYPE_VARBINARY) {
        return create_unsupport_wrapper("Cast to spatial type only supports raw Varbinary WKB");
    }
    return [](RuntimeState* /*state*/, const ColumnWithTypeAndName& source_column,
              ColumnWithTypeAndName& target_column, size_t /*input_rows_count*/,
              const NullMap::value_type* parent_null_map) {
        auto target = target_column.type->create_column();
        RETURN_IF_ERROR(append_spatial_wkb(source_column.column, *target, target_column.type,
                                           parent_null_map));
        target_column.column = std::move(target);
        return Status::OK();
    };
}

WrapperType bind_cast_executor(const CastOptions& options, const DataTypePtr& from_type,
                               const DataTypePtr& to_type) {
    const auto& from_nested = from_type;
    const auto& to_nested = to_type;

    if (from_type->is_null_literal()) {
        if (!to_nested->is_nullable()) {
            return CastWrapper::create_unsupport_wrapper(
                    "Cannot convert NULL to a non-nullable type");
        }

        return [](RuntimeState* /*state*/, const ColumnWithTypeAndName& /*source_column*/,
                  ColumnWithTypeAndName& target_column, size_t input_rows_count,
                  const NullMap::value_type* /*parent_null_map*/ = nullptr) {
            /// TODO: remove this in the future.
            target_column.column =
                    target_column.type->create_column_const_with_default_value(input_rows_count)
                            ->convert_to_full_column_if_const();
            return Status::OK();
        };
    }

    auto wrapper = prepare_remove_nullable(options, from_nested, to_nested);
    return [wrapper = std::move(wrapper), to_type](
                   RuntimeState* state, const ColumnWithTypeAndName& source_column,
                   ColumnWithTypeAndName& target_column, size_t input_rows_count,
                   const NullMap::value_type* parent_null_map = nullptr) {
        if (input_rows_count == 0) {
            target_column.column = to_type->create_column();
            return Status::OK();
        }

        const auto* const_column = check_and_get_column<ColumnConst>(source_column.column.get());
        if (const_column == nullptr) {
            return wrapper(state, source_column, target_column, input_rows_count, parent_null_map);
        }

        if (parent_null_map != nullptr && has_masked_row(parent_null_map, input_rows_count)) {
            ColumnWithTypeAndName expanded_source {const_column->convert_to_full_column(),
                                                   source_column.type, source_column.name};
            return wrapper(state, expanded_source, target_column, input_rows_count,
                           parent_null_map);
        }

        ColumnWithTypeAndName one_row_source {const_column->get_data_column_ptr(),
                                              source_column.type, source_column.name};
        RETURN_IF_ERROR(wrapper(state, one_row_source, target_column, 1, nullptr));
        target_column.column =
                ColumnConst::create(std::move(target_column.column), input_rows_count);
        return Status::OK();
    };
}

bool need_replace_null_data_to_default(const CastOptions& options, const DataTypePtr& from_type,
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

                if constexpr (IsDataTypeDecimal<ToDataType>) {
                    to_max_digits = NumberTraits::max_ascii_len<typename ToFieldType::NativeType>();

                    const auto* to_decimal_type =
                            check_and_get_data_type<ToDataType>(to_type.get());
                    to_precision = to_decimal_type->get_precision();
                    ToDataType::check_type_precision(to_precision);

                    to_scale = to_decimal_type->get_scale();
                    ToDataType::check_type_scale(to_scale);
                }
                if constexpr (IsIntegralV<ToFieldType> || std::is_floating_point_v<ToFieldType>) {
                    to_max_digits = NumberTraits::max_ascii_len<ToFieldType>();
                    to_precision = to_max_digits;
                }

                bool narrow_integral = options.check_overflow_for_decimal &&
                                       (to_precision - to_scale) <= (from_precision - from_scale);

                bool multiply_may_overflow = options.check_overflow_for_decimal;
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

ColumnPtr wrap_cast_result_with_source_nulls(ColumnPtr result, const ColumnWithTypeAndName& source,
                                             const NullableColumnInfo& source_info, size_t rows) {
    if (!source_info.has_null) {
        return make_nullable(std::move(result));
    }

    ColumnPtr nested_result = result;
    const NullMap* result_null_map = nullptr;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(result.get())) {
        nested_result = nullable->get_nested_column_ptr();
        result_null_map = &nullable->get_null_map_data();
    } else {
        return ColumnNullable::create(std::move(result), source.get_nullable_null_map_column());
    }

    ColumnUInt8::MutablePtr merged_null_map = ColumnUInt8::create(rows, 0);
    auto& merged_data = merged_null_map->get_data();
    const auto& source_null_map = source.get_nullable_null_map_column()->get_data();
    for (size_t row = 0; row < rows; ++row) {
        merged_data[row] =
                (*result_null_map)[row] | source_null_map[source_info.is_const ? 0 : row];
    }
    return ColumnNullable::create(std::move(nested_result), std::move(merged_null_map));
}

WrapperType prepare_remove_nullable(const CastOptions& options, const DataTypePtr& from_type,
                                    const DataTypePtr& to_type) {
    /// Determine whether pre-processing and/or post-processing must take place during conversion.
    bool result_is_nullable = to_type->is_nullable();

    if (result_is_nullable) {
        auto from_type_not_nullable = remove_nullable(from_type);
        auto to_type_not_nullable = remove_nullable(to_type);
        const bool replace_null_data_to_default = need_replace_null_data_to_default(
                options, from_type_not_nullable, to_type_not_nullable);
        auto nested_wrapper = prepare_impl(options, from_type_not_nullable, to_type_not_nullable);

        return [nested_wrapper = std::move(nested_wrapper), from_type_not_nullable,
                to_type_not_nullable, replace_null_data_to_default](
                       RuntimeState* state, const ColumnWithTypeAndName& source,
                       ColumnWithTypeAndName& target, size_t input_rows_count,
                       const NullMap::value_type* parent_null_map = nullptr) {
            NullableColumnInfo source_info;
            if (source.type->is_nullable()) {
                source_info = source.get_nullable_column_info();
            }
            auto nested_source =
                    source_info.is_nullable
                            ? source.unnest_nullable(source_info, replace_null_data_to_default)
                            : ColumnWithTypeAndName {source.column, from_type_not_nullable,
                                                     source.name};

            ColumnPtr source_null_mask_holder;
            const NullMap::value_type* child_parent_null_map = parent_null_map;
            if (source_info.has_null) {
                const auto* source_null_map =
                        source.get_nullable_null_map_column()->get_data().data();
                if (parent_null_map == nullptr ||
                    !has_masked_row(parent_null_map, input_rows_count)) {
                    child_parent_null_map = source_null_map;
                } else {
                    auto merged = ColumnUInt8::create(input_rows_count, 0);
                    auto& merged_data = merged->get_data();
                    const auto& source_null_map_data =
                            source.get_nullable_null_map_column()->get_data();
                    for (size_t row = 0; row < input_rows_count; ++row) {
                        merged_data[row] = parent_null_map[row] | source_null_map_data[row];
                    }
                    child_parent_null_map = merged_data.data();
                    source_null_mask_holder = std::move(merged);
                }
            }

            ColumnWithTypeAndName nested_target {nullptr, to_type_not_nullable, target.name};
            RETURN_IF_ERROR(nested_wrapper(state, nested_source, nested_target, input_rows_count,
                                           child_parent_null_map));
            target.column = wrap_cast_result_with_source_nulls(
                    std::move(nested_target.column), source, source_info, input_rows_count);
            return Status::OK();
        };
    } else {
        return prepare_impl(options, from_type, to_type);
    }
}

/// 'from_type' and 'to_type' are nested types in case of Nullable.
/// 'requested_result_is_nullable' is true if CAST to Nullable type is requested.
// NOLINTNEXTLINE(readability-function-size)
WrapperType prepare_impl(const CastOptions& options, const DataTypePtr& origin_from_type,
                         const DataTypePtr& origin_to_type) {
    auto to_type = get_serialized_type(origin_to_type);
    auto from_type = get_serialized_type(origin_from_type);
    if (from_type->equals(*to_type)) {
        return create_identity_wrapper(from_type);
    }

    const auto* from_variant_v2 =
            dynamic_cast<const DataTypeVariantV2*>(remove_nullable(from_type).get());
    const auto* to_variant_v2 =
            dynamic_cast<const DataTypeVariantV2*>(remove_nullable(to_type).get());

    // variant needs to be judged first
    if (to_type->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
        DORIS_CHECK(to_variant_v2 != nullptr);
        return create_cast_to_variant_v2_wrapper(from_type);
    }
    if (from_type->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
        DORIS_CHECK(from_variant_v2 != nullptr);
        auto wrapper = create_cast_from_variant_v2_wrapper(to_type, options);
        return [wrapper = std::move(wrapper)](RuntimeState* state,
                                              const ColumnWithTypeAndName& source_column,
                                              ColumnWithTypeAndName& target_column, size_t rows,
                                              const NullMap::value_type* parent_null_map) {
            RETURN_IF_ERROR(wrapper(state, source_column, target_column, rows, parent_null_map));
            if (parent_null_map == nullptr) {
                const auto* nullable = check_and_get_column<ColumnNullable>(*target_column.column);
                if (nullable != nullptr && !nullable->has_null()) {
                    target_column.column = nullable->get_nested_column_ptr();
                }
            }
            return Status::OK();
        };
    }

    if (from_type->get_primitive_type() == PrimitiveType::TYPE_JSONB) {
        return create_cast_from_jsonb_wrapper(static_cast<const DataTypeJsonb&>(*from_type),
                                              to_type, options);
    }

    switch (to_type->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return create_boolean_wrapper(options, from_type);
    case PrimitiveType::TYPE_TINYINT:
    case PrimitiveType::TYPE_SMALLINT:
    case PrimitiveType::TYPE_INT:
    case PrimitiveType::TYPE_BIGINT:
    case PrimitiveType::TYPE_LARGEINT:
        return create_int_wrapper(options, from_type, to_type->get_primitive_type());
    case PrimitiveType::TYPE_FLOAT:
    case PrimitiveType::TYPE_DOUBLE:
        return create_float_wrapper(options, from_type, to_type->get_primitive_type());
    case PrimitiveType::TYPE_DATE:
    case PrimitiveType::TYPE_DATETIME:
    case PrimitiveType::TYPE_DATEV2:
    case PrimitiveType::TYPE_DATETIMEV2:
    case PrimitiveType::TYPE_TIMEV2:
        return create_datelike_wrapper(options, from_type, to_type->get_primitive_type());
    case PrimitiveType::TYPE_TIMESTAMP_NS:
        return create_timestamp_ns_wrapper(options, from_type);
    case PrimitiveType::TYPE_TIMESTAMPTZ:
        return create_timestamptz_wrapper(options, from_type);
    case PrimitiveType::TYPE_IPV4:
    case PrimitiveType::TYPE_IPV6:
        return create_ip_wrapper(options, from_type, to_type->get_primitive_type());
    case PrimitiveType::TYPE_UUID:
        return create_uuid_wrapper(options, from_type);
    case PrimitiveType::TYPE_DECIMALV2:
    case PrimitiveType::TYPE_DECIMAL32:
    case PrimitiveType::TYPE_DECIMAL64:
    case PrimitiveType::TYPE_DECIMAL128I:
    case PrimitiveType::TYPE_DECIMAL256:
        return create_decimal_wrapper(options, from_type, to_type->get_primitive_type());
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING:
        return create_string_wrapper(from_type);
    case PrimitiveType::TYPE_ARRAY:
        return create_array_wrapper(options, from_type,
                                    static_cast<const DataTypeArray&>(*to_type));
    case PrimitiveType::TYPE_STRUCT:
        return create_struct_wrapper(options, from_type,
                                     static_cast<const DataTypeStruct&>(*to_type));
    case PrimitiveType::TYPE_MAP:
        return create_map_wrapper(options, from_type, static_cast<const DataTypeMap&>(*to_type));
    case PrimitiveType::TYPE_HLL:
        return create_hll_wrapper(options, from_type, static_cast<const DataTypeHLL&>(*to_type));
    case PrimitiveType::TYPE_BITMAP:
        return create_bitmap_wrapper(options, from_type,
                                     static_cast<const DataTypeBitMap&>(*to_type));
    case PrimitiveType::TYPE_QUANTILE_STATE:
        return create_quantile_state_wrapper(options, from_type,
                                             static_cast<const DataTypeQuantileState&>(*to_type));
    case PrimitiveType::TYPE_JSONB:
        return create_cast_to_jsonb_wrapper(from_type, static_cast<const DataTypeJsonb&>(*to_type),
                                            options);
    case PrimitiveType::TYPE_VARBINARY:
        return create_varbinary_wrapper(from_type);
    case PrimitiveType::TYPE_GEOMETRY:
    case PrimitiveType::TYPE_GEOGRAPHY:
        return create_spatial_wrapper(from_type);
    default:
        break;
    }

    return create_unsupport_wrapper(from_type->get_name(), to_type->get_name());
}

} // namespace CastWrapper

class PreparedFunctionCast : public PreparedFunctionImpl {
public:
    explicit PreparedFunctionCast(CastWrapper::FunctionContextWrapperType&& wrapper_function_,
                                  const char* name_)
            : wrapper_function(std::move(wrapper_function_)), name(name_) {}

    String get_name() const override { return name; }

protected:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return wrapper_function(context, block.get_by_position(arguments[0]),
                                block.get_by_position(result), input_rows_count, nullptr);
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

private:
    CastWrapper::FunctionContextWrapperType wrapper_function;
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
                CastWrapper::bind_cast_executor(context, get_argument_types()[0],
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
        return nullptr;
    }

    bool use_default_implementation_for_nulls() const override { return false; }
};

void register_function_cast(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBuilderCast>();
}
} // namespace doris
