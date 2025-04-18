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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionsConversion.h
// and modified by Doris
#include "function_cast.h"

#include "function_cast_to_string.h"
#include "vec/functions/simple_function_factory.h"
namespace doris::vectorized {

class FunctionCast final : public IFunctionBase {
public:
    using WrapperType =
            std::function<Status(FunctionContext*, Block&, const ColumnNumbers&, size_t, size_t)>;
    using ElementWrappers = std::vector<WrapperType>;

    FunctionCast(const char* name_, const DataTypes& argument_types_,
                 const DataTypePtr& return_type_)
            : name(name_), argument_types(argument_types_), return_type(return_type_) {}

    const DataTypes& get_argument_types() const override { return argument_types; }
    const DataTypePtr& get_return_type() const override { return return_type; }

    PreparedFunctionPtr prepare(FunctionContext* context, const Block& /*sample_block*/,
                                const ColumnNumbers& /*arguments*/,
                                uint32_t /*result*/) const override {
        return std::make_shared<PreparedFunctionCast>(
                prepare_unpack_dictionaries(context, get_argument_types()[0], get_return_type()),
                name);
    }

    String get_name() const override { return name; }

    bool is_use_default_implementation_for_constants() const override { return true; }

private:
    const char* name = nullptr;

    DataTypes argument_types;
    DataTypePtr return_type;

    template <typename DataType>
    WrapperType create_wrapper(const DataTypePtr& from_type, const DataType* const,
                               bool requested_result_is_nullable) const {
        FunctionPtr function;

        if (requested_result_is_nullable &&
            check_and_get_data_type<DataTypeString>(from_type.get())) {
            /// In case when converting to Nullable type, we apply different parsing rule,
            /// that will not throw an exception but return NULL in case of malformed input.
            function = FunctionConvertFromString<DataType, NameCast>::create();
        } else if (requested_result_is_nullable &&
                   (IsTimeType<DataType> || IsTimeV2Type<DataType>)&&!(
                           check_and_get_data_type<DataTypeDateTime>(from_type.get()) ||
                           check_and_get_data_type<DataTypeDate>(from_type.get()) ||
                           check_and_get_data_type<DataTypeDateV2>(from_type.get()) ||
                           check_and_get_data_type<DataTypeDateTimeV2>(from_type.get()))) {
            function = FunctionConvertToTimeType<DataType, NameCast>::create();
        } else {
            function = FunctionTo<DataType>::Type::create();
        }

        /// Check conversion using underlying function
        { function->get_return_type(ColumnsWithTypeAndName(1, {nullptr, from_type, ""})); }

        return [function](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
            return function->execute(context, block, arguments, result, input_rows_count);
        };
    }

    WrapperType create_string_wrapper(const DataTypePtr& from_type) const {
        FunctionPtr function = FunctionToString::create();

        /// Check conversion using underlying function
        { function->get_return_type(ColumnsWithTypeAndName(1, {nullptr, from_type, ""})); }

        return [function](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          const uint32_t result, size_t input_rows_count) {
            return function->execute(context, block, arguments, result, input_rows_count);
        };
    }

    template <typename FieldType>
    WrapperType create_decimal_wrapper(const DataTypePtr& from_type,
                                       const DataTypeDecimal<FieldType>* to_type) const {
        using ToDataType = DataTypeDecimal<FieldType>;

        TypeIndex type_index = from_type->get_type_id();
        UInt32 precision = to_type->get_precision();
        UInt32 scale = to_type->get_scale();

        WhichDataType which(type_index);
        bool ok = which.is_int() || which.is_native_uint() || which.is_decimal() ||
                  which.is_float() || which.is_date_or_datetime() ||
                  which.is_date_v2_or_datetime_v2() || which.is_string_or_fixed_string();
        if (!ok) {
            return create_unsupport_wrapper(from_type->get_name(), to_type->get_name());
        }

        return [type_index, precision, scale](FunctionContext* context, Block& block,
                                              const ColumnNumbers& arguments, const uint32_t result,
                                              size_t input_rows_count) {
            auto res = call_on_index_and_data_type<ToDataType>(
                    type_index, [&](const auto& types) -> bool {
                        using Types = std::decay_t<decltype(types)>;
                        using LeftDataType = typename Types::LeftType;
                        using RightDataType = typename Types::RightType;

                        auto state = ConvertImpl<LeftDataType, RightDataType, NameCast>::execute(
                                context, block, arguments, result, input_rows_count,
                                PrecisionScaleArg {precision, scale});
                        if (!state) {
                            throw Exception(state.code(), state.to_string());
                        }
                        return true;
                    });

            /// Additionally check if call_on_index_and_data_type wasn't called at all.
            if (!res) {
                auto to = DataTypeDecimal<FieldType>(precision, scale);
                return Status::RuntimeError("Conversion from {} to {} is not supported",
                                            getTypeName(type_index), to.get_name());
            }
            return Status::OK();
        };
    }

    WrapperType create_identity_wrapper(const DataTypePtr&) const {
        return [](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                  const uint32_t result, size_t /*input_rows_count*/) {
            block.get_by_position(result).column = block.get_by_position(arguments.front()).column;
            return Status::OK();
        };
    }

    WrapperType create_nothing_wrapper(const IDataType* to_type) const {
        ColumnPtr res = to_type->create_column_const_with_default_value(1);
        return [res](FunctionContext* context, Block& block, const ColumnNumbers&,
                     const uint32_t result, size_t input_rows_count) {
            /// Column of Nothing type is trivially convertible to any other column
            block.get_by_position(result).column =
                    res->clone_resized(input_rows_count)->convert_to_full_column_if_const();
            return Status::OK();
        };
    }

    WrapperType create_unsupport_wrapper(const String error_msg) const {
        return [error_msg](FunctionContext* /*context*/, Block& /*block*/,
                           const ColumnNumbers& /*arguments*/, const size_t /*result*/,
                           size_t /*input_rows_count*/) {
            return Status::InvalidArgument(error_msg);
        };
    }

    WrapperType create_unsupport_wrapper(const String from_type_name,
                                         const String to_type_name) const {
        const String error_msg = fmt::format("Conversion from {} to {} is not supported",
                                             from_type_name, to_type_name);
        return create_unsupport_wrapper(error_msg);
    }

    WrapperType create_hll_wrapper(FunctionContext* context, const DataTypePtr& from_type_untyped,
                                   const DataTypeHLL& to_type) const {
        /// Conversion from String through parsing.
        if (check_and_get_data_type<DataTypeString>(from_type_untyped.get())) {
            return &ConvertImplGenericFromString::execute;
        }

        //TODO if from is not string, it must be HLL?
        const auto* from_type = check_and_get_data_type<DataTypeHLL>(from_type_untyped.get());

        if (!from_type) {
            return create_unsupport_wrapper(
                    "CAST AS HLL can only be performed between HLL, String "
                    "types");
        }

        return nullptr;
    }

    WrapperType create_bitmap_wrapper(FunctionContext* context,
                                      const DataTypePtr& from_type_untyped,
                                      const DataTypeBitMap& to_type) const {
        /// Conversion from String through parsing.
        if (check_and_get_data_type<DataTypeString>(from_type_untyped.get())) {
            return &ConvertImplGenericFromString::execute;
        }

        //TODO if from is not string, it must be BITMAP?
        const auto* from_type = check_and_get_data_type<DataTypeBitMap>(from_type_untyped.get());

        if (!from_type) {
            return create_unsupport_wrapper(
                    "CAST AS BITMAP can only be performed between BITMAP, String "
                    "types");
        }

        return nullptr;
    }

    WrapperType create_array_wrapper(FunctionContext* context, const DataTypePtr& from_type_untyped,
                                     const DataTypeArray& to_type) const {
        /// Conversion from String through parsing.
        if (check_and_get_data_type<DataTypeString>(from_type_untyped.get())) {
            return &ConvertImplGenericFromString::execute;
        }

        const auto* from_type = check_and_get_data_type<DataTypeArray>(from_type_untyped.get());

        if (!from_type) {
            return create_unsupport_wrapper(
                    "CAST AS Array can only be performed between same-dimensional Array, String "
                    "types");
        }

        DataTypePtr from_nested_type = from_type->get_nested_type();

        /// In query SELECT CAST([] AS Array(Array(String))) from type is Array(Nothing)
        bool from_empty_array = is_nothing(from_nested_type);

        if (from_type->get_number_of_dimensions() != to_type.get_number_of_dimensions() &&
            !from_empty_array) {
            return create_unsupport_wrapper(
                    "CAST AS Array can only be performed between same-dimensional array types");
        }

        const DataTypePtr& to_nested_type = to_type.get_nested_type();

        /// Prepare nested type conversion
        const auto nested_function =
                prepare_unpack_dictionaries(context, from_nested_type, to_nested_type);

        return [nested_function, from_nested_type, to_nested_type](
                       FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                       const uint32_t result, size_t /*input_rows_count*/) -> Status {
            ColumnPtr from_column = block.get_by_position(arguments.front()).column;

            const ColumnArray* from_col_array =
                    check_and_get_column<ColumnArray>(from_column.get());

            if (from_col_array) {
                /// create columns for converting nested column containing original and result columns
                ColumnWithTypeAndName from_nested_column {from_col_array->get_data_ptr(),
                                                          from_nested_type, ""};

                /// convert nested column
                ColumnNumbers new_arguments {block.columns()};
                block.insert(from_nested_column);

                size_t nested_result = block.columns();
                block.insert({to_nested_type, ""});
                RETURN_IF_ERROR(nested_function(context, block, new_arguments, nested_result,
                                                from_col_array->get_data_ptr()->size()));
                auto nested_result_column = block.get_by_position(nested_result).column;

                /// set converted nested column to result
                block.get_by_position(result).column = ColumnArray::create(
                        nested_result_column, from_col_array->get_offsets_ptr());
            } else {
                return Status::RuntimeError("Illegal column {} for function CAST AS Array",
                                            from_column->get_name());
            }
            return Status::OK();
        };
    }

    // check jsonb value type and get to_type value
    WrapperType create_jsonb_wrapper(const DataTypeJsonb& from_type, const DataTypePtr& to_type,
                                     bool jsonb_string_as_string) const {
        switch (to_type->get_type_id()) {
        case TypeIndex::UInt8:
            return &ConvertImplFromJsonb<TypeIndex::UInt8, ColumnUInt8>::execute;
        case TypeIndex::Int8:
            return &ConvertImplFromJsonb<TypeIndex::Int8, ColumnInt8>::execute;
        case TypeIndex::Int16:
            return &ConvertImplFromJsonb<TypeIndex::Int16, ColumnInt16>::execute;
        case TypeIndex::Int32:
            return &ConvertImplFromJsonb<TypeIndex::Int32, ColumnInt32>::execute;
        case TypeIndex::Int64:
            return &ConvertImplFromJsonb<TypeIndex::Int64, ColumnInt64>::execute;
        case TypeIndex::Int128:
            return &ConvertImplFromJsonb<TypeIndex::Int128, ColumnInt128>::execute;
        case TypeIndex::Float64:
            return &ConvertImplFromJsonb<TypeIndex::Float64, ColumnFloat64>::execute;
        case TypeIndex::String:
            if (!jsonb_string_as_string) {
                // Conversion from String through parsing.
                return &ConvertImplGenericToString::execute2;
            } else {
                return ConvertImplGenericFromJsonb::execute;
            }
        default:
            return ConvertImplGenericFromJsonb::execute;
        }
    }

    // create cresponding jsonb value with type to_type
    // use jsonb writer to create jsonb value
    WrapperType create_jsonb_wrapper(const DataTypePtr& from_type, const DataTypeJsonb& to_type,
                                     bool string_as_jsonb_string) const {
        switch (from_type->get_type_id()) {
        case TypeIndex::UInt8:
            return &ConvertImplNumberToJsonb<ColumnUInt8>::execute;
        case TypeIndex::Int8:
            return &ConvertImplNumberToJsonb<ColumnInt8>::execute;
        case TypeIndex::Int16:
            return &ConvertImplNumberToJsonb<ColumnInt16>::execute;
        case TypeIndex::Int32:
            return &ConvertImplNumberToJsonb<ColumnInt32>::execute;
        case TypeIndex::Int64:
            return &ConvertImplNumberToJsonb<ColumnInt64>::execute;
        case TypeIndex::Int128:
            return &ConvertImplNumberToJsonb<ColumnInt128>::execute;
        case TypeIndex::Float64:
            return &ConvertImplNumberToJsonb<ColumnFloat64>::execute;
        case TypeIndex::String:
            if (string_as_jsonb_string) {
                // We convert column string to jsonb type just add a string jsonb field to dst column instead of parse
                // each line in original string column.
                return &ConvertImplStringToJsonbAsJsonbString::execute;
            } else {
                return &ConvertImplGenericFromString::execute;
            }
        case TypeIndex::Nothing:
            return &ConvertNothingToJsonb::execute;
        default:
            return &ConvertImplGenericToJsonb::execute;
        }
    }

    struct ConvertImplGenericFromVariant {
        static Status execute(const FunctionCast* fn, FunctionContext* context, Block& block,
                              const ColumnNumbers& arguments, const uint32_t result,
                              size_t input_rows_count) {
            auto& data_type_to = block.get_by_position(result).type;
            const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
            auto& col_from = col_with_type_and_name.column;
            auto& variant = assert_cast<const ColumnObject&>(*col_from);
            ColumnPtr col_to = data_type_to->create_column();
            if (!variant.is_finalized()) {
                // ColumnObject should be finalized before parsing, finalize maybe modify original column structure
                variant.assume_mutable()->finalize();
            }
            // It's important to convert as many elements as possible in this context. For instance,
            // if the root of this variant column is a number column, converting it to a number column
            // is acceptable. However, if the destination type is a string and root is none scalar root, then
            // we should convert the entire tree to a string.
            bool is_root_valuable =
                    variant.is_scalar_variant() ||
                    (!variant.is_null_root() &&
                     !WhichDataType(remove_nullable(variant.get_root_type())).is_nothing() &&
                     !WhichDataType(data_type_to).is_string());
            if (is_root_valuable) {
                ColumnPtr nested = variant.get_root();
                auto nested_from_type = variant.get_root_type();
                // DCHECK(nested_from_type->is_nullable());
                DCHECK(!data_type_to->is_nullable());
                auto new_context = context->clone();
                new_context->set_jsonb_string_as_string(true);
                // dst type nullable has been removed, so we should remove the inner nullable of root column
                auto wrapper = fn->prepare_impl(
                        new_context.get(), remove_nullable(nested_from_type), data_type_to, true);
                Block tmp_block {{remove_nullable(nested), remove_nullable(nested_from_type), ""}};
                tmp_block.insert({nullptr, data_type_to, ""});
                /// Perform the requested conversion.
                Status st = wrapper(new_context.get(), tmp_block, {0}, 1, input_rows_count);
                if (!st.ok()) {
                    // Fill with default values, which is null
                    col_to->assume_mutable()->insert_many_defaults(input_rows_count);
                    col_to = make_nullable(col_to, true);
                } else {
                    col_to = tmp_block.get_by_position(1).column;
                    // Note: here we should return the nullable result column
                    col_to = wrap_in_nullable(
                            col_to,
                            Block({{nested, nested_from_type, ""}, {col_to, data_type_to, ""}}),
                            {0}, 1, input_rows_count);
                }
            } else {
                if (variant.empty()) {
                    // TODO not found root cause, a tmp fix
                    col_to->assume_mutable()->insert_many_defaults(input_rows_count);
                    col_to = make_nullable(col_to, true);
                } else if (!data_type_to->is_nullable() &&
                           !WhichDataType(data_type_to).is_string()) {
                    col_to->assume_mutable()->insert_many_defaults(input_rows_count);
                    col_to = make_nullable(col_to, true);
                } else if (WhichDataType(data_type_to).is_string()) {
                    return ConvertImplGenericToString::execute2(context, block, arguments, result,
                                                                input_rows_count);
                } else {
                    assert_cast<ColumnNullable&>(*col_to->assume_mutable())
                            .insert_many_defaults(input_rows_count);
                }
            }
            if (col_to->size() != input_rows_count) {
                return Status::InternalError("Unmatched row count {}, expected {}", col_to->size(),
                                             input_rows_count);
            }

            block.replace_by_position(result, std::move(col_to));
            return Status::OK();
        }
    };

    struct ConvertImplGenericToVariant {
        static Status execute(FunctionContext* context, Block& block,
                              const ColumnNumbers& arguments, const uint32_t result,
                              size_t input_rows_count) {
            // auto& data_type_to = block.get_by_position(result).type;
            const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
            auto& from_type = col_with_type_and_name.type;
            auto& col_from = col_with_type_and_name.column;
            // set variant root column/type to from column/type
            auto variant = ColumnObject::create(true /*always nullable*/);
            variant->create_root(from_type, col_from->assume_mutable());
            block.replace_by_position(result, std::move(variant));
            return Status::OK();
        }
    };

    // create cresponding variant value to wrap from_type
    WrapperType create_variant_wrapper(const DataTypePtr& from_type,
                                       const DataTypeObject& to_type) const {
        return &ConvertImplGenericToVariant::execute;
    }

    // create cresponding type convert from variant
    WrapperType create_variant_wrapper(const DataTypeObject& from_type,
                                       const DataTypePtr& to_type) const {
        return [this](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                      const uint32_t result, size_t input_rows_count) -> Status {
            return ConvertImplGenericFromVariant::execute(this, context, block, arguments, result,
                                                          input_rows_count);
        };
    }

    //TODO(Amory) . Need support more cast for key , value for map
    WrapperType create_map_wrapper(FunctionContext* context, const DataTypePtr& from_type,
                                   const DataTypeMap& to_type) const {
        if (from_type->get_type_id() == TypeIndex::String) {
            return &ConvertImplGenericFromString::execute;
        }
        auto from = check_and_get_data_type<DataTypeMap>(from_type.get());
        if (!from) {
            return create_unsupport_wrapper(
                    fmt::format("CAST AS Map can only be performed between Map types or from "
                                "String. from type: {}, to type: {}",
                                from_type->get_name(), to_type.get_name()));
        }
        DataTypes from_kv_types;
        DataTypes to_kv_types;
        from_kv_types.reserve(2);
        to_kv_types.reserve(2);
        from_kv_types.push_back(from->get_key_type());
        from_kv_types.push_back(from->get_value_type());
        to_kv_types.push_back(to_type.get_key_type());
        to_kv_types.push_back(to_type.get_value_type());

        auto kv_wrappers = get_element_wrappers(context, from_kv_types, to_kv_types);
        return [kv_wrappers, from_kv_types, to_kv_types](
                       FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                       const uint32_t result, size_t /*input_rows_count*/) -> Status {
            auto& from_column = block.get_by_position(arguments.front()).column;
            auto from_col_map = check_and_get_column<ColumnMap>(from_column.get());
            if (!from_col_map) {
                return Status::RuntimeError("Illegal column {} for function CAST AS MAP",
                                            from_column->get_name());
            }

            Columns converted_columns(2);
            ColumnsWithTypeAndName columnsWithTypeAndName(2);
            columnsWithTypeAndName[0] = {from_col_map->get_keys_ptr(), from_kv_types[0], ""};
            columnsWithTypeAndName[1] = {from_col_map->get_values_ptr(), from_kv_types[1], ""};

            for (size_t i = 0; i < 2; ++i) {
                ColumnNumbers element_arguments {block.columns()};
                block.insert(columnsWithTypeAndName[i]);
                size_t element_result = block.columns();
                block.insert({to_kv_types[i], ""});
                RETURN_IF_ERROR(kv_wrappers[i](context, block, element_arguments, element_result,
                                               columnsWithTypeAndName[i].column->size()));
                converted_columns[i] = block.get_by_position(element_result).column;
            }

            block.get_by_position(result).column = ColumnMap::create(
                    converted_columns[0], converted_columns[1], from_col_map->get_offsets_ptr());
            return Status::OK();
        };
    }

    ElementWrappers get_element_wrappers(FunctionContext* context,
                                         const DataTypes& from_element_types,
                                         const DataTypes& to_element_types) const {
        DCHECK(from_element_types.size() == to_element_types.size());
        ElementWrappers element_wrappers;
        element_wrappers.reserve(from_element_types.size());
        for (size_t i = 0; i < from_element_types.size(); ++i) {
            const DataTypePtr& from_element_type = from_element_types[i];
            const DataTypePtr& to_element_type = to_element_types[i];
            element_wrappers.push_back(
                    prepare_unpack_dictionaries(context, from_element_type, to_element_type));
        }
        return element_wrappers;
    }

    // check struct value type and get to_type value
    // TODO: need handle another type to cast struct
    WrapperType create_struct_wrapper(FunctionContext* context, const DataTypePtr& from_type,
                                      const DataTypeStruct& to_type) const {
        // support CAST AS Struct from string
        if (from_type->get_type_id() == TypeIndex::String) {
            return &ConvertImplGenericFromString::execute;
        }

        // only support CAST AS Struct from struct or string types
        auto from = check_and_get_data_type<DataTypeStruct>(from_type.get());
        if (!from) {
            return create_unsupport_wrapper(
                    fmt::format("CAST AS Struct can only be performed between struct types or from "
                                "String. Left type: {}, right type: {}",
                                from_type->get_name(), to_type.get_name()));
        }

        const auto& from_element_types = from->get_elements();
        const auto& to_element_types = to_type.get_elements();
        // only support CAST AS Struct from struct type with same number of elements
        if (from_element_types.size() != to_element_types.size()) {
            return create_unsupport_wrapper(
                    fmt::format("CAST AS Struct can only be performed between struct types with "
                                "the same number of elements. Left type: {}, right type: {}",
                                from_type->get_name(), to_type.get_name()));
        }

        auto element_wrappers = get_element_wrappers(context, from_element_types, to_element_types);
        return [element_wrappers, from_element_types, to_element_types](
                       FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                       const uint32_t result, size_t /*input_rows_count*/) -> Status {
            auto& from_column = block.get_by_position(arguments.front()).column;
            auto from_col_struct = check_and_get_column<ColumnStruct>(from_column.get());
            if (!from_col_struct) {
                return Status::RuntimeError("Illegal column {} for function CAST AS Struct",
                                            from_column->get_name());
            }

            size_t elements_num = to_element_types.size();
            Columns converted_columns(elements_num);
            for (size_t i = 0; i < elements_num; ++i) {
                ColumnWithTypeAndName from_element_column {from_col_struct->get_column_ptr(i),
                                                           from_element_types[i], ""};
                ColumnNumbers element_arguments {block.columns()};
                block.insert(from_element_column);

                size_t element_result = block.columns();
                block.insert({to_element_types[i], ""});

                RETURN_IF_ERROR(element_wrappers[i](context, block, element_arguments,
                                                    element_result,
                                                    from_col_struct->get_column(i).size()));
                converted_columns[i] = block.get_by_position(element_result).column;
            }

            block.get_by_position(result).column = ColumnStruct::create(converted_columns);
            return Status::OK();
        };
    }

    WrapperType prepare_unpack_dictionaries(FunctionContext* context, const DataTypePtr& from_type,
                                            const DataTypePtr& to_type) const {
        const auto& from_nested = from_type;
        const auto& to_nested = to_type;

        if (from_type->is_null_literal()) {
            if (!to_nested->is_nullable()) {
                return create_unsupport_wrapper("Cannot convert NULL to a non-nullable type");
            }

            return [](FunctionContext* context, Block& block, const ColumnNumbers&,
                      const uint32_t result, size_t input_rows_count) {
                auto& res = block.get_by_position(result);
                res.column = res.type->create_column_const_with_default_value(input_rows_count)
                                     ->convert_to_full_column_if_const();
                return Status::OK();
            };
        }

        bool skip_not_null_check = false;
        auto wrapper =
                prepare_remove_nullable(context, from_nested, to_nested, skip_not_null_check);

        return wrapper;
    }

    static bool need_replace_null_data_to_default(FunctionContext* context,
                                                  const DataTypePtr& from_type,
                                                  const DataTypePtr& to_type) {
        if (from_type->equals(*to_type)) {
            return false;
        }

        auto make_default_wrapper = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using ToDataType = typename Types::LeftType;

            if constexpr (!(IsDataTypeDecimalOrNumber<ToDataType> || IsTimeType<ToDataType> ||
                            IsTimeV2Type<ToDataType> ||
                            std::is_same_v<ToDataType, DataTypeTimeV2>)) {
                return false;
            }
            return call_on_index_and_data_type<
                    ToDataType>(from_type->get_type_id(), [&](const auto& types2) -> bool {
                using Types2 = std::decay_t<decltype(types2)>;
                using FromDataType = typename Types2::LeftType;
                if constexpr (!(IsDataTypeDecimalOrNumber<FromDataType> ||
                                IsTimeType<FromDataType> || IsTimeV2Type<FromDataType> ||
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
                        to_max_digits =
                                NumberTraits::max_ascii_len<typename ToFieldType::NativeType>();

                        const auto* to_decimal_type =
                                check_and_get_data_type<ToDataType>(to_type.get());
                        to_precision = to_decimal_type->get_precision();
                        ToDataType::check_type_precision(to_precision);

                        to_scale = to_decimal_type->get_scale();
                        ToDataType::check_type_scale(to_scale);

                        max_result = ToDataType::get_max_digits_number(to_precision);
                        min_result = -max_result;
                    }
                    if constexpr (std::is_integral_v<ToFieldType> ||
                                  std::is_floating_point_v<ToFieldType>) {
                        max_result = type_limit<ToFieldType>::max();
                        min_result = type_limit<ToFieldType>::min();
                        to_max_digits = NumberTraits::max_ascii_len<ToFieldType>();
                        to_precision = to_max_digits;
                    }

                    bool narrow_integral =
                            context->check_overflow_for_decimal() &&
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

        return call_on_index_and_data_type<void>(to_type->get_type_id(), make_default_wrapper);
    }

    WrapperType prepare_remove_nullable(FunctionContext* context, const DataTypePtr& from_type,
                                        const DataTypePtr& to_type,
                                        bool skip_not_null_check) const {
        /// Determine whether pre-processing and/or post-processing must take place during conversion.
        bool result_is_nullable = to_type->is_nullable();

        if (result_is_nullable) {
            return [this, from_type, to_type](FunctionContext* context, Block& block,
                                              const ColumnNumbers& arguments, const uint32_t result,
                                              size_t input_rows_count) {
                auto from_type_not_nullable = remove_nullable(from_type);
                auto to_type_not_nullable = remove_nullable(to_type);

                bool replace_null_data_to_default = need_replace_null_data_to_default(
                        context, from_type_not_nullable, to_type_not_nullable);

                auto nested_result_index = block.columns();
                block.insert(block.get_by_position(result).get_nested());
                auto nested_source_index = block.columns();
                block.insert(block.get_by_position(arguments[0])
                                     .get_nested(replace_null_data_to_default));

                RETURN_IF_ERROR(prepare_impl(context, from_type_not_nullable, to_type_not_nullable,
                                             true)(context, block, {nested_source_index},
                                                   nested_result_index, input_rows_count));

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
    WrapperType prepare_impl(FunctionContext* context, const DataTypePtr& from_type,
                             const DataTypePtr& to_type, bool requested_result_is_nullable) const {
        if (from_type->equals(*to_type)) {
            return create_identity_wrapper(from_type);
        }

        // variant needs to be judged first
        if (to_type->get_type_id() == TypeIndex::VARIANT) {
            return create_variant_wrapper(from_type, static_cast<const DataTypeObject&>(*to_type));
        }
        if (from_type->get_type_id() == TypeIndex::VARIANT) {
            return create_variant_wrapper(static_cast<const DataTypeObject&>(*from_type), to_type);
        }

        switch (from_type->get_type_id()) {
        case TypeIndex::Nothing:
            return create_nothing_wrapper(to_type.get());
        case TypeIndex::JSONB:
            return create_jsonb_wrapper(static_cast<const DataTypeJsonb&>(*from_type), to_type,
                                        context ? context->jsonb_string_as_string() : false);
        default:
            break;
        }

        WrapperType ret;

        auto make_default_wrapper = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using ToDataType = typename Types::LeftType;

            if constexpr (std::is_same_v<ToDataType, DataTypeUInt8> ||
                          std::is_same_v<ToDataType, DataTypeUInt16> ||
                          std::is_same_v<ToDataType, DataTypeUInt32> ||
                          std::is_same_v<ToDataType, DataTypeUInt64> ||
                          std::is_same_v<ToDataType, DataTypeInt8> ||
                          std::is_same_v<ToDataType, DataTypeInt16> ||
                          std::is_same_v<ToDataType, DataTypeInt32> ||
                          std::is_same_v<ToDataType, DataTypeInt64> ||
                          std::is_same_v<ToDataType, DataTypeInt128> ||
                          std::is_same_v<ToDataType, DataTypeFloat32> ||
                          std::is_same_v<ToDataType, DataTypeFloat64> ||
                          std::is_same_v<ToDataType, DataTypeDate> ||
                          std::is_same_v<ToDataType, DataTypeDateTime> ||
                          std::is_same_v<ToDataType, DataTypeDateV2> ||
                          std::is_same_v<ToDataType, DataTypeDateTimeV2> ||
                          std::is_same_v<ToDataType, DataTypeTimeV2> ||
                          std::is_same_v<ToDataType, DataTypeIPv4> ||
                          std::is_same_v<ToDataType, DataTypeIPv6>) {
                ret = create_wrapper(from_type, check_and_get_data_type<ToDataType>(to_type.get()),
                                     requested_result_is_nullable);
                return true;
            }

            if constexpr (std::is_same_v<ToDataType, DataTypeDecimal<Decimal32>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal64>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal128V2>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal128V3>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal256>>) {
                ret = create_decimal_wrapper(from_type,
                                             check_and_get_data_type<ToDataType>(to_type.get()));
                return true;
            }

            return false;
        };

        if (call_on_index_and_data_type<void>(to_type->get_type_id(), make_default_wrapper)) {
            return ret;
        }

        switch (to_type->get_type_id()) {
        case TypeIndex::String:
            return create_string_wrapper(from_type);
        case TypeIndex::Array:
            return create_array_wrapper(context, from_type,
                                        static_cast<const DataTypeArray&>(*to_type));
        case TypeIndex::Struct:
            return create_struct_wrapper(context, from_type,
                                         static_cast<const DataTypeStruct&>(*to_type));
        case TypeIndex::Map:
            return create_map_wrapper(context, from_type,
                                      static_cast<const DataTypeMap&>(*to_type));
        case TypeIndex::HLL:
            return create_hll_wrapper(context, from_type,
                                      static_cast<const DataTypeHLL&>(*to_type));
        case TypeIndex::BitMap:
            return create_bitmap_wrapper(context, from_type,
                                         static_cast<const DataTypeBitMap&>(*to_type));
        case TypeIndex::JSONB:
            return create_jsonb_wrapper(from_type, static_cast<const DataTypeJsonb&>(*to_type),
                                        context ? context->string_as_jsonb_string() : false);
        default:
            break;
        }

        return create_unsupport_wrapper(from_type->get_name(), to_type->get_name());
    }
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

        for (size_t i = 0; i < arguments.size(); ++i) data_types[i] = arguments[i].type;

        return std::make_shared<FunctionCast>(name, data_types, return_type);
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        DataTypePtr type = arguments[1].type;
        DCHECK(type != nullptr);
        bool need_to_be_nullable = false;
        // 1. from_type is nullable
        need_to_be_nullable |= arguments[0].type->is_nullable();
        // 2. from_type is string, to_type is not string
        need_to_be_nullable |= (arguments[0].type->get_type_id() == TypeIndex::String) &&
                               (type->get_type_id() != TypeIndex::String);
        // 3. from_type is not DateTime/Date, to_type is DateTime/Date
        need_to_be_nullable |= (arguments[0].type->get_type_id() != TypeIndex::Date &&
                                arguments[0].type->get_type_id() != TypeIndex::DateTime) &&
                               (type->get_type_id() == TypeIndex::Date ||
                                type->get_type_id() == TypeIndex::DateTime);
        // 4. from_type is not DateTimeV2/DateV2, to_type is DateTimeV2/DateV2
        need_to_be_nullable |= (arguments[0].type->get_type_id() != TypeIndex::DateV2 &&
                                arguments[0].type->get_type_id() != TypeIndex::DateTimeV2) &&
                               (type->get_type_id() == TypeIndex::DateV2 ||
                                type->get_type_id() == TypeIndex::DateTimeV2);
        if (need_to_be_nullable && !type->is_nullable()) {
            return make_nullable(type);
        }

        return type;
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_low_cardinality_columns() const override { return false; }
};

void register_function_cast(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBuilderCast>();
}
} // namespace doris::vectorized
