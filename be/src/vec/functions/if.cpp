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

#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename A, typename B, typename ResultType>
struct NumIfImpl {
    using ArrayCond = PaddedPODArray<UInt8>;
    using ArrayA = PaddedPODArray<A>;
    using ArrayB = PaddedPODArray<B>;
    using ColVecResult = ColumnVector<ResultType>;

    static void vector_vector(const ArrayCond& cond, const ArrayA& a, const ArrayB& b, Block& block,
                              size_t result, UInt32) {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container& res = col_res->get_data();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b[i]);
        block.replace_by_position(result, std::move(col_res));
    }

    static void vector_constant(const ArrayCond& cond, const ArrayA& a, B b, Block& block,
                                size_t result, UInt32) {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container& res = col_res->get_data();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b);
        block.replace_by_position(result, std::move(col_res));
    }

    static void constant_vector(const ArrayCond& cond, A a, const ArrayB& b, Block& block,
                                size_t result, UInt32) {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container& res = col_res->get_data();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b[i]);
        block.replace_by_position(result, std::move(col_res));
    }

    static void constant_constant(const ArrayCond& cond, A a, B b, Block& block, size_t result,
                                  UInt32) {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container& res = col_res->get_data();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b);
        block.replace_by_position(result, std::move(col_res));
    }
};

template <typename A, typename B>
struct NumIfImpl<A, B, NumberTraits::Error> {
private:
    [[noreturn]] static void throw_error() {
        LOG(FATAL) << "Internal logic error: invalid types of arguments 2 and 3 of if";
    }

public:
    template <typename... Args>
    static void vector_vector(Args&&...) {
        throw_error();
    }
    template <typename... Args>
    static void vector_constant(Args&&...) {
        throw_error();
    }
    template <typename... Args>
    static void constant_vector(Args&&...) {
        throw_error();
    }
    template <typename... Args>
    static void constant_constant(Args&&...) {
        throw_error();
    }
};

// todo(wb) support llvm codegen
class FunctionIf : public IFunction {
public:
    static constexpr auto name = "if";

    static FunctionPtr create() { return std::make_shared<FunctionIf>(); }
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 3; }
    bool use_default_implementation_for_nulls() const override { return false; }
    ColumnNumbers get_arguments_that_dont_imply_nullable_return_type(
            size_t /*number_of_arguments*/) const override {
        return {0};
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return get_least_supertype({arguments[1], arguments[2]});
    }

    static ColumnPtr materialize_column_if_const(const ColumnPtr& column) {
        return column->convert_to_full_column_if_const();
    }

    static ColumnPtr make_nullable_column_if_not(const ColumnPtr& column) {
        if (is_column_nullable(*column)) return column;

        return ColumnNullable::create(materialize_column_if_const(column),
                                      ColumnUInt8::create(column->size(), 0));
    }

    static ColumnPtr get_nested_column(const ColumnPtr& column) {
        if (auto* nullable = check_and_get_column<ColumnNullable>(*column))
            return nullable->get_nested_column_ptr();

        return column;
    }

    Status execute_generic(Block& block, const ColumnUInt8* cond_col,
                           const ColumnWithTypeAndName& then_col_type_name,
                           const ColumnWithTypeAndName& else_col_type_name, size_t result,
                           size_t input_row_count) {
        MutableColumnPtr result_column = block.get_by_position(result).type->create_column();
        result_column->reserve(input_row_count);

        const IColumn& then_col = *then_col_type_name.column;
        const IColumn& else_col = *else_col_type_name.column;
        bool then_is_const = is_column_const(then_col);
        bool else_is_const = is_column_const(else_col);

        const auto& cond_array = cond_col->get_data();

        if (then_is_const && else_is_const) {
            const IColumn& then_nested_column =
                    assert_cast<const ColumnConst&>(then_col).get_data_column();
            const IColumn& else_nested_column =
                    assert_cast<const ColumnConst&>(else_col).get_data_column();
            for (size_t i = 0; i < input_row_count; i++) {
                if (cond_array[i])
                    result_column->insert_from(then_nested_column, 0);
                else
                    result_column->insert_from(else_nested_column, 0);
            }
        } else if (then_is_const) {
            const IColumn& then_nested_column =
                    assert_cast<const ColumnConst&>(then_col).get_data_column();

            for (size_t i = 0; i < input_row_count; i++) {
                if (cond_array[i])
                    result_column->insert_from(then_nested_column, 0);
                else
                    result_column->insert_from(else_col, i);
            }
        } else if (else_is_const) {
            const IColumn& else_nested_column =
                    assert_cast<const ColumnConst&>(else_col).get_data_column();

            for (size_t i = 0; i < input_row_count; i++) {
                if (cond_array[i])
                    result_column->insert_from(then_col, i);
                else
                    result_column->insert_from(else_nested_column, 0);
            }
        } else {
            for (size_t i = 0; i < input_row_count; i++) {
                result_column->insert_from(cond_array[i] ? then_col : else_col, i);
            }
        }
        block.replace_by_position(result, std::move(result_column));
        return Status::OK();
    }

    void execute_basic_type(Block& block, const ColumnUInt8* cond_col,
                            const ColumnWithTypeAndName& then_col,
                            const ColumnWithTypeAndName& else_col, size_t result, Status& status) {
        auto call = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using T0 = typename Types::LeftType;
            using T1 = typename Types::RightType;
            using result_type = typename Types::LeftType;

            // for doris, args type and return type must be sanme beacause of type cast has already done before, so here just need one type;
            // but code still need a better impelement
            using ColVecT0 = ColumnVector<T0>;

            if (auto col_then = check_and_get_column<ColVecT0>(then_col.column.get())) {
                if (auto col_else = check_and_get_column<ColVecT0>(else_col.column.get())) {
                    NumIfImpl<T0, T0, result_type>::vector_vector(
                            cond_col->get_data(), col_then->get_data(), col_else->get_data(), block,
                            result, 0);
                } else if (auto col_const_else =
                                   check_and_get_column_const<ColVecT0>(else_col.column.get())) {
                    NumIfImpl<T0, T0, result_type>::vector_constant(
                            cond_col->get_data(), col_then->get_data(),
                            col_const_else->template get_value<T0>(), block, result, 0);
                }
            } else if (auto col_const_then =
                               check_and_get_column_const<ColVecT0>(then_col.column.get())) {
                if (auto col_else = check_and_get_column<ColVecT0>(else_col.column.get())) {
                    NumIfImpl<T0, T0, result_type>::constant_vector(
                            cond_col->get_data(), col_const_then->template get_value<T0>(),
                            col_else->get_data(), block, result, 0);
                } else if (auto col_const_else =
                                   check_and_get_column_const<ColVecT0>(else_col.column.get())) {
                    NumIfImpl<T0, T0, result_type>::constant_constant(
                            cond_col->get_data(), col_const_then->template get_value<T0>(),
                            col_const_else->template get_value<T0>(), block, result, 0);
                }
            } else {
                status = Status::InternalError("unexpected args column type");
            }
            return true;
        };

        // todo(wb): a better way to determine type
        call_on_basic_types<true, true, false, false>(then_col.type->get_type_id(),
                                                      else_col.type->get_type_id(), call);
    }

    bool execute_for_null_then_else(Block& block, const ColumnWithTypeAndName& arg_cond,
                                    const ColumnWithTypeAndName& arg_then,
                                    const ColumnWithTypeAndName& arg_else, size_t result,
                                    size_t input_rows_count, Status& status) {
        bool then_is_null = arg_then.column->only_null();
        bool else_is_null = arg_else.column->only_null();

        if (!then_is_null && !else_is_null) return false;

        if (then_is_null && else_is_null) {
            block.get_by_position(result).column =
                    block.get_by_position(result).type->create_column_const_with_default_value(
                            input_rows_count);
            return true;
        }

        const ColumnUInt8* cond_col = typeid_cast<const ColumnUInt8*>(arg_cond.column.get());
        const ColumnConst* cond_const_col =
                check_and_get_column_const<ColumnVector<UInt8>>(arg_cond.column.get());

        /// If then is NULL, we create Nullable column with null mask OR-ed with condition.
        if (then_is_null) {
            if (cond_col) {
                if (is_column_nullable(*arg_else.column)) {
                    auto arg_else_column = arg_else.column;
                    auto result_column = (*std::move(arg_else_column)).mutate();
                    assert_cast<ColumnNullable&>(*result_column)
                            .apply_null_map(assert_cast<const ColumnUInt8&>(*arg_cond.column));
                    block.replace_by_position(result, std::move(result_column));
                } else {
                    block.replace_by_position(
                            result,
                            ColumnNullable::create(materialize_column_if_const(arg_else.column),
                                                   arg_cond.column));
                }
            } else if (cond_const_col) {
                if (cond_const_col->get_value<UInt8>()) {
                    block.get_by_position(result).column =
                            block.get_by_position(result).type->create_column()->clone_resized(
                                    input_rows_count);
                } else {
                    block.get_by_position(result).column =
                            make_nullable_column_if_not(arg_else.column);
                }
            } else {
                status = Status::InternalError("Illegal column " + arg_cond.column->get_name() +
                                               " of first argument of function " + get_name() +
                                               ". Must be ColumnUInt8 or ColumnConstUInt8.");
            }
            return true;
        }

        /// If else is NULL, we create Nullable column with null mask OR-ed with negated condition.
        if (else_is_null) {
            if (cond_col) {
                size_t size = input_rows_count;
                auto& null_map_data = cond_col->get_data();

                auto negated_null_map = ColumnUInt8::create();
                auto& negated_null_map_data = negated_null_map->get_data();
                negated_null_map_data.resize(size);

                for (size_t i = 0; i < size; ++i) {
                    negated_null_map_data[i] = !null_map_data[i];
                }

                if (is_column_nullable(*arg_then.column)) {
                    auto arg_then_column = arg_then.column;
                    auto result_column = (*std::move(arg_then_column)).mutate();
                    assert_cast<ColumnNullable&>(*result_column)
                            .apply_negated_null_map(
                                    assert_cast<const ColumnUInt8&>(*arg_cond.column));
                    block.replace_by_position(result, std::move(result_column));
                } else {
                    block.replace_by_position(
                            result,
                            ColumnNullable::create(materialize_column_if_const(arg_then.column),
                                                   std::move(negated_null_map)));
                }
            } else if (cond_const_col) {
                if (cond_const_col->get_value<UInt8>()) {
                    block.get_by_position(result).column =
                            make_nullable_column_if_not(arg_then.column);
                } else {
                    block.get_by_position(result).column =
                            block.get_by_position(result).type->create_column()->clone_resized(
                                    input_rows_count);
                }
            } else {
                status = Status::InternalError("Illegal column " + arg_cond.column->get_name() +
                                               " of first argument of function " + get_name() +
                                               ". Must be ColumnUInt8 or ColumnConstUInt8.");
            }
            return true;
        }

        return false;
    }

    bool execute_for_nullable_then_else(Block& block, const ColumnWithTypeAndName& arg_cond,
                                        const ColumnWithTypeAndName& arg_then,
                                        const ColumnWithTypeAndName& arg_else, size_t result,
                                        size_t input_rows_count) {
        auto* then_is_nullable = check_and_get_column<ColumnNullable>(*arg_then.column);
        auto* else_is_nullable = check_and_get_column<ColumnNullable>(*arg_else.column);

        if (!then_is_nullable && !else_is_nullable) return false;

        /** Calculate null mask of result and nested column separately.
          */
        ColumnPtr result_null_mask;
        {
            Block temporary_block(
                    {arg_cond,
                     {then_is_nullable ? then_is_nullable->get_null_map_column_ptr()
                                       : DataTypeUInt8().create_column_const_with_default_value(
                                                 input_rows_count),
                      std::make_shared<DataTypeUInt8>(), ""},
                     {else_is_nullable ? else_is_nullable->get_null_map_column_ptr()
                                       : DataTypeUInt8().create_column_const_with_default_value(
                                                 input_rows_count),
                      std::make_shared<DataTypeUInt8>(), ""},
                     {nullptr, std::make_shared<DataTypeUInt8>(), ""}});

            execute_impl(temporary_block, {0, 1, 2}, 3, temporary_block.rows());

            result_null_mask = temporary_block.get_by_position(3).column;
        }

        ColumnPtr result_nested_column;

        {
            Block temporary_block(
                    {arg_cond,
                     {get_nested_column(arg_then.column), remove_nullable(arg_then.type), ""},
                     {get_nested_column(arg_else.column), remove_nullable(arg_else.type), ""},
                     {nullptr, remove_nullable(block.get_by_position(result).type), ""}});

            execute_impl(temporary_block, {0, 1, 2}, 3, temporary_block.rows());

            result_nested_column = temporary_block.get_by_position(3).column;
        }

        auto column = ColumnNullable::create(materialize_column_if_const(result_nested_column),
                                             materialize_column_if_const(result_null_mask));
        block.replace_by_position(result, std::move(column));
        return true;
    }

    bool execute_for_null_condition(Block& block, const ColumnWithTypeAndName& arg_cond,
                                    const ColumnWithTypeAndName& arg_then,
                                    const ColumnWithTypeAndName& arg_else, size_t result) {
        bool cond_is_null = arg_cond.column->only_null();

        if (cond_is_null) {
            block.replace_by_position(result, arg_else.column);
            return true;
        }

        return false;
    }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
        const ColumnWithTypeAndName& arg_cond = block.get_by_position(arguments[0]);
        const ColumnWithTypeAndName& arg_then = block.get_by_position(arguments[1]);
        const ColumnWithTypeAndName& arg_else = block.get_by_position(arguments[2]);

        /// A case for identical then and else (pointers are the same).
        if (arg_then.column.get() == arg_else.column.get()) {
            /// Just point result to them.
            block.replace_by_position(result, arg_then.column);
            return Status::OK();
        }

        Status ret = Status::OK();
        if (execute_for_null_condition(block, arg_cond, arg_then, arg_else, result) ||
            execute_for_null_then_else(block, arg_cond, arg_then, arg_else, result,
                                       input_rows_count, ret) ||
            execute_for_nullable_then_else(block, arg_cond, arg_then, arg_else, result,
                                           input_rows_count)) {
            return ret;
        }

        const ColumnUInt8* cond_col = typeid_cast<const ColumnUInt8*>(arg_cond.column.get());
        const ColumnConst* cond_const_col =
                check_and_get_column_const<ColumnVector<UInt8>>(arg_cond.column.get());

        if (cond_const_col) {
            block.get_by_position(result).column =
                    cond_const_col->get_value<UInt8>() ? arg_then.column : arg_else.column;
            return Status::OK();
        }

        if (!cond_col) {
            return Status::InvalidArgument("Illegal column " + arg_cond.column->get_name() +
                                           " of first argument of function " + get_name() +
                                           ",Must be ColumnUInt8 or ColumnConstUInt8.");
        }

        WhichDataType which_type(arg_then.type);
        if (which_type.is_int() || which_type.is_float()) {
            Status status;
            execute_basic_type(block, cond_col, arg_then, arg_else, result, status);
            return status;
        } else {
            return execute_generic(block, cond_col, arg_then, arg_else, result, input_rows_count);
        }
    }
};

void register_function_if(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionIf>();
}

} // namespace doris::vectorized
