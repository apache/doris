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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/IFunction.h
// and modified by Doris

#pragma once

#include <memory>

#include "common/status.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/names.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

class Field;

// Only use dispose the variadic argument
template <typename T>
auto has_variadic_argument_types(T&& arg) -> decltype(T::get_variadic_argument_types()) {};
void has_variadic_argument_types(...);

/// The simplest executable object.
/// Motivation:
///  * Prepare something heavy once before main execution loop instead of doing it for each block.
///  * Provide const interface for IFunctionBase (later).
class IPreparedFunction {
public:
    virtual ~IPreparedFunction() = default;

    /// Get the main function name.
    virtual String get_name() const = 0;

    virtual Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                           size_t result, size_t input_rows_count, bool dry_run) = 0;
};

using PreparedFunctionPtr = std::shared_ptr<IPreparedFunction>;

class PreparedFunctionImpl : public IPreparedFunction {
public:
    Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                   size_t result, size_t input_rows_count, bool dry_run = false) final;

protected:
    virtual Status execute_impl_dry_run(FunctionContext* context, Block& block,
                                        const ColumnNumbers& arguments, size_t result,
                                        size_t input_rows_count) {
        return execute_impl(context, block, arguments, result, input_rows_count);
    }

    virtual Status execute_impl(FunctionContext* context, Block& block,
                                const ColumnNumbers& arguments, size_t result,
                                size_t input_rows_count) = 0;

    /** Default implementation in presence of Nullable arguments or NULL constants as arguments is the following:
      *  if some of arguments are NULL constants then return NULL constant,
      *  if some of arguments are Nullable, then execute function as usual for block,
      *   where Nullable columns are substituted with nested columns (they have arbitrary values in rows corresponding to NULL value)
      *   and wrap result in Nullable column where NULLs are in all rows where any of arguments are NULL.
      */
    virtual bool use_default_implementation_for_nulls() const { return true; }

    /** If the function have non-zero number of arguments,
      *  and if all arguments are constant, that we could automatically provide default implementation:
      *  arguments are converted to ordinary columns with single value, then function is executed as usual,
      *  and then the result is converted to constant column.
      */
    virtual bool use_default_implementation_for_constants() const { return false; }

    /** If function arguments has single low cardinality column and all other arguments are constants, call function on nested column.
      * Otherwise, convert all low cardinality columns to ordinary columns.
      * Returns ColumnLowCardinality if at least one argument is ColumnLowCardinality.
      */
    virtual bool use_default_implementation_for_low_cardinality_columns() const { return true; }

    /** Some arguments could remain constant during this implementation.
      */
    virtual ColumnNumbers get_arguments_that_are_always_constant() const { return {}; }

private:
    Status default_implementation_for_nulls(FunctionContext* context, Block& block,
                                            const ColumnNumbers& args, size_t result,
                                            size_t input_rows_count, bool dry_run, bool* executed);
    Status default_implementation_for_constant_arguments(FunctionContext* context, Block& block,
                                                         const ColumnNumbers& args, size_t result,
                                                         size_t input_rows_count, bool dry_run,
                                                         bool* executed);
    Status execute_without_low_cardinality_columns(FunctionContext* context, Block& block,
                                                   const ColumnNumbers& arguments, size_t result,
                                                   size_t input_rows_count, bool dry_run);
};

/// Function with known arguments and return type.
class IFunctionBase {
public:
    virtual ~IFunctionBase() = default;

    /// Get the main function name.
    virtual String get_name() const = 0;

    virtual const DataTypes& get_argument_types() const = 0;
    virtual const DataTypePtr& get_return_type() const = 0;

    /// Do preparations and return executable.
    /// sample_block should contain data types of arguments and values of constants, if relevant.
    virtual PreparedFunctionPtr prepare(FunctionContext* context, const Block& sample_block,
                                        const ColumnNumbers& arguments, size_t result) const = 0;

    /// Override this when function need to store state in the `FunctionContext`, or do some
    /// preparation work according to information from `FunctionContext`.
    virtual Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }

    /// TODO: make const
    virtual Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                           size_t result, size_t input_rows_count, bool dry_run = false) {
        return prepare(context, block, arguments, result)
                ->execute(context, block, arguments, result, input_rows_count, dry_run);
    }

    /// Do cleaning work when function is finished, i.e., release state variables in the
    /// `FunctionContext` which are registered in `prepare` phase.
    virtual Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }

    virtual bool is_stateful() const { return false; }

    /** Should we evaluate this function while constant folding, if arguments are constants?
      * Usually this is true. Notable counterexample is function 'sleep'.
      * If we will call it during query analysis, we will sleep extra amount of time.
      */
    virtual bool is_suitable_for_constant_folding() const { return true; }

    /** Some functions like ignore(...) or toTypeName(...) always return constant result which doesn't depend on arguments.
      * In this case we can calculate result and assume that it's constant in stream header.
      * There is no need to implement function if it has zero arguments.
      * Must return ColumnConst with single row or nullptr.
      */
    virtual ColumnPtr get_result_if_always_returns_constant_and_has_arguments(
            const Block& /*block*/, const ColumnNumbers& /*arguments*/) const {
        return nullptr;
    }

    /** Function is called "injective" if it returns different result for different values of arguments.
      * Example: hex, negate, tuple...
      *
      * Function could be injective with some arguments fixed to some constant values.
      * Examples:
      *  plus(const, x);
      *  multiply(const, x) where x is an integer and constant is not divisible by two;
      *  concat(x, 'const');
      *  concat(x, 'const', y) where const contain at least one non-numeric character;
      *  concat with FixedString
      *  dictGet... functions takes name of dictionary as its argument,
      *   and some dictionaries could be explicitly defined as injective.
      *
      * It could be used, for example, to remove useless function applications from GROUP BY.
      *
      * Sometimes, function is not really injective, but considered as injective, for purpose of query optimization.
      * For example, to_string function is not injective for Float64 data type,
      *  as it returns 'nan' for many different representation of NaNs.
      * But we assume, that it is injective. This could be documented as implementation-specific behaviour.
      *
      * sample_block should contain data types of arguments and values of constants, if relevant.
      */
    virtual bool get_is_injective(const Block& /*sample_block*/) { return false; }

    /** Function is called "deterministic", if it returns same result for same values of arguments.
      * Most of functions are deterministic. Notable counterexample is rand().
      * Sometimes, functions are "deterministic" in scope of single query
      *  (even for distributed query), but not deterministic it general.
      * Example: now(). Another example: functions that work with periodically updated dictionaries.
      */

    virtual bool is_deterministic() const = 0;

    virtual bool is_deterministic_in_scope_of_query() const = 0;

    /** Lets you know if the function is monotonic in a range of values.
      * This is used to work with the index in a sorted chunk of data.
      * And allows to use the index not only when it is written, for example `date >= const`, but also, for example, `toMonth(date) >= 11`.
      * All this is considered only for functions of one argument.
      */
    virtual bool has_information_about_monotonicity() const { return false; }

    /// The property of monotonicity for a certain range.
    struct Monotonicity {
        bool is_monotonic = false; /// Is the function monotonous (nondecreasing or nonincreasing).
        bool is_positive =
                true; /// true if the function is nondecreasing, false, if notincreasing. If is_monotonic = false, then it does not matter.
        bool is_always_monotonic =
                false; /// Is true if function is monotonic on the whole input range I

        Monotonicity(bool is_monotonic_ = false, bool is_positive_ = true,
                     bool is_always_monotonic_ = false)
                : is_monotonic(is_monotonic_),
                  is_positive(is_positive_),
                  is_always_monotonic(is_always_monotonic_) {}
    };

    /** Get information about monotonicity on a range of values. Call only if hasInformationAboutMonotonicity.
      * NULL can be passed as one of the arguments. This means that the corresponding range is unlimited on the left or on the right.
      */
    virtual Monotonicity get_monotonicity_for_range(const IDataType& /*type*/,
                                                    const Field& /*left*/,
                                                    const Field& /*right*/) const {
        LOG(FATAL) << fmt::format("Function {} has no information about its monotonicity.",
                                  get_name());
        return Monotonicity {};
    }
};

using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

/// Creates IFunctionBase from argument types list.
class IFunctionBuilder {
public:
    virtual ~IFunctionBuilder() = default;

    /// Get the main function name.
    virtual String get_name() const = 0;

    /// See the comment for the same method in IFunctionBase
    virtual bool is_deterministic() const = 0;

    virtual bool is_deterministic_in_scope_of_query() const = 0;

    /// Override and return true if function needs to depend on the state of the data.
    virtual bool is_stateful() const = 0;

    /// Override and return true if function could take different number of arguments.
    virtual bool is_variadic() const = 0;

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    virtual size_t get_number_of_arguments() const = 0;

    /// Throw if number of arguments is incorrect. Default implementation will check only in non-variadic case.
    virtual void check_number_of_arguments(size_t number_of_arguments) const = 0;

    /// Check arguments and return IFunctionBase.
    virtual FunctionBasePtr build(const ColumnsWithTypeAndName& arguments,
                                  const DataTypePtr& return_type) const = 0;

    /// For higher-order functions (functions, that have lambda expression as at least one argument).
    /// You pass data types with empty DataTypeFunction for lambda arguments.
    /// This function will replace it with DataTypeFunction containing actual types.
    virtual DataTypes get_variadic_argument_types() const = 0;

    /// Returns indexes of arguments, that must be ColumnConst
    virtual ColumnNumbers get_arguments_that_are_always_constant() const = 0;
    /// Returns indexes if arguments, that can be Nullable without making result of function Nullable
    /// (for functions like is_null(x))
    virtual ColumnNumbers get_arguments_that_dont_imply_nullable_return_type(
            size_t number_of_arguments) const = 0;
};

using FunctionBuilderPtr = std::shared_ptr<IFunctionBuilder>;

class FunctionBuilderImpl : public IFunctionBuilder {
public:
    FunctionBasePtr build(const ColumnsWithTypeAndName& arguments,
                          const DataTypePtr& return_type) const final {
        const DataTypePtr& func_return_type = get_return_type(arguments);
        DCHECK(return_type->equals(*func_return_type) ||
               // For null constant argument, `get_return_type` would return
               // Nullable<DataTypeNothing> when `use_default_implementation_for_nulls` is true.
               (return_type->is_nullable() && func_return_type->is_nullable() &&
                is_nothing(((DataTypeNullable*)func_return_type.get())->get_nested_type())) ||
               (is_date_or_datetime(
                        return_type->is_nullable()
                                ? ((DataTypeNullable*)return_type.get())->get_nested_type()
                                : return_type) &&
                is_date_or_datetime(get_return_type(arguments)->is_nullable()
                                            ? ((DataTypeNullable*)get_return_type(arguments).get())
                                                      ->get_nested_type()
                                            : get_return_type(arguments))))
                << " with " << return_type->get_name() << " and " << func_return_type->get_name();

        return build_impl(arguments, return_type);
    }

    bool is_deterministic() const override { return true; }
    bool is_deterministic_in_scope_of_query() const override { return true; }
    bool is_stateful() const override { return false; }
    bool is_variadic() const override { return false; }

    /// Default implementation. Will check only in non-variadic case.
    void check_number_of_arguments(size_t number_of_arguments) const override;

    DataTypePtr get_return_type(const ColumnsWithTypeAndName& arguments) const;

    DataTypes get_variadic_argument_types() const override {
        return get_variadic_argument_types_impl();
    }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {}; }
    ColumnNumbers get_arguments_that_dont_imply_nullable_return_type(
            size_t /*number_of_arguments*/) const override {
        return {};
    }

protected:
    /// Get the result type by argument type. If the function does not apply to these arguments, throw an exception.
    virtual DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i) data_types[i] = arguments[i].type;

        return get_return_type_impl(data_types);
    }

    virtual DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const {
        LOG(FATAL) << fmt::format("get_return_type is not implemented for {}", get_name());
        return nullptr;
    }

    /** If use_default_implementation_for_nulls() is true, than change arguments for get_return_type() and build_impl():
      *  if some of arguments are Nullable(Nothing) then don't call get_return_type(), call build_impl() with return_type = Nullable(Nothing),
      *  if some of arguments are Nullable, then:
      *   - Nullable types are substituted with nested types for get_return_type() function
      *   - wrap get_return_type() result in Nullable type and pass to build_impl
      *
      * Otherwise build returns build_impl(arguments, get_return_type(arguments));
      */
    virtual bool use_default_implementation_for_nulls() const { return true; }

    /** If use_default_implementation_for_nulls() is true, than change arguments for get_return_type() and build_impl().
      * If function arguments has low cardinality types, convert them to ordinary types.
      * get_return_type returns ColumnLowCardinality if at least one argument type is ColumnLowCardinality.
      */
    virtual bool use_default_implementation_for_low_cardinality_columns() const { return true; }

    /// If it isn't, will convert all ColumnLowCardinality arguments to full columns.
    virtual bool can_be_executed_on_low_cardinality_dictionary() const { return true; }

    virtual FunctionBasePtr build_impl(const ColumnsWithTypeAndName& arguments,
                                       const DataTypePtr& return_type) const = 0;

    virtual DataTypes get_variadic_argument_types_impl() const { return DataTypes(); }

private:
    DataTypePtr get_return_type_without_low_cardinality(
            const ColumnsWithTypeAndName& arguments) const;
};

/// Previous function interface.
class IFunction : public std::enable_shared_from_this<IFunction>,
                  public FunctionBuilderImpl,
                  public IFunctionBase,
                  public PreparedFunctionImpl {
public:
    String get_name() const override = 0;

    bool is_stateful() const override { return false; }

    /// TODO: make const
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override = 0;

    /// Override this functions to change default implementation behavior. See details in IMyFunction.
    bool use_default_implementation_for_nulls() const override { return true; }
    bool use_default_implementation_for_constants() const override { return false; }
    bool use_default_implementation_for_low_cardinality_columns() const override { return true; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {}; }
    bool can_be_executed_on_low_cardinality_dictionary() const override {
        return is_deterministic_in_scope_of_query();
    }
    bool is_deterministic() const override { return true; }
    bool is_deterministic_in_scope_of_query() const override { return true; }

    using PreparedFunctionImpl::execute;
    using PreparedFunctionImpl::execute_impl_dry_run;
    using FunctionBuilderImpl::get_return_type_impl;
    using FunctionBuilderImpl::get_variadic_argument_types_impl;
    using FunctionBuilderImpl::get_return_type;

    [[noreturn]] PreparedFunctionPtr prepare(FunctionContext* context,
                                             const Block& /*sample_block*/,
                                             const ColumnNumbers& /*arguments*/,
                                             size_t /*result*/) const final {
        LOG(FATAL) << "prepare is not implemented for IFunction";
        __builtin_unreachable();
    }

    Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        return Status::OK();
    }

    [[noreturn]] const DataTypes& get_argument_types() const final {
        LOG(FATAL) << "get_argument_types is not implemented for IFunction";
        __builtin_unreachable();
    }

    [[noreturn]] const DataTypePtr& get_return_type() const final {
        LOG(FATAL) << "get_return_type is not implemented for IFunction";
        __builtin_unreachable();
    }

protected:
    FunctionBasePtr build_impl(const ColumnsWithTypeAndName& /*arguments*/,
                               const DataTypePtr& /*return_type*/) const final {
        LOG(FATAL) << "build_impl is not implemented for IFunction";
        return {};
    }
};

/// Wrappers over IFunction.

class DefaultExecutable final : public PreparedFunctionImpl {
public:
    explicit DefaultExecutable(std::shared_ptr<IFunction> function_)
            : function(std::move(function_)) {}

    String get_name() const override { return function->get_name(); }

protected:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) final {
        return function->execute_impl(context, block, arguments, result, input_rows_count);
    }
    Status execute_impl_dry_run(FunctionContext* context, Block& block,
                                const ColumnNumbers& arguments, size_t result,
                                size_t input_rows_count) final {
        return function->execute_impl_dry_run(context, block, arguments, result, input_rows_count);
    }
    bool use_default_implementation_for_nulls() const final {
        return function->use_default_implementation_for_nulls();
    }
    bool use_default_implementation_for_constants() const final {
        return function->use_default_implementation_for_constants();
    }
    bool use_default_implementation_for_low_cardinality_columns() const final {
        return function->use_default_implementation_for_low_cardinality_columns();
    }
    ColumnNumbers get_arguments_that_are_always_constant() const final {
        return function->get_arguments_that_are_always_constant();
    }

private:
    std::shared_ptr<IFunction> function;
};

class DefaultFunction final : public IFunctionBase {
public:
    DefaultFunction(std::shared_ptr<IFunction> function_, DataTypes arguments_,
                    DataTypePtr return_type_)
            : function(std::move(function_)),
              arguments(std::move(arguments_)),
              return_type(std::move(return_type_)) {}

    String get_name() const override { return function->get_name(); }

    const DataTypes& get_argument_types() const override { return arguments; }
    const DataTypePtr& get_return_type() const override { return return_type; }

    PreparedFunctionPtr prepare(FunctionContext* context, const Block& /*sample_block*/,
                                const ColumnNumbers& /*arguments*/,
                                size_t /*result*/) const override {
        return std::make_shared<DefaultExecutable>(function);
    }

    Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        return function->prepare(context, scope);
    }

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        return function->close(context, scope);
    }

    bool is_suitable_for_constant_folding() const override {
        return function->is_suitable_for_constant_folding();
    }
    ColumnPtr get_result_if_always_returns_constant_and_has_arguments(
            const Block& block, const ColumnNumbers& arguments_) const override {
        return function->get_result_if_always_returns_constant_and_has_arguments(block, arguments_);
    }

    bool get_is_injective(const Block& sample_block) override {
        return function->get_is_injective(sample_block);
    }

    bool is_deterministic() const override { return function->is_deterministic(); }

    bool is_deterministic_in_scope_of_query() const override {
        return function->is_deterministic_in_scope_of_query();
    }

    bool has_information_about_monotonicity() const override {
        return function->has_information_about_monotonicity();
    }

    IFunctionBase::Monotonicity get_monotonicity_for_range(const IDataType& type, const Field& left,
                                                           const Field& right) const override {
        return function->get_monotonicity_for_range(type, left, right);
    }

private:
    std::shared_ptr<IFunction> function;
    DataTypes arguments;
    DataTypePtr return_type;
};

class DefaultFunctionBuilder : public FunctionBuilderImpl {
public:
    explicit DefaultFunctionBuilder(std::shared_ptr<IFunction> function_)
            : function(std::move(function_)) {}

    void check_number_of_arguments(size_t number_of_arguments) const override {
        return function->check_number_of_arguments(number_of_arguments);
    }

    bool is_deterministic() const override { return function->is_deterministic(); }
    bool is_deterministic_in_scope_of_query() const override {
        return function->is_deterministic_in_scope_of_query();
    }

    String get_name() const override { return function->get_name(); }
    bool is_stateful() const override { return function->is_stateful(); }
    bool is_variadic() const override { return function->is_variadic(); }
    size_t get_number_of_arguments() const override { return function->get_number_of_arguments(); }

    ColumnNumbers get_arguments_that_are_always_constant() const override {
        return function->get_arguments_that_are_always_constant();
    }
    ColumnNumbers get_arguments_that_dont_imply_nullable_return_type(
            size_t number_of_arguments) const override {
        return function->get_arguments_that_dont_imply_nullable_return_type(number_of_arguments);
    }

protected:
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return function->get_return_type_impl(arguments);
    }
    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return function->get_return_type_impl(arguments);
    }

    bool use_default_implementation_for_nulls() const override {
        return function->use_default_implementation_for_nulls();
    }
    bool use_default_implementation_for_low_cardinality_columns() const override {
        return function->use_default_implementation_for_low_cardinality_columns();
    }
    bool can_be_executed_on_low_cardinality_dictionary() const override {
        return function->can_be_executed_on_low_cardinality_dictionary();
    }

    FunctionBasePtr build_impl(const ColumnsWithTypeAndName& arguments,
                               const DataTypePtr& return_type) const override {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i) data_types[i] = arguments[i].type;
        return std::make_shared<DefaultFunction>(function, data_types, return_type);
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return function->get_variadic_argument_types_impl();
    }

private:
    std::shared_ptr<IFunction> function;
};

using FunctionPtr = std::shared_ptr<IFunction>;

/** Return ColumnNullable of src, with null map as OR-ed null maps of args columns in blocks.
  * Or ColumnConst(ColumnNullable) if the result is always NULL or if the result is constant and always not NULL.
  */
ColumnPtr wrap_in_nullable(const ColumnPtr& src, const Block& block, const ColumnNumbers& args,
                           size_t result, size_t input_rows_count);

} // namespace doris::vectorized
