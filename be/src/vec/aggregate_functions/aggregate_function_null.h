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

#pragma once

#include <array>

#include "common/logging.h"
#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

/// This class implements a wrapper around an aggregate function. Despite its name,
/// this is an adapter. It is used to handle aggregate functions that are called with
/// at least one nullable argument. It implements the logic according to which any
/// row that contains at least one NULL is skipped.

/// If all rows had NULL, the behaviour is determined by "result_is_nullable" template parameter.
///  true - return NULL; false - return value from empty aggregation state of nested function.

template <bool result_is_nullable, typename Derived>
class AggregateFunctionNullBase : public IAggregateFunctionHelper<Derived> {
protected:
    AggregateFunctionPtr nested_function;
    size_t prefix_size;

    /** In addition to data for nested aggregate function, we keep a flag
      *  indicating - was there at least one non-NULL value accumulated.
      * In case of no not-NULL values, the function will return NULL.
      *
      * We use prefix_size bytes for flag to satisfy the alignment requirement of nested state.
      */

    AggregateDataPtr nested_place(AggregateDataPtr place) const noexcept {
        return place + prefix_size;
    }

    ConstAggregateDataPtr nested_place(ConstAggregateDataPtr place) const noexcept {
        return place + prefix_size;
    }

    static void init_flag(AggregateDataPtr place) noexcept {
        if (result_is_nullable) place[0] = 0;
    }

    static void set_flag(AggregateDataPtr place) noexcept {
        if (result_is_nullable) place[0] = 1;
    }

    static bool get_flag(ConstAggregateDataPtr place) noexcept {
        return result_is_nullable ? place[0] : 1;
    }

public:
    AggregateFunctionNullBase(AggregateFunctionPtr nested_function_, const DataTypes& arguments,
                              const Array& params)
            : IAggregateFunctionHelper<Derived>(arguments, params),
              nested_function{nested_function_} {
        if (result_is_nullable)
            prefix_size = nested_function->align_of_data();
        else
            prefix_size = 0;
    }

    String get_name() const override {
        /// This is just a wrapper. The function for Nullable arguments is named the same as the nested function itself.
        return nested_function->get_name();
    }

    DataTypePtr get_return_type() const override {
        return result_is_nullable ? make_nullable(nested_function->get_return_type())
                                  : nested_function->get_return_type();
    }

    void create(AggregateDataPtr place) const override {
        init_flag(place);
        nested_function->create(nested_place(place));
    }

    void destroy(AggregateDataPtr place) const noexcept override {
        nested_function->destroy(nested_place(place));
    }

    bool has_trivial_destructor() const override {
        return nested_function->has_trivial_destructor();
    }

    size_t size_of_data() const override { return prefix_size + nested_function->size_of_data(); }

    size_t align_of_data() const override { return nested_function->align_of_data(); }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena* arena) const override {
        if (result_is_nullable && get_flag(rhs)) set_flag(place);

        nested_function->merge(nested_place(place), nested_place(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {
        bool flag = get_flag(place);
        if (result_is_nullable) write_binary(flag, buf);
        if (flag) {
            nested_function->serialize(nested_place(place), buf);
        }
    }

    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena* arena) const override {
        bool flag = true;
        if (result_is_nullable) read_binary(flag, buf);
        if (flag) {
            set_flag(place);
            nested_function->deserialize(nested_place(place), buf, arena);
        }
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        if (result_is_nullable) {
            ColumnNullable& to_concrete = assert_cast<ColumnNullable&>(to);
            if (get_flag(place)) {
                nested_function->insert_result_into(nested_place(place),
                                                    to_concrete.get_nested_column());
                to_concrete.get_null_map_data().push_back(0);
            } else {
                to_concrete.insert_default();
            }
        } else {
            nested_function->insert_result_into(nested_place(place), to);
        }
    }

    bool allocates_memory_in_arena() const override {
        return nested_function->allocates_memory_in_arena();
    }

    bool is_state() const override { return nested_function->is_state(); }

    const char* get_header_file_path() const override { return __FILE__; }
};

/** There are two cases: for single argument and variadic.
  * Code for single argument is much more efficient.
  */
template <bool result_is_nullable>
class AggregateFunctionNullUnary final
        : public AggregateFunctionNullBase<result_is_nullable,
                                           AggregateFunctionNullUnary<result_is_nullable>> {
public:
    AggregateFunctionNullUnary(AggregateFunctionPtr nested_function_, const DataTypes& arguments,
                               const Array& params)
            : AggregateFunctionNullBase<result_is_nullable,
                                        AggregateFunctionNullUnary<result_is_nullable>>(
                      std::move(nested_function_), arguments, params) {}

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        const ColumnNullable* column = assert_cast<const ColumnNullable*>(columns[0]);
        if (!column->is_null_at(row_num)) {
            this->set_flag(place);
            const IColumn* nested_column = &column->get_nested_column();
            this->nested_function->add(this->nested_place(place), &nested_column, row_num, arena);
        }
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        const ColumnNullable* column = assert_cast<const ColumnNullable*>(columns[0]);
        bool has_null = column->has_null();

        if (has_null) {
            for (size_t i = 0; i < batch_size; ++i) {
                if (!column->is_null_at(i)) {
                    this->set_flag(place);
                    this->add(place, columns, i, arena);
                }
            }
        } else {
            this->set_flag(place);
            const IColumn* nested_column = &column->get_nested_column();
            this->nested_function->add_batch_single_place(batch_size, this->nested_place(place),
                                                          &nested_column, arena);
        }
    }
};

template <bool result_is_nullable>
class AggregateFunctionNullVariadic final
        : public AggregateFunctionNullBase<result_is_nullable,
                                           AggregateFunctionNullVariadic<result_is_nullable>> {
public:
    AggregateFunctionNullVariadic(AggregateFunctionPtr nested_function_, const DataTypes& arguments,
                                  const Array& params)
            : AggregateFunctionNullBase<result_is_nullable,
                                        AggregateFunctionNullVariadic<result_is_nullable>>(
                      std::move(nested_function_), arguments, params),
              number_of_arguments(arguments.size()) {
        if (number_of_arguments == 1) {
            LOG(FATAL)
                    << "Logical error: single argument is passed to AggregateFunctionNullVariadic";
        }

        if (number_of_arguments > MAX_ARGS) {
            LOG(FATAL) << fmt::format(
                    "Maximum number of arguments for aggregate function with Nullable types is {}",
                    size_t(MAX_ARGS));
        }

        for (size_t i = 0; i < number_of_arguments; ++i)
            is_nullable[i] = arguments[i]->is_nullable();
    }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        /// This container stores the columns we really pass to the nested function.
        const IColumn* nested_columns[number_of_arguments];

        for (size_t i = 0; i < number_of_arguments; ++i) {
            if (is_nullable[i]) {
                const ColumnNullable& nullable_col =
                        assert_cast<const ColumnNullable&>(*columns[i]);
                if (nullable_col.is_null_at(row_num)) {
                    /// If at least one column has a null value in the current row,
                    /// we don't process this row.
                    return;
                }
                nested_columns[i] = &nullable_col.get_nested_column();
            } else
                nested_columns[i] = columns[i];
        }

        this->set_flag(place);
        this->nested_function->add(this->nested_place(place), nested_columns, row_num, arena);
    }

    bool allocates_memory_in_arena() const override {
        return this->nested_function->allocates_memory_in_arena();
    }

private:
    enum { MAX_ARGS = 8 };
    size_t number_of_arguments = 0;
    std::array<char, MAX_ARGS>
            is_nullable; /// Plain array is better than std::vector due to one indirection less.
};

} // namespace doris::vectorized
