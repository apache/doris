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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionNull.h
// and modified by Doris

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

template <typename NestFunction, bool result_is_nullable, typename Derived>
class AggregateFunctionNullBaseInline : public IAggregateFunctionHelper<Derived> {
protected:
    std::unique_ptr<NestFunction> nested_function;
    size_t prefix_size;

    /** In addition to data for nested aggregate function, we keep a flag
      *  indicating - was there at least one non-NULL value accumulated.
      * In case of no not-NULL values, the function will return NULL.
      *
      * We use prefix_size bytes for flag to satisfy the alignment requirement of nested state.
      */

    AggregateDataPtr nested_place(AggregateDataPtr __restrict place) const noexcept {
        return place + prefix_size;
    }

    ConstAggregateDataPtr nested_place(ConstAggregateDataPtr __restrict place) const noexcept {
        return place + prefix_size;
    }

    static void init_flag(AggregateDataPtr __restrict place) noexcept {
        if constexpr (result_is_nullable) {
            place[0] = false;
        }
    }

    static void set_flag(AggregateDataPtr __restrict place) noexcept {
        if constexpr (result_is_nullable) {
            place[0] = true;
        }
    }

    static bool get_flag(ConstAggregateDataPtr __restrict place) noexcept {
        return result_is_nullable ? place[0] : true;
    }

public:
    AggregateFunctionNullBaseInline(IAggregateFunction* nested_function_,
                                    const DataTypes& arguments)
            : IAggregateFunctionHelper<Derived>(arguments),
              nested_function {assert_cast<NestFunction*>(nested_function_)} {
        if (result_is_nullable) {
            prefix_size = nested_function->align_of_data();
        } else {
            prefix_size = 0;
        }
    }

    void set_version(const int version_) override {
        IAggregateFunctionHelper<Derived>::set_version(version_);
        nested_function->set_version(version_);
    }

    String get_name() const override {
        /// This is just a wrapper. The function for Nullable arguments is named the same as the nested function itself.
        return nested_function->get_name();
    }

    DataTypePtr get_return_type() const override {
        return result_is_nullable ? make_nullable(nested_function->get_return_type())
                                  : nested_function->get_return_type();
    }

    void create(AggregateDataPtr __restrict place) const override {
        init_flag(place);
        nested_function->create(nested_place(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        nested_function->destroy(nested_place(place));
    }
    void reset(AggregateDataPtr place) const override {
        init_flag(place);
        nested_function->reset(nested_place(place));
    }

    bool has_trivial_destructor() const override {
        return nested_function->has_trivial_destructor();
    }

    size_t size_of_data() const override { return prefix_size + nested_function->size_of_data(); }

    size_t align_of_data() const override { return nested_function->align_of_data(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        if (result_is_nullable && get_flag(rhs)) {
            set_flag(place);
        }

        nested_function->merge(nested_place(place), nested_place(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        bool flag = get_flag(place);
        if (result_is_nullable) {
            write_binary(flag, buf);
        }
        if (flag) {
            nested_function->serialize(nested_place(place), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        bool flag = true;
        if (result_is_nullable) {
            read_binary(flag, buf);
        }
        if (flag) {
            set_flag(place);
            nested_function->deserialize(nested_place(place), buf, arena);
        }
    }

    void deserialize_and_merge(AggregateDataPtr __restrict place, AggregateDataPtr __restrict rhs,
                               BufferReadable& buf, Arena* arena) const override {
        bool flag = true;
        if (result_is_nullable) {
            read_binary(flag, buf);
        }
        if (flag) {
            set_flag(rhs);
            set_flag(place);
            nested_function->deserialize_and_merge(nested_place(place), nested_place(rhs), buf,
                                                   arena);
        }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        if constexpr (result_is_nullable) {
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
};

/** There are two cases: for single argument and variadic.
  * Code for single argument is much more efficient.
  */
template <typename NestFuction, bool result_is_nullable>
class AggregateFunctionNullUnaryInline final
        : public AggregateFunctionNullBaseInline<
                  NestFuction, result_is_nullable,
                  AggregateFunctionNullUnaryInline<NestFuction, result_is_nullable>> {
public:
    AggregateFunctionNullUnaryInline(IAggregateFunction* nested_function_,
                                     const DataTypes& arguments)
            : AggregateFunctionNullBaseInline<
                      NestFuction, result_is_nullable,
                      AggregateFunctionNullUnaryInline<NestFuction, result_is_nullable>>(
                      nested_function_, arguments) {}

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        const ColumnNullable* column = assert_cast<const ColumnNullable*>(columns[0]);
        if (!column->is_null_at(row_num)) {
            this->set_flag(place);
            const IColumn* nested_column = &column->get_nested_column();
            this->nested_function->add(this->nested_place(place), &nested_column, row_num, arena);
        }
    }

    void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                   const IColumn** columns, Arena* arena, bool agg_many) const override {
        const ColumnNullable* column = assert_cast<const ColumnNullable*>(columns[0]);
        // The overhead introduced is negligible here, just an extra memory read from NullMap
        const auto* __restrict null_map_data = column->get_null_map_data().data();
        const IColumn* nested_column = &column->get_nested_column();
        for (int i = 0; i < batch_size; ++i) {
            if (!null_map_data[i]) {
                AggregateDataPtr __restrict place = places[i] + place_offset;
                this->set_flag(place);
                this->nested_function->add(this->nested_place(place), &nested_column, i, arena);
            }
        }
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        const ColumnNullable* column = assert_cast<const ColumnNullable*>(columns[0]);
        bool has_null = column->has_null();

        if (has_null) {
            for (size_t i = 0; i < batch_size; ++i) {
                this->add(place, columns, i, arena);
            }
        } else {
            this->set_flag(place);
            const IColumn* nested_column = &column->get_nested_column();
            this->nested_function->add_batch_single_place(batch_size, this->nested_place(place),
                                                          &nested_column, arena);
        }
    }

    void add_batch_range(size_t batch_begin, size_t batch_end, AggregateDataPtr place,
                         const IColumn** columns, Arena* arena, bool has_null) override {
        const ColumnNullable* column = assert_cast<const ColumnNullable*>(columns[0]);

        if (has_null) {
            for (size_t i = batch_begin; i <= batch_end; ++i) {
                this->add(place, columns, i, arena);
            }
        } else {
            this->set_flag(place);
            const IColumn* nested_column = &column->get_nested_column();
            this->nested_function->add_batch_range(batch_begin, batch_end,
                                                   this->nested_place(place), &nested_column, arena,
                                                   false);
        }
    }
};

template <typename NestFuction, bool result_is_nullable>
class AggregateFunctionNullVariadicInline final
        : public AggregateFunctionNullBaseInline<
                  NestFuction, result_is_nullable,
                  AggregateFunctionNullVariadicInline<NestFuction, result_is_nullable>> {
public:
    AggregateFunctionNullVariadicInline(IAggregateFunction* nested_function_,
                                        const DataTypes& arguments)
            : AggregateFunctionNullBaseInline<
                      NestFuction, result_is_nullable,
                      AggregateFunctionNullVariadicInline<NestFuction, result_is_nullable>>(
                      nested_function_, arguments),
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

        for (size_t i = 0; i < number_of_arguments; ++i) {
            is_nullable[i] = arguments[i]->is_nullable();
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
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
            } else {
                nested_columns[i] = columns[i];
            }
        }

        this->set_flag(place);
        this->nested_function->add(this->nested_place(place), nested_columns, row_num, arena);
    }

    bool allocates_memory_in_arena() const override {
        return this->nested_function->allocates_memory_in_arena();
    }

private:
    // The array length is fixed in the implementation of some aggregate functions.
    // Therefore we choose 256 as the appropriate maximum length limit.
    static const size_t MAX_ARGS = 256;
    size_t number_of_arguments = 0;
    std::array<char, MAX_ARGS>
            is_nullable; /// Plain array is better than std::vector due to one indirection less.
};
} // namespace doris::vectorized
