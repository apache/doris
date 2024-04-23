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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/Combinators/AggregateFunctionForEach.h
// and modified by Doris

#pragma once

#include "common/logging.h"
#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

struct AggregateFunctionForEachData {
    size_t dynamic_array_size = 0;
    char* array_of_aggregate_datas = nullptr;
};

/** Adaptor for aggregate functions.
  * Adding -ForEach suffix to aggregate function
  *  will convert that aggregate function to a function, accepting arrays,
  *  and applies aggregation for each corresponding elements of arrays independently,
  *  returning arrays of aggregated values on corresponding positions.
  *
  * Example: sumForEach of:
  *  [1, 2],
  *  [3, 4, 5],
  *  [6, 7]
  * will return:
  *  [10, 13, 5]
  *
  * TODO Allow variable number of arguments.
  */
class AggregateFunctionForEach : public IAggregateFunctionDataHelper<AggregateFunctionForEachData,
                                                                     AggregateFunctionForEach> {
protected:
    using Base =
            IAggregateFunctionDataHelper<AggregateFunctionForEachData, AggregateFunctionForEach>;

    AggregateFunctionPtr nested_function;
    const size_t nested_size_of_data;
    const size_t num_arguments;

    AggregateFunctionForEachData& ensure_aggregate_data(AggregateDataPtr __restrict place,
                                                        size_t new_size, Arena& arena) const {
        AggregateFunctionForEachData& state = data(place);

        /// Ensure we have aggregate states for new_size elements, allocate
        /// from arena if needed. When reallocating, we can't copy the
        /// states to new buffer with memcpy, because they may contain pointers
        /// to themselves. In particular, this happens when a state contains
        /// a PODArrayWithStackMemory, which stores small number of elements
        /// inline. This is why we create new empty states in the new buffer,
        /// and merge the old states to them.
        size_t old_size = state.dynamic_array_size;
        if (old_size < new_size) {
            static constexpr size_t MAX_ARRAY_SIZE = 100 * 1000000000ULL;
            if (new_size > MAX_ARRAY_SIZE) {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "Suspiciously large array size ({}) in -ForEach aggregate function",
                                new_size);
            }

            size_t allocation_size = 0;
            if (common::mul_overflow(new_size, nested_size_of_data, allocation_size)) {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "Allocation size ({} * {}) overflows in -ForEach aggregate "
                                "function, but it should've been prevented by previous checks",
                                new_size, nested_size_of_data);
            }

            char* old_state = state.array_of_aggregate_datas;

            char* new_state =
                    arena.aligned_alloc(allocation_size, nested_function->align_of_data());

            size_t i;
            try {
                for (i = 0; i < new_size; ++i) {
                    nested_function->create(&new_state[i * nested_size_of_data]);
                }
            } catch (...) {
                size_t cleanup_size = i;

                for (i = 0; i < cleanup_size; ++i) {
                    nested_function->destroy(&new_state[i * nested_size_of_data]);
                }

                throw;
            }

            for (i = 0; i < old_size; ++i) {
                nested_function->merge(&new_state[i * nested_size_of_data],
                                       &old_state[i * nested_size_of_data], &arena);
                nested_function->destroy(&old_state[i * nested_size_of_data]);
            }

            state.array_of_aggregate_datas = new_state;
            state.dynamic_array_size = new_size;
        }

        return state;
    }

public:
    constexpr static auto AGG_FOREACH_SUFFIX = "_foreach";
    AggregateFunctionForEach(AggregateFunctionPtr nested_function_, const DataTypes& arguments)
            : Base(arguments),
              nested_function {std::move(nested_function_)},
              nested_size_of_data(nested_function->size_of_data()),
              num_arguments(arguments.size()) {
        if (arguments.empty()) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Aggregate function {} require at least one argument", get_name());
        }
    }
    void set_version(const int version_) override {
        Base::set_version(version_);
        nested_function->set_version(version_);
    }

    String get_name() const override { return nested_function->get_name() + AGG_FOREACH_SUFFIX; }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(nested_function->get_return_type());
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        AggregateFunctionForEachData& state = data(place);

        char* nested_state = state.array_of_aggregate_datas;
        for (size_t i = 0; i < state.dynamic_array_size; ++i) {
            nested_function->destroy(nested_state);
            nested_state += nested_size_of_data;
        }
    }

    bool has_trivial_destructor() const override {
        return std::is_trivially_destructible_v<Data> && nested_function->has_trivial_destructor();
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        const AggregateFunctionForEachData& rhs_state = data(rhs);
        AggregateFunctionForEachData& state =
                ensure_aggregate_data(place, rhs_state.dynamic_array_size, *arena);

        const char* rhs_nested_state = rhs_state.array_of_aggregate_datas;
        char* nested_state = state.array_of_aggregate_datas;

        for (size_t i = 0; i < state.dynamic_array_size && i < rhs_state.dynamic_array_size; ++i) {
            nested_function->merge(nested_state, rhs_nested_state, arena);

            rhs_nested_state += nested_size_of_data;
            nested_state += nested_size_of_data;
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        const AggregateFunctionForEachData& state = data(place);
        write_binary(state.dynamic_array_size, buf);
        const char* nested_state = state.array_of_aggregate_datas;
        for (size_t i = 0; i < state.dynamic_array_size; ++i) {
            nested_function->serialize(nested_state, buf);
            nested_state += nested_size_of_data;
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        AggregateFunctionForEachData& state = data(place);

        size_t new_size = 0;
        read_binary(new_size, buf);

        ensure_aggregate_data(place, new_size, *arena);

        char* nested_state = state.array_of_aggregate_datas;
        for (size_t i = 0; i < new_size; ++i) {
            nested_function->deserialize(nested_state, buf, arena);
            nested_state += nested_size_of_data;
        }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        const AggregateFunctionForEachData& state = data(place);

        auto& arr_to = assert_cast<ColumnArray&>(to);
        auto& offsets_to = arr_to.get_offsets();
        IColumn& elems_to = arr_to.get_data();

        char* nested_state = state.array_of_aggregate_datas;
        for (size_t i = 0; i < state.dynamic_array_size; ++i) {
            nested_function->insert_result_into(nested_state, elems_to);
            nested_state += nested_size_of_data;
        }

        offsets_to.push_back(offsets_to.back() + state.dynamic_array_size);
    }

    bool allocates_memory_in_arena() const override {
        return nested_function->allocates_memory_in_arena();
    }

    bool is_state() const override { return nested_function->is_state(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena* arena) const override {
        std::vector<const IColumn*> nested(num_arguments);

        for (size_t i = 0; i < num_arguments; ++i) {
            nested[i] = &assert_cast<const ColumnArray&>(*columns[i]).get_data();
        }

        const auto& first_array_column = assert_cast<const ColumnArray&>(*columns[0]);
        const auto& offsets = first_array_column.get_offsets();

        size_t begin = offsets[row_num - 1];
        size_t end = offsets[row_num];

        /// Sanity check. NOTE We can implement specialization for a case with single argument, if the check will hurt performance.
        for (size_t i = 1; i < num_arguments; ++i) {
            const auto& ith_column = assert_cast<const ColumnArray&>(*columns[i]);
            const auto& ith_offsets = ith_column.get_offsets();

            if (ith_offsets[row_num] != end ||
                (row_num != 0 && ith_offsets[row_num - 1] != begin)) {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "Arrays passed to {} aggregate function have different sizes",
                                get_name());
            }
        }

        AggregateFunctionForEachData& state = ensure_aggregate_data(place, end - begin, *arena);

        char* nested_state = state.array_of_aggregate_datas;
        for (size_t i = begin; i < end; ++i) {
            nested_function->add(nested_state, nested.data(), i, arena);
            nested_state += nested_size_of_data;
        }
    }
};
} // namespace doris::vectorized
