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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/IAggregateFunction.h
// and modified by Doris

#pragma once

#include "vec/common/exception.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

namespace doris::vectorized {

class Arena;
class IColumn;
class IDataType;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

using AggregateDataPtr = char*;
using ConstAggregateDataPtr = const char*;

/** Aggregate functions interface.
  * Instances of classes with this interface do not contain the data itself for aggregation,
  *  but contain only metadata (description) of the aggregate function,
  *  as well as methods for creating, deleting and working with data.
  * The data resulting from the aggregation (intermediate computing states) is stored in other objects
  *  (which can be created in some memory pool),
  *  and IAggregateFunction is the external interface for manipulating them.
  */
class IAggregateFunction {
public:
    IAggregateFunction(const DataTypes& argument_types_, const Array& parameters_)
            : argument_types(argument_types_), parameters(parameters_) {}

    /// Get main function name.
    virtual String get_name() const = 0;

    /// Get the result type.
    virtual DataTypePtr get_return_type() const = 0;

    virtual ~IAggregateFunction() = default;

    /** Create empty data for aggregation with `placement new` at the specified location.
      * You will have to destroy them using the `destroy` method.
      */
    virtual void create(AggregateDataPtr __restrict place) const = 0;

    /// Delete data for aggregation.
    virtual void destroy(AggregateDataPtr __restrict place) const noexcept = 0;

    /// Reset aggregation state
    virtual void reset(AggregateDataPtr place) const = 0;

    /// It is not necessary to delete data.
    virtual bool has_trivial_destructor() const = 0;

    /// Get `sizeof` of structure with data.
    virtual size_t size_of_data() const = 0;

    /// How the data structure should be aligned. NOTE: Currently not used (structures with aggregation state are put without alignment).
    virtual size_t align_of_data() const = 0;

    /** Adds a value into aggregation data on which place points to.
     *  columns points to columns containing arguments of aggregation function.
     *  row_num is number of row which should be added.
     *  Additional parameter arena should be used instead of standard memory allocator if the addition requires memory allocation.
     */
    virtual void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
                     Arena* arena) const = 0;

    /// Merges state (on which place points to) with other state of current aggregation function.
    virtual void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
                       Arena* arena) const = 0;

    /// Serializes state (to transmit it over the network, for example).
    virtual void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const = 0;

    /// Deserializes state. This function is called only for empty (just created) states.
    virtual void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                             Arena* arena) const = 0;

    /// Returns true if a function requires Arena to handle own states (see add(), merge(), deserialize()).
    virtual bool allocates_memory_in_arena() const { return false; }

    /// Inserts results into a column.
    virtual void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const = 0;

    /** Returns true for aggregate functions of type -State.
      * They are executed as other aggregate functions, but not finalized (return an aggregation state that can be combined with another).
      */
    virtual bool is_state() const { return false; }

    /** Contains a loop with calls to "add" function. You can collect arguments into array "places"
      *  and do a single call to "add_batch" for devirtualization and inlining.
      */
    virtual void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                           const IColumn** columns, Arena* arena) const = 0;

    /** The same for single place.
      */
    virtual void add_batch_single_place(size_t batch_size, AggregateDataPtr place,
                                        const IColumn** columns, Arena* arena) const = 0;

    // only used at agg reader
    virtual void add_batch_range(size_t batch_begin, size_t batch_end, AggregateDataPtr place,
                                 const IColumn** columns, Arena* arena, bool has_null = false) = 0;

    // only used at window function
    virtual void add_range_single_place(int64_t partition_start, int64_t partition_end,
                                        int64_t frame_start, int64_t frame_end,
                                        AggregateDataPtr place, const IColumn** columns,
                                        Arena* arena) const = 0;

    const DataTypes& get_argument_types() const { return argument_types; }
    const Array& get_parameters() const { return parameters; }

protected:
    DataTypes argument_types;
    Array parameters;
};

/// Implement method to obtain an address of 'add' function.
template <typename Derived>
class IAggregateFunctionHelper : public IAggregateFunction {
public:
    IAggregateFunctionHelper(const DataTypes& argument_types_, const Array& parameters_)
            : IAggregateFunction(argument_types_, parameters_) {}

    void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                   const IColumn** columns, Arena* arena) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            static_cast<const Derived*>(this)->add(places[i] + place_offset, columns, i, arena);
        }
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            static_cast<const Derived*>(this)->add(place, columns, i, arena);
        }
    }
    //now this is use for sum/count/avg/min/max win function, other win function should override this function in class
    //stddev_pop/stddev_samp/variance_pop/variance_samp
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        frame_start = std::max<int64_t>(frame_start, partition_start);
        frame_end = std::min<int64_t>(frame_end, partition_end);
        for (int64_t i = frame_start; i < frame_end; ++i) {
            static_cast<const Derived*>(this)->add(place, columns, i, arena);
        }
    }

    void add_batch_range(size_t batch_begin, size_t batch_end, AggregateDataPtr place,
                         const IColumn** columns, Arena* arena, bool has_null) override {
        for (size_t i = batch_begin; i <= batch_end; ++i) {
            static_cast<const Derived*>(this)->add(place, columns, i, arena);
        }
    }
};

/// Implements several methods for manipulation with data. T - type of structure with data for aggregation.
template <typename T, typename Derived>
class IAggregateFunctionDataHelper : public IAggregateFunctionHelper<Derived> {
protected:
    using Data = T;

    static Data& data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data*>(place); }
    static const Data& data(ConstAggregateDataPtr __restrict place) {
        return *reinterpret_cast<const Data*>(place);
    }

public:
    IAggregateFunctionDataHelper(const DataTypes& argument_types_, const Array& parameters_)
            : IAggregateFunctionHelper<Derived>(argument_types_, parameters_) {}

    void create(AggregateDataPtr __restrict place) const override { new (place) Data; }

    void destroy(AggregateDataPtr __restrict place) const noexcept override { data(place).~Data(); }

    bool has_trivial_destructor() const override { return std::is_trivially_destructible_v<Data>; }

    size_t size_of_data() const override { return sizeof(Data); }

    /// NOTE: Currently not used (structures with aggregation state are put without alignment).
    size_t align_of_data() const override { return alignof(Data); }

    void reset(AggregateDataPtr place) const override {}
};

using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

} // namespace doris::vectorized
