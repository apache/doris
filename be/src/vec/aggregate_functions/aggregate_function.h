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

#include <parallel_hashmap/phmap.h>

#include "vec/columns/column_complex.h"
#include "vec/common/exception.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

class Arena;
class IColumn;
class IDataType;

template <bool nullable, typename ColVecType>
class AggregateFunctionBitmapCount;
template <typename Op>
class AggregateFunctionBitmapOp;
struct AggregateFunctionBitmapUnionOp;

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

    virtual void destroy_vec(AggregateDataPtr __restrict place,
                             const size_t num_rows) const noexcept = 0;

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

    virtual void add_many(AggregateDataPtr __restrict place, const IColumn** columns,
                          std::vector<int>& rows, Arena* arena) const {}

    /// Merges state (on which place points to) with other state of current aggregation function.
    virtual void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
                       Arena* arena) const = 0;

    virtual void merge_vec(const AggregateDataPtr* places, size_t offset, ConstAggregateDataPtr rhs,
                           Arena* arena, const size_t num_rows) const = 0;

    // same as merge_vec, but only call "merge" function when place is not nullptr
    virtual void merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                    ConstAggregateDataPtr rhs, Arena* arena,
                                    const size_t num_rows) const = 0;

    /// Serializes state (to transmit it over the network, for example).
    virtual void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const = 0;

    virtual void serialize_vec(const std::vector<AggregateDataPtr>& places, size_t offset,
                               BufferWritable& buf, const size_t num_rows) const = 0;

    virtual void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                                     MutableColumnPtr& dst, const size_t num_rows) const = 0;

    virtual void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                                 MutableColumnPtr& dst) const = 0;

    /// Deserializes state. This function is called only for empty (just created) states.
    virtual void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                             Arena* arena) const = 0;

    virtual void deserialize_vec(AggregateDataPtr places, const ColumnString* column, Arena* arena,
                                 size_t num_rows) const = 0;

    virtual void deserialize_from_column(AggregateDataPtr places, const IColumn& column,
                                         Arena* arena, size_t num_rows) const = 0;

    /// Deserializes state and merge it with current aggregation function.
    virtual void deserialize_and_merge(AggregateDataPtr __restrict place, BufferReadable& buf,
                                       Arena* arena) const = 0;

    virtual void deserialize_and_merge_from_column(AggregateDataPtr __restrict place,
                                                   const IColumn& column, Arena* arena) const = 0;

    /// Returns true if a function requires Arena to handle own states (see add(), merge(), deserialize()).
    virtual bool allocates_memory_in_arena() const { return false; }

    /// Inserts results into a column.
    virtual void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const = 0;

    virtual void insert_result_into_vec(const std::vector<AggregateDataPtr>& places,
                                        const size_t offset, IColumn& to,
                                        const size_t num_rows) const = 0;

    /** Returns true for aggregate functions of type -State.
      * They are executed as other aggregate functions, but not finalized (return an aggregation state that can be combined with another).
      */
    virtual bool is_state() const { return false; }

    /** Contains a loop with calls to "add" function. You can collect arguments into array "places"
      *  and do a single call to "add_batch" for devirtualization and inlining.
      */
    virtual void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                           const IColumn** columns, Arena* arena, bool agg_many = false) const = 0;

    // same as add_batch, but only call "add" function when place is not nullptr
    virtual void add_batch_selected(size_t batch_size, AggregateDataPtr* places,
                                    size_t place_offset, const IColumn** columns,
                                    Arena* arena) const = 0;

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

    virtual void streaming_agg_serialize(const IColumn** columns, BufferWritable& buf,
                                         const size_t num_rows, Arena* arena) const = 0;

    virtual void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                                   const size_t num_rows, Arena* arena) const = 0;

    const DataTypes& get_argument_types() const { return argument_types; }
    const Array& get_parameters() const { return parameters; }

    virtual MutableColumnPtr create_serialize_column() const { return ColumnString::create(); }

    virtual DataTypePtr get_serialized_type() const { return std::make_shared<DataTypeString>(); }

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

    void destroy_vec(AggregateDataPtr __restrict place,
                     const size_t num_rows) const noexcept override {
        const size_t size_of_data_ = size_of_data();
        for (size_t i = 0; i != num_rows; ++i) {
            static_cast<const Derived*>(this)->destroy(place + size_of_data_ * i);
        }
    }

    void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                   const IColumn** columns, Arena* arena, bool agg_many) const override {
        if constexpr (std::is_same_v<Derived, AggregateFunctionBitmapCount<false, ColumnBitmap>> ||
                      std::is_same_v<Derived, AggregateFunctionBitmapCount<true, ColumnBitmap>> ||
                      std::is_same_v<Derived,
                                     AggregateFunctionBitmapOp<AggregateFunctionBitmapUnionOp>>) {
            if (agg_many) {
                phmap::flat_hash_map<AggregateDataPtr, std::vector<int>> place_rows;
                for (int i = 0; i < batch_size; ++i) {
                    auto iter = place_rows.find(places[i] + place_offset);
                    if (iter == place_rows.end()) {
                        std::vector<int> rows;
                        rows.push_back(i);
                        place_rows.emplace(places[i] + place_offset, rows);
                    } else {
                        iter->second.push_back(i);
                    }
                }
                auto iter = place_rows.begin();
                while (iter != place_rows.end()) {
                    static_cast<const Derived*>(this)->add_many(iter->first, columns, iter->second,
                                                                arena);
                    iter++;
                }
                return;
            }
        }

        for (size_t i = 0; i < batch_size; ++i) {
            static_cast<const Derived*>(this)->add(places[i] + place_offset, columns, i, arena);
        }
    }

    void add_batch_selected(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                            const IColumn** columns, Arena* arena) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            if (places[i]) {
                static_cast<const Derived*>(this)->add(places[i] + place_offset, columns, i, arena);
            }
        }
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            static_cast<const Derived*>(this)->add(place, columns, i, arena);
        }
    }
    //now this is use for sum/count/avg/min/max win function, other win function should override this function in class
    //stddev_pop/stddev_samp/variance_pop/variance_samp/hll_union_agg/group_concat
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

    void insert_result_into_vec(const std::vector<AggregateDataPtr>& places, const size_t offset,
                                IColumn& to, const size_t num_rows) const override {
        for (size_t i = 0; i != num_rows; ++i) {
            static_cast<const Derived*>(this)->insert_result_into(places[i] + offset, to);
        }
    }

    void serialize_vec(const std::vector<AggregateDataPtr>& places, size_t offset,
                       BufferWritable& buf, const size_t num_rows) const override {
        for (size_t i = 0; i != num_rows; ++i) {
            static_cast<const Derived*>(this)->serialize(places[i] + offset, buf);
            buf.commit();
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        VectorBufferWriter writter(assert_cast<ColumnString&>(*dst));
        serialize_vec(places, offset, writter, num_rows);
    }

    void streaming_agg_serialize(const IColumn** columns, BufferWritable& buf,
                                 const size_t num_rows, Arena* arena) const override {
        char place[size_of_data()];
        for (size_t i = 0; i != num_rows; ++i) {
            static_cast<const Derived*>(this)->create(place);
            static_cast<const Derived*>(this)->add(place, columns, i, arena);
            static_cast<const Derived*>(this)->serialize(place, buf);
            buf.commit();
            static_cast<const Derived*>(this)->destroy(place);
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena* arena) const override {
        VectorBufferWriter writter(static_cast<ColumnString&>(*dst));
        streaming_agg_serialize(columns, writter, num_rows, arena);
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         MutableColumnPtr& dst) const override {
        VectorBufferWriter writter(static_cast<ColumnString&>(*dst));
        static_cast<const Derived*>(this)->serialize(place, writter);
        writter.commit();
    }

    void deserialize_vec(AggregateDataPtr places, const ColumnString* column, Arena* arena,
                         size_t num_rows) const override {
        const auto size_of_data = static_cast<const Derived*>(this)->size_of_data();
        for (size_t i = 0; i != num_rows; ++i) {
            auto place = places + size_of_data * i;
            VectorBufferReader buffer_reader(column->get_data_at(i));
            static_cast<const Derived*>(this)->create(place);
            static_cast<const Derived*>(this)->deserialize(place, buffer_reader, arena);
        }
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena* arena,
                                 size_t num_rows) const override {
        deserialize_vec(places, assert_cast<const ColumnString*>(&column), arena, num_rows);
    }

    void merge_vec(const AggregateDataPtr* places, size_t offset, ConstAggregateDataPtr rhs,
                   Arena* arena, const size_t num_rows) const override {
        const auto size_of_data = static_cast<const Derived*>(this)->size_of_data();
        for (size_t i = 0; i != num_rows; ++i) {
            static_cast<const Derived*>(this)->merge(places[i] + offset, rhs + size_of_data * i,
                                                     arena);
        }
    }

    void merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                            ConstAggregateDataPtr rhs, Arena* arena,
                            const size_t num_rows) const override {
        const auto size_of_data = static_cast<const Derived*>(this)->size_of_data();
        for (size_t i = 0; i != num_rows; ++i) {
            if (places[i]) {
                static_cast<const Derived*>(this)->merge(places[i] + offset, rhs + size_of_data * i,
                                                         arena);
            }
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

    void deserialize_and_merge(AggregateDataPtr __restrict place, BufferReadable& buf,
                               Arena* arena) const override {
        char deserialized_data[size_of_data()];
        AggregateDataPtr deserialized_place = (AggregateDataPtr)deserialized_data;

        auto derived = static_cast<const Derived*>(this);
        derived->create(deserialized_place);
        derived->deserialize(deserialized_place, buf, arena);
        derived->merge(place, deserialized_place, arena);
        derived->destroy(deserialized_place);
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        size_t num_rows = column.size();
        for (size_t i = 0; i != num_rows; ++i) {
            VectorBufferReader buffer_reader(
                    (assert_cast<const ColumnString&>(column)).get_data_at(i));
            deserialize_and_merge(place, buffer_reader, arena);
        }
    }
};

using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

class AggregateFunctionGuard {
public:
    using AggregateData = std::remove_pointer_t<AggregateDataPtr>;

    explicit AggregateFunctionGuard(const IAggregateFunction* function)
            : _function(function),
              _data(std::make_unique<AggregateData[]>(function->size_of_data())) {
        _function->create(_data.get());
    };
    ~AggregateFunctionGuard() { _function->destroy(_data.get()); }
    AggregateDataPtr data() { return _data.get(); };

private:
    const IAggregateFunction* _function;
    std::unique_ptr<AggregateData[]> _data;
};

} // namespace doris::vectorized
