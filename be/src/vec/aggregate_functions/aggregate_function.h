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

#include "util/defer_op.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/phmap_fwd_decl.h"
#include "vec/common/string_buffer.hpp"
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

#define SAFE_CREATE(create, destroy) \
    do {                             \
        try {                        \
            create;                  \
        } catch (...) {              \
            destroy;                 \
            throw;                   \
        }                            \
    } while (0)

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
    IAggregateFunction(const DataTypes& argument_types_) : argument_types(argument_types_) {}

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
    virtual void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
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
                                                 IColumn& to) const = 0;

    /// Deserializes state. This function is called only for empty (just created) states.
    virtual void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                             Arena* arena) const = 0;

    virtual void deserialize_vec(AggregateDataPtr places, const ColumnString* column, Arena* arena,
                                 size_t num_rows) const = 0;

    virtual void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                           AggregateDataPtr rhs, const IColumn* column,
                                           Arena* arena, const size_t num_rows) const = 0;

    virtual void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                                    AggregateDataPtr rhs, const IColumn* column,
                                                    Arena* arena, const size_t num_rows) const = 0;

    virtual void deserialize_from_column(AggregateDataPtr places, const IColumn& column,
                                         Arena* arena, size_t num_rows) const = 0;

    /// Deserializes state and merge it with current aggregation function.
    virtual void deserialize_and_merge(AggregateDataPtr __restrict place,
                                       AggregateDataPtr __restrict rhs, BufferReadable& buf,
                                       Arena* arena) const = 0;

    virtual void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                         const IColumn& column, size_t begin,
                                                         size_t end, Arena* arena) const = 0;

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

    virtual MutableColumnPtr create_serialize_column() const { return ColumnString::create(); }

    virtual DataTypePtr get_serialized_type() const { return std::make_shared<DataTypeString>(); }

    virtual void set_version(const int version_) { version = version_; }

protected:
    DataTypes argument_types;
    int version {};
};

/// Implement method to obtain an address of 'add' function.
template <typename Derived>
class IAggregateFunctionHelper : public IAggregateFunction {
public:
    IAggregateFunctionHelper(const DataTypes& argument_types_)
            : IAggregateFunction(argument_types_) {}

    void destroy_vec(AggregateDataPtr __restrict place,
                     const size_t num_rows) const noexcept override {
        const size_t size_of_data_ = size_of_data();
        for (size_t i = 0; i != num_rows; ++i) {
            assert_cast<const Derived*>(this)->destroy(place + size_of_data_ * i);
        }
    }

    void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                   const IColumn** columns, Arena* arena, bool agg_many) const override {
        if constexpr (std::is_same_v<Derived, AggregateFunctionBitmapCount<false, ColumnBitmap>> ||
                      std::is_same_v<Derived, AggregateFunctionBitmapCount<true, ColumnBitmap>> ||
                      std::is_same_v<Derived,
                                     AggregateFunctionBitmapOp<AggregateFunctionBitmapUnionOp>>) {
            if (agg_many) {
                flat_hash_map<AggregateDataPtr, std::vector<int>> place_rows;
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
                    assert_cast<const Derived*>(this)->add_many(iter->first, columns, iter->second,
                                                                arena);
                    iter++;
                }
                return;
            }
        }

        for (size_t i = 0; i < batch_size; ++i) {
            assert_cast<const Derived*>(this)->add(places[i] + place_offset, columns, i, arena);
        }
    }

    void add_batch_selected(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                            const IColumn** columns, Arena* arena) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            if (places[i]) {
                assert_cast<const Derived*>(this)->add(places[i] + place_offset, columns, i, arena);
            }
        }
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            assert_cast<const Derived*>(this)->add(place, columns, i, arena);
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
            assert_cast<const Derived*>(this)->add(place, columns, i, arena);
        }
    }

    void add_batch_range(size_t batch_begin, size_t batch_end, AggregateDataPtr place,
                         const IColumn** columns, Arena* arena, bool has_null) override {
        for (size_t i = batch_begin; i <= batch_end; ++i) {
            assert_cast<const Derived*>(this)->add(place, columns, i, arena);
        }
    }

    void insert_result_into_vec(const std::vector<AggregateDataPtr>& places, const size_t offset,
                                IColumn& to, const size_t num_rows) const override {
        for (size_t i = 0; i != num_rows; ++i) {
            assert_cast<const Derived*>(this)->insert_result_into(places[i] + offset, to);
        }
    }

    void serialize_vec(const std::vector<AggregateDataPtr>& places, size_t offset,
                       BufferWritable& buf, const size_t num_rows) const override {
        for (size_t i = 0; i != num_rows; ++i) {
            assert_cast<const Derived*>(this)->serialize(places[i] + offset, buf);
            buf.commit();
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        VectorBufferWriter writer(assert_cast<ColumnString&>(*dst));
        serialize_vec(places, offset, writer, num_rows);
    }

    void streaming_agg_serialize(const IColumn** columns, BufferWritable& buf,
                                 const size_t num_rows, Arena* arena) const override {
        std::vector<char> place(size_of_data());
        for (size_t i = 0; i != num_rows; ++i) {
            assert_cast<const Derived*>(this)->create(place.data());
            DEFER({ assert_cast<const Derived*>(this)->destroy(place.data()); });
            assert_cast<const Derived*>(this)->add(place.data(), columns, i, arena);
            assert_cast<const Derived*>(this)->serialize(place.data(), buf);
            buf.commit();
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena* arena) const override {
        VectorBufferWriter writer(assert_cast<ColumnString&>(*dst));
        streaming_agg_serialize(columns, writer, num_rows, arena);
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        VectorBufferWriter writter(assert_cast<ColumnString&>(to));
        assert_cast<const Derived*>(this)->serialize(place, writter);
        writter.commit();
    }

    void deserialize_vec(AggregateDataPtr places, const ColumnString* column, Arena* arena,
                         size_t num_rows) const override {
        const auto size_of_data = assert_cast<const Derived*>(this)->size_of_data();
        for (size_t i = 0; i != num_rows; ++i) {
            try {
                auto place = places + size_of_data * i;
                VectorBufferReader buffer_reader(column->get_data_at(i));
                assert_cast<const Derived*>(this)->create(place);
                assert_cast<const Derived*>(this)->deserialize(place, buffer_reader, arena);
            } catch (...) {
                for (int j = 0; j < i; ++j) {
                    auto place = places + size_of_data * j;
                    assert_cast<const Derived*>(this)->destroy(place);
                }
                throw;
            }
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena* arena,
                                   const size_t num_rows) const override {
        const auto size_of_data = assert_cast<const Derived*>(this)->size_of_data();
        const auto* column_string = assert_cast<const ColumnString*>(column);
        for (size_t i = 0; i != num_rows; ++i) {
            try {
                auto rhs_place = rhs + size_of_data * i;
                VectorBufferReader buffer_reader(column_string->get_data_at(i));
                assert_cast<const Derived*>(this)->create(rhs_place);
                assert_cast<const Derived*>(this)->deserialize_and_merge(
                        places[i] + offset, rhs_place, buffer_reader, arena);
            } catch (...) {
                for (int j = 0; j < i; ++j) {
                    auto place = rhs + size_of_data * j;
                    assert_cast<const Derived*>(this)->destroy(place);
                }
                throw;
            }
        }
        assert_cast<const Derived*>(this)->destroy_vec(rhs, num_rows);
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena* arena, const size_t num_rows) const override {
        const auto size_of_data = assert_cast<const Derived*>(this)->size_of_data();
        const auto* column_string = assert_cast<const ColumnString*>(column);
        for (size_t i = 0; i != num_rows; ++i) {
            try {
                auto rhs_place = rhs + size_of_data * i;
                VectorBufferReader buffer_reader(column_string->get_data_at(i));
                assert_cast<const Derived*>(this)->create(rhs_place);
                if (places[i]) {
                    assert_cast<const Derived*>(this)->deserialize_and_merge(
                            places[i] + offset, rhs_place, buffer_reader, arena);
                }
            } catch (...) {
                for (int j = 0; j < i; ++j) {
                    auto place = rhs + size_of_data * j;
                    assert_cast<const Derived*>(this)->destroy(place);
                }
                throw;
            }
        }
        assert_cast<const Derived*>(this)->destroy_vec(rhs, num_rows);
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena* arena,
                                 size_t num_rows) const override {
        deserialize_vec(places, assert_cast<const ColumnString*>(&column), arena, num_rows);
    }

    void merge_vec(const AggregateDataPtr* places, size_t offset, ConstAggregateDataPtr rhs,
                   Arena* arena, const size_t num_rows) const override {
        const auto size_of_data = assert_cast<const Derived*>(this)->size_of_data();
        for (size_t i = 0; i != num_rows; ++i) {
            assert_cast<const Derived*>(this)->merge(places[i] + offset, rhs + size_of_data * i,
                                                     arena);
        }
    }

    void merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                            ConstAggregateDataPtr rhs, Arena* arena,
                            const size_t num_rows) const override {
        const auto size_of_data = assert_cast<const Derived*>(this)->size_of_data();
        for (size_t i = 0; i != num_rows; ++i) {
            if (places[i]) {
                assert_cast<const Derived*>(this)->merge(places[i] + offset, rhs + size_of_data * i,
                                                         arena);
            }
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena* arena) const override {
        DCHECK(end <= column.size() && begin <= end)
                << ", begin:" << begin << ", end:" << end << ", column.size():" << column.size();
        std::vector<char> deserialized_data(size_of_data());
        auto* deserialized_place = (AggregateDataPtr)deserialized_data.data();
        for (size_t i = begin; i <= end; ++i) {
            VectorBufferReader buffer_reader(
                    (assert_cast<const ColumnString&>(column)).get_data_at(i));
            assert_cast<const Derived*>(this)->create(deserialized_place);
            DEFER({ assert_cast<const Derived*>(this)->destroy(deserialized_place); });
            assert_cast<const Derived*>(this)->deserialize_and_merge(place, deserialized_place,
                                                                     buffer_reader, arena);
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        if (column.empty()) {
            return;
        }
        deserialize_and_merge_from_column_range(place, column, 0, column.size() - 1, arena);
    }

    void deserialize_and_merge(AggregateDataPtr __restrict place, AggregateDataPtr __restrict rhs,
                               BufferReadable& buf, Arena* arena) const override {
        assert_cast<const Derived*>(this)->deserialize(rhs, buf, arena);
        assert_cast<const Derived*>(this)->merge(place, rhs, arena);
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
    IAggregateFunctionDataHelper(const DataTypes& argument_types_)
            : IAggregateFunctionHelper<Derived>(argument_types_) {}

    void create(AggregateDataPtr __restrict place) const override { new (place) Data; }

    void destroy(AggregateDataPtr __restrict place) const noexcept override { data(place).~Data(); }

    bool has_trivial_destructor() const override { return std::is_trivially_destructible_v<Data>; }

    size_t size_of_data() const override { return sizeof(Data); }

    /// NOTE: Currently not used (structures with aggregation state are put without alignment).
    size_t align_of_data() const override { return alignof(Data); }

    void reset(AggregateDataPtr place) const override {
        destroy(place);
        create(place);
    }

    void deserialize_and_merge(AggregateDataPtr __restrict place, AggregateDataPtr __restrict rhs,
                               BufferReadable& buf, Arena* arena) const override {
        assert_cast<const Derived*>(this)->deserialize(rhs, buf, arena);
        assert_cast<const Derived*>(this)->merge(place, rhs, arena);
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
    }
    ~AggregateFunctionGuard() { _function->destroy(_data.get()); }
    AggregateDataPtr data() { return _data.get(); }

private:
    const IAggregateFunction* _function;
    std::unique_ptr<AggregateData[]> _data;
};

} // namespace doris::vectorized
