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

#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <string>
#include <vector>

#include "util/bitmap_value.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_bitmap.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionBitmapAggData {
    BitmapValue value;

    void add(const T& value_) { value.add(value_); }

    void reset() { value.reset(); }

    void merge(const AggregateFunctionBitmapAggData& other) { value |= other.value; }

    void write(BufferWritable& buf) const { DataTypeBitMap::serialize_as_stream(value, buf); }

    void read(BufferReadable& buf) { DataTypeBitMap::deserialize_as_stream(value, buf); }
};

template <bool arg_nullable, typename T>
class AggregateFunctionBitmapAgg final
        : public IAggregateFunctionDataHelper<AggregateFunctionBitmapAggData<T>,
                                              AggregateFunctionBitmapAgg<arg_nullable, T>> {
public:
    using ColVecType = ColumnVector<T>;
    using Data = AggregateFunctionBitmapAggData<T>;

    AggregateFunctionBitmapAgg(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionBitmapAgg<arg_nullable, T>>(
                      argument_types_) {}

    std::string get_name() const override { return "bitmap_agg"; }
    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeBitMap>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena* arena) const override {
        DCHECK_LT(row_num, columns[0]->size());
        if constexpr (arg_nullable) {
            auto& nullable_col =
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*columns[0]);
            auto& nullable_map = nullable_col.get_null_map_data();
            if (!nullable_map[row_num]) {
                auto& col = assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(
                        nullable_col.get_nested_column());
                this->data(place).add(col.get_data()[row_num]);
            }
        } else {
            auto& col = assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0]);
            this->data(place).add(col.get_data()[row_num]);
        }
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        if constexpr (arg_nullable) {
            auto& nullable_column = assert_cast<const ColumnNullable&>(*columns[0]);
            const auto& column =
                    assert_cast<const ColVecType&>(nullable_column.get_nested_column());
            std::vector<T> values;
            for (int i = 0; i < batch_size; ++i) {
                if (!nullable_column.is_null_at(i)) {
                    values.push_back(column.get_data()[i]);
                }
            }
            this->data(place).value.add_many(values.data(), values.size());
        } else {
            const auto& column = assert_cast<const ColVecType&>(*columns[0]);
            this->data(place).value.add_many(column.get_data().data(), column.size());
        }
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        this->data(place).merge(this->data(rhs));
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& column = assert_cast<ColumnBitmap&>(to);
        column.get_data().push_back(this->data(place).value);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena* arena) const override {
        auto& col = assert_cast<ColumnBitmap&>(*dst);
        char place[sizeof(Data)];
        col.resize(num_rows);
        auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            this->create(place);
            DEFER({ this->destroy(place); });
            this->add(place, columns, i, arena);
            data[i] = std::move(this->data(place).value);
        }
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena* arena,
                                 size_t num_rows) const override {
        auto& col = assert_cast<const ColumnBitmap&>(column);
        DCHECK(col.size() >= num_rows) << "source column's size should greater than num_rows";
        auto* src = col.get_data().data();
        auto* data = &(this->data(places));
        for (size_t i = 0; i != num_rows; ++i) {
            data[i].value = src[i];
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        auto& col = assert_cast<ColumnBitmap&>(*dst);
        col.resize(num_rows);
        auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            data[i] = this->data(places[i] + offset).value;
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        auto& col = assert_cast<const ColumnBitmap&>(column);
        const size_t num_rows = column.size();
        auto* data = col.get_data().data();

        for (size_t i = 0; i != num_rows; ++i) {
            this->data(place).value |= data[i];
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena* arena) const override {
        DCHECK(end <= column.size() && begin <= end)
                << ", begin:" << begin << ", end:" << end << ", column.size():" << column.size();
        auto& col = assert_cast<const ColumnBitmap&>(column);
        auto* data = col.get_data().data();
        for (size_t i = begin; i <= end; ++i) {
            this->data(place).value |= data[i];
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena* arena,
                                   const size_t num_rows) const override {
        const auto& col = assert_cast<const ColumnBitmap&>(*column);
        const auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            this->data(places[i] + offset).value |= data[i];
        }
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena* arena, const size_t num_rows) const override {
        const auto& col = assert_cast<const ColumnBitmap&>(*column);
        const auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            if (places[i]) {
                this->data(places[i] + offset).value |= data[i];
            }
        }
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        auto& col = assert_cast<ColumnBitmap&>(to);
        size_t old_size = col.size();
        col.resize(old_size + 1);
        col.get_data()[old_size] = this->data(place).value;
    }

    [[nodiscard]] MutableColumnPtr create_serialize_column() const override {
        return ColumnBitmap::create();
    }

    [[nodiscard]] DataTypePtr get_serialized_type() const override {
        return std::make_shared<DataTypeBitMap>();
    }
};

} // namespace doris::vectorized