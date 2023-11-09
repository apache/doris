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

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "util/bitmap_value.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_number.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

struct AggregateFunctionBitmapUnionOp {
    static constexpr auto name = "bitmap_union";

    template <typename T>
    static void add(BitmapValue& res, const T& data, bool& is_first) {
        res.add(data);
    }

    static void add(BitmapValue& res, const BitmapValue& data, bool& is_first) {
        if (UNLIKELY(is_first)) {
            res = data;
            is_first = false;
        } else {
            res |= data;
        }
    }

    static void add_batch(BitmapValue& res, std::vector<const BitmapValue*>& data, bool& is_first) {
        res.fastunion(data);
    }

    static void merge(BitmapValue& res, const BitmapValue& data, bool& is_first) {
        if (UNLIKELY(is_first)) {
            res = data;
            is_first = false;
        } else {
            res |= data;
        }
    }
};

struct AggregateFunctionBitmapIntersectOp {
    static constexpr auto name = "bitmap_intersect";

    static void add(BitmapValue& res, const BitmapValue& data, bool& is_first) {
        if (UNLIKELY(is_first)) {
            res = data;
            is_first = false;
        } else {
            res &= data;
        }
    }

    static void merge(BitmapValue& res, const BitmapValue& data, bool& is_first) {
        if (UNLIKELY(is_first)) {
            res = data;
            is_first = false;
        } else {
            res &= data;
        }
    }
};

struct AggregateFunctionGroupBitmapXorOp {
    static constexpr auto name = "group_bitmap_xor";

    static void add(BitmapValue& res, const BitmapValue& data, bool& is_first) {
        if (UNLIKELY(is_first)) {
            res = data;
            is_first = false;
        } else {
            res ^= data;
        }
    }

    static void merge(BitmapValue& res, const BitmapValue& data, bool& is_first) {
        if (UNLIKELY(is_first)) {
            res = data;
            is_first = false;
        } else {
            res ^= data;
        }
    }
};

template <typename Op>
struct AggregateFunctionBitmapData {
    BitmapValue value;
    bool is_first = true;

    template <typename T>
    void add(const T& data) {
        Op::add(value, data, is_first);
    }

    void add_batch(std::vector<const BitmapValue*>& data) { Op::add_batch(value, data, is_first); }

    void merge(const BitmapValue& data) { Op::merge(value, data, is_first); }

    void write(BufferWritable& buf) const { DataTypeBitMap::serialize_as_stream(value, buf); }

    void read(BufferReadable& buf) { DataTypeBitMap::deserialize_as_stream(value, buf); }

    void reset() {
        is_first = true;
        value.reset(); // it's better to call reset function by self firstly.
    }

    BitmapValue& get() { return value; }
};

template <typename Data, typename Derived>
class AggregateFunctionBitmapSerializationHelper
        : public IAggregateFunctionDataHelper<Data, Derived> {
public:
    using BaseHelper = IAggregateFunctionHelper<Derived>;

    AggregateFunctionBitmapSerializationHelper(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, Derived>(argument_types_) {}

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena* arena) const override {
        if (version >= 3) {
            auto& col = assert_cast<ColumnBitmap&>(*dst);
            char place[sizeof(Data)];
            col.resize(num_rows);
            auto* data = col.get_data().data();
            for (size_t i = 0; i != num_rows; ++i) {
                assert_cast<const Derived*>(this)->create(place);
                DEFER({ assert_cast<const Derived*>(this)->destroy(place); });
                assert_cast<const Derived*>(this)->add(place, columns, i, arena);
                data[i] = std::move(this->data(place).value);
            }
        } else {
            BaseHelper::streaming_agg_serialize_to_column(columns, dst, num_rows, arena);
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        if (version >= 3) {
            auto& col = assert_cast<ColumnBitmap&>(*dst);
            col.resize(num_rows);
            auto* data = col.get_data().data();
            for (size_t i = 0; i != num_rows; ++i) {
                data[i] = std::move(this->data(places[i] + offset).value);
            }
        } else {
            BaseHelper::serialize_to_column(places, offset, dst, num_rows);
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        if (version >= 3) {
            auto& col = assert_cast<const ColumnBitmap&>(column);
            const size_t num_rows = column.size();
            auto* data = col.get_data().data();

            for (size_t i = 0; i != num_rows; ++i) {
                this->data(place).merge(data[i]);
            }
        } else {
            BaseHelper::deserialize_and_merge_from_column(place, column, arena);
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena* arena) const override {
        DCHECK(end <= column.size() && begin <= end)
                << ", begin:" << begin << ", end:" << end << ", column.size():" << column.size();
        if (version >= 3) {
            auto& col = assert_cast<const ColumnBitmap&>(column);
            auto* data = col.get_data().data();
            for (size_t i = begin; i <= end; ++i) {
                this->data(place).merge(data[i]);
            }
        } else {
            BaseHelper::deserialize_and_merge_from_column_range(place, column, begin, end, arena);
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const ColumnString* column, Arena* arena,
                                   const size_t num_rows) const override {
        if (version >= 3) {
            auto& col = assert_cast<const ColumnBitmap&>(*assert_cast<const IColumn*>(column));
            auto* data = col.get_data().data();
            for (size_t i = 0; i != num_rows; ++i) {
                this->data(places[i] + offset).merge(data[i]);
            }
        } else {
            BaseHelper::deserialize_and_merge_vec(places, offset, rhs, column, arena, num_rows);
        }
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const ColumnString* column,
                                            Arena* arena, const size_t num_rows) const override {
        if (version >= 3) {
            auto& col = assert_cast<const ColumnBitmap&>(*assert_cast<const IColumn*>(column));
            auto* data = col.get_data().data();
            for (size_t i = 0; i != num_rows; ++i) {
                if (places[i]) {
                    this->data(places[i] + offset).merge(data[i]);
                }
            }
        } else {
            BaseHelper::deserialize_and_merge_vec_selected(places, offset, rhs, column, arena,
                                                           num_rows);
        }
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        if (version >= 3) {
            auto& col = assert_cast<ColumnBitmap&>(to);
            size_t old_size = col.size();
            col.resize(old_size + 1);
            col.get_data()[old_size] = std::move(this->data(place).value);
        } else {
            BaseHelper::serialize_without_key_to_column(place, to);
        }
    }

    [[nodiscard]] MutableColumnPtr create_serialize_column() const override {
        if (version >= 3) {
            return ColumnBitmap::create();
        } else {
            return ColumnString::create();
        }
    }

    [[nodiscard]] DataTypePtr get_serialized_type() const override {
        if (version >= 3) {
            return std::make_shared<DataTypeBitMap>();
        } else {
            return IAggregateFunction::get_serialized_type();
        }
    }

protected:
    using IAggregateFunction::version;
};

template <typename Op>
class AggregateFunctionBitmapOp final
        : public AggregateFunctionBitmapSerializationHelper<AggregateFunctionBitmapData<Op>,
                                                            AggregateFunctionBitmapOp<Op>> {
public:
    using ResultDataType = BitmapValue;
    using ColVecType = ColumnBitmap;
    using ColVecResult = ColumnBitmap;

    String get_name() const override { return Op::name; }

    AggregateFunctionBitmapOp(const DataTypes& argument_types_)
            : AggregateFunctionBitmapSerializationHelper<AggregateFunctionBitmapData<Op>,
                                                         AggregateFunctionBitmapOp<Op>>(
                      argument_types_) {}

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeBitMap>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& column = assert_cast<const ColVecType&>(*columns[0]);
        this->data(place).add(column.get_data()[row_num]);
    }

    void add_many(AggregateDataPtr __restrict place, const IColumn** columns,
                  std::vector<int>& rows, Arena*) const override {
        if constexpr (std::is_same_v<Op, AggregateFunctionBitmapUnionOp>) {
            const auto& column = assert_cast<const ColVecType&>(*columns[0]);
            std::vector<const BitmapValue*> values;
            for (int i = 0; i < rows.size(); ++i) {
                values.push_back(&(column.get_data()[rows[i]]));
            }
            this->data(place).add_batch(values);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(
                const_cast<AggregateFunctionBitmapData<Op>&>(this->data(rhs)).get());
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& column = assert_cast<ColVecResult&>(to);
        column.get_data().push_back(
                const_cast<AggregateFunctionBitmapData<Op>&>(this->data(place)).get());
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }
};

template <bool arg_is_nullable, typename ColVecType>
class AggregateFunctionBitmapCount final
        : public AggregateFunctionBitmapSerializationHelper<
                  AggregateFunctionBitmapData<AggregateFunctionBitmapUnionOp>,
                  AggregateFunctionBitmapCount<arg_is_nullable, ColVecType>> {
public:
    // using ColVecType = ColumnBitmap;
    using ColVecResult = ColumnVector<Int64>;
    using AggFunctionData = AggregateFunctionBitmapData<AggregateFunctionBitmapUnionOp>;

    AggregateFunctionBitmapCount(const DataTypes& argument_types_)
            : AggregateFunctionBitmapSerializationHelper<
                      AggregateFunctionBitmapData<AggregateFunctionBitmapUnionOp>,
                      AggregateFunctionBitmapCount<arg_is_nullable, ColVecType>>(argument_types_) {}

    String get_name() const override { return "count"; }
    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        if constexpr (arg_is_nullable) {
            auto& nullable_column = assert_cast<const ColumnNullable&>(*columns[0]);
            if (!nullable_column.is_null_at(row_num)) {
                const auto& column =
                        assert_cast<const ColVecType&>(nullable_column.get_nested_column());
                this->data(place).add(column.get_data()[row_num]);
            }
        } else {
            const auto& column = assert_cast<const ColVecType&>(*columns[0]);
            this->data(place).add(column.get_data()[row_num]);
        }
    }

    void add_many(AggregateDataPtr __restrict place, const IColumn** columns,
                  std::vector<int>& rows, Arena*) const override {
        if constexpr (arg_is_nullable && std::is_same_v<ColVecType, ColumnBitmap>) {
            auto& nullable_column = assert_cast<const ColumnNullable&>(*columns[0]);
            const auto& column =
                    assert_cast<const ColVecType&>(nullable_column.get_nested_column());
            std::vector<const BitmapValue*> values;
            for (int i = 0; i < rows.size(); ++i) {
                if (!nullable_column.is_null_at(rows[i])) {
                    values.push_back(&(column.get_data()[rows[i]]));
                }
            }
            this->data(place).add_batch(values);
        } else if constexpr (std::is_same_v<ColVecType, ColumnBitmap>) {
            const auto& column = assert_cast<const ColVecType&>(*columns[0]);
            std::vector<const BitmapValue*> values;
            for (int i = 0; i < rows.size(); ++i) {
                values.push_back(&(column.get_data()[rows[i]]));
            }
            this->data(place).add_batch(values);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(const_cast<AggFunctionData&>(this->data(rhs)).get());
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& value_data = const_cast<AggFunctionData&>(this->data(place)).get();
        auto& column = assert_cast<ColVecResult&>(to);
        column.get_data().push_back(value_data.cardinality());
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }
};

AggregateFunctionPtr create_aggregate_function_bitmap_union(const std::string& name,
                                                            const DataTypes& argument_types,
                                                            const bool result_is_nullable);

} // namespace doris::vectorized