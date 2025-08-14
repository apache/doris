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

#include "agent/be_exec_version_manager.h"
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

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class Arena;
class BufferReadable;
class BufferWritable;
class IColumn;
struct AggregateFunctionBitmapUnionOp {
    static constexpr auto name = "bitmap_union";

    template <typename T>
    static void add(BitmapValue& res, const T& data, bool& is_first) {
        res.add(data);
        is_first = false;
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
        // after fastunion, res myabe have many datas, so is_first should be false
        // then call add function will not reset res
        is_first = false;
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
                                           const size_t num_rows, Arena& arena) const override {
        auto& col = assert_cast<ColumnBitmap&>(*dst);
        char place[sizeof(Data)];
        col.resize(num_rows);
        auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            assert_cast<const Derived*, TypeCheckOnRelease::DISABLE>(this)->create(place);
            DEFER({
                assert_cast<const Derived*, TypeCheckOnRelease::DISABLE>(this)->destroy(place);
            });
            assert_cast<const Derived*, TypeCheckOnRelease::DISABLE>(this)->add(place, columns, i,
                                                                                arena);
            data[i] = std::move(this->data(place).value);
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        auto& col = assert_cast<ColumnBitmap&>(*dst);
        col.resize(num_rows);
        auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            data[i] = std::move(this->data(places[i] + offset).value);
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena&) const override {
        const auto& col = assert_cast<const ColumnBitmap&>(column);
        const size_t num_rows = column.size();
        const auto* data = col.get_data().data();

        for (size_t i = 0; i != num_rows; ++i) {
            this->data(place).merge(data[i]);
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena&) const override {
        DCHECK(end <= column.size() && begin <= end)
                << ", begin:" << begin << ", end:" << end << ", column.size():" << column.size();
        const auto& col = assert_cast<const ColumnBitmap&>(column);
        const auto* data = col.get_data().data();
        for (size_t i = begin; i <= end; ++i) {
            this->data(place).merge(data[i]);
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena&,
                                   const size_t num_rows) const override {
        const auto& col = assert_cast<const ColumnBitmap&>(*column);
        const auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            this->data(places[i] + offset).merge(data[i]);
        }
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column, Arena&,
                                            const size_t num_rows) const override {
        const auto& col = assert_cast<const ColumnBitmap&>(*column);
        const auto* data = col.get_data().data();
        for (size_t i = 0; i != num_rows; ++i) {
            if (places[i]) {
                this->data(places[i] + offset).merge(data[i]);
            }
        }
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        auto& col = assert_cast<ColumnBitmap&>(to);
        size_t old_size = col.size();
        col.resize(old_size + 1);
        col.get_data()[old_size] = std::move(this->data(place).value);
    }

    [[nodiscard]] MutableColumnPtr create_serialize_column() const override {
        return ColumnBitmap::create();
    }

    [[nodiscard]] DataTypePtr get_serialized_type() const override {
        return std::make_shared<DataTypeBitMap>();
    }

protected:
    using IAggregateFunction::version;
};

template <typename Op>
class AggregateFunctionBitmapOp final
        : public AggregateFunctionBitmapSerializationHelper<AggregateFunctionBitmapData<Op>,
                                                            AggregateFunctionBitmapOp<Op>>,
          UnaryExpression,
          NullableAggregateFunction {
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

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        const auto& column =
                assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        this->data(place).add(column.get_data()[row_num]);
    }

    void add_many(AggregateDataPtr __restrict place, const IColumn** columns,
                  std::vector<int>& rows, Arena&) const override {
        // now this only for bitmap_union function
        if constexpr (std::is_same_v<Op, AggregateFunctionBitmapUnionOp>) {
            const auto& column = assert_cast<const ColVecType&>(*columns[0]);
            std::vector<const BitmapValue*> values;
            for (auto row : rows) {
                values.push_back(&(column.get_data()[row]));
            }
            this->data(place).add_batch(values);
        }
    }

    void add_batch_range(size_t batch_begin, size_t batch_end, AggregateDataPtr place,
                         const IColumn** columns, Arena& arena, bool has_null) override {
        // now this only for bitmap_union function
        if constexpr (std::is_same_v<Op, AggregateFunctionBitmapUnionOp>) {
            const auto& column = assert_cast<const ColVecType&>(*columns[0]);
            std::vector<const BitmapValue*> values;
            for (size_t i = batch_begin; i <= batch_end; ++i) {
                values.push_back(&(column.get_data()[i]));
            }
            this->data(place).add_batch(values);
        } else {
            for (size_t i = batch_begin; i <= batch_end; ++i) {
                this->add(place, columns, i, arena);
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(
                const_cast<AggregateFunctionBitmapData<Op>&>(this->data(rhs)).get());
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
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
                  AggregateFunctionBitmapCount<arg_is_nullable, ColVecType>>,
          UnaryExpression,
          NotNullableAggregateFunction {
public:
    // using ColVecType = ColumnBitmap;
    using ColVecResult = ColumnInt64;
    using AggFunctionData = AggregateFunctionBitmapData<AggregateFunctionBitmapUnionOp>;

    AggregateFunctionBitmapCount(const DataTypes& argument_types_)
            : AggregateFunctionBitmapSerializationHelper<
                      AggregateFunctionBitmapData<AggregateFunctionBitmapUnionOp>,
                      AggregateFunctionBitmapCount<arg_is_nullable, ColVecType>>(argument_types_) {}

    String get_name() const override {
        if constexpr (std::is_same_v<ColVecType, ColumnBitmap>) {
            return "bitmap_union_count";
        } else {
            return "bitmap_union_int";
        }
    }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        if constexpr (arg_is_nullable) {
            const auto& nullable_column = assert_cast<const ColumnNullable&>(*columns[0]);
            if (!nullable_column.is_null_at(row_num)) {
                const auto& column = assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(
                        nullable_column.get_nested_column());
                this->data(place).add(column.get_data()[row_num]);
            }
        } else {
            const auto& column =
                    assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0]);
            this->data(place).add(column.get_data()[row_num]);
        }
    }

    void add_many(AggregateDataPtr __restrict place, const IColumn** columns,
                  std::vector<int>& rows, Arena&) const override {
        // now this only for bitmap_union_count function
        if constexpr (arg_is_nullable && std::is_same_v<ColVecType, ColumnBitmap>) {
            const auto& nullable_column = assert_cast<const ColumnNullable&>(*columns[0]);
            const auto& column =
                    assert_cast<const ColVecType&>(nullable_column.get_nested_column());
            std::vector<const BitmapValue*> values;
            for (auto row : rows) {
                if (!nullable_column.is_null_at(row)) {
                    values.push_back(&(column.get_data()[row]));
                }
            }
            this->data(place).add_batch(values);
        } else if constexpr (std::is_same_v<ColVecType, ColumnBitmap>) {
            const auto& column = assert_cast<const ColVecType&>(*columns[0]);
            std::vector<const BitmapValue*> values;
            for (auto row : rows) {
                values.push_back(&(column.get_data()[row]));
            }
            this->data(place).add_batch(values);
        }
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena) const override {
        // now this only for bitmap_union_count function
        if constexpr (std::is_same_v<ColVecType, ColumnBitmap>) {
            auto lambda_function = [&](const IColumn& data_column, const NullMap* null_map) {
                const auto& column = assert_cast<const ColVecType&>(data_column);
                std::vector<const BitmapValue*> values;
                for (size_t i = 0; i < batch_size; ++i) {
                    if constexpr (arg_is_nullable) {
                        if ((*null_map)[i]) {
                            continue; // skip null value
                        }
                    }
                    values.push_back(&(column.get_data()[i]));
                }
                this->data(place).add_batch(values);
            };

            if constexpr (arg_is_nullable) {
                const auto& nullable_column = assert_cast<const ColumnNullable&>(*columns[0]);
                lambda_function(nullable_column.get_nested_column(),
                                &(nullable_column.get_null_map_data()));
            } else {
                lambda_function(*columns[0], nullptr);
            }
        } else {
            for (size_t i = 0; i < batch_size; ++i) {
                this->add(place, columns, i, arena);
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(const_cast<AggFunctionData&>(this->data(rhs)).get());
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
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
#include "common/compile_check_end.h"
