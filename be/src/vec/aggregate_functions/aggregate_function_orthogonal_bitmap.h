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

#include <glog/logging.h>

#include <boost/iterator/iterator_facade.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>

#include "util/bitmap_expr_calculation.h"
#include "util/bitmap_intersect.h"
#include "util/bitmap_value.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
class IColumn;
template <typename T>
class ColumnStr;
using ColumnString = ColumnStr<UInt32>;

template <typename T>
struct AggOrthBitmapBaseData {
public:
    using ColVecData = std::conditional_t<IsNumber<T>, ColumnVector<T>, ColumnString>;

    void add(const IColumn** columns, size_t row_num) {
        const auto& bitmap_col =
                assert_cast<const ColumnBitmap&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& data_col =
                assert_cast<const ColVecData&, TypeCheckOnRelease::DISABLE>(*columns[1]);
        const auto& bitmap_value = bitmap_col.get_element(row_num);

        if constexpr (IsNumber<T>) {
            bitmap.update(data_col.get_element(row_num), bitmap_value);
        }
        if constexpr (std::is_same_v<T, std::string_view>) {
            // TODO: rethink here we really need to do a virtual function call
            auto sr = data_col.get_data_at(row_num);
            bitmap.update(std::string_view {sr.data, sr.size}, bitmap_value);
        }
    }

    void init_add_key(const IColumn** columns, size_t row_num, int argument_size) {
        if (first_init) {
            DCHECK(argument_size > 1);
            for (int idx = 2; idx < argument_size; ++idx) {
                const auto& col =
                        assert_cast<const ColVecData&, TypeCheckOnRelease::DISABLE>(*columns[idx]);
                if constexpr (IsNumber<T>) {
                    bitmap.add_key(col.get_element(row_num));
                }
                if constexpr (std::is_same_v<T, std::string_view>) {
                    auto sr = col.get_data_at(row_num);
                    bitmap.add_key(std::string_view {sr.data, sr.size});
                }
            }
            first_init = false;
        }
    }

protected:
    doris::BitmapIntersect<T> bitmap;
    bool first_init = true;
};

template <typename T>
struct AggOrthBitMapIntersect : public AggOrthBitmapBaseData<T> {
public:
    static constexpr auto name = "orthogonal_bitmap_intersect";

    static DataTypePtr get_return_type() { return std::make_shared<DataTypeBitMap>(); }

    void merge(const AggOrthBitMapIntersect& rhs) {
        if (rhs.first_init) {
            return;
        }
        result |= rhs.result;
        AggOrthBitmapBaseData<T>::first_init = false;
    }

    void write(BufferWritable& buf) {
        write_binary(AggOrthBitmapBaseData<T>::first_init, buf);
        result = AggOrthBitmapBaseData<T>::bitmap.intersect();
        DataTypeBitMap::serialize_as_stream(result, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(AggOrthBitmapBaseData<T>::first_init, buf);
        DataTypeBitMap::deserialize_as_stream(result, buf);
    }

    void get(IColumn& to) const {
        auto& column = assert_cast<ColumnBitmap&>(to);
        column.get_data().emplace_back(result);
    }

private:
    BitmapValue result;
};

template <typename T>
struct AggIntersectCount : public AggOrthBitmapBaseData<T> {
public:
    static constexpr auto name = "intersect_count";

    static DataTypePtr get_return_type() { return std::make_shared<DataTypeInt64>(); }

    void merge(const AggIntersectCount& rhs) {
        if (rhs.first_init) {
            return;
        }
        AggOrthBitmapBaseData<T>::bitmap.merge(rhs.bitmap);
        AggOrthBitmapBaseData<T>::first_init = false;
    }

    void write(BufferWritable& buf) {
        write_binary(AggOrthBitmapBaseData<T>::first_init, buf);
        std::string data;
        data.resize(AggOrthBitmapBaseData<T>::bitmap.size());
        AggOrthBitmapBaseData<T>::bitmap.serialize(data.data());
        write_binary(data, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(AggOrthBitmapBaseData<T>::first_init, buf);
        std::string data;
        read_binary(data, buf);
        AggOrthBitmapBaseData<T>::bitmap.deserialize(data.data());
    }

    void get(IColumn& to) const {
        auto& column = assert_cast<ColumnVector<Int64>&>(to);
        column.get_data().emplace_back(AggOrthBitmapBaseData<T>::bitmap.intersect_count());
    }
};

template <typename T>
struct AggOrthBitMapIntersectCount : public AggOrthBitmapBaseData<T> {
public:
    static constexpr auto name = "orthogonal_bitmap_intersect_count";

    static DataTypePtr get_return_type() { return std::make_shared<DataTypeInt64>(); }

    void merge(const AggOrthBitMapIntersectCount& rhs) {
        if (rhs.first_init) {
            return;
        }
        result += rhs.result;
        AggOrthBitmapBaseData<T>::first_init = false;
    }

    void write(BufferWritable& buf) {
        write_binary(AggOrthBitmapBaseData<T>::first_init, buf);
        result = AggOrthBitmapBaseData<T>::bitmap.intersect_count();
        write_binary(result, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(AggOrthBitmapBaseData<T>::first_init, buf);
        read_binary(result, buf);
    }

    void get(IColumn& to) const {
        auto& column = assert_cast<ColumnVector<Int64>&>(to);
        column.get_data().emplace_back(result ? result
                                              : AggOrthBitmapBaseData<T>::bitmap.intersect_count());
    }

private:
    Int64 result = 0;
};

template <typename T>
struct AggOrthBitmapExprCalBaseData {
public:
    using ColVecData = std::conditional_t<IsNumber<T>, ColumnVector<T>, ColumnString>;

    void add(const IColumn** columns, size_t row_num) {
        const auto& bitmap_col =
                assert_cast<const ColumnBitmap&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        const auto& data_col =
                assert_cast<const ColVecData&, TypeCheckOnRelease::DISABLE>(*columns[1]);
        const auto& bitmap_value = bitmap_col.get_element(row_num);
        std::string update_key = data_col.get_data_at(row_num).to_string();
        bitmap_expr_cal.update(update_key, bitmap_value);
    }

    void init_add_key(const IColumn** columns, size_t row_num, int argument_size) {
        if (first_init) {
            DCHECK(argument_size > 1);
            const auto& col =
                    assert_cast<const ColVecData&, TypeCheckOnRelease::DISABLE>(*columns[2]);
            std::string expr = col.get_data_at(row_num).to_string();
            bitmap_expr_cal.bitmap_calculation_init(expr);
            first_init = false;
        }
    }

protected:
    doris::BitmapExprCalculation bitmap_expr_cal;
    bool first_init = true;
};

template <typename T>
struct AggOrthBitMapExprCal : public AggOrthBitmapExprCalBaseData<T> {
public:
    static constexpr auto name = "orthogonal_bitmap_expr_calculate";

    static DataTypePtr get_return_type() { return std::make_shared<DataTypeBitMap>(); }

    void merge(const AggOrthBitMapExprCal& rhs) {
        if (rhs.first_init) {
            return;
        }
        result |= rhs.result;
    }

    void write(BufferWritable& buf) {
        write_binary(AggOrthBitmapExprCalBaseData<T>::first_init, buf);
        result = AggOrthBitmapExprCalBaseData<T>::bitmap_expr_cal.bitmap_calculate();
        DataTypeBitMap::serialize_as_stream(result, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(AggOrthBitmapExprCalBaseData<T>::first_init, buf);
        DataTypeBitMap::deserialize_as_stream(result, buf);
    }

    void get(IColumn& to) const {
        auto& column = assert_cast<ColumnBitmap&>(to);
        column.get_data().emplace_back(!result.empty()
                                               ? result
                                               : const_cast<AggOrthBitMapExprCal*>(this)
                                                         ->bitmap_expr_cal.bitmap_calculate());
    }

private:
    BitmapValue result;
};

template <typename T>
struct AggOrthBitMapExprCalCount : public AggOrthBitmapExprCalBaseData<T> {
public:
    static constexpr auto name = "orthogonal_bitmap_expr_calculate_count";

    static DataTypePtr get_return_type() { return std::make_shared<DataTypeInt64>(); }

    void merge(const AggOrthBitMapExprCalCount& rhs) {
        if (rhs.first_init) {
            return;
        }
        result += rhs.result;
    }

    void write(BufferWritable& buf) {
        write_binary(AggOrthBitmapExprCalBaseData<T>::first_init, buf);
        result = AggOrthBitmapExprCalBaseData<T>::bitmap_expr_cal.bitmap_calculate_count();
        write_binary(result, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(AggOrthBitmapExprCalBaseData<T>::first_init, buf);
        read_binary(result, buf);
    }

    void get(IColumn& to) const {
        auto& column = assert_cast<ColumnVector<Int64>&>(to);
        column.get_data().emplace_back(result ? result
                                              : const_cast<AggOrthBitMapExprCalCount*>(this)
                                                        ->bitmap_expr_cal.bitmap_calculate_count());
    }

private:
    int64_t result = 0;
};

template <typename T>
struct OrthBitmapUnionCountData {
    static constexpr auto name = "orthogonal_bitmap_union_count";

    static DataTypePtr get_return_type() { return std::make_shared<DataTypeInt64>(); }
    // Here no need doing anything, so only given an function declaration
    void init_add_key(const IColumn** columns, size_t row_num, int argument_size) {}

    void add(const IColumn** columns, size_t row_num) {
        const auto& column =
                assert_cast<const ColumnBitmap&, TypeCheckOnRelease::DISABLE>(*columns[0]);
        value |= column.get_data()[row_num];
    }
    void merge(const OrthBitmapUnionCountData& rhs) { result += rhs.result; }

    void write(BufferWritable& buf) {
        result = value.cardinality();
        write_binary(result, buf);
    }

    void read(BufferReadable& buf) { read_binary(result, buf); }

    void get(IColumn& to) const {
        auto& column = assert_cast<ColumnVector<Int64>&>(to);
        column.get_data().emplace_back(result ? result : value.cardinality());
    }

private:
    BitmapValue value;
    int64_t result = 0;
};

template <typename Impl>
class AggFunctionOrthBitmapFunc final
        : public IAggregateFunctionDataHelper<Impl, AggFunctionOrthBitmapFunc<Impl>> {
public:
    String get_name() const override { return Impl::name; }

    AggFunctionOrthBitmapFunc(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Impl, AggFunctionOrthBitmapFunc<Impl>>(argument_types_),
              _argument_size(argument_types_.size()) {}

    DataTypePtr get_return_type() const override { return Impl::get_return_type(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        this->data(place).init_add_key(columns, row_num, _argument_size);
        this->data(place).add(columns, row_num);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(const_cast<AggregateDataPtr>(place)).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).get(to);
    }

private:
    int _argument_size;
};
} // namespace doris::vectorized
