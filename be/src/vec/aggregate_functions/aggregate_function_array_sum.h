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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_array.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_array.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

template <PrimitiveType T>
struct AggregateFunctionArraySumData {
    using ElementType = typename PrimitiveTypeTraits<T>::ColumnItemType;
    using ColVecType = typename PrimitiveTypeTraits<T>::ColumnType;
    using SelfType = AggregateFunctionArraySumData<T>;
    PaddedPODArray<ElementType, 16 * sizeof(ElementType)> data;

    void add(const IColumn& column, size_t row_num) {
        ColumnArrayExecutionData array_data;
        // extract array column
        if (!extract_column_array_info(column, array_data)) {
            LOG(ERROR) << "execute failed, argument of agg_array_sum should be array";
            return;
        }
        if (array_data.array_nullmap_data != nullptr && array_data.array_nullmap_data[row_num]) {
            return;
        }
        ssize_t start = (*array_data.offsets_ptr)[row_num - 1];
        ssize_t size = (*array_data.offsets_ptr)[row_num] - start;
        ssize_t n = std::min(size, (ssize_t)data.size());
        const auto& rhs =
                assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*array_data.nested_col)
                        .get_data();
        for (ssize_t i = 0; i < n; ++i) {
            if (array_data.nested_nullmap_data != nullptr &&
                array_data.nested_nullmap_data[i + start]) {
                continue;
            }
            data[i] = data[i] + rhs[i + start];
        }
        for (ssize_t i = n; i < size; ++i) {
            if (array_data.nested_nullmap_data != nullptr &&
                array_data.nested_nullmap_data[i + start]) {
                data.resize_fill(data.size() + 1);
                continue;
            }
            data.push_back(rhs[i + start]);
        }
    }

    void merge(const SelfType& rhs) {
        ssize_t n = std::min(rhs.data.size(), data.size());
        for (ssize_t i = 0; i < n; ++i) {
            data[i] = data[i] + rhs.data[i];
        }
        for (ssize_t i = n; i < rhs.data.size(); ++i) {
            data.push_back(rhs.data[i]);
        }
    }

    void write(BufferWritable& buf) const {
        buf.write_var_uint(data.size());
        buf.write(data.raw_data(), data.size() * sizeof(ElementType));
    }

    void read(BufferReadable& buf) {
        UInt64 rows = 0;
        buf.read_var_uint(rows);
        data.resize(rows);
        buf.read(reinterpret_cast<char*>(data.data()), rows * sizeof(ElementType));
    }

    void reset() { data.clear(); }

    void insert_result_into(IColumn& to) const {
        auto& vec = assert_cast<ColVecType&>(to).get_data();
        size_t old_size = vec.size();
        vec.resize(old_size + data.size());
        memcpy(vec.data() + old_size, data.data(), data.size() * sizeof(ElementType));
    }
};

template <typename Data>
class AggregateFunctionArraySum
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionArraySum<Data>>,
          UnaryExpression,
          NotNullableAggregateFunction {
public:
    AggregateFunctionArraySum(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionArraySum<Data>>(argument_types_),
              _argument_type(argument_types_[0]) {}

    std::string get_name() const override { return "agg_array_sum"; }

    DataTypePtr get_return_type() const override { return remove_nullable(_argument_type); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        this->data(place).add(*columns[0], row_num);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena& arena) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        if (to_nested_col.is_nullable()) {
            auto* col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            this->data(place).insert_result_into(col_null->get_nested_column());
            col_null->get_null_map_data().resize_fill(col_null->get_nested_column().size(), 0);
        } else {
            this->data(place).insert_result_into(to_nested_col);
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }

private:
    DataTypePtr _argument_type;
};

} // namespace doris::vectorized