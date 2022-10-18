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

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/key_holder_helpers.h"
#include "vec/columns/column_array.h"
#include "vec/common/aggregation_common.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionCollectSetData {
    using ElementType = T;
    using ColVecType = ColumnVectorOrDecimal<ElementType>;
    using ElementNativeType = typename NativeType<T>::Type;
    using Set = HashSetWithStackMemory<ElementNativeType, DefaultHash<ElementNativeType>, 4>;
    Set set;

    void add(const IColumn& column, size_t row_num) {
        const auto& vec = assert_cast<const ColVecType&>(column).get_data();
        set.insert(vec[row_num]);
    }
    void merge(const AggregateFunctionCollectSetData& rhs) { set.merge(rhs.set); }
    void write(BufferWritable& buf) const { set.write(buf); }
    void read(BufferReadable& buf) { set.read(buf); }
    void reset() { set.clear(); }
    void insert_result_into(IColumn& to) const {
        auto& vec = assert_cast<ColVecType&>(to).get_data();
        vec.reserve(set.size());
        for (auto item : set) {
            vec.push_back(item.key);
        }
    }
};

template <>
struct AggregateFunctionCollectSetData<StringRef> {
    using ElementType = StringRef;
    using ColVecType = ColumnString;
    using Set = HashSetWithSavedHashWithStackMemory<ElementType, DefaultHash<ElementType>, 4>;
    Set set;

    void add(const IColumn& column, size_t row_num, Arena* arena) {
        Set::LookupResult it;
        bool inserted;
        auto key_holder = get_key_holder<true>(column, row_num, *arena);
        set.emplace(key_holder, it, inserted);
    }

    void merge(const AggregateFunctionCollectSetData& rhs, Arena* arena) {
        Set::LookupResult it;
        bool inserted;
        for (const auto& elem : rhs.set) {
            set.emplace(ArenaKeyHolder {elem.get_value(), *arena}, it, inserted);
        }
    }
    void write(BufferWritable& buf) const {
        write_var_uint(set.size(), buf);
        for (const auto& elem : set) {
            write_string_binary(elem.get_value(), buf);
        }
    }
    void read(BufferReadable& buf) {
        UInt64 rows;
        read_var_uint(rows, buf);

        StringRef ref;
        for (size_t i = 0; i < rows; ++i) {
            read_string_binary(ref, buf);
            set.insert(ref);
        }
    }
    void reset() { set.clear(); }
    void insert_result_into(IColumn& to) const {
        auto& vec = assert_cast<ColVecType&>(to);
        vec.reserve(set.size());
        for (const auto& item : set) {
            vec.insert_data(item.key.data, item.key.size);
        }
    }
};

template <typename T>
struct AggregateFunctionCollectListData {
    using ElementType = T;
    using ColVecType = ColumnVectorOrDecimal<ElementType>;
    PaddedPODArray<ElementType> data;

    void add(const IColumn& column, size_t row_num) {
        const auto& vec = assert_cast<const ColVecType&>(column).get_data();
        data.push_back(vec[row_num]);
    }
    void merge(const AggregateFunctionCollectListData& rhs) {
        data.insert(rhs.data.begin(), rhs.data.end());
    }
    void write(BufferWritable& buf) const {
        write_var_uint(data.size(), buf);
        buf.write(data.raw_data(), data.size() * sizeof(ElementType));
    }
    void read(BufferReadable& buf) {
        UInt64 rows = 0;
        read_var_uint(rows, buf);
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

template <>
struct AggregateFunctionCollectListData<StringRef> {
    using ElementType = StringRef;
    using ColVecType = ColumnString;
    MutableColumnPtr data;

    AggregateFunctionCollectListData<ElementType>() { data = ColVecType::create(); }

    void add(const IColumn& column, size_t row_num) { data->insert_from(column, row_num); }

    void merge(const AggregateFunctionCollectListData& rhs) {
        data->insert_range_from(*rhs.data, 0, rhs.data->size());
    }

    void write(BufferWritable& buf) const {
        auto& col = assert_cast<ColVecType&>(*data);

        write_var_uint(col.size(), buf);
        buf.write(col.get_offsets().raw_data(), col.size() * sizeof(IColumn::Offset));

        write_var_uint(col.get_chars().size(), buf);
        buf.write(col.get_chars().raw_data(), col.get_chars().size());
    }

    void read(BufferReadable& buf) {
        auto& col = assert_cast<ColVecType&>(*data);
        UInt64 offs_size = 0;
        read_var_uint(offs_size, buf);
        col.get_offsets().resize(offs_size);
        buf.read(reinterpret_cast<char*>(col.get_offsets().data()),
                 offs_size * sizeof(IColumn::Offset));

        UInt64 chars_size = 0;
        read_var_uint(chars_size, buf);
        col.get_chars().resize(chars_size);
        buf.read(reinterpret_cast<char*>(col.get_chars().data()), chars_size);
    }

    void reset() { data->clear(); }

    void insert_result_into(IColumn& to) const {
        auto& to_str = assert_cast<ColVecType&>(to);
        to_str.insert_range_from(*data, 0, data->size());
    }
};

template <typename Data>
class AggregateFunctionCollect final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionCollect<Data>> {
public:
    static constexpr bool alloc_memory_in_arena =
            std::is_same_v<Data, AggregateFunctionCollectSetData<StringRef>>;

    AggregateFunctionCollect(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionCollect<Data>>(argument_types_,
                                                                                 {}),
              _argument_type(argument_types_[0]) {}

    std::string get_name() const override {
        if constexpr (std::is_same_v<AggregateFunctionCollectListData<typename Data::ElementType>,
                                     Data>) {
            return "collect_list";
        } else {
            return "collect_set";
        }
    }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(make_nullable(_argument_type));
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        assert(!columns[0]->is_null_at(row_num));
        if constexpr (alloc_memory_in_arena) {
            this->data(place).add(*columns[0], row_num, arena);
        } else {
            this->data(place).add(*columns[0], row_num);
        }
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        if constexpr (alloc_memory_in_arena) {
            this->data(place).merge(this->data(rhs), arena);
        } else {
            this->data(place).merge(this->data(rhs));
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        if (to_nested_col.is_nullable()) {
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            this->data(place).insert_result_into(col_null->get_nested_column());
            col_null->get_null_map_data().resize_fill(col_null->get_nested_column().size(), 0);
        } else {
            this->data(place).insert_result_into(to_nested_col);
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }

    bool allocates_memory_in_arena() const override { return alloc_memory_in_arena; }

private:
    DataTypePtr _argument_type;
};

} // namespace doris::vectorized
