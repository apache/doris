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

#include <limits>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/key_holder_helpers.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/common/arena.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"
#include "vec/io/var_int.h"

namespace doris::vectorized {

//for numric datas
template <typename T>
struct AggregateFunctionGroupUniqArrayData {
    using ElementType = T;
    using ColVecType = ColumnVectorOrDecimal<ElementType>;
    using ElementNativeType = typename NativeType<T>::Type;
    using SelfType = AggregateFunctionGroupUniqArrayData;
    //we use a hashset to filterout duplicated datas
    using Set = HashSetWithStackMemory<ElementNativeType, DefaultHash<ElementNativeType>, 4>;
    Set data_set;
    UInt64 max_size;

    size_t size() const { return data_set.size(); }

    void add(const IColumn& column, size_t row_num) {
        data_set.insert(assert_cast<const ColVecType&>(column).get_data()[row_num]);
    }
    void merge(const SelfType& rhs) { data_set.merge(rhs.data_set); }

    void merge(const SelfType& rhs, bool has_limit) {
        if (!has_limit) {
            merge(rhs);
            return;
        }
        for (auto& rhs_elem : rhs.data_set) {
            if (size() >= max_size) {
                return;
            }
            data_set.insert(rhs_elem.get_value());
        }
    }

    void write(BufferWritable& buf) const { data_set.write(buf); }

    void read(BufferReadable& buf) { data_set.read(buf); }

    void insert_result_into(IColumn& to) const {
        auto& vec = assert_cast<ColVecType&>(to).get_data();
        vec.reserve(size());
        for (auto item : data_set) {
            vec.push_back(item.key);
        }
    }

    void reset() { data_set.clear(); }
};

template <>
struct AggregateFunctionGroupUniqArrayData<StringRef> {
    using ElementType = StringRef;
    using ColVecType = ColumnString;
    using SelfType = AggregateFunctionGroupUniqArrayData<ElementType>;
    using Set = HashSetWithSavedHashWithStackMemory<ElementType, DefaultHash<ElementType>, 4>;
    Set data_set;
    UInt64 max_size;

    size_t size() const { return data_set.size(); }

    void add(const IColumn& column, size_t row_num, Arena* arena) {
        Set::LookupResult it;
        bool inserted;
        auto key_holder = get_key_holder<true>(column, row_num, *arena);
        data_set.emplace(key_holder, it, inserted);
    }

    void merge(const SelfType& rhs, bool has_limit, Arena* arena) {
        bool inserted;
        Set::LookupResult it;
        for (auto& rhs_elem : rhs.data_set) {
            if (has_limit && size() >= max_size) {
                return;
            }
            assert(arena != nullptr);
            data_set.emplace(ArenaKeyHolder {rhs_elem.get_value(), *arena}, it, inserted);
        }
    }

    void write(BufferWritable& buf) const {
        write_var_uint(size(), buf);
        for (const auto& elem : data_set) {
            write_string_binary(elem.get_value(), buf);
        }
    }

    void read(BufferReadable& buf) {
        size_t size;
        read_var_uint(size, buf);
        StringRef ref;
        for (size_t i = 0; i < size; ++i) {
            read_string_binary(ref, buf);
            data_set.insert(ref);
        }
    }

    void insert_result_into(IColumn& to) const {
        auto& vec = assert_cast<ColVecType&>(to);
        vec.reserve(size());
        for (const auto& item : data_set) {
            vec.insert_data(item.key.data, item.key.size);
        }
    }

    void reset() { data_set.clear(); }
};

template <typename T, typename HasLimit>
class AggregateFunctionGroupUniqArray
        : public IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayData<T>,
                                              AggregateFunctionGroupUniqArray<T, HasLimit>> {
    using GenericType = AggregateFunctionGroupUniqArrayData<StringRef>;
    using Data = AggregateFunctionGroupUniqArrayData<T>;

    static constexpr bool HAS_LIMIT = HasLimit::value;
    static constexpr bool ENABLE_ARENA = std::is_same_v<Data, GenericType>;

private:
    DataTypePtr return_type;

public:
    AggregateFunctionGroupUniqArray(const DataTypePtr& argument_type, const Array& parameters_,
                                    UInt64 max_size_ = std::numeric_limits<UInt64>::max())
            : IAggregateFunctionDataHelper<Data, AggregateFunctionGroupUniqArray<T, HasLimit>>(
                      {argument_type}, parameters_),
              return_type(argument_type) {}

    AggregateFunctionGroupUniqArray(const DataTypePtr& argument_type, const Array& parameters_,
                                    const DataTypePtr& return_type_,
                                    UInt64 max_size_ = std::numeric_limits<UInt64>::max())
            : IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayData<T>,
                                           AggregateFunctionGroupUniqArray<T, HasLimit>>(
                      {argument_type}, parameters_),
              return_type(return_type_) {}

    std::string get_name() const override { return "group_uniq_array"; }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(make_nullable(return_type));
    }

    bool allocates_memory_in_arena() const override { return ENABLE_ARENA; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        auto& data = this->data(place);
        if constexpr (HAS_LIMIT) {
            data.max_size =
                    (UInt64) static_cast<const ColumnInt32*>(columns[1])->get_element(row_num);
            if (data.size() >= data.max_size) {
                return;
            }
        }
        if constexpr (ENABLE_ARENA) {
            data.add(*columns[0], row_num, arena);
        } else {
            data.add(*columns[0], row_num);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        auto& data = this->data(place);
        auto& rhs_data = this->data(rhs);
        if constexpr (ENABLE_ARENA) {
            data.merge(rhs_data, HAS_LIMIT, arena);
        } else {
            data.merge(rhs_data, HAS_LIMIT);
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
};
} // namespace doris::vectorized