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

#include <assert.h>
#include <glog/logging.h>
#include <string.h>

#include <limits>
#include <memory>
#include <new>
#include <string>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/key_holder_helpers.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/hash_table/hash_table_key_holder.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/io/io_helper.h"
#include "vec/io/var_int.h"

namespace doris {
namespace vectorized {
class Arena;
} // namespace vectorized
} // namespace doris
template <typename, typename>
struct DefaultHash;

namespace doris::vectorized {

template <typename T, typename HasLimit>
struct AggregateFunctionCollectSetData {
    using ElementType = T;
    using ColVecType = ColumnVectorOrDecimal<ElementType>;
    using ElementNativeType = typename NativeType<T>::Type;
    using SelfType = AggregateFunctionCollectSetData;
    using Set = HashSetWithStackMemory<ElementNativeType, DefaultHash<ElementNativeType>, 4>;
    Set data_set;
    Int64 max_size = -1;

    size_t size() const { return data_set.size(); }

    void add(const IColumn& column, size_t row_num) {
        data_set.insert(assert_cast<const ColVecType&>(column).get_data()[row_num]);
    }

    void merge(const SelfType& rhs) {
        if constexpr (HasLimit::value) {
            if (max_size == -1) {
                max_size = rhs.max_size;
            }

            for (auto& rhs_elem : rhs.data_set) {
                if (size() >= max_size) {
                    return;
                }
                data_set.insert(rhs_elem.get_value());
            }
        } else {
            data_set.merge(rhs.data_set);
        }
    }

    void write(BufferWritable& buf) const {
        data_set.write(buf);
        write_var_int(max_size, buf);
    }

    void read(BufferReadable& buf) {
        data_set.read(buf);
        read_var_int(max_size, buf);
    }

    void insert_result_into(IColumn& to) const {
        auto& vec = assert_cast<ColVecType&>(to).get_data();
        vec.reserve(size());
        for (const auto& item : data_set) {
            vec.push_back(item.key);
        }
    }

    void reset() { data_set.clear(); }
};

template <typename HasLimit>
struct AggregateFunctionCollectSetData<StringRef, HasLimit> {
    using ElementType = StringRef;
    using ColVecType = ColumnString;
    using SelfType = AggregateFunctionCollectSetData<ElementType, HasLimit>;
    using Set = HashSetWithSavedHashWithStackMemory<ElementType, DefaultHash<ElementType>, 4>;
    Set data_set;
    Int64 max_size = -1;

    size_t size() const { return data_set.size(); }

    void add(const IColumn& column, size_t row_num, Arena* arena) {
        Set::LookupResult it;
        bool inserted;
        auto key_holder = get_key_holder<true>(column, row_num, *arena);
        data_set.emplace(key_holder, it, inserted);
    }

    void merge(const SelfType& rhs, Arena* arena) {
        bool inserted;
        Set::LookupResult it;
        if (max_size == -1) {
            max_size = rhs.max_size;
        }
        max_size = rhs.max_size;

        for (auto& rhs_elem : rhs.data_set) {
            if constexpr (HasLimit::value) {
                if (size() >= max_size) {
                    return;
                }
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
        write_var_int(max_size, buf);
    }

    void read(BufferReadable& buf) {
        UInt64 size;
        read_var_uint(size, buf);
        StringRef ref;
        for (size_t i = 0; i < size; ++i) {
            read_string_binary(ref, buf);
            data_set.insert(ref);
        }
        read_var_int(max_size, buf);
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
struct AggregateFunctionCollectListData {
    using ElementType = T;
    using ColVecType = ColumnVectorOrDecimal<ElementType>;
    using SelfType = AggregateFunctionCollectListData<ElementType, HasLimit>;
    PaddedPODArray<ElementType> data;
    Int64 max_size = -1;

    size_t size() const { return data.size(); }

    void add(const IColumn& column, size_t row_num) {
        const auto& vec = assert_cast<const ColVecType&>(column).get_data();
        data.push_back(vec[row_num]);
    }

    void merge(const SelfType& rhs) {
        if constexpr (HasLimit::value) {
            if (max_size == -1) {
                max_size = rhs.max_size;
            }
            max_size = rhs.max_size;
            for (auto& rhs_elem : rhs.data) {
                if (size() >= max_size) {
                    return;
                }
                data.push_back(rhs_elem);
            }
        } else {
            data.insert(rhs.data.begin(), rhs.data.end());
        }
    }

    void write(BufferWritable& buf) const {
        write_var_uint(size(), buf);
        buf.write(data.raw_data(), size() * sizeof(ElementType));
        write_var_int(max_size, buf);
    }

    void read(BufferReadable& buf) {
        UInt64 rows = 0;
        read_var_uint(rows, buf);
        data.resize(rows);
        buf.read(reinterpret_cast<char*>(data.data()), rows * sizeof(ElementType));
        read_var_int(max_size, buf);
    }

    void reset() { data.clear(); }

    void insert_result_into(IColumn& to) const {
        auto& vec = assert_cast<ColVecType&>(to).get_data();
        size_t old_size = vec.size();
        vec.resize(old_size + size());
        memcpy(vec.data() + old_size, data.data(), size() * sizeof(ElementType));
    }
};

template <typename HasLimit>
struct AggregateFunctionCollectListData<StringRef, HasLimit> {
    using ElementType = StringRef;
    using ColVecType = ColumnString;
    MutableColumnPtr data;
    Int64 max_size = -1;

    AggregateFunctionCollectListData() { data = ColVecType::create(); }

    size_t size() const { return data->size(); }

    void add(const IColumn& column, size_t row_num) { data->insert_from(column, row_num); }

    void merge(const AggregateFunctionCollectListData& rhs) {
        if constexpr (HasLimit::value) {
            if (max_size == -1) {
                max_size = rhs.max_size;
            }
            max_size = rhs.max_size;

            data->insert_range_from(*rhs.data, 0,
                                    std::min(assert_cast<size_t>(max_size - size()), rhs.size()));
        } else {
            data->insert_range_from(*rhs.data, 0, rhs.size());
        }
    }

    void write(BufferWritable& buf) const {
        auto& col = assert_cast<ColVecType&>(*data);

        write_var_uint(col.size(), buf);
        buf.write(col.get_offsets().raw_data(), col.size() * sizeof(IColumn::Offset));

        write_var_uint(col.get_chars().size(), buf);
        buf.write(col.get_chars().raw_data(), col.get_chars().size());
        write_var_int(max_size, buf);
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
        read_var_int(max_size, buf);
    }

    void reset() { data->clear(); }

    void insert_result_into(IColumn& to) const {
        auto& to_str = assert_cast<ColVecType&>(to);
        to_str.insert_range_from(*data, 0, size());
    }
};

template <typename Data, typename HasLimit>
class AggregateFunctionCollect
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionCollect<Data, HasLimit>> {
    using GenericType = AggregateFunctionCollectSetData<StringRef, HasLimit>;

    static constexpr bool ENABLE_ARENA = std::is_same_v<Data, GenericType>;

public:
    AggregateFunctionCollect(const DataTypes& argument_types,
                             UInt64 max_size_ = std::numeric_limits<UInt64>::max())
            : IAggregateFunctionDataHelper<Data, AggregateFunctionCollect<Data, HasLimit>>(
                      {argument_types}),
              return_type(argument_types[0]) {}

    std::string get_name() const override {
        if constexpr (std::is_same_v<AggregateFunctionCollectListData<typename Data::ElementType,
                                                                      HasLimit>,
                                     Data>) {
            return "collect_list";
        } else {
            return "collect_set";
        }
    }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(make_nullable(return_type));
    }

    bool allocates_memory_in_arena() const override { return ENABLE_ARENA; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        auto& data = this->data(place);
        if constexpr (HasLimit::value) {
            if (data.max_size == -1) {
                data.max_size =
                        (UInt64)assert_cast<const ColumnInt32*>(columns[1])->get_element(row_num);
            }
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
            data.merge(rhs_data, arena);
        } else {
            data.merge(rhs_data);
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

private:
    DataTypePtr return_type;
};

} // namespace doris::vectorized