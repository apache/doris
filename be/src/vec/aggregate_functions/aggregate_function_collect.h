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
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/hash_set.h"
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
    using Set = HashSetWithStackMemory<ElementType, DefaultHash<ElementType>, 4>;
    Set data_set;
    Int64 max_size = -1;

    size_t size() const { return data_set.size(); }

    void add(const IColumn& column, size_t row_num, Arena* arena) {
        Set::LookupResult it;
        bool inserted;
        auto key = column.get_data_at(row_num);
        key.data = arena->insert(key.data, key.size);
        data_set.emplace(key, it, inserted);
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
            StringRef key = rhs_elem.get_value();
            key.data = arena->insert(key.data, key.size);
            data_set.emplace(key, it, inserted);
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

template <typename T>
struct AggregateFunctionArrayAggData {
    using ElementType = T;
    using ColVecType = ColumnVectorOrDecimal<ElementType>;
    MutableColumnPtr column_data;
    ColVecType* nested_column;
    NullMap* null_map;

    AggregateFunctionArrayAggData(const DataTypes& argument_types) {
        if constexpr (IsDecimalNumber<T>) {
            DataTypePtr column_type = make_nullable(argument_types[0]);
            column_data = column_type->create_column();
            null_map = &(assert_cast<ColumnNullable&>(*column_data).get_null_map_data());
            nested_column = assert_cast<ColVecType*>(
                    assert_cast<ColumnNullable&>(*column_data).get_nested_column_ptr().get());
        }
    }

    AggregateFunctionArrayAggData() {
        if constexpr (!IsDecimalNumber<T>) {
            column_data = ColumnNullable::create(ColVecType::create(), ColumnUInt8::create());
            null_map = &(assert_cast<ColumnNullable&>(*column_data).get_null_map_data());
            nested_column = assert_cast<ColVecType*>(
                    assert_cast<ColumnNullable&>(*column_data).get_nested_column_ptr().get());
        }
    }

    void add(const IColumn& column, size_t row_num) {
        const auto& col = assert_cast<const ColumnNullable&>(column);
        const auto& vec = assert_cast<const ColVecType&>(col.get_nested_column()).get_data();
        null_map->push_back(col.get_null_map_data()[row_num]);
        nested_column->get_data().push_back(vec[row_num]);
        DCHECK(null_map->size() == nested_column->size());
    }

    void deserialize_and_merge(const IColumn& column, size_t row_num) {
        auto& to_arr = assert_cast<const ColumnArray&>(column);
        auto& to_nested_col = to_arr.get_data();
        auto col_null = reinterpret_cast<const ColumnNullable*>(&to_nested_col);
        const auto& vec = assert_cast<const ColVecType&>(col_null->get_nested_column()).get_data();
        auto start = to_arr.get_offsets()[row_num - 1];
        auto end = start + to_arr.get_offsets()[row_num] - to_arr.get_offsets()[row_num - 1];
        for (auto i = start; i < end; ++i) {
            null_map->push_back(col_null->get_null_map_data()[i]);
            nested_column->get_data().push_back(vec[i]);
        }
    }

    void reset() {
        null_map->clear();
        nested_column->clear();
    }

    void insert_result_into(IColumn& to) const {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
        auto& vec = assert_cast<ColVecType&>(col_null->get_nested_column()).get_data();
        size_t num_rows = null_map->size();
        auto& nested_column_data = nested_column->get_data();
        for (size_t i = 0; i < num_rows; ++i) {
            col_null->get_null_map_data().push_back((*null_map)[i]);
            vec.push_back(nested_column_data[i]);
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }
};

template <>
struct AggregateFunctionArrayAggData<StringRef> {
    using ElementType = StringRef;
    using ColVecType = ColumnString;
    MutableColumnPtr column_data;
    ColVecType* nested_column;
    NullMap* null_map;

    AggregateFunctionArrayAggData() {
        column_data = ColumnNullable::create(ColVecType::create(), ColumnUInt8::create());
        null_map = &(assert_cast<ColumnNullable&>(*column_data).get_null_map_data());
        nested_column = assert_cast<ColVecType*>(
                assert_cast<ColumnNullable&>(*column_data).get_nested_column_ptr().get());
    }

    void add(const IColumn& column, size_t row_num) {
        const auto& col = assert_cast<const ColumnNullable&>(column);
        const auto& vec = assert_cast<const ColVecType&>(col.get_nested_column());
        null_map->push_back(col.get_null_map_data()[row_num]);
        nested_column->insert_from(vec, row_num);
        DCHECK(null_map->size() == nested_column->size());
    }

    void deserialize_and_merge(const IColumn& column, size_t row_num) {
        auto& to_arr = assert_cast<const ColumnArray&>(column);
        auto& to_nested_col = to_arr.get_data();
        auto col_null = reinterpret_cast<const ColumnNullable*>(&to_nested_col);
        const auto& vec = assert_cast<const ColVecType&>(col_null->get_nested_column());
        auto start = to_arr.get_offsets()[row_num - 1];
        auto end = start + to_arr.get_offsets()[row_num] - to_arr.get_offsets()[row_num - 1];
        for (auto i = start; i < end; ++i) {
            null_map->push_back(col_null->get_null_map_data()[i]);
            nested_column->insert_from(vec, i);
        }
    }

    void reset() {
        null_map->clear();
        nested_column->clear();
    }

    void insert_result_into(IColumn& to) const {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
        auto& vec = assert_cast<ColVecType&>(col_null->get_nested_column());
        size_t num_rows = null_map->size();
        for (size_t i = 0; i < num_rows; ++i) {
            col_null->get_null_map_data().push_back((*null_map)[i]);
            vec.insert_from(*nested_column, i);
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }
};

//ShowNull is just used to support array_agg because array_agg needs to display NULL
//todo: Supports order by sorting for array_agg
template <typename Data, typename HasLimit, typename ShowNull>
class AggregateFunctionCollect
        : public IAggregateFunctionDataHelper<Data,
                                              AggregateFunctionCollect<Data, HasLimit, ShowNull>> {
    using GenericType = AggregateFunctionCollectSetData<StringRef, HasLimit>;

    static constexpr bool ENABLE_ARENA = std::is_same_v<Data, GenericType>;

public:
    using BaseHelper = IAggregateFunctionHelper<AggregateFunctionCollect<Data, HasLimit, ShowNull>>;

    AggregateFunctionCollect(const DataTypes& argument_types_,
                             UInt64 max_size_ = std::numeric_limits<UInt64>::max())
            : IAggregateFunctionDataHelper<Data,
                                           AggregateFunctionCollect<Data, HasLimit, ShowNull>>(
                      {argument_types_}),
              return_type(argument_types_[0]) {}

    std::string get_name() const override {
        if constexpr (ShowNull::value) {
            return "array_agg";
        } else if constexpr (std::is_same_v<AggregateFunctionCollectListData<
                                                    typename Data::ElementType, HasLimit>,
                                            Data>) {
            return "collect_list";
        } else {
            return "collect_set";
        }
    }

    void create(AggregateDataPtr __restrict place) const override {
        if constexpr (ShowNull::value) {
            if constexpr (IsDecimalNumber<typename Data::ElementType>) {
                new (place) Data(argument_types);
            } else {
                new (place) Data();
            }
        } else {
            new (place) Data();
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
        } else if constexpr (!ShowNull::value) {
            data.merge(rhs_data);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        if constexpr (!ShowNull::value) {
            this->data(place).write(buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        if constexpr (!ShowNull::value) {
            this->data(place).read(buf);
        }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        if constexpr (ShowNull::value) {
            DCHECK(to_nested_col.is_nullable());
            this->data(place).insert_result_into(to);
        } else {
            if (to_nested_col.is_nullable()) {
                auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
                this->data(place).insert_result_into(col_null->get_nested_column());
                col_null->get_null_map_data().resize_fill(col_null->get_nested_column().size(), 0);
            } else {
                this->data(place).insert_result_into(to_nested_col);
            }
            to_arr.get_offsets().push_back(to_nested_col.size());
        }
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        if constexpr (ShowNull::value) {
            this->data(place).insert_result_into(to);
        } else {
            if (version >= 4) {
                auto& to_arr = assert_cast<ColumnArray&>(to);
                auto& to_data_col =
                        assert_cast<ColumnNullable&>(to_arr.get_data()).get_nested_column();
                auto& to_offsets = to_arr.get_offsets();
                this->data(place).insert_result_into(to_data_col);
                assert_cast<ColumnNullable&>(to_arr.get_data())
                        .get_null_map_data()
                        .resize_fill(to_data_col.size(), 0);
                to_offsets.push_back(to_data_col.size());
                return;
            }
            return BaseHelper::serialize_without_key_to_column(place, to);
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        if constexpr (ShowNull::value) {
            const size_t num_rows = column.size();
            for (size_t i = 0; i != num_rows; ++i) {
                this->data(place).deserialize_and_merge(column, i);
            }
        } else {
            if (version >= 4) {
                const auto& src_column = assert_cast<const ColumnArray&>(column);
                const auto& src_data_col = assert_cast<const ColumnNullable&>(src_column.get_data())
                                                   .get_nested_column();
                const auto& offsets = src_column.get_offsets();
                const auto rows = offsets.size();
                for (size_t i = 0; i != rows; ++i) {
                    auto off_start = offsets[i - 1];
                    auto off_end = offsets[i];
                    for (auto off = off_start; off != off_end; ++off) {
                        if constexpr (ENABLE_ARENA) {
                            this->data(place).add(src_data_col, off, arena);
                        } else {
                            this->data(place).add(src_data_col, off);
                        }
                    }
                }
                return;
            }
            return BaseHelper::deserialize_and_merge_from_column(place, column, arena);
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const ColumnString* column, Arena* arena,
                                   const size_t num_rows) const override {
        if constexpr (ShowNull::value) {
            for (size_t i = 0; i != num_rows; ++i) {
                this->data(places[i]).deserialize_and_merge(*assert_cast<const IColumn*>(column),
                                                            i);
            }
        } else {
            if (version >= 4) {
                const auto& src_column =
                        assert_cast<const ColumnArray&>(*assert_cast<const IColumn*>(column));
                const auto& src_data_col = assert_cast<const ColumnNullable&>(src_column.get_data())
                                                   .get_nested_column();
                const auto& offsets = src_column.get_offsets();
                for (size_t i = 0; i != num_rows; ++i) {
                    auto off_start = offsets[i - 1];
                    auto off_end = offsets[i];
                    for (auto off = off_start; off != off_end; ++off) {
                        if constexpr (ENABLE_ARENA) {
                            this->data(places[i]).add(src_data_col, off, arena);
                        } else {
                            this->data(places[i]).add(src_data_col, off);
                        }
                    }
                }
                return;
            }
            return BaseHelper::deserialize_and_merge_vec(places, offset, rhs, column, arena,
                                                         num_rows);
        }
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena* arena,
                                 size_t num_rows) const override {
        if constexpr (ShowNull::value) {
            for (size_t i = 0; i != num_rows; ++i) {
                this->data(places).deserialize_and_merge(column, i);
            }
        } else {
            if (version >= 4) {
                const auto& src_column = assert_cast<const ColumnArray&>(column);
                const auto& src_data_col = assert_cast<const ColumnNullable&>(src_column.get_data())
                                                   .get_nested_column();
                const auto& offsets = src_column.get_offsets();
                for (size_t i = 0; i != num_rows; ++i) {
                    auto off_start = offsets[i - 1];
                    auto off_end = offsets[i];
                    for (auto off = off_start; off != off_end; ++off) {
                        if constexpr (ENABLE_ARENA) {
                            this->data(places).add(src_data_col, off, arena);
                        } else {
                            this->data(places).add(src_data_col, off);
                        }
                    }
                }
                return;
            }
            return BaseHelper::deserialize_from_column(places, column, arena, num_rows);
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena* arena) const override {
        if constexpr (ShowNull::value) {
            DCHECK(end <= column.size() && begin <= end) << ", begin:" << begin << ", end:" << end
                                                         << ", column.size():" << column.size();
            for (size_t i = begin; i <= end; ++i) {
                this->data(place).deserialize_and_merge(column, i);
            }
        } else {
            if (version >= 4) {
                const auto& src_column = assert_cast<const ColumnArray&>(column);
                const auto& src_data_col = assert_cast<const ColumnNullable&>(src_column.get_data())
                                                   .get_nested_column();
                const auto& offsets = src_column.get_offsets();
                for (size_t i = begin; i <= end; ++i) {
                    auto off_start = offsets[i - 1];
                    auto off_end = offsets[i];
                    for (auto off = off_start; off != off_end; ++off) {
                        if constexpr (ENABLE_ARENA) {
                            this->data(place).add(src_data_col, off, arena);
                        } else {
                            this->data(place).add(src_data_col, off);
                        }
                    }
                }
                return;
            }
            return BaseHelper::deserialize_and_merge_from_column_range(place, column, begin, end,
                                                                       arena);
        }
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const ColumnString* column,
                                            Arena* arena, const size_t num_rows) const override {
        if constexpr (ShowNull::value) {
            for (size_t i = 0; i != num_rows; ++i) {
                if (places[i]) {
                    this->data(places[i]).deserialize_and_merge(
                            *assert_cast<const IColumn*>(column), i);
                }
            }
        } else {
            if (version >= 4) {
                const auto& src_column =
                        assert_cast<const ColumnArray&>(*assert_cast<const IColumn*>(column));
                const auto& src_data_col = assert_cast<const ColumnNullable&>(src_column.get_data())
                                                   .get_nested_column();
                const auto& offsets = src_column.get_offsets();
                for (size_t i = 0; i != num_rows; ++i) {
                    auto off_start = offsets[i - 1];
                    auto off_end = offsets[i];
                    for (auto off = off_start; off != off_end; ++off) {
                        if constexpr (ENABLE_ARENA) {
                            this->data(places[i]).add(src_data_col, off, arena);
                        } else {
                            this->data(places[i]).add(src_data_col, off);
                        }
                    }
                }
                return;
            }
            return BaseHelper::deserialize_and_merge_vec_selected(places, offset, rhs, column,
                                                                  arena, num_rows);
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        if constexpr (ShowNull::value) {
            for (size_t i = 0; i != num_rows; ++i) {
                Data& data_ = this->data(places[i] + offset);
                data_.insert_result_into(*dst);
            }
        } else {
            if (version >= 4) {
                auto& to_arr = assert_cast<ColumnArray&>(*dst);
                auto& to_data_col =
                        assert_cast<ColumnNullable&>(to_arr.get_data()).get_nested_column();
                auto& to_offsets = to_arr.get_offsets();

                for (size_t i = 0; i != num_rows; ++i) {
                    Data& data_ = this->data(places[i] + offset);
                    data_.insert_result_into(to_data_col);
                    to_offsets.push_back(to_data_col.size());
                }
                assert_cast<ColumnNullable&>(to_arr.get_data())
                        .get_null_map_data()
                        .resize_fill(to_data_col.size(), 0);
                return;
            }
            return BaseHelper::serialize_to_column(places, offset, dst, num_rows);
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena* arena) const override {
        if constexpr (ShowNull::value) {
            auto& to_arr = assert_cast<ColumnArray&>(*dst);
            auto& to_nested_col = to_arr.get_data();
            DCHECK(num_rows == columns[0]->size());
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            const auto& col_src = assert_cast<const ColumnNullable&>(*(columns[0]));

            for (size_t i = 0; i < num_rows; ++i) {
                col_null->get_null_map_data().push_back(col_src.get_null_map_data()[i]);
                if constexpr (std::is_same_v<StringRef, typename Data::ElementType>) {
                    auto& vec = assert_cast<ColumnString&>(col_null->get_nested_column());
                    const auto& vec_src =
                            assert_cast<const ColumnString&>(col_src.get_nested_column());
                    vec.insert_from(vec_src, i);
                } else {
                    using ColVecType = ColumnVectorOrDecimal<typename Data::ElementType>;
                    auto& vec = assert_cast<ColVecType&>(col_null->get_nested_column()).get_data();
                    auto& vec_src =
                            assert_cast<const ColVecType&>(col_src.get_nested_column()).get_data();
                    vec.push_back(vec_src[i]);
                }
                to_arr.get_offsets().push_back(to_nested_col.size());
            }
        } else {
            if (version >= 4) {
                auto& to_arr = assert_cast<ColumnArray&>(*dst);
                auto& to_offsets = to_arr.get_offsets();
                auto& to_data_col =
                        assert_cast<ColumnNullable&>(to_arr.get_data()).get_nested_column();
                const auto old_rows = to_data_col.size();
                const auto& src_column = *columns[0];
                if (src_column.is_nullable()) {
                    auto& src_nullable = assert_cast<const ColumnNullable&>(src_column);
                    auto& src_data_column = src_nullable.get_nested_column();

                    size_t non_null_rows = 0;
                    for (size_t i = 0; i != num_rows; ++i) {
                        if (src_nullable.is_null_at(i)) {
                            continue;
                        }
                        ++non_null_rows;

                        Data data;

                        if constexpr (ENABLE_ARENA) {
                            data.add(src_data_column, i, arena);
                        } else {
                            data.add(src_data_column, i);
                        }

                        data.insert_result_into(to_data_col);
                        to_offsets.push_back(to_offsets.back() + 1);
                    }

                    if (LIKELY(non_null_rows > 0)) {
                        assert_cast<ColumnNullable&>(to_arr.get_data())
                                .get_null_map_data()
                                .resize_fill(old_rows + non_null_rows, 0);
                    }
                } else {
                    for (size_t i = 0; i != num_rows; ++i) {
                        Data data;

                        if constexpr (ENABLE_ARENA) {
                            data.add(src_column, i, arena);
                        } else {
                            data.add(src_column, i);
                        }

                        data.insert_result_into(to_data_col);
                        to_offsets.push_back(to_offsets.back() + 1);
                    }
                    assert_cast<ColumnNullable&>(to_arr.get_data())
                            .get_null_map_data()
                            .resize_fill(old_rows + num_rows, 0);
                }
                return;
            }
            return BaseHelper::streaming_agg_serialize_to_column(columns, dst, num_rows, arena);
        }
    }

    [[nodiscard]] MutableColumnPtr create_serialize_column() const override {
        if constexpr (ShowNull::value) {
            return get_return_type()->create_column();
        } else {
            if (version >= 4) {
                return get_return_type()->create_column();
            }
            return ColumnString::create();
        }
    }

    [[nodiscard]] DataTypePtr get_serialized_type() const override {
        if constexpr (ShowNull::value) {
            return std::make_shared<DataTypeArray>(make_nullable(return_type));
        } else {
            if (version >= 4) {
                return std::make_shared<DataTypeArray>(make_nullable(return_type));
            }
            return IAggregateFunction::get_serialized_type();
        }
    }

private:
    DataTypePtr return_type;
    using IAggregateFunction::argument_types;
    using IAggregateFunction::version;
};

} // namespace doris::vectorized