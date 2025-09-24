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

#include <cstddef>
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
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/io/io_helper.h"
#include "vec/io/var_int.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
template <PrimitiveType T, bool HasLimit>
struct AggregateFunctionCollectSetData {
    static constexpr PrimitiveType PType = T;
    using ElementType = typename PrimitiveTypeTraits<T>::ColumnItemType;
    using ColVecType = typename PrimitiveTypeTraits<T>::ColumnType;
    using SelfType = AggregateFunctionCollectSetData;
    using Set = phmap::flat_hash_set<ElementType>;
    Set data_set;
    Int64 max_size = -1;

    AggregateFunctionCollectSetData(const DataTypes& argument_types) {}

    size_t size() const { return data_set.size(); }

    void add(const IColumn& column, size_t row_num) {
        data_set.insert(assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(column)
                                .get_data()[row_num]);
    }

    void merge(const SelfType& rhs) {
        if constexpr (HasLimit) {
            if (max_size == -1) {
                max_size = rhs.max_size;
            }

            for (auto& rhs_elem : rhs.data_set) {
                if (size() >= max_size) {
                    return;
                }
                data_set.insert(rhs_elem);
            }
        } else {
            data_set.merge(Set(rhs.data_set));
        }
    }

    void write(BufferWritable& buf) const {
        buf.write_var_uint(data_set.size());
        for (const auto& value : data_set) {
            buf.write_binary(value);
        }
        write_var_int(max_size, buf);
    }

    void read(BufferReadable& buf) {
        uint64_t new_size = 0;
        buf.read_var_uint(new_size);
        ElementType x;
        for (size_t i = 0; i < new_size; ++i) {
            buf.read_binary(x);
            data_set.insert(x);
        }
        read_var_int(max_size, buf);
    }

    void insert_result_into(IColumn& to) const {
        auto& vec = assert_cast<ColVecType&>(to).get_data();
        vec.reserve(size());
        for (const auto& item : data_set) {
            vec.push_back(item);
        }
    }

    void reset() { data_set.clear(); }
};

template <PrimitiveType T, bool HasLimit>
    requires(is_string_type(T))
struct AggregateFunctionCollectSetData<T, HasLimit> {
    static constexpr PrimitiveType PType = T;
    using ElementType = StringRef;
    using ColVecType = ColumnString;
    using SelfType = AggregateFunctionCollectSetData<T, HasLimit>;
    using Set = phmap::flat_hash_set<ElementType>;
    Set data_set;
    Int64 max_size = -1;

    AggregateFunctionCollectSetData(const DataTypes& argument_types) {}

    size_t size() const { return data_set.size(); }

    void add(const IColumn& column, size_t row_num, Arena& arena) {
        auto key = column.get_data_at(row_num);
        key.data = arena.insert(key.data, key.size);
        data_set.insert(key);
    }

    void merge(const SelfType& rhs, Arena& arena) {
        if (max_size == -1) {
            max_size = rhs.max_size;
        }
        max_size = rhs.max_size;

        for (const auto& rhs_elem : rhs.data_set) {
            if constexpr (HasLimit) {
                if (size() >= max_size) {
                    return;
                }
            }
            StringRef key = rhs_elem;
            key.data = arena.insert(key.data, key.size);
            data_set.insert(key);
        }
    }

    void write(BufferWritable& buf) const {
        buf.write_var_uint(size());
        for (const auto& elem : data_set) {
            buf.write_binary(elem);
        }
        write_var_int(max_size, buf);
    }

    void read(BufferReadable& buf) {
        UInt64 size;
        buf.read_var_uint(size);
        StringRef ref;
        for (size_t i = 0; i < size; ++i) {
            buf.read_binary(ref);
            data_set.insert(ref);
        }
        read_var_int(max_size, buf);
    }

    void insert_result_into(IColumn& to) const {
        auto& vec = assert_cast<ColVecType&>(to);
        vec.reserve(size());
        for (const auto& item : data_set) {
            vec.insert_data(item.data, item.size);
        }
    }

    void reset() { data_set.clear(); }
};

template <PrimitiveType T, bool HasLimit>
struct AggregateFunctionCollectListData {
    static constexpr PrimitiveType PType = T;
    using ElementType = typename PrimitiveTypeTraits<T>::ColumnItemType;
    using ColVecType = typename PrimitiveTypeTraits<T>::ColumnType;
    using SelfType = AggregateFunctionCollectListData<T, HasLimit>;
    PaddedPODArray<ElementType> data;
    Int64 max_size = -1;

    AggregateFunctionCollectListData(const DataTypes& argument_types) {}

    size_t size() const { return data.size(); }

    void add(const IColumn& column, size_t row_num) {
        const auto& vec =
                assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(column).get_data();
        data.push_back(vec[row_num]);
    }

    void merge(const SelfType& rhs) {
        if constexpr (HasLimit) {
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
        buf.write_var_uint(size());
        buf.write(data.raw_data(), size() * sizeof(ElementType));
        write_var_int(max_size, buf);
    }

    void read(BufferReadable& buf) {
        UInt64 rows = 0;
        buf.read_var_uint(rows);
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

template <PrimitiveType T, bool HasLimit>
    requires(is_string_type(T))
struct AggregateFunctionCollectListData<T, HasLimit> {
    static constexpr PrimitiveType PType = T;
    using ElementType = StringRef;
    using ColVecType = ColumnString;
    MutableColumnPtr data;
    Int64 max_size = -1;

    AggregateFunctionCollectListData(const DataTypes& argument_types) {
        data = ColVecType::create();
    }

    size_t size() const { return data->size(); }

    void add(const IColumn& column, size_t row_num) { data->insert_from(column, row_num); }

    void merge(const AggregateFunctionCollectListData& rhs) {
        if constexpr (HasLimit) {
            if (max_size == -1) {
                max_size = rhs.max_size;
            }
            max_size = rhs.max_size;

            data->insert_range_from(*rhs.data, 0,
                                    std::min(assert_cast<size_t, TypeCheckOnRelease::DISABLE>(
                                                     static_cast<size_t>(max_size - size())),
                                             rhs.size()));
        } else {
            data->insert_range_from(*rhs.data, 0, rhs.size());
        }
    }

    void write(BufferWritable& buf) const {
        auto& col = assert_cast<ColVecType&>(*data);

        buf.write_var_uint(col.size());
        buf.write(col.get_offsets().raw_data(), col.size() * sizeof(IColumn::Offset));

        buf.write_var_uint(col.get_chars().size());
        buf.write(col.get_chars().raw_data(), col.get_chars().size());
        write_var_int(max_size, buf);
    }

    void read(BufferReadable& buf) {
        auto& col = assert_cast<ColVecType&>(*data);
        UInt64 offs_size = 0;
        buf.read_var_uint(offs_size);
        col.get_offsets().resize(offs_size);
        buf.read(reinterpret_cast<char*>(col.get_offsets().data()),
                 offs_size * sizeof(IColumn::Offset));

        UInt64 chars_size = 0;
        buf.read_var_uint(chars_size);
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

template <PrimitiveType T, bool HasLimit>
    requires(!is_string_type(T) && !is_int_or_bool(T) && !is_float_or_double(T) && !is_decimal(T) &&
             !is_date_type(T) && !is_ip(T))
struct AggregateFunctionCollectListData<T, HasLimit> {
    static constexpr PrimitiveType PType = T;
    using ElementType = StringRef;
    using Self = AggregateFunctionCollectListData<T, HasLimit>;
    DataTypeSerDeSPtr serde; // for complex serialize && deserialize from multi BE
    MutableColumnPtr column_data;
    Int64 max_size = -1;

    AggregateFunctionCollectListData(const DataTypes& argument_types) {
        DataTypePtr column_type = argument_types[0];
        column_data = column_type->create_column();
        serde = column_type->get_serde();
    }

    size_t size() const { return column_data->size(); }

    void add(const IColumn& column, size_t row_num) { column_data->insert_from(column, row_num); }

    void merge(const AggregateFunctionCollectListData& rhs) {
        if constexpr (HasLimit) {
            if (max_size == -1) {
                max_size = rhs.max_size;
            }
            max_size = rhs.max_size;

            column_data->insert_range_from(
                    *rhs.column_data, 0,
                    std::min(assert_cast<size_t, TypeCheckOnRelease::DISABLE>(
                                     static_cast<size_t>(max_size - size())),
                             rhs.size()));
        } else {
            column_data->insert_range_from(*rhs.column_data, 0, rhs.size());
        }
    }

    void write(BufferWritable& buf) const {
        const size_t size = column_data->size();
        buf.write_binary(size);

        DataTypeSerDe::FormatOptions opt;
        auto tmp_str = ColumnString::create();
        VectorBufferWriter tmp_buf(*tmp_str.get());

        for (size_t i = 0; i < size; i++) {
            tmp_str->clear();
            if (Status st = serde->serialize_one_cell_to_json(*column_data, i, tmp_buf, opt); !st) {
                throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                       "Failed to serialize data for " + column_data->get_name() +
                                               " error: " + st.to_string());
            }
            tmp_buf.commit();
            buf.write_binary(tmp_str->get_data_at(0));
        }

        write_var_int(max_size, buf);
    }

    void read(BufferReadable& buf) {
        size_t size = 0;
        buf.read_binary(size);
        column_data->clear();
        column_data->reserve(size);

        StringRef s;
        DataTypeSerDe::FormatOptions opt;
        for (size_t i = 0; i < size; i++) {
            buf.read_binary(s);
            Slice slice(s.data, s.size);
            if (Status st = serde->deserialize_one_cell_from_json(*column_data, slice, opt); !st) {
                throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                       "Failed to deserialize data for " + column_data->get_name() +
                                               " error: " + st.to_string());
            }
        }
        read_var_int(max_size, buf);
    }

    void reset() { column_data->clear(); }

    void insert_result_into(IColumn& to) const { to.insert_range_from(*column_data, 0, size()); }
};

template <typename Data, bool HasLimit>
class AggregateFunctionCollect
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionCollect<Data, HasLimit>, true>,
          VarargsExpression,
          NotNullableAggregateFunction {
    static constexpr bool ENABLE_ARENA =
            std::is_same_v<Data, AggregateFunctionCollectSetData<TYPE_STRING, HasLimit>> ||
            std::is_same_v<Data, AggregateFunctionCollectSetData<TYPE_CHAR, HasLimit>> ||
            std::is_same_v<Data, AggregateFunctionCollectSetData<TYPE_VARCHAR, HasLimit>>;

public:
    AggregateFunctionCollect(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionCollect<Data, HasLimit>, true>(
                      {argument_types_}),
              return_type(std::make_shared<DataTypeArray>(make_nullable(argument_types_[0]))) {}

    std::string get_name() const override {
        if constexpr (std::is_same_v<AggregateFunctionCollectListData<Data::PType, HasLimit>,
                                     Data>) {
            return "collect_list";
        } else {
            return "collect_set";
        }
    }

    DataTypePtr get_return_type() const override { return return_type; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        auto& data = this->data(place);
        if constexpr (HasLimit) {
            if (data.max_size == -1) {
                data.max_size =
                        (UInt64)assert_cast<const ColumnInt32*, TypeCheckOnRelease::DISABLE>(
                                columns[1])
                                ->get_element(row_num);
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
               Arena& arena) const override {
        auto& data = this->data(place);
        const auto& rhs_data = this->data(rhs);
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
                     Arena&) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        auto* col_null = assert_cast<ColumnNullable*>(&to_nested_col);
        this->data(place).insert_result_into(col_null->get_nested_column());
        col_null->get_null_map_data().resize_fill(col_null->get_nested_column().size(), 0);
        to_arr.get_offsets().push_back(to_nested_col.size());
    }

private:
    DataTypePtr return_type;
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
