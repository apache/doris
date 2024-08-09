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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionUniq.h
// and modified by Doris

#pragma once

#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <vector>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_uniq.h"
#include "vec/columns/column.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_fixed_length_object.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/var_int.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
} // namespace vectorized
} // namespace doris
template <typename T>
struct HashCRC32;
namespace doris::vectorized {

template <typename T>
struct AggregateFunctionUniqDistributeKeyData {
    static constexpr bool is_string_key = std::is_same_v<T, String>;
    using Key = std::conditional_t<is_string_key, UInt128, T>;
    using Hash = std::conditional_t<is_string_key, UInt128TrivialHash, HashCRC32<Key>>;

    using Set = flat_hash_set<Key, Hash>;

    // TODO: replace SipHash with xxhash to speed up
    static UInt128 ALWAYS_INLINE get_key(const StringRef& value) {
        auto hash_value = XXH_INLINE_XXH128(value.data, value.size, 0);
        return UInt128 {hash_value.high64, hash_value.low64};
    }

    Set set;
    UInt64 count = 0;
};

template <typename T, typename Data>
class AggregateFunctionUniqDistributeKey final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionUniqDistributeKey<T, Data>> {
public:
    using KeyType = std::conditional_t<std::is_same_v<T, String>, UInt128, T>;
    AggregateFunctionUniqDistributeKey(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionUniqDistributeKey<T, Data>>(
                      argument_types_) {}

    String get_name() const override { return "multi_distinct_distribute_key"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        detail::OneAdder<T, Data>::add(this->data(place), *columns[0], row_num);
    }

    static ALWAYS_INLINE const KeyType* get_keys(std::vector<KeyType>& keys_container,
                                                 const IColumn& column, size_t batch_size) {
        if constexpr (std::is_same_v<T, String>) {
            keys_container.resize(batch_size);
            for (size_t i = 0; i != batch_size; ++i) {
                StringRef value = column.get_data_at(i);
                keys_container[i] = Data::get_key(value);
            }
            return keys_container.data();
        } else {
            using ColumnType =
                    std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
            return assert_cast<const ColumnType&>(column).get_data().data();
        }
    }

    void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                   const IColumn** columns, Arena* arena, bool /*agg_many*/) const override {
        std::vector<KeyType> keys_container;
        const KeyType* keys = get_keys(keys_container, *columns[0], batch_size);

        std::vector<typename Data::Set*> array_of_data_set(batch_size);

        for (size_t i = 0; i != batch_size; ++i) {
            array_of_data_set[i] = &(this->data(places[i] + place_offset).set);
        }

        for (size_t i = 0; i != batch_size; ++i) {
            if (i + HASH_MAP_PREFETCH_DIST < batch_size) {
                array_of_data_set[i + HASH_MAP_PREFETCH_DIST]->prefetch(
                        keys[i + HASH_MAP_PREFETCH_DIST]);
            }

            array_of_data_set[i]->insert(keys[i]);
        }
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        std::vector<KeyType> keys_container;
        const KeyType* keys = get_keys(keys_container, *columns[0], batch_size);
        auto& set = this->data(place).set;

        for (size_t i = 0; i != batch_size; ++i) {
            if (i + HASH_MAP_PREFETCH_DIST < batch_size) {
                set.prefetch(keys[i + HASH_MAP_PREFETCH_DIST]);
            }
            set.insert(keys[i]);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).count += this->data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        write_var_uint(this->data(place).set.size(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        read_var_uint(this->data(place).count, buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(this->data(place).count);
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena* arena,
                                 size_t num_rows) const override {
        auto data = reinterpret_cast<const UInt64*>(
                assert_cast<const ColumnFixedLengthObject&>(column).get_data().data());
        for (size_t i = 0; i != num_rows; ++i) {
            auto rhs_place = places + sizeof(Data) * i;
            this->create(rhs_place);
            (reinterpret_cast<Data*>(rhs_place))->count = data[i];
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        auto& col = assert_cast<ColumnFixedLengthObject&>(*dst);
        CHECK(col.item_size() == sizeof(UInt64))
                << "size is not equal: " << col.item_size() << " " << sizeof(UInt64);
        col.resize(num_rows);
        auto* data = reinterpret_cast<UInt64*>(col.get_data().data());
        for (size_t i = 0; i != num_rows; ++i) {
            data[i] = this->data(places[i] + offset).set.size();
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena* arena) const override {
        auto& dst_col = assert_cast<ColumnFixedLengthObject&>(*dst);
        CHECK(dst_col.item_size() == sizeof(UInt64))
                << "size is not equal: " << dst_col.item_size() << " " << sizeof(UInt64);
        dst_col.resize(num_rows);
        auto* data = reinterpret_cast<UInt64*>(dst_col.get_data().data());
        for (size_t i = 0; i != num_rows; ++i) {
            data[i] = 1;
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
        const size_t num_rows = column.size();
        auto* data = reinterpret_cast<const UInt64*>(col.get_data().data());
        for (size_t i = 0; i != num_rows; ++i) {
            AggregateFunctionUniqDistributeKey::data(place).count += data[i];
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena* arena) const override {
        CHECK(end <= column.size() && begin <= end)
                << ", begin:" << begin << ", end:" << end << ", column.size():" << column.size();
        auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
        auto* data = reinterpret_cast<const UInt64*>(col.get_data().data());
        for (size_t i = begin; i <= end; ++i) {
            this->data(place).count += data[i];
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena* arena,
                                   const size_t num_rows) const override {
        this->deserialize_from_column(rhs, *column, arena, num_rows);
        DEFER({ this->destroy_vec(rhs, num_rows); });
        this->merge_vec(places, offset, rhs, arena, num_rows);
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena* arena, const size_t num_rows) const override {
        this->deserialize_from_column(rhs, *column, arena, num_rows);
        DEFER({ this->destroy_vec(rhs, num_rows); });
        this->merge_vec_selected(places, offset, rhs, arena, num_rows);
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        auto& col = assert_cast<ColumnFixedLengthObject&>(to);
        CHECK(col.item_size() == sizeof(UInt64))
                << "size is not equal: " << col.item_size() << " " << sizeof(UInt64);
        size_t old_size = col.size();
        col.resize(old_size + 1);
        *reinterpret_cast<UInt64*>(col.get_data().data() + old_size) =
                AggregateFunctionUniqDistributeKey::data(place).set.size();
    }

    MutableColumnPtr create_serialize_column() const override {
        return ColumnFixedLengthObject::create(sizeof(UInt64));
    }

    DataTypePtr get_serialized_type() const override {
        return std::make_shared<DataTypeFixedLengthObject>();
    }
};

} // namespace doris::vectorized
