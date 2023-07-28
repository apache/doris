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

#include <parallel_hashmap/phmap.h>
#include <string.h>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/hash_table_key_holder.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_map.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

template <typename K>
struct AggregateFunctionMapAggData {
    using KeyType = std::conditional_t<std::is_same_v<K, String>, StringRef, K>;
    using Map = phmap::flat_hash_map<StringRef, int64_t>;
    AggregateFunctionMapAggData() { __builtin_unreachable(); }

    AggregateFunctionMapAggData(const DataTypes& argument_types) {
        _key_type = remove_nullable(argument_types[0]);
        _value_type = make_nullable(argument_types[1]);
        _key_column = _key_type->create_column();
        _value_column = _value_type->create_column();
    }

    void reset() {
        _map.clear();
        _key_column->clear();
        _value_column->clear();
    }

    void add(const StringRef& key, const StringRef& value) {
        DCHECK(key.data != nullptr);
        if (UNLIKELY(_map.find(key) != _map.end())) {
            return;
        }

        ArenaKeyHolder key_holder {key, _arena};
        if (key.size > 0) {
            key_holder_persist_key(key_holder);
        }

        _map.emplace(key_holder.key, _key_column->size());
        _key_column->insert_data(key_holder.key.data, key_holder.key.size);
        _value_column->insert_data(value.data, value.size);
    }

    void merge(const AggregateFunctionMapAggData& other) {
        const size_t num_rows = other._key_column->size();
        if (num_rows == 0) {
            return;
        }

        auto& other_key_column = assert_cast<KeyColumnType&>(*other._key_column);

        for (size_t i = 0; i != num_rows; ++i) {
            auto key = static_cast<KeyColumnType&>(other_key_column).get_data_at(i);
            if (_map.find(key) != _map.cend()) {
                continue;
            }
            ArenaKeyHolder key_holder {key, _arena};
            if (key.size > 0) {
                key_holder_persist_key(key_holder);
            }

            _map.emplace(key_holder.key, _key_column->size());
            static_cast<KeyColumnType&>(*_key_column)
                    .insert_data(key_holder.key.data, key_holder.key.size);

            auto value = other._value_column->get_data_at(i);
            _value_column->insert_data(value.data, value.size);
        }
    }

    static void serialize(BufferWritable& buf, const IColumn& key_column,
                          const IColumn& value_column, const DataTypePtr& key_type,
                          const DataTypePtr& value_type) {
        size_t element_number = key_column.size();
        write_binary(element_number, buf);

        DCHECK(!key_column.is_nullable());
        DCHECK(!key_type->is_nullable());
        DCHECK(value_column.is_nullable());
        DCHECK(value_type->is_nullable());

        if (element_number > 0) {
            size_t serialized_size = key_type->get_uncompressed_serialized_bytes(key_column, 0);
            serialized_size += value_type->get_uncompressed_serialized_bytes(value_column, 0);

            std::string serialized_buffer;
            serialized_buffer.resize(serialized_size);
            auto* serialized_data = serialized_buffer.data();

            serialized_data = key_type->serialize(key_column, serialized_data, 0);
            value_type->serialize(value_column, serialized_data, 0);

            write_binary(serialized_size, buf);
            buf.write(serialized_buffer.data(), serialized_buffer.size());
        }
    }

    void write(BufferWritable& buf) const {
        serialize(buf, *_key_column, *_value_column, _key_type, _value_type);
    }

    void read(BufferReadable& buf) {
        size_t element_number = 0;
        read_binary(element_number, buf);

        if (element_number > 0) {
            _map.reserve(element_number);

            size_t serialized_size;
            read_binary(serialized_size, buf);
            std::string serialized_buffer;
            serialized_buffer.resize(serialized_size);

            buf.read(serialized_buffer.data(), serialized_size);
            const auto* serialized_data = serialized_buffer.data();
            serialized_data = _key_type->deserialize(serialized_data, _key_column.get(), 0);
            _value_type->deserialize(serialized_data, _value_column.get(), 0);

            DCHECK_EQ(element_number, _key_column->size());
            DCHECK_EQ(element_number, _value_column->size());

            for (size_t i = 0; i != element_number; ++i) {
                auto key = static_cast<KeyColumnType&>(*_key_column).get_data_at(i);
                DCHECK(_map.find(key) == _map.cend());
                _map.emplace(key, i);
            }
        }
    }

    void insert_result_into(IColumn& to) const {
        auto& dst = assert_cast<ColumnMap&>(to);
        size_t num_rows = _key_column->size();
        auto& offsets = dst.get_offsets();
        auto& dst_key_column = assert_cast<ColumnNullable&>(dst.get_keys());
        dst_key_column.get_null_map_data().resize_fill(dst_key_column.get_null_map_data().size() +
                                                       num_rows);
        dst_key_column.get_nested_column().insert_range_from(*_key_column, 0, num_rows);
        dst.get_values().insert_range_from(*_value_column, 0, num_rows);
        if (offsets.size() == 0) {
            offsets.push_back(num_rows);
        } else {
            offsets.push_back(offsets.back() + num_rows);
        }
    }

private:
    using KeyColumnType =
            std::conditional_t<std::is_same_v<String, K>, ColumnString, ColumnVectorOrDecimal<K>>;
    Map _map;
    Arena _arena;
    IColumn::MutablePtr _key_column;
    IColumn::MutablePtr _value_column;
    DataTypePtr _key_type;
    DataTypePtr _value_type;
};

template <typename Data, typename K>
class AggregateFunctionMapAgg final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionMapAgg<Data, K>> {
public:
    using KeyColumnType =
            std::conditional_t<std::is_same_v<String, K>, ColumnString, ColumnVectorOrDecimal<K>>;
    AggregateFunctionMapAgg() = default;
    AggregateFunctionMapAgg(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionMapAgg<Data, K>>(
                      argument_types_) {}

    std::string get_name() const override { return "map_agg"; }

    DataTypePtr get_return_type() const override {
        /// keys and values column of `ColumnMap` are always nullable.
        return std::make_shared<DataTypeMap>(make_nullable(argument_types[0]),
                                             make_nullable(argument_types[1]));
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        if (columns[0]->is_nullable()) {
            auto& nullable_col = assert_cast<const ColumnNullable&>(*columns[0]);
            auto& nullable_map = nullable_col.get_null_map_data();
            if (nullable_map[row_num]) {
                return;
            }
            this->data(place).add(
                    assert_cast<const KeyColumnType&>(nullable_col.get_nested_column())
                            .get_data_at(row_num),
                    columns[1]->get_data_at(row_num));
        } else {
            this->data(place).add(
                    assert_cast<const KeyColumnType&>(*columns[0]).get_data_at(row_num),
                    columns[1]->get_data_at(row_num));
        }
    }

    void create(AggregateDataPtr __restrict place) const override {
        new (place) Data(argument_types);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    template <bool key_nullable, bool value_nullable>
    void streaming_agg_serialize_to_column_impl(const size_t num_rows, const IColumn& key_column,
                                                const IColumn& value_column,
                                                const NullMap& null_map,
                                                BufferWritable& writer) const {
        auto& key_col = assert_cast<const KeyColumnType&>(key_column);
        auto key_to_serialize = key_col.clone_empty();
        auto val_to_serialize = value_column.clone_empty();
        auto key_type = remove_nullable(argument_types[0]);
        auto val_type = make_nullable(argument_types[1]);
        for (size_t i = 0; i != num_rows; ++i) {
            key_to_serialize->clear();
            val_to_serialize->clear();
            if constexpr (key_nullable) {
                if (!null_map[i]) {
                    key_to_serialize->insert_range_from(key_col, i, 1);
                    val_to_serialize->insert_range_from(value_column, i, 1);
                }
            } else {
                key_to_serialize->insert_range_from(key_col, i, 1);
                val_to_serialize->insert_range_from(value_column, i, 1);
            }

            if constexpr (value_nullable) {
                Data::serialize(writer, *key_to_serialize, *val_to_serialize, key_type, val_type);
            } else {
                auto nullable_value_col = make_nullable(val_to_serialize->assume_mutable(), false);
                Data::serialize(writer, *key_to_serialize, *nullable_value_col, key_type, val_type);
                val_to_serialize = value_column.clone_empty();
            }
            writer.commit();
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena* arena) const override {
        auto& col = assert_cast<ColumnString&>(*dst);
        col.reserve(num_rows);
        VectorBufferWriter writer(col);

        if (columns[0]->is_nullable()) {
            auto& nullable_col = assert_cast<const ColumnNullable&>(*columns[0]);
            auto& null_map = nullable_col.get_null_map_data();
            if (columns[0]->is_nullable()) {
                this->streaming_agg_serialize_to_column_impl<true, true>(
                        num_rows, nullable_col.get_nested_column(), *columns[1], null_map, writer);
            } else {
                this->streaming_agg_serialize_to_column_impl<true, false>(
                        num_rows, nullable_col.get_nested_column(), *columns[1], null_map, writer);
            }
        } else {
            if (columns[0]->is_nullable()) {
                this->streaming_agg_serialize_to_column_impl<false, true>(num_rows, *columns[0],
                                                                          *columns[1], {}, writer);
            } else {
                this->streaming_agg_serialize_to_column_impl<false, false>(num_rows, *columns[0],
                                                                           *columns[1], {}, writer);
            }
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

protected:
    using IAggregateFunction::argument_types;
};

} // namespace doris::vectorized
