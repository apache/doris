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
#include <parallel_hashmap/phmap.h>

#include <string>

#include "common/check.h"
#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_map.h"
#include "core/field.h"
#include "core/pod_array.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

template <bool use_exact_key_frame>
struct AggregateFunctionMapAggDataV2 {
    using Map = doris::flat_hash_map<Field, int64_t>;

    AggregateFunctionMapAggDataV2() {
        throw Exception(Status::FatalError("__builtin_unreachable"));
    }

    AggregateFunctionMapAggDataV2(const DataTypes& argument_types, const int be_exec_version)
            : _be_version(be_exec_version) {
        _key_type = make_nullable(argument_types[0]);
        _value_type = make_nullable(argument_types[1]);
        _key_column = _key_type->create_column();
        _value_column = _value_type->create_column();
    }

    void reset() {
        _map.clear();
        _key_column->clear();
        _value_column->clear();
    }

    void add_single(const Field& key, const Field& value) {
        if (UNLIKELY(_map.find(key) != _map.end())) {
            return;
        }

        _map.emplace(key, _key_column->size());
        _key_column->insert(key);
        _value_column->insert(value);
    }

    void add(const Field& key_, const Field& value) {
        DCHECK(!key_.is_null());
        auto key_array = key_.get<TYPE_ARRAY>();
        auto value_array = value.get<TYPE_ARRAY>();

        const auto count = key_array.size();
        DCHECK_EQ(count, value_array.size());

        for (size_t i = 0; i != count; ++i) {
            const auto& key = key_array[i];

            if (UNLIKELY(_map.find(key) != _map.end())) {
                continue;
            }

            _map.emplace(key, _key_column->size());
            _key_column->insert(key);
            _value_column->insert(value_array[i]);
        }
    }

    void merge(const AggregateFunctionMapAggDataV2& other) {
        const size_t num_rows = other._key_column->size();
        if (num_rows == 0) {
            return;
        }

        auto& other_key_column_nullable = assert_cast<ColumnNullable&>(*other._key_column);
        for (size_t i = 0; i != num_rows; ++i) {
            const auto& key = other_key_column_nullable[i];
            if (_map.find(key) != _map.cend()) {
                continue;
            }

            _map.emplace(key, _key_column->size());
            _key_column->insert(key);

            _value_column->insert((*other._value_column)[i]);
        }
    }

    void insert_result_into(IColumn& to) const {
        auto& dst = assert_cast<ColumnMap&, TypeCheckOnRelease::DISABLE>(to);
        size_t num_rows = _key_column->size();
        auto& offsets = dst.get_offsets();
        auto& dst_key_column =
                assert_cast<ColumnNullable&, TypeCheckOnRelease::DISABLE>(dst.get_keys());
        dst_key_column.insert_range_from(*_key_column, 0, num_rows);
        dst.get_values().insert_range_from(*_value_column, 0, num_rows);
        if (offsets.empty()) {
            offsets.push_back(num_rows);
        } else {
            offsets.push_back(offsets.back() + num_rows);
        }
    }

    void write(BufferWritable& buf) const {
        write_column(*_key_type, *_key_column, buf, use_exact_key_frame);
        write_column(*_value_type, *_value_column, buf, true);
    }

    void read(BufferReadable& buf) {
        read_column(*_key_type, &_key_column, buf, use_exact_key_frame);
        read_column(*_value_type, &_value_column, buf, true);
    }

private:
    void write_column(const IDataType& type, const IColumn& column, BufferWritable& buf,
                      bool use_exact_frame) const {
        const auto max_serialized_bytes =
                type.get_uncompressed_serialized_bytes(column, _be_version);
        std::string serialized_buffer(max_serialized_bytes, '\0');

        const auto* end = type.serialize(column, serialized_buffer.data(), _be_version);
        const auto written_bytes = end - serialized_buffer.data();
        DORIS_CHECK_LE(written_bytes, max_serialized_bytes);

        if (use_exact_frame) {
            serialized_buffer.resize(written_bytes);
        }
        buf.write_binary(serialized_buffer);
    }

    void read_column(const IDataType& type, MutableColumnPtr* column, BufferReadable& buf,
                     bool use_exact_frame) const {
        PaddedPODArray<UInt8> frame;
        buf.read_binary(frame);

        const auto* begin = reinterpret_cast<const char*>(frame.data());
        const auto* frame_end = begin + frame.size();
        const auto* end = type.deserialize(begin, column, _be_version);
        if (use_exact_frame) {
            DORIS_CHECK_EQ(end, frame_end);
            return;
        }

        DORIS_CHECK_LE(end, frame_end);
        const auto expected_frame_size =
                type.get_uncompressed_serialized_bytes(**column, _be_version);
        DORIS_CHECK_EQ(frame.size(), expected_frame_size);
    }

    Map _map;
    Arena _arena;
    IColumn::MutablePtr _key_column;
    IColumn::MutablePtr _value_column;
    DataTypePtr _key_type;
    DataTypePtr _value_type;

    int _be_version;
};

template <typename Data>
class AggregateFunctionMapAggV2 final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionMapAggV2<Data>>,
          MultiExpression,
          NotNullableAggregateFunction {
public:
    AggregateFunctionMapAggV2() = default;
    AggregateFunctionMapAggV2(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionMapAggV2<Data>>(argument_types_) {
    }

    using IAggregateFunctionDataHelper<Data, AggregateFunctionMapAggV2<Data>>::version;

    std::string get_name() const override { return "map_agg_v2"; }

    DataTypePtr get_return_type() const override {
        /// keys and values column of `ColumnMap` are always nullable.
        return std::make_shared<DataTypeMap>(make_nullable(argument_types[0]),
                                             make_nullable(argument_types[1]));
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        Field key, value;
        columns[0]->get(row_num, key);
        columns[1]->get(row_num, value);
        this->data(place).add_single(key, value);
    }

    void create(AggregateDataPtr __restrict place) const override {
        new (place) Data(argument_types, version);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena&) const override {
        auto& col = assert_cast<ColumnMap&>(*dst);
        for (size_t i = 0; i != num_rows; ++i) {
            Field key, value;
            columns[0]->get(i, key);
            columns[1]->get(i, value);
            col.insert(Field::create_field<TYPE_MAP>(
                    Map {Field::create_field<TYPE_ARRAY>(Array {key}),
                         Field::create_field<TYPE_ARRAY>(Array {value})}));
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        for (size_t i = 0; i != num_rows; ++i) {
            Data& data_ = this->data(places[i] + offset);
            data_.insert_result_into(*dst);
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena&) const override {
        DCHECK(end <= column.size() && begin <= end)
                << ", begin:" << begin << ", end:" << end << ", column.size():" << column.size();
        const auto& col = assert_cast<const ColumnMap&>(column);
        for (size_t i = begin; i <= end; ++i) {
            auto map = col[i].get<TYPE_MAP>();
            this->data(place).add(map[0], map[1]);
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena&,
                                   const size_t num_rows) const override {
        const auto& col = assert_cast<const ColumnMap&>(*column);
        for (size_t i = 0; i != num_rows; ++i) {
            auto map = col[i].get<TYPE_MAP>();
            this->data(places[i] + offset).add(map[0], map[1]);
        }
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column, Arena&,
                                            const size_t num_rows) const override {
        const auto& col = assert_cast<const ColumnMap&>(*column);
        for (size_t i = 0; i != num_rows; ++i) {
            if (places[i]) {
                auto map = col[i].get<TYPE_MAP>();
                this->data(places[i] + offset).add(map[0], map[1]);
            }
        }
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    [[nodiscard]] MutableColumnPtr create_serialize_column() const override {
        return get_return_type()->create_column();
    }

    [[nodiscard]] DataTypePtr get_serialized_type() const override { return get_return_type(); }

protected:
    using IAggregateFunction::argument_types;
};

} // namespace doris
