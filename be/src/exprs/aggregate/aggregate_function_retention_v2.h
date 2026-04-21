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

// This file is based on aggregate_function_retention.h and provides an optimized
// version that uses a uint32_t bitmask instead of a uint8_t[32] array.
// The serialization format is incompatible with v1 — use retention_v2 only when
// all BE nodes have been upgraded to support it.

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <boost/iterator/iterator_facade.hpp>
#include <memory>

#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"

namespace doris {
class Arena;
class BufferReadable;
class BufferWritable;
} // namespace doris

namespace doris {

// Optimized state: stores event flags as a uint32_t bitmask (4 bytes) instead of
// a uint8_t[32] array (32 bytes).  This is 8× smaller, leading to:
//   - 8× better cache line utilisation in the hash table (high-cardinality GROUP BY)
//   - merge():       single OR instruction instead of 32-byte loop
//   - write/read():  fixed 4-byte binary instead of manual bit-pack + VarInt
//
// IMPORTANT: This format is NOT compatible with RetentionState (v1).
struct RetentionStateV2 {
    static constexpr size_t MAX_EVENTS = 32;
    uint32_t events = 0;

    RetentionStateV2() = default;

    void reset() { events = 0; }

    void set(int event) { events |= (1u << event); }

    void merge(const RetentionStateV2& other) { events |= other.events; }

    void write(BufferWritable& out) const { out.write_binary(events); }

    void read(BufferReadable& in) { in.read_binary(events); }

    void insert_result_into(IColumn& to, size_t events_size) const {
        auto& data_to = assert_cast<ColumnUInt8&>(to).get_data();

        ColumnArray::Offset64 current_offset = data_to.size();
        data_to.resize(current_offset + events_size);

        bool first_flag = (events >> 0) & 1u;
        data_to[current_offset] = first_flag;
        ++current_offset;

        for (size_t i = 1; i < events_size; ++i) {
            data_to[current_offset] = (first_flag && ((events >> i) & 1u));
            ++current_offset;
        }
    }
};

class AggregateFunctionRetentionV2 final
        : public IAggregateFunctionDataHelper<RetentionStateV2, AggregateFunctionRetentionV2>,
          VarargsExpression,
          NullableAggregateFunction {
public:
    AggregateFunctionRetentionV2(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<RetentionStateV2, AggregateFunctionRetentionV2>(
                      argument_types_) {}

    String get_name() const override { return "retention_v2"; }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeUInt8>()));
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, const ssize_t row_num,
             Arena&) const override {
        for (int i = 0; i < get_argument_types().size(); i++) {
            auto event = assert_cast<const ColumnUInt8*, TypeCheckOnRelease::DISABLE>(columns[i])
                                 ->get_data()[row_num];
            if (event) {
                this->data(place).set(i);
            }
        }
    }

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

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        if (to_nested_col.is_nullable()) {
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            this->data(place).insert_result_into(col_null->get_nested_column(),
                                                 get_argument_types().size());
            col_null->get_null_map_data().resize_fill(col_null->get_nested_column().size(), 0);
        } else {
            this->data(place).insert_result_into(to_nested_col, get_argument_types().size());
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }
};

} // namespace doris
