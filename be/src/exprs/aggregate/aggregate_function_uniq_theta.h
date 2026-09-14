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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/ThetaSketchData.h
// and modified by Doris

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <stdexcept>
#include <string>

// roaring/portability.h (pulled in via the precompiled header) defines
// IS_BIG_ENDIAN as a function-like macro, which collides with the enum member
// of the same name in datasketches theta_sketch.hpp. This TU does not use
// roaring's macro, so drop it before including the sketch headers.
#ifdef IS_BIG_ENDIAN
#undef IS_BIG_ENDIAN
#endif
#include <DataSketches/theta_sketch.hpp>
#include <DataSketches/theta_union.hpp>

#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {
#include "common/compile_check_begin.h"
class Arena;
class BufferReadable;
class BufferWritable;
class IColumn;
} // namespace doris

namespace doris {

// Mirrors ClickHouse ThetaSketchData: sk_update takes raw inserts; once a merge
// happens the accumulated sk_update is folded into sk_union and reset. The two
// datasketches types are distinct and cannot be unified.
struct AggregateFunctionUniqThetaData {
    std::unique_ptr<datasketches::update_theta_sketch> sk_update;
    std::unique_ptr<datasketches::theta_union> sk_union;

    AggregateFunctionUniqThetaData() = default;
    ~AggregateFunctionUniqThetaData() = default;
    AggregateFunctionUniqThetaData(const AggregateFunctionUniqThetaData&) = delete;
    AggregateFunctionUniqThetaData& operator=(const AggregateFunctionUniqThetaData&) = delete;

private:
    datasketches::update_theta_sketch* get_sk_update() {
        if (!sk_update) {
            sk_update = std::make_unique<datasketches::update_theta_sketch>(
                    datasketches::update_theta_sketch::builder().build());
        }
        return sk_update.get();
    }

    datasketches::theta_union* get_sk_union() {
        if (!sk_union) {
            sk_union = std::make_unique<datasketches::theta_union>(
                    datasketches::theta_union::builder().build());
        }
        return sk_union.get();
    }

public:
    // Feed raw value bytes; datasketches hashes internally.
    void add(const void* data_ptr, size_t len) {
        get_sk_update()->update(data_ptr, len);
        // add() may be called after a merge() (e.g. incremental aggregation); keep
        // sk_union authoritative so those inserts are not lost by get()/serialize().
        if (sk_union) {
            sk_union->update(*sk_update);
            sk_update.reset();
        }
    }

    void merge(const AggregateFunctionUniqThetaData& rhs) {
        datasketches::theta_union* u = get_sk_union();
        if (sk_update) {
            u->update(*sk_update);
            sk_update.reset();
        }
        if (rhs.sk_update) {
            u->update(*rhs.sk_update);
        } else if (rhs.sk_union) {
            u->update(rhs.sk_union->get_result());
        }
    }

    int64_t get() const {
        if (sk_union) {
            return static_cast<int64_t>(sk_union->get_result().get_estimate());
        }
        if (sk_update) {
            return static_cast<int64_t>(sk_update->get_estimate());
        }
        return 0;
    }

    void write(BufferWritable& buf) const {
        std::string bytes;
        if (sk_update) {
            auto vec = sk_update->compact().serialize();
            bytes.assign(reinterpret_cast<const char*>(vec.data()), vec.size());
        } else if (sk_union) {
            auto vec = sk_union->get_result().serialize();
            bytes.assign(reinterpret_cast<const char*>(vec.data()), vec.size());
        }
        // Empty state serializes to a zero-length blob; never serialize() an empty
        // sketch (that would emit an 8-byte preamble instead).
        buf.write_binary(bytes);
    }

    void read(BufferReadable& buf) {
        std::string bytes;
        buf.read_binary(bytes);
        if (bytes.empty()) {
            return;
        }
        try {
            auto sk = datasketches::compact_theta_sketch::deserialize(bytes.data(), bytes.size());
            get_sk_union()->update(sk);
        } catch (const std::bad_alloc&) {
            throw;
        } catch (const std::exception& e) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "uniq_theta: cannot deserialize theta sketch: {}", e.what());
        }
    }

    void reset() {
        sk_update.reset();
        sk_union.reset();
    }
};

template <PrimitiveType type>
class AggregateFunctionUniqTheta final
        : public IAggregateFunctionDataHelper<AggregateFunctionUniqThetaData,
                                              AggregateFunctionUniqTheta<type>>,
          UnaryExpression,
          NotNullableAggregateFunction {
public:
    using ColumnDataType = typename PrimitiveTypeTraits<type>::ColumnType;
    String get_name() const override { return "uniq_theta"; }

    AggregateFunctionUniqTheta(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<AggregateFunctionUniqThetaData,
                                           AggregateFunctionUniqTheta<type>>(argument_types_) {}

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        if constexpr (is_decimal(type) || is_int_or_bool(type) || is_ip(type) ||
                      is_date_type(type) || is_timestamptz_type(type) || is_float_or_double(type) ||
                      type == TYPE_TIMEV2) {
            auto column =
                    assert_cast<const ColumnDataType*, TypeCheckOnRelease::DISABLE>(columns[0]);
            auto value = column->get_element(row_num);
            this->data(place).add(&value, sizeof(value));
        } else {
            auto value = assert_cast<const ColumnDataType*, TypeCheckOnRelease::DISABLE>(columns[0])
                                 ->get_data_at(row_num);
            this->data(place).add(value.data, value.size);
        }
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

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& column = assert_cast<ColumnInt64&>(to);
        column.get_data().push_back(this->data(place).get());
    }
};

} // namespace doris

#include "common/compile_check_end.h"
