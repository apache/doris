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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

struct AggregateFuntionBoolOrData {
    bool value = false;
    bool has_value = false;

    void add(bool x) {
        value |= x;
        has_value = true;
    }

    void merge(const AggregateFuntionBoolOrData& rhs) {
        if (rhs.has_value) {
            value |= rhs.value;
            has_value = true;
        }
    }

    void write(BufferWritable& buf) const {
        write_binary(value, buf);
        write_binary(has_value, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(value, buf);
        read_binary(has_value, buf);
    }

    void reset() {
        value = false;
        has_value = false;
    }
};

template <bool nullable>
class AggregateFuntionBoolOr
        : public IAggregateFunctionDataHelper<AggregateFuntionBoolOrData,
                                              AggregateFuntionBoolOr<nullable>> {
public:
    explicit AggregateFuntionBoolOr(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<AggregateFuntionBoolOrData,
                                           AggregateFuntionBoolOr<nullable>>(argument_types_) {
        DCHECK(!argument_types_.empty());
    }

    String get_name() const override { return "bool_or"; }

    DataTypePtr get_return_type() const override {
        return make_nullable(std::make_shared<DataTypeBool>());
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        if constexpr (nullable) {
            const auto& column =
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*columns[0]);
            if (column.is_null_at(row_num)) {
                return;
            }
        }
        bool value =
                assert_cast<const ColumnVector<UInt8>&, TypeCheckOnRelease::DISABLE>(*columns[0])
                        .get_data()[row_num];
        this->data(place).add(value);
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        frame_start = std::max<int64_t>(frame_start, partition_start);
        frame_end = std::min<int64_t>(frame_end, partition_end);

        for (int64_t i = frame_start; i < frame_end; ++i) {
            if constexpr (nullable) {
                const auto& column = assert_cast<const ColumnNullable&>(*columns[0]);
                if (!column.is_null_at(i)) {
                    bool value = assert_cast<const ColumnVector<UInt8>&>(column.get_nested_column())
                                         .get_data()[i];
                    this->data(place).add(value);

                    if (this->data(place).value) {
                        break;
                    }
                }
            } else {
                bool value = assert_cast<const ColumnVector<UInt8>&>(*columns[0]).get_data()[i];
                this->data(place).add(value);

                if (this->data(place).value) {
                    break;
                }
            }
        }
    }

    void add_batch_range(size_t batch_begin, size_t batch_end, AggregateDataPtr place,
                         const IColumn** columns, Arena* arena, bool has_null) override {
        for (size_t i = batch_begin; i <= batch_end; ++i) {
            if constexpr (nullable) {
                const auto& column = assert_cast<const ColumnNullable&>(*columns[0]);
                if (!column.is_null_at(i)) {
                    bool value = assert_cast<const ColumnVector<UInt8>&>(column.get_nested_column())
                                         .get_data()[i];
                    this->data(place).add(value);

                    if (this->data(place).value) {
                        break;
                    }
                }
            } else {
                bool value = assert_cast<const ColumnVector<UInt8>&>(*columns[0]).get_data()[i];
                this->data(place).add(value);

                if (this->data(place).value) {
                    break;
                }
            }
        }
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            if constexpr (nullable) {
                const auto& column = assert_cast<const ColumnNullable&>(*columns[0]);
                if (!column.is_null_at(i)) {
                    bool value = assert_cast<const ColumnVector<UInt8>&>(column.get_nested_column())
                                         .get_data()[i];
                    this->data(place).add(value);

                    // 短路条件：已找到true值
                    if (this->data(place).value) {
                        break;
                    }
                }
            } else {
                bool value = assert_cast<const ColumnVector<UInt8>&>(*columns[0]).get_data()[i];
                this->data(place).add(value);

                if (this->data(place).value) {
                    break;
                }
            }
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        const auto& data = this->data(place);
        auto& dst_column_with_nullable = assert_cast<ColumnNullable&>(to);
        auto& dst_column =
                assert_cast<ColumnVector<UInt8>&>(dst_column_with_nullable.get_nested_column());

        if (!data.has_value) {
            dst_column_with_nullable.get_null_map_data().push_back(1);
            dst_column.insert_default();
        } else {
            dst_column_with_nullable.get_null_map_data().push_back(0);
            dst_column.get_data().push_back(data.value ? 1 : 0);
        }
    }
};
} // namespace doris::vectorized

#include "common/compile_check_end.h"