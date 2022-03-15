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

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
template <typename VT, typename KT>
struct AggregateFunctionMaxByData {
private:
    VT value;
    KT key;
public:
    using Self = AggregateFunctionMaxByData;

    bool change_if_better(const IColumn& value_column, const IColumn& key_column, size_t row_num, Arena* arena) {
        if (key.change_if_greater(key_column, row_num, arena)) {
            value.change(value_column, row_num, arena);
        }
    }

    bool change_if_better(const Self& to, Arena* arena) {
        if (key.change_if_greater(to.key, arena)) {
            value.change(to.value, arena);
        }
    }
};

template <typename Data, bool AllocatesMemoryInArena>
class AggregateFunctionsMinMaxBy final
        : public IAggregateFunctionDataHelper<
                  Data, AggregateFunctionsMinMaxBy<Data, AllocatesMemoryInArena>> {
private:
    DataTypePtr& type;

public:
    AggregateFunctionsMinMaxBy(const DataTypePtr& type_)
            : IAggregateFunctionDataHelper<
                      Data, AggregateFunctionsMinMaxBy<Data, AllocatesMemoryInArena>>({type_},
                                                                                         {}),
              type(this->argument_types[0]) {
        if (StringRef(Data::name()) == StringRef("min") ||
            StringRef(Data::name()) == StringRef("max")) {
            if (!type->is_comparable()) {
                LOG(FATAL) << fmt::format(
                        "Illegal type {} of argument of aggregate function {} because the values "
                        "of that data type are not comparable",
                        type->get_name(), get_name());
            }
        }
    }

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override { return type; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        this->data(place).change_if_better(*columns[0], row_num, arena);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        this->data(place).change_if_better(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    bool allocates_memory_in_arena() const override { return AllocatesMemoryInArena; }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }
};

AggregateFunctionPtr create_aggregate_function_max_by(const std::string& name,
                                                   const DataTypes& argument_types,
                                                   const Array& parameters,
                                                   const bool result_is_nullable);

AggregateFunctionPtr create_aggregate_function_min_by(const std::string& name,
                                                   const DataTypes& argument_types,
                                                   const Array& parameters,
                                                   const bool result_is_nullable);
} // namespace doris::vectorized
