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
#include "vec/data_types/data_type_agg_state.h"

namespace doris::vectorized {
const static std::string AGG_UNION_SUFFIX = "_union";

class AggregateStateUnion : public IAggregateFunctionHelper<AggregateStateUnion> {
public:
    AggregateStateUnion(AggregateFunctionPtr function, const DataTypes& argument_types_,
                        const DataTypePtr& return_type)
            : IAggregateFunctionHelper(argument_types_),
              _function(function),
              _return_type(return_type) {}
    ~AggregateStateUnion() override = default;

    static AggregateFunctionPtr create(AggregateFunctionPtr function,
                                       const DataTypes& argument_types_,
                                       const DataTypePtr& return_type) {
        CHECK(argument_types_.size() == 1);
        if (function == nullptr) {
            return nullptr;
        }
        return std::make_shared<AggregateStateUnion>(function, argument_types_, return_type);
    }

    void set_version(const int version_) override {
        IAggregateFunctionHelper::set_version(version_);
        _function->set_version(version_);
    }

    void create(AggregateDataPtr __restrict place) const override { _function->create(place); }

    String get_name() const override { return _function->get_name() + AGG_UNION_SUFFIX; }

    DataTypePtr get_return_type() const override { return _return_type; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        //the range is [begin, end]
        _function->deserialize_and_merge_from_column_range(place, *columns[0], row_num, row_num,
                                                           arena);
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        _function->deserialize_and_merge_from_column_range(place, *columns[0], 0, batch_size - 1,
                                                           arena);
    }

    void add_batch(size_t batch_size, AggregateDataPtr* places, size_t place_offset,
                   const IColumn** columns, Arena* arena, bool agg_many) const override {
        for (size_t i = 0; i < batch_size; ++i) {
            //the range is [i, i]
            _function->deserialize_and_merge_from_column_range(places[i] + place_offset,
                                                               *columns[0], i, i, arena);
        }
    }
    void reset(AggregateDataPtr place) const override { _function->reset(place); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        _function->merge(place, rhs, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        _function->serialize(place, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        _function->deserialize(place, buf, arena);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        _function->serialize_without_key_to_column(place, to);
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        _function->destroy(place);
    }

    bool has_trivial_destructor() const override { return _function->has_trivial_destructor(); }

    size_t size_of_data() const override { return _function->size_of_data(); }

    size_t align_of_data() const override { return _function->align_of_data(); }

protected:
    AggregateFunctionPtr _function;
    DataTypePtr _return_type;
};

} // namespace doris::vectorized
