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

#include "vec/aggregate_functions/aggregate_function_state_union.h"

namespace doris::vectorized {
const static std::string AGG_MERGE_SUFFIX = "_merge";

class AggregateStateMerge : public AggregateStateUnion {
public:
    using AggregateStateUnion::create;

    AggregateStateMerge(AggregateFunctionPtr function, const DataTypes& argument_types_,
                        const DataTypePtr& return_type)
            : AggregateStateUnion(function, argument_types_, return_type) {}

    static AggregateFunctionPtr create(AggregateFunctionPtr function,
                                       const DataTypes& argument_types_,
                                       const DataTypePtr& return_type) {
        CHECK(argument_types_.size() == 1);
        if (function == nullptr) {
            return nullptr;
        }
        return std::make_shared<AggregateStateMerge>(function, argument_types_, return_type);
    }

    void set_version(const int version_) override {
        IAggregateFunctionHelper::set_version(version_);
        _function->set_version(version_);
    }

    String get_name() const override { return _function->get_name() + AGG_MERGE_SUFFIX; }

    DataTypePtr get_return_type() const override { return _function->get_return_type(); }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        _function->insert_result_into(place, to);
    }
};

} // namespace doris::vectorized
