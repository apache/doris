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

#include <gtest/gtest.h>

#include "agent/be_exec_version_manager.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

TEST(AggregateFunctionMapAggV2Test, SupportsIpKeyTypes) {
    const DataTypes key_types {std::make_shared<DataTypeIPv4>(), std::make_shared<DataTypeIPv6>()};

    for (const auto& key_type : key_types) {
        const DataTypes argument_types {make_nullable(key_type),
                                        make_nullable(std::make_shared<DataTypeInt32>())};
        auto function = AggregateFunctionSimpleFactory::instance().get(
                "map_agg_v2", argument_types, nullptr, false,
                BeExecVersionManager::get_newest_version());

        ASSERT_NE(function, nullptr) << key_type->get_name();
    }
}

} // namespace doris
