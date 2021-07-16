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
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

inline void assert_no_parameters(const std::string& name, const Array& parameters) {
    CHECK(parameters.empty()) << fmt::format("Aggregate function {} cannot have parameters", name);
}

inline void assert_unary(const std::string& name, const DataTypes& argument_types) {
    CHECK_EQ(argument_types.size(), 1)
            << fmt::format("Aggregate function {} require single argument", name);
}

inline void assert_binary(const std::string& name, const DataTypes& argument_types) {
    CHECK_EQ(argument_types.size(), 2)
            << fmt::format("Aggregate function {} require two arguments") << name;
}

template <std::size_t maximal_arity>
inline void assert_arity_at_most(const std::string& name, const DataTypes& argument_types) {
    if (argument_types.size() <= maximal_arity) return;

    if constexpr (maximal_arity == 0) {
        LOG(FATAL) << fmt::format("Aggregate function {} cannot have arguments", name);
    }

    if constexpr (maximal_arity == 1) {
        LOG(FATAL) << fmt::format("Aggregate function {} requires zero or one argument", name);
    }

    LOG(FATAL) << fmt::format("Aggregate function {} requires at most {} arguments", name,
                              maximal_arity);
}

} // namespace doris::vectorized
