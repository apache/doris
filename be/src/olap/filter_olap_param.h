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

#include <string>

#include "util/runtime_profile.h"
namespace doris {
template <typename T>
struct FilterOlapParam {
    FilterOlapParam(std::string column_name, T filter, int runtime_filter_id,
                    std::shared_ptr<RuntimeProfile::Counter> filtered_counter,
                    std::shared_ptr<RuntimeProfile::Counter> input_counter)
            : column_name(std::move(column_name)),
              filter(std::move(filter)),
              runtime_filter_id(runtime_filter_id) {
        DCHECK(filtered_rows_counter != nullptr);
        DCHECK(input_rows_counter != nullptr);
        if (filtered_counter != nullptr) {
            filtered_rows_counter = filtered_counter;
        }
        if (input_counter != nullptr) {
            input_rows_counter = input_counter;
        }
    }

    std::string column_name;
    T filter;
    int runtime_filter_id;
    std::shared_ptr<RuntimeProfile::Counter> filtered_rows_counter =
            std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0);
    std::shared_ptr<RuntimeProfile::Counter> input_rows_counter =
            std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0);
};

} // namespace doris
