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

#include <cstdint>
#include <utility>

#include "common/status.h"
namespace doris {
class AlgoUtil {
    // descent the value step by step not linear continuity
    double descent_by_step(int step_num, int64_t low_bound, int64_t high_bound, int64_t current) {
        if (current <= low_bound) {
            return 1;
        }
        if (current >= high_bound) {
            return 0;
        }
        if (high_bound <= low_bound) {
            // Invalid
            return 0;
        }
        int64_t step_size = (int64_t)std::floor((high_bound - low_bound) / step_num);
        int64_t used_step = (int64_t)std::ceil((current - low_bound) / step_size);
        int64_t left_step = step_num - used_step;
        return left_step / step_num;
    }
};
} // namespace doris