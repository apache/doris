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

#include "common/status.h"

namespace doris {

enum WorkloadCompareOperator { EQUAL, GREATER, GREATER_EQUAL, LESS, LESS_EQUAL, INVALID_OP };

class WorkloadCompareUtils {
public:
    static WorkloadCompareOperator get_workload_compare_operator(TCompareOperator::type t_op) {
        if (TCompareOperator::type::EQUAL == t_op) {
            return EQUAL;
        } else if (TCompareOperator::type::GREATER == t_op) {
            return GREATER;
        } else if (TCompareOperator::type::GREATER_EQUAL == t_op) {
            return GREATER_EQUAL;
        } else if (TCompareOperator::type::LESS == t_op) {
            return LESS;
        } else if (TCompareOperator::type::LESS_EQUAL == t_op) {
            return LESS_EQUAL;
        }
        LOG(ERROR) << "can not find a valid op ";
        return INVALID_OP;
    }

    static bool compare_signed_integer(WorkloadCompareOperator op, int64_t first_val,
                                       int64_t second_val) {
        switch (op) {
        case EQUAL:
            return first_val == second_val;
        case GREATER:
            return first_val > second_val;
        case GREATER_EQUAL:
            return first_val >= second_val;
        case LESS:
            return first_val < second_val;
        case LESS_EQUAL:
            return first_val <= second_val;
        default:
            LOG(ERROR) << "unexpected signed integer compare operator " << op;
            return false;
        }
    }

    static bool compare_string(WorkloadCompareOperator op, std::string first_val,
                               std::string second_val) {
        switch (op) {
        case EQUAL:
            return first_val == second_val;
        default:
            LOG(ERROR) << "unexpected string compare operator " << op;
            return false;
        }
    }
};

} // namespace doris