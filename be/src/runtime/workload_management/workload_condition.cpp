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

#include "runtime/workload_management/workload_condition.h"

namespace doris {

// query time
WorkloadConditionQueryTime::WorkloadConditionQueryTime(WorkloadCompareOperator op,
                                                       std::string str_val) {
    _op = op;
    _query_time = std::stol(str_val);
}

bool WorkloadConditionQueryTime::eval(std::string str_val) {
    int64_t query_time_args = std::stol(str_val);
    return WorkloadCompareUtils::compare_signed_integer(_op, query_time_args, _query_time);
}

// scan rows
WorkloadConditionScanRows::WorkloadConditionScanRows(WorkloadCompareOperator op,
                                                     std::string str_val) {
    _op = op;
    _scan_rows = std::stol(str_val);
}

bool WorkloadConditionScanRows::eval(std::string str_val) {
    int64_t scan_rows_args = std::stol(str_val);
    return WorkloadCompareUtils::compare_signed_integer(_op, scan_rows_args, _scan_rows);
}

// scan bytes
WorkloadConditionScanBytes::WorkloadConditionScanBytes(WorkloadCompareOperator op,
                                                       std::string str_val) {
    _op = op;
    _scan_bytes = std::stol(str_val);
}

// todo(wb): need handle invalid input value
bool WorkloadConditionScanBytes::eval(std::string str_val) {
    int64_t scan_bytes_args = std::stol(str_val);
    return WorkloadCompareUtils::compare_signed_integer(_op, scan_bytes_args, _scan_bytes);
}

// query memory
WorkloadConditionQueryMemory::WorkloadConditionQueryMemory(WorkloadCompareOperator op,
                                                           std::string str_val) {
    _op = op;
    _query_memory_bytes = std::stol(str_val);
}

bool WorkloadConditionQueryMemory::eval(std::string str_val) {
    int64_t query_memory_bytes = std::stol(str_val);
    return WorkloadCompareUtils::compare_signed_integer(_op, query_memory_bytes,
                                                        _query_memory_bytes);
}

} // namespace doris