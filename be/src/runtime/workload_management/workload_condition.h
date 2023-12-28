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

#include <gen_cpp/BackendService_types.h>

#include "runtime/workload_management/workload_comparator.h"

namespace doris {

enum WorkloadMetricType { QUERY_TIME, SCAN_ROWS, SCAN_BYTES };

class WorkloadCondition {
public:
    WorkloadCondition() = default;
    virtual ~WorkloadCondition() = default;

    virtual bool eval(std::string str_val) = 0;

    virtual WorkloadMetricType get_workload_metric_type() = 0;
};

class WorkloadConditionQueryTime : public WorkloadCondition {
public:
    WorkloadConditionQueryTime(WorkloadCompareOperator op, std::string str_val);

    bool eval(std::string str_val) override;

    WorkloadMetricType get_workload_metric_type() override {
        return WorkloadMetricType::QUERY_TIME;
    }

private:
    int64_t _query_time;
    WorkloadCompareOperator _op;
};

class WorkloadConditionScanRows : public WorkloadCondition {
public:
    WorkloadConditionScanRows(WorkloadCompareOperator op, std::string str_val);
    bool eval(std::string str_val) override;
    WorkloadMetricType get_workload_metric_type() override { return WorkloadMetricType::SCAN_ROWS; }

private:
    int64_t _scan_rows;
    WorkloadCompareOperator _op;
};

class WorkloadConditionScanBytes : public WorkloadCondition {
public:
    WorkloadConditionScanBytes(WorkloadCompareOperator op, std::string str_val);
    bool eval(std::string str_val) override;
    WorkloadMetricType get_workload_metric_type() override {
        return WorkloadMetricType::SCAN_BYTES;
    }

private:
    int64_t _scan_bytes;
    WorkloadCompareOperator _op;
};

class WorkloadConditionFactory {
public:
    static std::unique_ptr<WorkloadCondition> create_workload_condition(
            TWorkloadCondition* t_cond) {
        WorkloadCompareOperator op =
                WorkloadCompareUtils::get_workload_compare_operator(t_cond->op);
        std::string str_val = t_cond->value;
        TWorkloadMetricType::type metric_name = t_cond->metric_name;
        if (TWorkloadMetricType::type::QUERY_TIME == metric_name) {
            return std::make_unique<WorkloadConditionQueryTime>(op, str_val);
        } else if (TWorkloadMetricType::type::SCAN_ROWS == metric_name) {
            return std::make_unique<WorkloadConditionScanRows>(op, str_val);
        } else if (TWorkloadMetricType::type::SCAN_BYTES == metric_name) {
            return std::make_unique<WorkloadConditionScanBytes>(op, str_val);
        }
        LOG(ERROR) << "not find a metric name " << metric_name;
        return nullptr;
    }
};

} // namespace doris