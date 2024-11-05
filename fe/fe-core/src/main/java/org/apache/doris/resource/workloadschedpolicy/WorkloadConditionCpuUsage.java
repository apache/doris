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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.common.UserException;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class WorkloadConditionCpuUsage implements WorkloadCondition {

    public static final Set<WorkloadMetricType> CPU_METRIC_SET = new ImmutableSet.Builder<WorkloadMetricType>()
            .add(WorkloadMetricType.LAST_10S_CPU_USAGE_PERCENT)
            .add(WorkloadMetricType.LAST_20S_CPU_USAGE_PERCENT)
            .add(WorkloadMetricType.LAST_30S_CPU_USAGE_PERCENT).build();

    private double value;

    private WorkloadConditionOperator op;

    private WorkloadMetricType type;

    public WorkloadConditionCpuUsage(WorkloadMetricType type, WorkloadConditionOperator op, double value) {
        this.type = type;
        this.op = op;
        this.value = value;
    }

    @Override
    public boolean eval(String strValue) {
        return false;
    }

    @Override
    public WorkloadMetricType getMetricType() {
        return type;
    }

    public static WorkloadConditionCpuUsage createWorkloadCondition(WorkloadMetricType metricType,
            WorkloadConditionOperator op,
            String value) throws UserException {
        double doubleValue = 0;
        try {
            doubleValue = Double.parseDouble(value);
        } catch (NumberFormatException t) {
            throw new UserException("cpu usage value is not a valid number, input value is " + value);
        }


        String[] ss = value.split("\\.");
        if (ss.length == 2 && ss[1].length() > 2) {
            throw new UserException(
                    "cpu usage value cannot exceed two decimal places at most, but input value is " + value);
        }

        if (doubleValue < 0 || doubleValue > 100) {
            throw new UserException("cpu usage value should between 0 and 100");
        }
        return new WorkloadConditionCpuUsage(metricType, op, doubleValue);
    }
}
