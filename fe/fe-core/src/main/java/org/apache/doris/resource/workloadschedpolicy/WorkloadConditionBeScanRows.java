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

public class WorkloadConditionBeScanRows implements WorkloadCondition {

    private long value;

    private WorkloadConditionOperator op;

    public WorkloadConditionBeScanRows(WorkloadConditionOperator op, long value) {
        this.op = op;
        this.value = value;
    }

    @Override
    public boolean eval(String strValue) {
        // currently not support run in fe, so this condition never match
        return false;
    }

    public static WorkloadConditionBeScanRows createWorkloadCondition(WorkloadConditionOperator op, String value)
            throws UserException {
        long longValue = -1;
        try {
            longValue = Long.parseLong(value);
            if (longValue < 0) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            throw new UserException("invalid scan rows value: " + value + ", it requires >= 0");
        }
        return new WorkloadConditionBeScanRows(op, longValue);
    }

    @Override
    public WorkloadMetricType getMetricType() {
        return WorkloadMetricType.BE_SCAN_ROWS;
    }
}
