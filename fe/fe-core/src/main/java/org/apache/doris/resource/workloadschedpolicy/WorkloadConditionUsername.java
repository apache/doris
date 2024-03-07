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

import org.apache.hbase.thirdparty.com.google.gson.annotations.SerializedName;

public class WorkloadConditionUsername implements WorkloadCondition {

    @SerializedName(value = "op")
    private WorkloadConditionOperator op;
    @SerializedName(value = "value")
    private String value;

    public WorkloadConditionUsername(WorkloadConditionOperator op, String value) {
        this.op = op;
        this.value = value;
    }

    @Override
    public boolean eval(String inputStrValue) {
        return WorkloadConditionCompareUtils.compareString(op, inputStrValue, value);
    }

    @Override
    public WorkloadMetricType getMetricType() {
        return WorkloadMetricType.USERNAME;
    }

    public static WorkloadConditionUsername createWorkloadCondition(WorkloadConditionOperator op, String value) {
        // todo(wb) check whether input username is valid
        return new WorkloadConditionUsername(op, value);
    }
}
