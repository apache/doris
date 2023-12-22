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

import com.google.gson.annotations.SerializedName;


public class WorkloadConditionMeta {

    @SerializedName(value = "metricName")
    public WorkloadMetricType metricName;

    @SerializedName(value = "op")
    public WorkloadConditionOperator op;

    @SerializedName(value = "value")
    public String value;

    public WorkloadConditionMeta(String metricName, String op, String value) throws UserException {
        this.metricName = getMetricType(metricName);
        this.op = WorkloadConditionCompareUtils.getOperator(op);
        this.value = value;
    }

    private static WorkloadMetricType getMetricType(String metricStr) throws UserException {
        if (WorkloadMetricType.USERNAME.toString().equalsIgnoreCase(metricStr)) {
            return WorkloadMetricType.USERNAME;
        } else if (WorkloadMetricType.QUERY_TIME.toString().equalsIgnoreCase(metricStr)) {
            return WorkloadMetricType.QUERY_TIME;
        }
        throw new UserException("invalid metric name:" + metricStr);
    }

    public String toString() {
        return metricName + " " + WorkloadConditionCompareUtils.getOperatorStr(op) + " " + value;
    }
}
