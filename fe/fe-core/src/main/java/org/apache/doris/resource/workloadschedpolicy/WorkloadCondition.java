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


public interface WorkloadCondition {

    boolean eval(String strValue);

    WorkloadMetricType getMetricType();

    // NOTE(wb) currently createPolicyCondition is also used when replay meta, it better not contains heavy check
    static WorkloadCondition createWorkloadCondition(WorkloadConditionMeta cm)
            throws UserException {
        if (WorkloadMetricType.USERNAME.equals(cm.metricName)) {
            return WorkloadConditionUsername.createWorkloadCondition(cm.op, cm.value);
        } else if (WorkloadMetricType.QUERY_TIME.equals(cm.metricName)) {
            return WorkloadConditionQueryTime.createWorkloadCondition(cm.op, cm.value);
        } else if (WorkloadMetricType.BE_SCAN_ROWS.equals(cm.metricName)) {
            return WorkloadConditionBeScanRows.createWorkloadCondition(cm.op, cm.value);
        } else if (WorkloadMetricType.BE_SCAN_BYTES.equals(cm.metricName)) {
            return WorkloadConditionBeScanBytes.createWorkloadCondition(cm.op, cm.value);
        } else if (WorkloadMetricType.QUERY_BE_MEMORY_BYTES.equals(cm.metricName)) {
            return WorkloadConditionQueryBeMemory.createWorkloadCondition(cm.op, cm.value);
        }
        throw new UserException("invalid metric name:" + cm.metricName);
    }

}
