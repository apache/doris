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

package org.apache.doris.resource;

import org.apache.doris.resource.workloadschedpolicy.WorkloadCondition;
import org.apache.doris.resource.workloadschedpolicy.WorkloadConditionOperator;
import org.apache.doris.resource.workloadschedpolicy.WorkloadConditionQueryTime;
import org.apache.doris.resource.workloadschedpolicy.WorkloadConditionUsername;
import org.apache.doris.resource.workloadschedpolicy.WorkloadMetricType;
import org.apache.doris.resource.workloadschedpolicy.WorkloadQueryInfo;
import org.apache.doris.resource.workloadschedpolicy.WorkloadSchedPolicy;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class WorkloadSchedTest {

    @Test
    public void testPolicyCondition() {
            // 1 test compare operator
            // 1.1 >
            {
                List<WorkloadCondition> operatorList = new ArrayList<>();
                WorkloadCondition intCondition = new WorkloadConditionQueryTime(WorkloadConditionOperator.GREATER, 100);
                operatorList.add(intCondition);

                WorkloadSchedPolicy workloadSchedPolicy1 = new WorkloadSchedPolicy();
                workloadSchedPolicy1.setWorkloadConditionList(operatorList);

                WorkloadQueryInfo queryInfo = new WorkloadQueryInfo();
                queryInfo.metricMap = new HashMap<>();
                queryInfo.metricMap.put(WorkloadMetricType.QUERY_TIME, "101");

                // match
                Assert.assertTrue(workloadSchedPolicy1.isMatch(queryInfo));

                // not match
                queryInfo.metricMap.put(WorkloadMetricType.QUERY_TIME, "100");
                Assert.assertFalse(workloadSchedPolicy1.isMatch(queryInfo));
            }

            // 1.2 >=
            {
                List<WorkloadCondition> operatorList = new ArrayList<>();
                WorkloadCondition intCondition = new WorkloadConditionQueryTime(WorkloadConditionOperator.GREATER_EQUAL, 100);
                operatorList.add(intCondition);

                WorkloadSchedPolicy workloadSchedPolicy1 = new WorkloadSchedPolicy();
                workloadSchedPolicy1.setWorkloadConditionList(operatorList);

                WorkloadQueryInfo queryInfo = new WorkloadQueryInfo();
                queryInfo.metricMap = new HashMap<>();
                queryInfo.metricMap.put(WorkloadMetricType.QUERY_TIME, "100");

                // match
                Assert.assertTrue(workloadSchedPolicy1.isMatch(queryInfo));

                // not match
                queryInfo.metricMap.put(WorkloadMetricType.QUERY_TIME, "10");
                Assert.assertFalse(workloadSchedPolicy1.isMatch(queryInfo));
            }

            // 1.3 =
            {
                List<WorkloadCondition> operatorList = new ArrayList<>();
                WorkloadCondition intCondition = new WorkloadConditionQueryTime(WorkloadConditionOperator.EQUAL, 100);
                operatorList.add(intCondition);

                WorkloadSchedPolicy workloadSchedPolicy1 = new WorkloadSchedPolicy();
                workloadSchedPolicy1.setWorkloadConditionList(operatorList);

                WorkloadQueryInfo queryInfo = new WorkloadQueryInfo();
                queryInfo.metricMap = new HashMap<>();
                queryInfo.metricMap.put(WorkloadMetricType.QUERY_TIME, "100");

                // match
                Assert.assertTrue(workloadSchedPolicy1.isMatch(queryInfo));

                // not match
                queryInfo.metricMap.put(WorkloadMetricType.QUERY_TIME, "10");
                Assert.assertFalse(workloadSchedPolicy1.isMatch(queryInfo));
            }

            // 1.4 <
            {
                List<WorkloadCondition> operatorList = new ArrayList<>();
                WorkloadCondition intCondition = new WorkloadConditionQueryTime(WorkloadConditionOperator.LESS, 100);
                operatorList.add(intCondition);

                WorkloadSchedPolicy workloadSchedPolicy1 = new WorkloadSchedPolicy();
                workloadSchedPolicy1.setWorkloadConditionList(operatorList);

                WorkloadQueryInfo queryInfo = new WorkloadQueryInfo();
                queryInfo.metricMap = new HashMap<>();
                queryInfo.metricMap.put(WorkloadMetricType.QUERY_TIME, "99");

                // match
                Assert.assertTrue(workloadSchedPolicy1.isMatch(queryInfo));

                // not match
                queryInfo.metricMap.put(WorkloadMetricType.QUERY_TIME, "100");
                Assert.assertFalse(workloadSchedPolicy1.isMatch(queryInfo));
            }

            // 1.5 <=
            {
                List<WorkloadCondition> operatorList = new ArrayList<>();
                WorkloadCondition intCondition = new WorkloadConditionQueryTime(WorkloadConditionOperator.LESS_EQUAl, 100);
                operatorList.add(intCondition);

                WorkloadSchedPolicy workloadSchedPolicy1 = new WorkloadSchedPolicy();
                workloadSchedPolicy1.setWorkloadConditionList(operatorList);

                WorkloadQueryInfo queryInfo = new WorkloadQueryInfo();
                queryInfo.metricMap = new HashMap<>();
                queryInfo.metricMap.put(WorkloadMetricType.QUERY_TIME, "100");

                // match
                Assert.assertTrue(workloadSchedPolicy1.isMatch(queryInfo));

                // not match
                queryInfo.metricMap.put(WorkloadMetricType.QUERY_TIME, "101");
                Assert.assertFalse(workloadSchedPolicy1.isMatch(queryInfo));
            }

            // 2 string compare
            {
                List<WorkloadCondition> operatorList = new ArrayList<>();
                WorkloadCondition strCondition = new WorkloadConditionUsername(WorkloadConditionOperator.EQUAL, "root");
                operatorList.add(strCondition);

                WorkloadSchedPolicy workloadSchedPolicy1 = new WorkloadSchedPolicy();
                workloadSchedPolicy1.setWorkloadConditionList(operatorList);

                WorkloadQueryInfo queryInfo = new WorkloadQueryInfo();
                queryInfo.metricMap = new HashMap<>();
                queryInfo.metricMap.put(WorkloadMetricType.USERNAME, "root");

                // match
                Assert.assertTrue(workloadSchedPolicy1.isMatch(queryInfo));

                // not match
                queryInfo.metricMap.put(WorkloadMetricType.USERNAME, "abc");
                Assert.assertFalse(workloadSchedPolicy1.isMatch(queryInfo));
            }

            // 3 mixed condition
            {
                List<WorkloadCondition> operatorList = new ArrayList<>();
                WorkloadCondition strCondition = new WorkloadConditionUsername(WorkloadConditionOperator.EQUAL, "root");
                operatorList.add(strCondition);

                WorkloadCondition intCondition = new WorkloadConditionQueryTime(WorkloadConditionOperator.EQUAL, 100);
                operatorList.add(intCondition);

                WorkloadSchedPolicy workloadSchedPolicy1 = new WorkloadSchedPolicy();
                workloadSchedPolicy1.setWorkloadConditionList(operatorList);

                WorkloadQueryInfo queryInfo = new WorkloadQueryInfo();
                queryInfo.metricMap = new HashMap<>();
                queryInfo.metricMap.put(WorkloadMetricType.USERNAME, "root");
                queryInfo.metricMap.put(WorkloadMetricType.QUERY_TIME, "100");

                // match
                Assert.assertTrue(workloadSchedPolicy1.isMatch(queryInfo));

                // not match 1
                queryInfo.metricMap.remove(WorkloadMetricType.USERNAME);
                Assert.assertFalse(workloadSchedPolicy1.isMatch(queryInfo));

                // not match 2
                queryInfo.metricMap.put(WorkloadMetricType.USERNAME, "abc");
                Assert.assertFalse(workloadSchedPolicy1.isMatch(queryInfo));
            }

    }

}
