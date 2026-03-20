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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.EditLog;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class WorkloadSchedPolicyMgrTest {

    @Mocked
    private Env env;

    @Injectable
    private EditLog editLog;

    @Before
    public void setUp() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };
    }

    @Test
    public void testCheckPolicyCondition() {
        WorkloadSchedPolicyMgr mgr = new WorkloadSchedPolicyMgr();

        // Case 1: USERNAME (Shared) + BE Metric + BE Action -> OK
        try {
            List<WorkloadConditionMeta> conditionMetas = new ArrayList<>();
            conditionMetas.add(new WorkloadConditionMeta("username", "=", "user1"));
            conditionMetas.add(new WorkloadConditionMeta("be_scan_rows", ">", "1000"));

            List<WorkloadActionMeta> actionMetas = new ArrayList<>();
            actionMetas.add(new WorkloadActionMeta("cancel_query", ""));

            mgr.createWorkloadSchedPolicy("policy_mixed_be", false, conditionMetas, actionMetas, null);
        } catch (UserException e) {
            Assert.fail("Should not throw exception for mixed USERNAME and BE metrics: " + e.getMessage());
        }

        // Case 2: USERNAME (Shared) + FE Action -> OK
        try {
            List<WorkloadConditionMeta> conditionMetas = new ArrayList<>();
            conditionMetas.add(new WorkloadConditionMeta("username", "=", "user1"));

            List<WorkloadActionMeta> actionMetas = new ArrayList<>();
            actionMetas.add(new WorkloadActionMeta("set_session_variable", "workload_group=normal"));

            mgr.createWorkloadSchedPolicy("policy_fe_only", false, conditionMetas, actionMetas, null);
        } catch (UserException e) {
            Assert.fail("Should not throw exception for USERNAME + FE Action: " + e.getMessage());
        }

        // Case 3: USERNAME (Shared) + BE Action -> OK
        try {
            List<WorkloadConditionMeta> conditionMetas = new ArrayList<>();
            conditionMetas.add(new WorkloadConditionMeta("username", "=", "user1"));

            List<WorkloadActionMeta> actionMetas = new ArrayList<>();
            actionMetas.add(new WorkloadActionMeta("cancel_query", ""));

            mgr.createWorkloadSchedPolicy("policy_username_be_action", false, conditionMetas, actionMetas, null);
        } catch (UserException e) {
            Assert.fail("Should not throw exception for USERNAME + BE Action: " + e.getMessage());
        }

        // Case 4: BE Metric + FE Action -> Error
        try {
            List<WorkloadConditionMeta> conditionMetas = new ArrayList<>();
            conditionMetas.add(new WorkloadConditionMeta("query_time", ">", "1000"));

            List<WorkloadActionMeta> actionMetas = new ArrayList<>();
            actionMetas.add(new WorkloadActionMeta("set_session_variable", "workload_group=normal"));

            mgr.createWorkloadSchedPolicy("policy_error_1", false, conditionMetas, actionMetas, null);
            Assert.fail("Should throw exception for BE Metric + FE Action");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("action and metric must run in FE together or run in BE together"));
        }

        // Case 5: USERNAME + BE Metric + FE Action -> Error
        try {
            List<WorkloadConditionMeta> conditionMetas = new ArrayList<>();
            conditionMetas.add(new WorkloadConditionMeta("username", "=", "user1"));
            conditionMetas.add(new WorkloadConditionMeta("query_time", ">", "1000"));

            List<WorkloadActionMeta> actionMetas = new ArrayList<>();
            actionMetas.add(new WorkloadActionMeta("set_session_variable", "workload_group=normal"));

            mgr.createWorkloadSchedPolicy("policy_error_2", false, conditionMetas, actionMetas, null);
            Assert.fail("Should throw exception for USERNAME + BE Metric + FE Action");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("action and metric must run in FE together or run in BE together"));
        }
    }

    @Test
    public void testCheckProperties() throws UserException {
        WorkloadSchedPolicyMgr mgr = new WorkloadSchedPolicyMgr();
        List<WorkloadConditionMeta> conditionMetas = new ArrayList<>();
        conditionMetas.add(new WorkloadConditionMeta("username", "=", "user1"));
        List<WorkloadActionMeta> actionMetas = new ArrayList<>();
        actionMetas.add(new WorkloadActionMeta("cancel_query", ""));

        // Test valid priority
        try {
            java.util.Map<String, String> props = new java.util.HashMap<>();
            props.put("priority", "10");
            props.put("enabled", "true");
            mgr.createWorkloadSchedPolicy("policy_prop_valid", false, conditionMetas, actionMetas, props);
        } catch (UserException e) {
            Assert.fail("Should not throw exception for valid properties: " + e.getMessage());
        }

        // Test invalid priority
        try {
            java.util.Map<String, String> props = new java.util.HashMap<>();
            props.put("priority", "101");
            mgr.createWorkloadSchedPolicy("policy_prop_invalid_prio", false, conditionMetas, actionMetas, props);
            Assert.fail("Should throw exception for invalid priority");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("policy's priority can only between 0 ~ 100"));
        }

        // Test invalid enabled
        try {
            java.util.Map<String, String> props = new java.util.HashMap<>();
            props.put("enabled", "yes");
            mgr.createWorkloadSchedPolicy("policy_prop_invalid_enabled", false, conditionMetas, actionMetas, props);
            Assert.fail("Should throw exception for invalid enabled");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("invalid enabled property value"));
        }
    }
}
