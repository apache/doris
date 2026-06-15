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
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.EditLog;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for workload schedule policy validation paths.
 */
public class WorkloadSchedPolicyMgrTest {

    private Env env;
    private EditLog editLog;
    private MockedStatic<Env> mockedEnv;
    private String originDeployMode;
    private String originCloudUniqueId;
    private WorkloadSchedPolicyMgr mgr;

    @Before
    public void setUp() {
        originDeployMode = Config.deploy_mode;
        originCloudUniqueId = Config.cloud_unique_id;
        mgr = new WorkloadSchedPolicyMgr();
        env = Mockito.mock(Env.class);
        editLog = Mockito.mock(EditLog.class);
        mockedEnv = Mockito.mockStatic(Env.class);

        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
    }

    @After
    public void tearDown() {
        Config.deploy_mode = originDeployMode;
        Config.cloud_unique_id = originCloudUniqueId;
        if (mockedEnv != null) {
            mockedEnv.close();
        }
    }

    private Map<String, String> propsWith(String workloadGroupValue) {
        Map<String, String> p = new HashMap<>();
        p.put(WorkloadSchedPolicy.WORKLOAD_GROUP, workloadGroupValue);
        return p;
    }

    @Test
    public void testCheckPolicyCondition() {
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
        List<WorkloadConditionMeta> conditionMetas = new ArrayList<>();
        conditionMetas.add(new WorkloadConditionMeta("username", "=", "user1"));
        List<WorkloadActionMeta> actionMetas = new ArrayList<>();
        actionMetas.add(new WorkloadActionMeta("cancel_query", ""));

        // Test valid priority.
        try {
            Map<String, String> props = new HashMap<>();
            props.put("priority", "10");
            props.put("enabled", "true");
            mgr.createWorkloadSchedPolicy("policy_prop_valid", false, conditionMetas, actionMetas, props);
        } catch (UserException e) {
            Assert.fail("Should not throw exception for valid properties: " + e.getMessage());
        }

        // Test invalid priority.
        try {
            Map<String, String> props = new HashMap<>();
            props.put("priority", "101");
            mgr.createWorkloadSchedPolicy("policy_prop_invalid_prio", false, conditionMetas, actionMetas, props);
            Assert.fail("Should throw exception for invalid priority");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("policy's priority can only between 0 ~ 100"));
        }

        // Test invalid enabled.
        try {
            Map<String, String> props = new HashMap<>();
            props.put("enabled", "yes");
            mgr.createWorkloadSchedPolicy("policy_prop_invalid_enabled", false, conditionMetas, actionMetas, props);
            Assert.fail("Should throw exception for invalid enabled");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("invalid enabled property value"));
        }
    }

    @Test
    public void testUsernameConditionRejectsBlankValue() throws UserException {
        List<WorkloadConditionMeta> conditionMetas = new ArrayList<>();
        List<WorkloadActionMeta> actionMetas = new ArrayList<>();
        actionMetas.add(new WorkloadActionMeta("cancel_query", ""));

        // Reject an explicit empty username to avoid matching queries without user metadata.
        try {
            conditionMetas.add(new WorkloadConditionMeta("username", "=", ""));
            mgr.createWorkloadSchedPolicy("policy_empty_username", false, conditionMetas, actionMetas, null);
            Assert.fail("Should throw exception for empty username");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("username can not be empty"));
        }

        conditionMetas.clear();

        // Reject a blank username for the same reason.
        try {
            conditionMetas.add(new WorkloadConditionMeta("username", "=", "   "));
            mgr.createWorkloadSchedPolicy("policy_blank_username", false, conditionMetas, actionMetas, null);
            Assert.fail("Should throw exception for blank username");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("username can not be empty"));
        }
    }

    @Test
    public void testCloudModeRejectsUnqualifiedWorkloadGroup() {
        Config.cloud_unique_id = "ut_cloud";
        Assert.assertTrue(Config.isCloudMode());

        try {
            mgr.checkProperties(propsWith("superset"), new ArrayList<>());
            Assert.fail("expected UserException for unqualified workload_group in cloud mode");
        } catch (UserException e) {
            Assert.assertTrue("message should mention <compute_group>.<workload_group>; got: " + e.getMessage(),
                    e.getMessage().contains("<compute_group>.<workload_group>"));
            Assert.assertTrue("message should mention cloud mode; got: " + e.getMessage(),
                    e.getMessage().contains("cloud mode"));
        }
    }

    @Test
    public void testCloudModeRejectsTooManyDotsInWorkloadGroup() {
        Config.cloud_unique_id = "ut_cloud";
        Assert.assertTrue(Config.isCloudMode());

        try {
            mgr.checkProperties(propsWith("etl.superset.extra"), new ArrayList<>());
            Assert.fail("expected UserException for over-qualified workload_group in cloud mode");
        } catch (UserException e) {
            Assert.assertTrue("message should mention <compute_group>.<workload_group>; got: " + e.getMessage(),
                    e.getMessage().contains("<compute_group>.<workload_group>"));
        }
    }

    @Test
    public void testNonCloudModeRejectsTooManyDotsInWorkloadGroup() {
        // The '<resource_group>.<workload_group>' form is allowed in non-cloud mode, but
        // anything with more than one dot is still ambiguous and must be rejected before
        // any lookup.
        Config.deploy_mode = "share_nothing";
        Config.cloud_unique_id = "";
        Assert.assertFalse(Config.isCloudMode());

        try {
            mgr.checkProperties(propsWith("etl.superset.extra"), new ArrayList<>());
            Assert.fail("expected UserException for over-qualified workload_group in non-cloud mode");
        } catch (UserException e) {
            Assert.assertTrue("message should mention the allowed forms; got: " + e.getMessage(),
                    e.getMessage().contains("<workload_group>"));
            Assert.assertTrue("message should mention non-cloud mode; got: " + e.getMessage(),
                    e.getMessage().contains("non-cloud mode"));
        }
    }

    @Test
    public void testEmptyOrMissingWorkloadGroupPropertyIsAccepted() throws Exception {
        Config.cloud_unique_id = "ut_cloud";
        Assert.assertTrue(Config.isCloudMode());

        // Absent property is OK: the workload_group binding is simply not set.
        mgr.checkProperties(new HashMap<>(), new ArrayList<>());

        // Explicit empty string is OK too: ignored like absent.
        mgr.checkProperties(propsWith(""), new ArrayList<>());
    }

    @Test
    public void testCloudModeRejectsTrailingDotInWorkloadGroup() {
        Config.cloud_unique_id = "ut_cloud";
        Assert.assertTrue(Config.isCloudMode());

        // "etl." splits to ["etl", ""] under split(".", -1); the empty workload-group
        // segment must be rejected before reaching the compute-group lookup.
        try {
            mgr.checkProperties(propsWith("etl."), new ArrayList<>());
            Assert.fail("expected UserException for trailing-dot workload_group in cloud mode");
        } catch (UserException e) {
            Assert.assertTrue("message should mention <compute_group>.<workload_group>; got: " + e.getMessage(),
                    e.getMessage().contains("<compute_group>.<workload_group>"));
        }
    }

    @Test
    public void testCloudModeRejectsLeadingDotInWorkloadGroup() {
        Config.cloud_unique_id = "ut_cloud";
        Assert.assertTrue(Config.isCloudMode());

        // ".superset" splits to ["", "superset"]; the empty compute-group segment must
        // be rejected rather than falling through with an empty cg name.
        try {
            mgr.checkProperties(propsWith(".superset"), new ArrayList<>());
            Assert.fail("expected UserException for leading-dot workload_group in cloud mode");
        } catch (UserException e) {
            Assert.assertTrue("message should mention <compute_group>.<workload_group>; got: " + e.getMessage(),
                    e.getMessage().contains("<compute_group>.<workload_group>"));
        }
    }

    @Test
    public void testNonCloudModeRejectsTrailingDotInWorkloadGroup() {
        Config.deploy_mode = "share_nothing";
        Config.cloud_unique_id = "";
        Assert.assertFalse(Config.isCloudMode());

        // "wg." splits to ["wg", ""]; previously split("\\.") would drop the trailing
        // empty segment and let this pass. With split(..., -1) the empty workload-group
        // component is detected and rejected before lookup.
        try {
            mgr.checkProperties(propsWith("wg."), new ArrayList<>());
            Assert.fail("expected UserException for trailing-dot workload_group in non-cloud mode");
        } catch (UserException e) {
            Assert.assertTrue("message should mention the allowed forms; got: " + e.getMessage(),
                    e.getMessage().contains("<workload_group>"));
            Assert.assertTrue("message should mention non-cloud mode; got: " + e.getMessage(),
                    e.getMessage().contains("non-cloud mode"));
        }
    }

    @Test
    public void testNonCloudModeRejectsLeadingDotInWorkloadGroup() {
        Config.deploy_mode = "share_nothing";
        Config.cloud_unique_id = "";
        Assert.assertFalse(Config.isCloudMode());

        // ".wg" splits to ["", "wg"]; the empty resource-group component must be
        // rejected rather than falling through with an empty cg name.
        try {
            mgr.checkProperties(propsWith(".wg"), new ArrayList<>());
            Assert.fail("expected UserException for leading-dot workload_group in non-cloud mode");
        } catch (UserException e) {
            Assert.assertTrue("message should mention the allowed forms; got: " + e.getMessage(),
                    e.getMessage().contains("<workload_group>"));
            Assert.assertTrue("message should mention non-cloud mode; got: " + e.getMessage(),
                    e.getMessage().contains("non-cloud mode"));
        }
    }
}
