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

import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the workload_group property format enforced by
 * {@link WorkloadSchedPolicyMgr#checkProperties(Map, List)}.
 *
 * The contract:
 *
 *   - Cloud mode    : workload_group must be '<compute_group>.<workload_group>'.
 *   - Non-cloud mode: workload_group may be '<workload_group>' (defaulting the
 *                     resource group to Tag.VALUE_DEFAULT_TAG) or the
 *                     '<resource_group>.<workload_group>' form — the dotted
 *                     prefix is a resource group (Tag) here, sharing the cloud-mode
 *                     grammar purely for consistency.
 *
 * Invalid forms must be rejected BEFORE any compute-group lookup, so the
 * rejection is exercisable here without bootstrapping the full Env.
 */
public class WorkloadSchedPolicyMgrTest {

    private String originDeployMode;
    private String originCloudUniqueId;
    private WorkloadSchedPolicyMgr mgr;

    @Before
    public void setUp() {
        originDeployMode = Config.deploy_mode;
        originCloudUniqueId = Config.cloud_unique_id;
        mgr = new WorkloadSchedPolicyMgr();
    }

    @After
    public void tearDown() {
        Config.deploy_mode = originDeployMode;
        Config.cloud_unique_id = originCloudUniqueId;
    }

    private Map<String, String> propsWith(String workloadGroupValue) {
        Map<String, String> p = new HashMap<>();
        p.put(WorkloadSchedPolicy.WORKLOAD_GROUP, workloadGroupValue);
        return p;
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
