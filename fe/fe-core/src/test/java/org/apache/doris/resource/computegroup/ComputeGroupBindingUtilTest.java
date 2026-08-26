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

package org.apache.doris.resource.computegroup;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

/**
 * Validation of the transitional {@code compute_group} declaration.
 */
public class ComputeGroupBindingUtilTest {

    private static final String CG_OK = "cg_ok";

    private String originalDeployMode;
    private String originalCloudUniqueId;
    private SystemInfoService originalSystemInfo;
    private SystemInfoService originalCgMgrSystemInfo;
    private AccessControllerManager originalAccessManager;
    private ConnectContext ctx;

    @Before
    public void setUp() {
        originalDeployMode = Config.deploy_mode;
        originalCloudUniqueId = Config.cloud_unique_id;
        originalSystemInfo = Env.getCurrentSystemInfo();
        originalCgMgrSystemInfo = Deencapsulation.getField(
                Env.getCurrentEnv().getComputeGroupMgr(), "systemInfoService");
        originalAccessManager = Env.getCurrentEnv().getAccessManager();
        ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
    }

    @After
    public void tearDown() {
        Config.deploy_mode = originalDeployMode;
        Config.cloud_unique_id = originalCloudUniqueId;
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfo);
        Deencapsulation.setField(Env.getCurrentEnv().getComputeGroupMgr(), "systemInfoService",
                originalCgMgrSystemInfo);
        Deencapsulation.setField(Env.getCurrentEnv(), "accessManager", originalAccessManager);
        ConnectContext.remove();
    }

    private void enterCloudMode(boolean hasPriv, List<String> existingClusters) {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(accessManager.checkCloudPriv(Mockito.any(UserIdentity.class), Mockito.anyString(),
                Mockito.any(PrivPredicate.class), Mockito.any(ResourceTypeEnum.class))).thenReturn(hasPriv);
        Deencapsulation.setField(Env.getCurrentEnv(), "accessManager", accessManager);
        TestCloudSystemInfoService svc = new TestCloudSystemInfoService(existingClusters);
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", svc);
        // ComputeGroupMgr captured its own reference at construction, so replacing only Env's would
        // leave getComputeGroupByName() casting the real non-cloud service.
        Deencapsulation.setField(Env.getCurrentEnv().getComputeGroupMgr(), "systemInfoService", svc);
    }

    // An empty declaration means "not declared" and must never fail, not even in non-cloud mode,
    // otherwise every existing job would break.
    @Test
    public void testEmptyDeclarationIsNoOp() throws UserException {
        Config.deploy_mode = "";
        Config.cloud_unique_id = "";
        ComputeGroupBindingUtil.validateDeclaredComputeGroup(ctx, null);
        ComputeGroupBindingUtil.validateDeclaredComputeGroup(ctx, "");
    }

    // Non-cloud is out of scope for this transitional change: declaring a compute group must be
    // rejected so that no non-cloud metadata ever carries the key.
    @Test
    public void testRejectedInNonCloudMode() {
        Config.deploy_mode = "";
        Config.cloud_unique_id = "";
        UserException e = Assert.assertThrows(UserException.class,
                () -> ComputeGroupBindingUtil.validateDeclaredComputeGroup(ctx, CG_OK));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("only supported in cloud mode"));
    }

    // DEFAULT is reserved by the final binding design ("follow the owner's default group").
    // Pinning a group literally named DEFAULT would silently change behaviour after upgrading.
    @Test
    public void testRejectReservedDefaultValue() {
        enterCloudMode(true, Lists.newArrayList("DEFAULT", "default", CG_OK));
        for (String reserved : new String[] {"DEFAULT", "default", "Default"}) {
            UserException e = Assert.assertThrows(UserException.class,
                    () -> ComputeGroupBindingUtil.validateDeclaredComputeGroup(ctx, reserved));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("reserved value"));
        }
    }

    @Test
    public void testRejectWhenNoUsagePrivilege() {
        enterCloudMode(false, Lists.newArrayList(CG_OK));
        UserException e = Assert.assertThrows(UserException.class,
                () -> ComputeGroupBindingUtil.validateDeclaredComputeGroup(ctx, CG_OK));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("USAGE denied"));
    }

    @Test
    public void testRejectWhenComputeGroupDoesNotExist() {
        enterCloudMode(true, Lists.newArrayList("some_other_cg"));
        UserException e = Assert.assertThrows(UserException.class,
                () -> ComputeGroupBindingUtil.validateDeclaredComputeGroup(ctx, CG_OK));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("not found"));
    }

    @Test
    public void testAcceptValidDeclaration() throws UserException {
        enterCloudMode(true, Lists.newArrayList(CG_OK, "cg_other"));
        ComputeGroupBindingUtil.validateDeclaredComputeGroup(ctx, CG_OK);
    }

    // The privilege check must run before the existence check, so that probing for the existence of
    // a compute group the user has no access to is not possible.
    @Test
    public void testPrivilegeIsCheckedBeforeExistence() {
        enterCloudMode(false, Lists.newArrayList("some_other_cg"));
        UserException e = Assert.assertThrows(UserException.class,
                () -> ComputeGroupBindingUtil.validateDeclaredComputeGroup(ctx, CG_OK));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("USAGE denied"));
        Assert.assertFalse(e.getMessage(), e.getMessage().contains("not found"));
    }

    // ---- checkComputeGroupBeforeTask: re-checked before every task, against the job's owner ----

    // Jobs created before the owner was persisted have nothing to check against.
    @Test
    public void testRuntimeCheckSkippedWhenOwnerUnknown() throws UserException {
        enterCloudMode(false, Lists.newArrayList());
        ComputeGroupBindingUtil.checkComputeGroupBeforeTask(null, "cg_gone");
    }

    // Nothing is bound, so there is nothing to re-check.
    @Test
    public void testRuntimeCheckSkippedWhenNothingBound() throws UserException {
        enterCloudMode(false, Lists.newArrayList());
        ComputeGroupBindingUtil.checkComputeGroupBeforeTask(UserIdentity.ADMIN, null);
    }

    // The compute group was dropped while the job kept running.
    //
    // This runs without a thread-local ConnectContext on purpose: the callers are background
    // threads (the routine load scheduler, the MV task runner) that have none, so the check must
    // not reach any code path that needs one.
    @Test
    public void testRuntimeCheckFailsWhenComputeGroupDropped() {
        enterCloudMode(true, Lists.newArrayList());
        ConnectContext.remove();
        UserException e = Assert.assertThrows(UserException.class,
                () -> ComputeGroupBindingUtil.checkComputeGroupBeforeTask(UserIdentity.ADMIN, "cg_gone"));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("not found"));
    }

    // The owner's USAGE on the compute group was revoked while the job kept running. This is the
    // case creation-time validation alone cannot catch.
    @Test
    public void testRuntimeCheckFailsWhenComputeGroupUsageRevoked() {
        enterCloudMode(false, Lists.newArrayList(CG_OK));
        UserException e = Assert.assertThrows(UserException.class,
                () -> ComputeGroupBindingUtil.checkComputeGroupBeforeTask(UserIdentity.ADMIN, CG_OK));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("compute group"));
    }

    // Non-cloud has no named compute group, so the check is skipped entirely.
    @Test
    public void testRuntimeCheckSkipsComputeGroupInNonCloudMode() throws UserException {
        Config.deploy_mode = "";
        Config.cloud_unique_id = "";
        ComputeGroupBindingUtil.checkComputeGroupBeforeTask(UserIdentity.ADMIN, "cg_gone");
    }

    private static class TestCloudSystemInfoService extends CloudSystemInfoService {
        private final List<String> clusterNames;

        private TestCloudSystemInfoService(List<String> clusterNames) {
            this.clusterNames = clusterNames;
        }

        @Override
        public List<String> getCloudClusterNames() {
            return clusterNames;
        }

        // getComputeGroupByName() resolves through these two, so a known name has to look
        // resolvable here or the existence check fires before anything else can be exercised.
        @Override
        public String getPhysicalCluster(String clusterName) {
            return clusterName;
        }

        @Override
        public String getCloudClusterIdByName(String clusterName) {
            return clusterNames.contains(clusterName) ? clusterName + "_id" : "";
        }
    }
}
