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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.job.extensions.mtmv.MTMVTaskContext;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.persist.AlterMTMV;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

/**
 * The transitional {@code compute_group} declaration on async materialized views.
 *
 * <p>A declared compute group pins every refresh - automatic and manual alike - to that group.
 * Only when nothing is declared does a manual REFRESH keep borrowing the triggering session's group,
 * which is the behaviour every existing MV keeps.
 */
public class MTMVComputeGroupTest {

    private static final String DECLARED_CG = "cg_declared";
    private static final String SESSION_CG = "cg_from_session";

    private String originalDeployMode;
    private String originalCloudUniqueId;
    private SystemInfoService originalSystemInfo;

    @BeforeEach
    public void setUp() {
        originalDeployMode = Config.deploy_mode;
        originalCloudUniqueId = Config.cloud_unique_id;
        originalSystemInfo = Env.getCurrentSystemInfo();
    }

    @AfterEach
    public void tearDown() {
        Config.deploy_mode = originalDeployMode;
        Config.cloud_unique_id = originalCloudUniqueId;
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfo);
        ConnectContext.remove();
    }

    private MTMV newMTMV(String declaredComputeGroup) {
        MTMV mtmv = new MTMV();
        Map<String, String> mvProperties = Maps.newHashMap();
        if (declaredComputeGroup != null) {
            mvProperties.put(PropertyAnalyzer.PROPERTIES_COMPUTE_GROUP, declaredComputeGroup);
        }
        mtmv.setMvProperties(mvProperties);
        return mtmv;
    }

    private MTMVTask newTask(MTMV mtmv, MTMVTaskTriggerMode triggerMode, String taskContextComputeGroup) {
        MTMVTaskContext taskContext = MTMVTaskContext.of(
                triggerMode, Lists.newArrayList(), RefreshMode.AUTO, true, taskContextComputeGroup);
        return new MTMVTask(mtmv, null, taskContext);
    }

    private String resolveComputeGroup(MTMVTask task) {
        ConnectContext ctx = new ConnectContext();
        Deencapsulation.invoke(task, "applyComputeGroup", ctx);
        return ctx.getSessionVariable().getCloudCluster();
    }

    @Test
    public void testGetComputeGroupFromProperty() {
        Assertions.assertEquals(DECLARED_CG, newMTMV(DECLARED_CG).getComputeGroup().orElse(null));
        Assertions.assertFalse(newMTMV(null).getComputeGroup().isPresent());
        Assertions.assertFalse(newMTMV("").getComputeGroup().isPresent());
    }

    @Test
    public void testAlterComputeGroupProperty() {
        MTMV mtmv = newMTMV(DECLARED_CG);
        Map<String, String> changed = Maps.newHashMap();
        changed.put(PropertyAnalyzer.PROPERTIES_COMPUTE_GROUP, "cg_after_alter");
        AlterMTMV alterMTMV = new AlterMTMV(new TableNameInfo("db", "mv"), MTMVAlterOpType.ALTER_PROPERTY);
        alterMTMV.setMvProperties(changed);
        // A replay applies the property change without writing an edit log.
        mtmv.alterMvProperties(alterMTMV, true);
        Assertions.assertEquals("cg_after_alter", mtmv.getComputeGroup().orElse(null));
    }

    // Automatic refresh has no compute group on the task context; the declaration is what pins it.
    @Test
    public void testAutoRefreshUsesDeclaredComputeGroup() {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        MTMVTask task = newTask(newMTMV(DECLARED_CG), MTMVTaskTriggerMode.SYSTEM, null);
        Assertions.assertEquals(DECLARED_CG, resolveComputeGroup(task));
    }

    // The one deliberate behaviour change: a declared compute group also wins for a manual REFRESH,
    // so the same MV never refreshes in two different places depending on who triggered it.
    @Test
    public void testManualRefreshPrefersDeclaredOverSessionComputeGroup() {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        MTMVTask task = newTask(newMTMV(DECLARED_CG), MTMVTaskTriggerMode.MANUAL, SESSION_CG);
        Assertions.assertEquals(DECLARED_CG, resolveComputeGroup(task));
    }

    // Existing MVs declare nothing, so a manual REFRESH must keep borrowing the session's group.
    @Test
    public void testManualRefreshFallsBackToSessionComputeGroup() {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        MTMVTask task = newTask(newMTMV(null), MTMVTaskTriggerMode.MANUAL, SESSION_CG);
        Assertions.assertEquals(SESSION_CG, resolveComputeGroup(task));
    }

    // Nothing declared and nothing on the task context leaves the context untouched, which keeps the
    // existing implicit resolution (admin's default compute group, or one picked by policy).
    @Test
    public void testAutoRefreshWithoutDeclarationLeavesContextUntouched() {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        MTMVTask task = newTask(newMTMV(null), MTMVTaskTriggerMode.SYSTEM, null);
        // untouched, i.e. left at the session variable's default so the implicit resolution applies
        Assertions.assertTrue(Strings.isNullOrEmpty(resolveComputeGroup(task)));
    }

    // Non-cloud is out of scope; the declaration can not be created there, and even if metadata
    // somehow carried one it must not touch the context.
    @Test
    public void testNoopInNonCloudMode() {
        Config.deploy_mode = "";
        Config.cloud_unique_id = "";
        MTMVTask task = newTask(newMTMV(DECLARED_CG), MTMVTaskTriggerMode.MANUAL, SESSION_CG);
        Assertions.assertTrue(Strings.isNullOrEmpty(resolveComputeGroup(task)));
    }

    /**
     * Isolation: two MVs declaring different compute groups resolve to different clusters, which is
     * what makes their refreshes pick disjoint backend sets downstream.
     */
    @Test
    public void testMvsWithDifferentComputeGroupsResolveIndependently() {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        MTMVTask taskA = newTask(newMTMV("cg_a"), MTMVTaskTriggerMode.SYSTEM, null);
        MTMVTask taskB = newTask(newMTMV("cg_b"), MTMVTaskTriggerMode.SYSTEM, null);

        String resolvedA = resolveComputeGroup(taskA);
        String resolvedB = resolveComputeGroup(taskB);
        Assertions.assertEquals("cg_a", resolvedA);
        Assertions.assertEquals("cg_b", resolvedB);
        Assertions.assertNotEquals(resolvedA, resolvedB);
    }

    // Both refresh paths, IVM incremental and partition / complete, set their context up through
    // setupComputeGroup(), which must apply the declaration and record the group the refresh runs in.
    @Test
    public void testRefreshContextSetupAppliesAndRecordsDeclaredComputeGroup() {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        MTMVTask task = newTask(newMTMV(DECLARED_CG), MTMVTaskTriggerMode.MANUAL, SESSION_CG);
        ConnectContext ctx = new ConnectContext();
        Deencapsulation.invoke(task, "setupComputeGroup", ctx);
        Assertions.assertEquals(DECLARED_CG, ctx.getSessionVariable().getCloudCluster());
        Assertions.assertEquals(DECLARED_CG, Deencapsulation.getField(task, "computeGroup"));
    }

    // A refresh re-checks the group the MV declares, which can be dropped while the MV exists. An MV
    // that declares none is not checked: it never chose the group it borrows from the session.
    @Test
    public void testRefreshRechecksOnlyTheDeclaredComputeGroup() throws Exception {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        CloudSystemInfoService systemInfo = Mockito.mock(CloudSystemInfoService.class);
        Mockito.when(systemInfo.getCloudClusterNames()).thenReturn(Lists.newArrayList(SESSION_CG));
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", systemInfo);
        // both refresh paths set their context up, and re-check it, through setupComputeGroup()
        Method check = MTMVTask.class.getDeclaredMethod("setupComputeGroup", ConnectContext.class);
        check.setAccessible(true);
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);

        MTMVTask declaredButDropped = newTask(newMTMV(DECLARED_CG), MTMVTaskTriggerMode.SYSTEM, null);
        InvocationTargetException e = Assertions.assertThrows(InvocationTargetException.class,
                () -> check.invoke(declaredButDropped, ctx));
        Assertions.assertInstanceOf(UserException.class, e.getCause());
        Assertions.assertTrue(e.getCause().getMessage().contains("Compute group '" + DECLARED_CG + "' not found."),
                e.getCause().getMessage());

        check.invoke(newTask(newMTMV(null), MTMVTaskTriggerMode.MANUAL, "cg_dropped_from_session"), ctx);
    }

    /**
     * A refresh context is re-checked against the group it is bound to.
     *
     * <p>The declaration is read once per context. The check used to read it a second time, so an ALTER
     * MATERIALIZED VIEW landing in between had it pass for the new group while the refresh ran in the old one.
     * When the old group had been dropped, which is what such an ALTER is often for, the refresh then failed
     * later on a generic "no backend" error instead of the "not found" this check exists to give.
     */
    @Test
    public void testRefreshContextChecksTheGroupItBinds() {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        // cg_a has been dropped, cg_b exists
        CloudSystemInfoService systemInfo = Mockito.mock(CloudSystemInfoService.class);
        Mockito.when(systemInfo.getCloudClusterNames()).thenReturn(Lists.newArrayList("cg_b"));
        Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", systemInfo);
        // the first read sees cg_a, any later one the ALTER to cg_b that lands right after it
        MTMV mtmv = Mockito.spy(newMTMV("cg_a"));
        Mockito.doReturn(Optional.of("cg_a"), Optional.of("cg_b")).when(mtmv).getComputeGroup();
        MTMVTask task = newTask(mtmv, MTMVTaskTriggerMode.SYSTEM, null);
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);

        UserException e = Assertions.assertThrows(UserException.class,
                () -> Deencapsulation.invoke(task, "setupComputeGroup", ctx));
        Assertions.assertTrue(e.getMessage().contains("Compute group 'cg_a' not found."), e.getMessage());
        Assertions.assertEquals("cg_a", ctx.getSessionVariable().getCloudCluster());
        Mockito.verify(mtmv, Mockito.times(1)).getComputeGroup();
    }
}
