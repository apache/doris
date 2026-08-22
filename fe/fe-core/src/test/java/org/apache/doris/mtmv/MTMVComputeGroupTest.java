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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.job.extensions.mtmv.MTMVTaskContext;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

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

    @Before
    public void setUp() {
        originalDeployMode = Config.deploy_mode;
        originalCloudUniqueId = Config.cloud_unique_id;
    }

    @After
    public void tearDown() {
        Config.deploy_mode = originalDeployMode;
        Config.cloud_unique_id = originalCloudUniqueId;
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
        MTMVTaskContext taskContext = new MTMVTaskContext(
                triggerMode, Lists.newArrayList(), false, taskContextComputeGroup);
        return new MTMVTask(mtmv, null, taskContext);
    }

    private String resolveComputeGroup(MTMVTask task) {
        ConnectContext ctx = new ConnectContext();
        Deencapsulation.invoke(task, "setComputeGroup", ctx);
        return ctx.getSessionVariable().getCloudCluster();
    }

    @Test
    public void testGetComputeGroupFromProperty() {
        Assert.assertEquals(DECLARED_CG, newMTMV(DECLARED_CG).getComputeGroup().orElse(null));
        Assert.assertFalse(newMTMV(null).getComputeGroup().isPresent());
        Assert.assertFalse(newMTMV("").getComputeGroup().isPresent());
    }

    @Test
    public void testAlterComputeGroupProperty() {
        MTMV mtmv = newMTMV(DECLARED_CG);
        Map<String, String> changed = Maps.newHashMap();
        changed.put(PropertyAnalyzer.PROPERTIES_COMPUTE_GROUP, "cg_after_alter");
        mtmv.alterMvProperties(changed);
        Assert.assertEquals("cg_after_alter", mtmv.getComputeGroup().orElse(null));
    }

    // Automatic refresh has no compute group on the task context; the declaration is what pins it.
    @Test
    public void testAutoRefreshUsesDeclaredComputeGroup() {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        MTMVTask task = newTask(newMTMV(DECLARED_CG), MTMVTaskTriggerMode.SYSTEM, null);
        Assert.assertEquals(DECLARED_CG, resolveComputeGroup(task));
    }

    // The one deliberate behaviour change: a declared compute group also wins for a manual REFRESH,
    // so the same MV never refreshes in two different places depending on who triggered it.
    @Test
    public void testManualRefreshPrefersDeclaredOverSessionComputeGroup() {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        MTMVTask task = newTask(newMTMV(DECLARED_CG), MTMVTaskTriggerMode.MANUAL, SESSION_CG);
        Assert.assertEquals(DECLARED_CG, resolveComputeGroup(task));
    }

    // Existing MVs declare nothing, so a manual REFRESH must keep borrowing the session's group.
    @Test
    public void testManualRefreshFallsBackToSessionComputeGroup() {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        MTMVTask task = newTask(newMTMV(null), MTMVTaskTriggerMode.MANUAL, SESSION_CG);
        Assert.assertEquals(SESSION_CG, resolveComputeGroup(task));
    }

    // Nothing declared and nothing on the task context leaves the context untouched, which keeps the
    // existing implicit resolution (admin's default compute group, or one picked by policy).
    @Test
    public void testAutoRefreshWithoutDeclarationLeavesContextUntouched() {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        MTMVTask task = newTask(newMTMV(null), MTMVTaskTriggerMode.SYSTEM, null);
        // untouched, i.e. left at the session variable's default so the implicit resolution applies
        Assert.assertTrue(Strings.isNullOrEmpty(resolveComputeGroup(task)));
    }

    // Non-cloud is out of scope; the declaration can not be created there, and even if metadata
    // somehow carried one it must not touch the context.
    @Test
    public void testNoopInNonCloudMode() {
        Config.deploy_mode = "";
        Config.cloud_unique_id = "";
        MTMVTask task = newTask(newMTMV(DECLARED_CG), MTMVTaskTriggerMode.MANUAL, SESSION_CG);
        Assert.assertTrue(Strings.isNullOrEmpty(resolveComputeGroup(task)));
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
        Assert.assertEquals("cg_a", resolvedA);
        Assert.assertEquals("cg_b", resolvedB);
        Assert.assertNotEquals(resolvedA, resolvedB);
    }
}
