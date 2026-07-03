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

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.job.extensions.mtmv.MTMVTaskContext;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mtmv.ivm.IvmException;
import org.apache.doris.mtmv.ivm.IvmFailureReason;
import org.apache.doris.mtmv.ivm.IvmInfo;
import org.apache.doris.mtmv.ivm.IvmNormalizeResult;
import org.apache.doris.mtmv.ivm.IvmPlanSignature;
import org.apache.doris.mtmv.ivm.IvmPlanSignatureGenerator;
import org.apache.doris.mtmv.ivm.IvmRefreshManager;
import org.apache.doris.mtmv.ivm.IvmRefreshResult;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.commands.UpdateMvByPartitionCommand;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MTMVTaskTest {
    private String poneName = "p1";
    private String ptwoName = "p2";
    private List<String> allPartitionNames = Lists.newArrayList(poneName, ptwoName);
    private MTMVRelation relation = new MTMVRelation(Sets.newHashSet(), Sets.newHashSet(), Sets.newHashSet(),
            Sets.newHashSet(), Sets.newHashSet());

    private MTMV mtmv = Mockito.mock(MTMV.class);
    private MTMVPartitionInfo mtmvPartitionInfo = Mockito.mock(MTMVPartitionInfo.class);
    private MTMVRefreshInfo mtmvRefreshInfo = Mockito.mock(MTMVRefreshInfo.class);
    private MockedStatic<MTMVUtil> mtmvUtilStatic;
    private MockedStatic<MTMVPartitionUtil> mtmvPartitionUtilStatic;
    private static final String COMPUTE_GROUP = "ComputeGroup";
    private static final String IVM_FALLBACK_REASON = "IvmFallbackReason";
    private static final int COMPUTE_GROUP_OLD_INDEX = 19;

    @Before
    public void setUp()
            throws NoSuchMethodException, SecurityException, AnalysisException, DdlException, MetaNotFoundException {

        mtmvUtilStatic = Mockito.mockStatic(MTMVUtil.class);
        mtmvPartitionUtilStatic = Mockito.mockStatic(MTMVPartitionUtil.class);

        mtmvUtilStatic.when(() -> MTMVUtil.getMTMV(Mockito.anyLong(), Mockito.anyLong())).thenReturn(mtmv);

        Mockito.when(mtmv.getPartitionNames()).thenReturn(Sets.newHashSet(poneName, ptwoName));

        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(mtmvPartitionInfo);

        Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.FOLLOW_BASE_TABLE);
        Mockito.when(mtmvPartitionInfo.getPctTables()).thenReturn(Sets.newHashSet());

        // mtmvPartitionUtil.getPartitionsIdsByNames(mtmv, Lists.newArrayList(poneName));
        // minTimes = 0;
        // result = poneId;

        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.isMTMVSync(Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class), Mockito.nullable(Set.class))).thenReturn(true);
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class)))
                .thenReturn(Lists.newArrayList());

        Mockito.when(mtmv.getRefreshInfo()).thenReturn(mtmvRefreshInfo);

        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.COMPLETE);

        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(true);
    }

    @After
    public void tearDown() {
        mtmvUtilStatic.close();
        mtmvPartitionUtilStatic.close();
    }

    @Test
    public void testCalculateNeedRefreshPartitionsManualComplete() throws AnalysisException, JobException {
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null, RefreshMode.COMPLETE);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        List<String> result = task.calculateNeedRefreshPartitions(null);
        Assert.assertEquals(allPartitionNames, result);
    }

    @Test
    public void testCalculateNeedRefreshPartitionsManualPartitions() throws AnalysisException, JobException {
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, Lists.newArrayList(poneName),
                RefreshMode.AUTO);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        List<String> result = task.calculateNeedRefreshPartitions(null);
        Assert.assertEquals(Lists.newArrayList(poneName), result);
    }

    @Test
    public void testCalculateNeedRefreshPartitionsSystem() throws AnalysisException, JobException {
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.AUTO);
        MTMVTaskContext context = new MTMVTaskContext(MTMVTaskTriggerMode.SYSTEM);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        List<String> result = task.calculateNeedRefreshPartitions(null);
        Assert.assertTrue(CollectionUtils.isEmpty(result));
        mtmvPartitionUtilStatic.verify(() -> MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class)), Mockito.never());
    }

    @Test
    public void testCalculateNeedRefreshPartitionsSystemComplete() throws AnalysisException, JobException {
        MTMVTaskContext context = new MTMVTaskContext(MTMVTaskTriggerMode.SYSTEM);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        List<String> result = task.calculateNeedRefreshPartitions(null);
        Assert.assertEquals(allPartitionNames, result);
    }

    @Test
    public void testCalculateNeedRefreshPartitionsSystemAutoUsesPartitionDiff() throws AnalysisException, JobException {
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.AUTO);
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.isMTMVSync(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class),
                Mockito.nullable(Set.class))).thenReturn(false);
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class)))
                .thenReturn(Lists.newArrayList(ptwoName));

        MTMVTaskContext context = new MTMVTaskContext(MTMVTaskTriggerMode.SYSTEM);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        List<String> result = task.calculateNeedRefreshPartitions(null);

        Assert.assertEquals(Lists.newArrayList(ptwoName), result);
        mtmvPartitionUtilStatic.verify(() -> MTMVPartitionUtil.isMTMVSync(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class),
                Mockito.nullable(Set.class)));
    }

    @Test
    public void testCalculateNeedRefreshPartitionsManualPartitionsKeepsExplicitScope()
            throws AnalysisException, JobException {
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, Lists.newArrayList(poneName),
                RefreshMode.PARTITIONS, false, null);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        List<String> result = task.calculateNeedRefreshPartitions(null);

        Assert.assertEquals(Lists.newArrayList(poneName), result);
    }

    @Test
    public void testExplicitPartitionRefreshAlignsMvPartitions() throws Exception {
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, Lists.newArrayList(poneName),
                RefreshMode.PARTITIONS, false, null);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.alignMvPartition(mtmv))
                .thenReturn(org.apache.doris.common.Pair.of(Lists.newArrayList(), Lists.newArrayList()));
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getBaseVersions(mtmv))
                .thenReturn(new MTMVBaseVersions(Collections.emptyMap(), Collections.emptyMap()));
        Mockito.when(mtmv.calculatePartitionMappings()).thenReturn(Collections.emptyMap());
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        Object plan = Deencapsulation.invoke(task, "planPartitionRefresh", Mockito.mock(ConnectContext.class),
                Lists.newArrayList(), request);

        Assert.assertTrue(Deencapsulation.getField(plan, "canRefreshByPartitions"));
        Assert.assertEquals(Lists.newArrayList(poneName), Deencapsulation.getField(plan, "partitions"));
        mtmvPartitionUtilStatic.verify(() -> MTMVPartitionUtil.alignMvPartition(mtmv));
    }

    @Test
    public void testCompleteRefreshAlignsMvPartitions() throws Exception {
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null, RefreshMode.COMPLETE);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.alignMvPartition(mtmv))
                .thenReturn(org.apache.doris.common.Pair.of(Lists.newArrayList(), Lists.newArrayList()));
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getBaseVersions(mtmv))
                .thenReturn(new MTMVBaseVersions(Collections.emptyMap(), Collections.emptyMap()));
        Mockito.when(mtmv.calculatePartitionMappings()).thenReturn(Collections.emptyMap());
        Mockito.when(mtmv.getPartitionNames()).thenReturn(Sets.newHashSet());

        Deencapsulation.invoke(task, "executeCompleteAttempt", Mockito.mock(ConnectContext.class),
                Lists.newArrayList());

        mtmvPartitionUtilStatic.verify(() -> MTMVPartitionUtil.alignMvPartition(mtmv));
    }

    @Test
    public void testIvmRefreshAlignsMvPartitionsBeforeIncrementalAttempt() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.getQuerySql()).thenReturn("select * from t");
        Mockito.when(mtmv.getStatus()).thenReturn(new MTMVStatus(MTMVState.NORMAL, null));
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(true);

        TableIf baseTable = Mockito.mock(TableIf.class);
        Mockito.when(baseTable.getId()).thenReturn(1L);
        Pair<Set<TableIf>, Set<TableIf>> tablesInPlan = Pair.of(Sets.newHashSet(baseTable), Sets.newHashSet());
        MTMVRelation generatedRelation = new MTMVRelation(Sets.newHashSet(), Sets.newHashSet(), Sets.newHashSet(),
                Sets.newHashSet(), Sets.newHashSet());
        PartitionKeyDesc newPartition = PartitionKeyDesc.createLessThan(
                Lists.newArrayList(new PartitionValue("2020-01-01")));

        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.INCREMENTAL, false, null);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        task.before();

        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.alignMvPartition(mtmv))
                .thenReturn(Pair.of(Lists.newArrayList(), Lists.newArrayList(newPartition)));
        try (MockedStatic<MTMVPlanUtil> mtmvPlanUtilStatic = Mockito.mockStatic(MTMVPlanUtil.class);
                MockedConstruction<IvmRefreshManager> ignored = Mockito.mockConstruction(IvmRefreshManager.class,
                        (mock, constructionContext) -> Mockito.when(mock.doRefresh(mtmv)).thenReturn(
                                IvmRefreshResult.success()))) {
            mtmvPlanUtilStatic.when(() -> MTMVPlanUtil.createMTMVContext(Mockito.eq(mtmv), Mockito.anyList()))
                    .thenReturn(new ConnectContext());
            mtmvPlanUtilStatic.when(() -> MTMVPlanUtil.getBaseTableFromQuery(Mockito.eq("select * from t"),
                    Mockito.nullable(ConnectContext.class))).thenReturn(tablesInPlan);
            mtmvPlanUtilStatic.when(() -> MTMVPlanUtil.generateMTMVRelation(Mockito.eq(tablesInPlan.first),
                    Mockito.eq(tablesInPlan.second))).thenReturn(generatedRelation);

            task.run();
        }

        mtmvPartitionUtilStatic.verify(() -> MTMVPartitionUtil.alignMvPartition(mtmv));
        mtmvPartitionUtilStatic.verify(() -> MTMVPartitionUtil.addPartition(mtmv, newPartition));
    }

    @Test
    public void testPartitionRefreshReturnsNoopWhenMtmvIsFresh() throws Exception {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.alignMvPartition(mtmv))
                .thenReturn(org.apache.doris.common.Pair.of(Lists.newArrayList(), Lists.newArrayList()));
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getBaseVersions(mtmv))
                .thenReturn(new MTMVBaseVersions(Collections.emptyMap(), Collections.emptyMap()));
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.isMTMVSync(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class), Mockito.nullable(Set.class)))
                .thenReturn(true);
        Mockito.when(mtmv.calculatePartitionMappings()).thenReturn(Collections.emptyMap());

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        Object plan = Deencapsulation.invoke(task, "planPartitionRefresh", Mockito.mock(ConnectContext.class),
                Lists.newArrayList(), request);

        Assert.assertTrue(Deencapsulation.getField(plan, "canRefreshByPartitions"));
        Assert.assertTrue(((List<?>) Deencapsulation.getField(plan, "partitions")).isEmpty());
        mtmvPartitionUtilStatic.verify(() -> MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class)), Mockito.never());
    }

    @Test
    public void testPartitionRefreshFallbacksWhenFreshCheckFails() throws Exception {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.alignMvPartition(mtmv))
                .thenReturn(org.apache.doris.common.Pair.of(Lists.newArrayList(), Lists.newArrayList()));
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getBaseVersions(mtmv))
                .thenReturn(new MTMVBaseVersions(Collections.emptyMap(), Collections.emptyMap()));
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.isMTMVSync(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class), Mockito.nullable(Set.class)))
                .thenThrow(new AnalysisException("fresh check failed"));
        Mockito.when(mtmv.calculatePartitionMappings()).thenReturn(Collections.emptyMap());

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        Object plan = Deencapsulation.invoke(task, "planPartitionRefresh", Mockito.mock(ConnectContext.class),
                Lists.newArrayList(), request);

        Assert.assertFalse(Deencapsulation.getField(plan, "canRefreshByPartitions"));
        Assert.assertTrue(((String) Deencapsulation.getField(plan, "fallbackReason"))
                .contains("fresh check failed"));
        mtmvPartitionUtilStatic.verify(() -> MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class)), Mockito.never());
    }

    @Test
    public void testCalculateNeedRefreshPartitionsSystemNotSyncComplete() throws AnalysisException, JobException {
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.isMTMVSync(Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class), Mockito.nullable(Set.class))).thenReturn(false);
        MTMVTaskContext context = new MTMVTaskContext(MTMVTaskTriggerMode.SYSTEM);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        List<String> result = task.calculateNeedRefreshPartitions(null);
        Assert.assertEquals(allPartitionNames, result);
    }

    @Test
    public void testCalculateNeedRefreshPartitionsSystemAutoRefreshesNeededPartitions()
            throws AnalysisException, JobException {
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.AUTO);

        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.isMTMVSync(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class),
                Mockito.nullable(Set.class))).thenReturn(false);
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class)))
                .thenReturn(Lists.newArrayList(ptwoName));
        MTMVTaskContext context = new MTMVTaskContext(MTMVTaskTriggerMode.SYSTEM);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        List<String> result = task.calculateNeedRefreshPartitions(null);
        Assert.assertEquals(Lists.newArrayList(ptwoName), result);
    }

    @Test
    public void testManualIncrementalUsesIvmAttemptOnly() throws JobException {
        Mockito.when(mtmv.isIvm()).thenReturn(false);
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.INCREMENTAL, false, null);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = Deencapsulation.invoke(task, "buildAttempts", request);

        Assert.assertFalse(Deencapsulation.getField(request, "allowFallback"));
        Assert.assertEquals(Lists.newArrayList("IVM"), attempts.stream()
                .map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testManualIncrementalFallbackUsesFullFallbackChain() throws JobException {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.INCREMENTAL, true, null);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = Deencapsulation.invoke(task, "buildAttempts", request);

        Assert.assertTrue(Deencapsulation.getField(request, "allowFallback"));
        Assert.assertEquals(Lists.newArrayList("IVM", "PARTITIONS", "COMPLETE"), attempts.stream()
                .map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testAutoAttemptsForCompletePolicyUsesCompleteOnly() throws JobException {
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.COMPLETE);
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = Deencapsulation.invoke(task, "buildAttempts", request);

        Assert.assertEquals(Lists.newArrayList("COMPLETE"), attempts.stream()
                .map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testAutoAttemptsForPartitionsPolicyFallbacksToComplete() throws JobException {
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.PARTITIONS);
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = Deencapsulation.invoke(task, "buildAttempts", request);

        Assert.assertEquals(Lists.newArrayList("PARTITIONS", "COMPLETE"), attempts.stream()
                .map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testAutoAttemptsForIncrementalPolicyStartsFromIvm() throws JobException {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = Deencapsulation.invoke(task, "buildAttempts", request);

        Assert.assertEquals(Lists.newArrayList("IVM", "PARTITIONS", "COMPLETE"), attempts.stream()
                .map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testAutoAttemptsForAutoPartitionPolicyStartsFromPartitions() throws JobException {
        Mockito.when(mtmv.isIvm()).thenReturn(false);
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.AUTO);
        Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.FOLLOW_BASE_TABLE);
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = Deencapsulation.invoke(task, "buildAttempts", request);

        Assert.assertEquals(Lists.newArrayList("PARTITIONS", "COMPLETE"), attempts.stream()
                .map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testMvDefaultUnknownRefreshMethodRejected() throws AnalysisException {
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(null);
        MTMVTaskContext context = MTMVTaskContext.forMvDefault(MTMVTaskTriggerMode.SYSTEM);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        JobException exception = Assert.assertThrows(JobException.class,
                () -> task.calculateNeedRefreshPartitions(null));

        Assert.assertTrue(exception.getMessage().contains("unknown refresh method"));
    }

    @Test
    public void testTaskSchemaContainsComputeGroup() {
        Assert.assertEquals(COMPUTE_GROUP, MTMVTask.SCHEMA.get(COMPUTE_GROUP_OLD_INDEX).getName());
        Assert.assertEquals(COMPUTE_GROUP_OLD_INDEX,
                MTMVTask.COLUMN_TO_INDEX.get(COMPUTE_GROUP.toLowerCase()).intValue());
    }

    @Test
    public void testTaskSchemaAppendsIvmFallbackReason() {
        Column lastColumn = MTMVTask.SCHEMA.get(MTMVTask.SCHEMA.size() - 1);
        Assert.assertEquals(IVM_FALLBACK_REASON, lastColumn.getName());
        Assert.assertEquals(MTMVTask.SCHEMA.size() - 1,
                MTMVTask.COLUMN_TO_INDEX.get(IVM_FALLBACK_REASON.toLowerCase()).intValue());
    }

    @Test
    public void testGetTvfInfoReturnsComputeGroup() {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Deencapsulation.setField(task, "computeGroup", "cg1");

        TRow row = task.getTvfInfo("job1");

        Assert.assertEquals("cg1", row.getColumnValue()
                .get(MTMVTask.COLUMN_TO_INDEX.get(COMPUTE_GROUP.toLowerCase())).getStringVal());
    }

    @Test
    public void testRecordComputeGroupFromContext() {
        String originCloudUniqueId = Config.cloud_unique_id;
        try {
            Config.cloud_unique_id = "test_cloud";
            ConnectContext ctx = new ConnectContext();
            ctx.setCloudCluster("cg1");
            MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

            Deencapsulation.invoke(task, "recordComputeGroup", ctx);
            TRow row = task.getTvfInfo("job1");

            Assert.assertEquals("cg1", row.getColumnValue()
                    .get(MTMVTask.COLUMN_TO_INDEX.get(COMPUTE_GROUP.toLowerCase())).getStringVal());
        } finally {
            Config.cloud_unique_id = originCloudUniqueId;
        }
    }

    @Test
    public void testSetComputeGroupFromTaskContext() {
        String originCloudUniqueId = Config.cloud_unique_id;
        try {
            Config.cloud_unique_id = "test_cloud";
            ConnectContext ctx = new ConnectContext();
            MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                    RefreshMode.COMPLETE, true, "cg1");
            MTMVTask task = new MTMVTask(mtmv, relation, context);

            Deencapsulation.invoke(task, "setComputeGroup", ctx);

            Assert.assertEquals("cg1", ctx.getSessionVariable().getCloudCluster());
        } finally {
            Config.cloud_unique_id = originCloudUniqueId;
        }
    }

    @Test
    public void testGetTvfInfoReturnsNullStringForMissingComputeGroup() {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        TRow row = task.getTvfInfo("job1");

        Assert.assertEquals(FeConstants.null_string, row.getColumnValue()
                .get(MTMVTask.COLUMN_TO_INDEX.get(COMPUTE_GROUP.toLowerCase())).getStringVal());
    }

    @Test
    public void testDeserializeOldTaskWithoutComputeGroup() {
        MTMVTask task = GsonUtils.GSON.fromJson("{\"di\":1,\"mi\":2}", MTMVTask.class);

        TRow row = task.getTvfInfo("job1");

        Assert.assertEquals(FeConstants.null_string, row.getColumnValue()
                .get(MTMVTask.COLUMN_TO_INDEX.get(COMPUTE_GROUP.toLowerCase())).getStringVal());
    }

    @Test
    public void testExecCarriesExcludedTriggerTablesIntoStatementContext() throws Exception {
        Set<TableNameInfo> excludedTriggerTables = Sets.newHashSet(
                new TableNameInfo("internal", "test_db", "excluded_agg"));
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(excludedTriggerTables);
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.getDatabase()).thenReturn(null);
        Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.FOLLOW_BASE_TABLE);

        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        ConnectContext mtmvCtx = new ConnectContext();
        mtmvCtx.setThreadLocalInfo();

        ConnectContext executorCtx = new ConnectContext();
        executorCtx.setQueryId(new TUniqueId(1L, 2L));
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getContext()).thenReturn(executorCtx);
        UpdateMvByPartitionCommand command = Mockito.mock(UpdateMvByPartitionCommand.class);

        try (MockedStatic<MTMVPlanUtil> mtmvPlanUtilStatic = Mockito.mockStatic(MTMVPlanUtil.class);
                MockedStatic<UpdateMvByPartitionCommand> updateMvStatic
                        = Mockito.mockStatic(UpdateMvByPartitionCommand.class)) {
            mtmvPlanUtilStatic.when(() -> MTMVPlanUtil.createMTMVContext(Mockito.eq(mtmv), Mockito.anyList()))
                    .thenReturn(mtmvCtx);
            updateMvStatic.when(() -> UpdateMvByPartitionCommand.from(
                    Mockito.eq(mtmv), Mockito.anySet(), Mockito.anyMap(), Mockito.any(StatementContext.class)))
                    .thenAnswer(new Answer<UpdateMvByPartitionCommand>() {
                        @Override
                        public UpdateMvByPartitionCommand answer(InvocationOnMock invocation) {
                            StatementContext statementContext = invocation.getArgument(3);
                            Assert.assertEquals(excludedTriggerTables, statementContext.getExcludedTriggerTables());
                            return command;
                        }
                    });
            mtmvPlanUtilStatic.when(() -> MTMVPlanUtil.executeCommand(
                    Mockito.eq(mtmvCtx), Mockito.eq(command), Mockito.any(StatementContext.class),
                    Mockito.anyString(), Mockito.eq(true), Mockito.any())).thenAnswer(new Answer<StmtExecutor>() {
                        @Override
                        public StmtExecutor answer(InvocationOnMock invocation) {
                            StatementContext statementContext = invocation.getArgument(2);
                            Assert.assertEquals(excludedTriggerTables, statementContext.getExcludedTriggerTables());
                            return executor;
                        }
                    });

            Deencapsulation.invoke(task, "exec", Sets.newHashSet(poneName), Collections.emptyMap());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testTaskInfoContainsIvmFallbackReasonColumn() {
        Mockito.when(mtmv.getQualifiedDbName()).thenReturn("test_db");
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Deencapsulation.setField(task, "dbId", 1L);
        Deencapsulation.setField(task, "mtmvId", 2L);
        Deencapsulation.setField(task, "ivmFallbackReason", IvmFailureReason.BINLOG_NOT_ENABLED.name());

        List<TCell> cells = task.getTvfInfo("job").getColumnValue();

        int columnIndex = MTMVTask.COLUMN_TO_INDEX.get("ivmfallbackreason");
        Assert.assertEquals(MTMVTask.SCHEMA.size(), cells.size());
        Assert.assertEquals(IvmFailureReason.BINLOG_NOT_ENABLED.name(), cells.get(columnIndex).getStringVal());
    }

    @Test
    public void testExecuteIvmAttemptRecordsFallbackReason() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(true);
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        try (MockedConstruction<IvmRefreshManager> ignored = Mockito.mockConstruction(IvmRefreshManager.class,
                (mock, context) -> Mockito.when(mock.doRefresh(mtmv)).thenReturn(
                        IvmRefreshResult.fallback(IvmFailureReason.BINLOG_NOT_ENABLED, "no_binlog")))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt", new Object[] {request});
            Assert.assertEquals("FALLBACK_ALLOWED", result.toString());
        }

        Assert.assertEquals(IvmFailureReason.BINLOG_NOT_ENABLED.name(),
                Deencapsulation.getField(task, "ivmFallbackReason"));
    }

    @Test
    public void testExecuteIvmAttemptTriesIncrementalWithIncompleteRefreshSnapshot() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(false);
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null, RefreshMode.INCREMENTAL);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        try (MockedConstruction<IvmRefreshManager> ignored = Mockito.mockConstruction(IvmRefreshManager.class,
                (mock, constructionContext) -> Mockito.when(mock.doRefresh(mtmv)).thenReturn(
                        IvmRefreshResult.success()))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt", new Object[] {request});

            Assert.assertEquals("SUCCESS", result.toString());
            Assert.assertEquals(1, ignored.constructed().size());
        }

        Assert.assertNull(Deencapsulation.getField(task, "ivmFallbackReason"));
    }

    @Test
    public void testExecuteIvmAttemptAllowsIncompletePctSnapshotWhenBaselineExists() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(true);
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null, RefreshMode.INCREMENTAL);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        try (MockedConstruction<IvmRefreshManager> ignored = Mockito.mockConstruction(IvmRefreshManager.class,
                (mock, constructionContext) -> Mockito.when(mock.doRefresh(mtmv)).thenReturn(
                        IvmRefreshResult.success()))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt", new Object[] {request});
            Assert.assertEquals("SUCCESS", result.toString());
            Assert.assertEquals(1, ignored.constructed().size());
        }
    }

    @Test
    public void testSystemDefaultIncrementalInitializesBaselineWithCompleteRefresh() {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(false);
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        MTMVTaskContext context = MTMVTaskContext.forMvDefault(MTMVTaskTriggerMode.SYSTEM);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = Deencapsulation.invoke(task, "buildAttempts", request);

        Assert.assertEquals(RefreshMode.INCREMENTAL, Deencapsulation.getField(request, "refreshMode"));
        Assert.assertFalse(Deencapsulation.getField(request, "allowFallback"));
        Assert.assertEquals(Lists.newArrayList("COMPLETE"), attempts.stream()
                .map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testManualIncrementalDoesNotInitializeBaselineWithCompleteRefresh() {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(false);
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null, RefreshMode.INCREMENTAL);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = Deencapsulation.invoke(task, "buildAttempts", request);
        Assert.assertEquals(RefreshMode.INCREMENTAL, Deencapsulation.getField(request, "refreshMode"));
        Assert.assertFalse(Deencapsulation.getField(request, "allowFallback"));
        Assert.assertEquals(Lists.newArrayList("IVM"), attempts.stream()
                .map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testManualDefaultIncrementalKeepsIncrementalWithoutSnapshot() {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(false);
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        MTMVTaskContext context = MTMVTaskContext.forMvDefault(MTMVTaskTriggerMode.MANUAL);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = Deencapsulation.invoke(task, "buildAttempts", request);
        Assert.assertEquals(RefreshMode.INCREMENTAL, Deencapsulation.getField(request, "refreshMode"));
        Assert.assertFalse(Deencapsulation.getField(request, "allowFallback"));
        Assert.assertEquals(Lists.newArrayList("IVM"), attempts.stream()
                .map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testExecuteIvmAttemptFallsBackToCompleteForPlanSignatureMismatchInAutoMode() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(true);
        IvmPlanSignature currentSignature = new IvmPlanSignature("canonical", "new");
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        try (MockedConstruction<IvmRefreshManager> ignored = Mockito.mockConstruction(IvmRefreshManager.class,
                (mock, context) -> Mockito.when(mock.doRefresh(mtmv)).thenReturn(
                        IvmRefreshResult.fallback(IvmFailureReason.PLAN_SIGNATURE_MISMATCH,
                                "layout drift", currentSignature)))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt", new Object[] {request});
            Assert.assertEquals("FALLBACK_TO_COMPLETE", result.toString());
        }

        Assert.assertEquals(IvmFailureReason.PLAN_SIGNATURE_MISMATCH.name(),
                Deencapsulation.getField(task, "ivmFallbackReason"));
        Assert.assertEquals("new", Deencapsulation.getField(task, "ivmFallbackPlanSignature"));
    }

    @Test
    public void testExecuteIvmAttemptKeepsRefreshScopeForNonSignatureFallbackInAutoMode() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(true);
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Deencapsulation.setField(task, "needRefreshPartitions", Lists.newArrayList(poneName));
        Deencapsulation.setField(task, "refreshMode", MTMVTask.MTMVTaskRefreshMode.PARTIAL);

        try (MockedConstruction<IvmRefreshManager> ignored = Mockito.mockConstruction(IvmRefreshManager.class,
                (mock, context) -> Mockito.when(mock.doRefresh(mtmv)).thenReturn(
                        IvmRefreshResult.fallback(IvmFailureReason.BINLOG_NOT_ENABLED, "no_binlog")))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt", new Object[] {request});
            Assert.assertEquals("FALLBACK_ALLOWED", result.toString());
        }

        Assert.assertEquals(Lists.newArrayList(poneName),
                Deencapsulation.getField(task, "needRefreshPartitions"));
        Assert.assertEquals(MTMVTask.MTMVTaskRefreshMode.PARTIAL,
                Deencapsulation.getField(task, "refreshMode"));
        Assert.assertNull(Deencapsulation.getField(task, "ivmFallbackPlanSignature"));
    }

    @Test
    public void testDebugPlanSignatureDriftFallsBackToFullRefresh() throws Exception {
        boolean originalEnableDebugPoints = Config.enable_debug_points;
        try {
            Config.enable_debug_points = true;
            DebugPointUtil.clearDebugPoints();
            IvmPlanSignature storedSignature = signatureForDebugDriftTest();
            DebugPointUtil.addDebugPointWithValue(IvmPlanSignatureGenerator.DEBUG_POINT_SIGNATURE_SALT,
                    "plan_changed");
            IvmPlanSignature currentSignature = signatureForDebugDriftTest();
            Assert.assertNotEquals(storedSignature.getSha256(), currentSignature.getSha256());

            IvmInfo ivmInfo = new IvmInfo();
            ivmInfo.setPlanSignature(storedSignature.getSha256());
            Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
            Mockito.when(mtmv.isIvm()).thenReturn(true);
            Mockito.when(mtmv.getName()).thenReturn("test_mv");
            Mockito.when(mtmv.getPartitionNames()).thenReturn(Sets.newHashSet(poneName, ptwoName));

            IvmNormalizeResult normalizeResult = new IvmNormalizeResult();
            normalizeResult.setPlanSignature(currentSignature);
            MTMVAnalyzeQueryInfo queryInfo = new MTMVAnalyzeQueryInfo(
                    Collections.emptyList(), Collections.emptyList(), null, null, Collections.emptyMap());
            queryInfo.setIvmNormalizeResult(normalizeResult);
            ConnectContext connectContext = new ConnectContext();
            MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
            Deencapsulation.setField(task, "needRefreshPartitions", Lists.newArrayList(poneName));
            Deencapsulation.setField(task, "refreshMode", MTMVTask.MTMVTaskRefreshMode.PARTIAL);

            try (MockedStatic<MTMVPlanUtil> mtmvPlanUtilStatic = Mockito.mockStatic(MTMVPlanUtil.class)) {
                mtmvPlanUtilStatic.when(() -> MTMVPlanUtil.createMTMVContext(
                        Mockito.eq(mtmv), Mockito.anyList())).thenReturn(connectContext);
                mtmvPlanUtilStatic.when(() -> MTMVPlanUtil.analyzeQueryWithSql(
                        Mockito.eq(mtmv), Mockito.eq(connectContext), Mockito.eq(true))).thenReturn(queryInfo);

                Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
                Object result = Deencapsulation.invoke(task, "executeIvmAttempt", new Object[] {request});
                Assert.assertEquals("FALLBACK_TO_COMPLETE", result.toString());
            }

            Assert.assertEquals(IvmFailureReason.PLAN_SIGNATURE_MISMATCH.name(),
                    Deencapsulation.getField(task, "ivmFallbackReason"));
            Assert.assertEquals(currentSignature.getSha256(),
                    Deencapsulation.getField(task, "ivmFallbackPlanSignature"));
            Assert.assertEquals(currentSignature.getCanonicalString(),
                    Deencapsulation.getField(task, "ivmFallbackPlanCanonicalString"));
        } finally {
            DebugPointUtil.clearDebugPoints();
            Config.enable_debug_points = originalEnableDebugPoints;
        }
    }

    @Test
    public void testFullRefreshUpdatesIvmPlanSignatureFromFallbackResult() throws Exception {
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.setPlanSignature("old");
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
        Mockito.when(mtmv.getQualifiedDbName()).thenReturn("test_db");
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Deencapsulation.setField(task, "ivmFallbackPlanSignature", "new");
        Deencapsulation.setField(task, "ivmFallbackPlanCanonicalString", "canonical");

        try (MockedStatic<IvmRefreshManager> managerStatic = Mockito.mockStatic(IvmRefreshManager.class)) {
            Deencapsulation.invoke(task, "updateIvmPlanSignatureAfterFullRefreshIfNeeded");

            managerStatic.verify(() -> IvmRefreshManager.updatePlanSignatureAfterFullRefresh(
                    mtmv, "new", "canonical"));
        }
        Assert.assertNull(Deencapsulation.getField(task, "ivmFallbackPlanSignature"));
        Assert.assertNull(Deencapsulation.getField(task, "ivmFallbackPlanCanonicalString"));
    }

    @Test
    public void testIvmExecutionFailureDoesNotFallback() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.INCREMENTAL, true, null);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        try (MockedConstruction<IvmRefreshManager> ignored = Mockito.mockConstruction(IvmRefreshManager.class,
                (mock, constructionContext) -> Mockito.when(mock.doRefresh(mtmv)).thenThrow(
                        new IvmException(IvmFailureReason.INCREMENTAL_EXECUTION_FAILED, "delta failed")))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            JobException exception = Assert.assertThrows(JobException.class,
                    () -> Deencapsulation.invoke(task, "executeIvmAttempt", new Object[] {request}));
            Assert.assertTrue(exception.getMessage().contains("INCREMENTAL_EXECUTION_FAILED"));
        }

        Assert.assertEquals(IvmFailureReason.INCREMENTAL_EXECUTION_FAILED.name(),
                Deencapsulation.getField(task, "ivmFallbackReason"));
    }

    @Test
    public void testOldTaskJsonWithoutIvmFallbackReasonDeserializes() {
        String oldJson = "{\"di\":1,\"mi\":2,\"taskContext\":{\"triggerMode\":\"MANUAL\"}}";

        MTMVTask task = GsonUtils.GSON.fromJson(oldJson, MTMVTask.class);

        Assert.assertNotNull(task);
        Assert.assertNull(Deencapsulation.getField(task, "ivmFallbackReason"));
    }

    private IvmPlanSignature signatureForDebugDriftTest() {
        IvmNormalizeResult normalizeResult = new IvmNormalizeResult();
        normalizeResult.setNormalizedPlan(buildSignaturePlan());
        return new IvmPlanSignatureGenerator().generate(normalizeResult);
    }

    private LogicalResultSink<?> buildSignaturePlan() {
        OlapTable table = PlanConstructor.newOlapTable(100L, "signature_t", 0, KeysType.UNIQUE_KEYS);
        table.setQualifiedDbName("test_db");
        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(), table,
                Lists.newArrayList("test_db"));
        List<NamedExpression> outputs = Lists.newArrayList();
        outputs.addAll(scan.getOutput());
        LogicalProject<?> project = new LogicalProject<>(outputs, scan);
        return new LogicalResultSink<>(outputs, project);
    }
}
