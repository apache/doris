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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.job.extensions.mtmv.MTMVTaskContext;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.ivm.IvmFailureReason;
import org.apache.doris.mtmv.ivm.IvmIncrRefreshContext;
import org.apache.doris.mtmv.ivm.IvmIncrRefreshManager;
import org.apache.doris.mtmv.ivm.IvmIncrRefreshResult;
import org.apache.doris.mtmv.ivm.IvmInfo;
import org.apache.doris.mtmv.ivm.IvmPlanSignature;
import org.apache.doris.mtmv.ivm.IvmPlanSignatureGenerator;
import org.apache.doris.mtmv.ivm.IvmRewriteResult;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
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
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
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

    @BeforeEach
    public void setUp()
            throws NoSuchMethodException, SecurityException, AnalysisException, DdlException, MetaNotFoundException {

        mtmvUtilStatic = Mockito.mockStatic(MTMVUtil.class);
        mtmvPartitionUtilStatic = Mockito.mockStatic(MTMVPartitionUtil.class);

        mtmvUtilStatic.when(() -> MTMVUtil.getMTMV(Mockito.anyLong(), Mockito.anyLong())).thenReturn(mtmv);

        Mockito.when(mtmv.getPartitionNames()).thenReturn(Sets.newHashSet(poneName, ptwoName));

        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(mtmvPartitionInfo);

        Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.FOLLOW_BASE_TABLE);

        // mtmvPartitionUtil.getPartitionsIdsByNames(mtmv, Lists.newArrayList(poneName));
        // minTimes = 0;
        // result = poneId;

        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.isMTMVSync(Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class), Mockito.nullable(Set.class))).thenReturn(true);

        Mockito.when(mtmv.getRefreshInfo()).thenReturn(mtmvRefreshInfo);
        Mockito.when(mtmv.getIvmInfo()).thenReturn(new IvmInfo());

        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.COMPLETE);

        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(true);
    }

    @AfterEach
    public void tearDown() {
        mtmvUtilStatic.close();
        mtmvPartitionUtilStatic.close();
    }

    @Test
    public void testGenerateRefreshModeDistinguishesFullAndPartialScope() {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        Object complete = Deencapsulation.invoke(task, "generateRefreshMode", allPartitionNames);
        Object partial = Deencapsulation.invoke(task, "generateRefreshMode", Lists.newArrayList(poneName));
        Object differentPartitions = Deencapsulation.invoke(
                task, "generateRefreshMode", Lists.newArrayList(poneName, "p3"));

        Assertions.assertEquals(MTMVTask.MTMVTaskRefreshMode.COMPLETE, complete);
        Assertions.assertEquals(MTMVTask.MTMVTaskRefreshMode.PARTIAL, partial);
        Assertions.assertEquals(MTMVTask.MTMVTaskRefreshMode.PARTIAL, differentPartitions);
    }

    @Test
    public void testBuildAttemptsAutoCompleteMethodSkipsPartitionsAttempt() {
        // setUp stubs refreshMethod=COMPLETE. The PARTITIONS attempt must be skipped:
        // its sync check treats non-MTMVRelatedTableIf base tables as always synchronous,
        // so a COMPLETE-method MV would never refresh through it.
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);
        Assertions.assertEquals(Lists.newArrayList("COMPLETE"), toNames(attempts));
    }

    @Test
    public void testBuildAttemptsAutoNonCompleteMethodKeepsPartitionsAttempt() {
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.AUTO);
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);
        Assertions.assertEquals(Lists.newArrayList("PARTITIONS", "COMPLETE"), toNames(attempts));
    }

    @Test
    public void testBuildAttemptsAutoAfterSuccessfulRefreshWithoutSnapshotUsesComplete() {
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.AUTO);
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(false);
        MTMVStatus status = new MTMVStatus(MTMVRefreshState.SUCCESS);
        status.setState(MTMVState.NORMAL);
        Mockito.when(mtmv.getStatus()).thenReturn(status);

        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);

        Assertions.assertEquals(Lists.newArrayList("COMPLETE"), toNames(attempts));
    }

    @Test
    public void testBuildAttemptsInitialAutoWithoutSnapshotKeepsFallbackChain() {
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.AUTO);
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(false);
        Mockito.when(mtmv.getStatus()).thenReturn(new MTMVStatus());

        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);

        Assertions.assertEquals(Lists.newArrayList("PARTITIONS", "COMPLETE"), toNames(attempts));
    }

    @Test
    public void testBuildAttemptsAutoIvmIncrementalKeepsFullChain() {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);
        Assertions.assertEquals(Lists.newArrayList("IVM", "PARTITIONS", "COMPLETE"), toNames(attempts));
    }

    @Test
    public void testBuildAttemptsExplicitPartitionsNeverUpgradeToComplete() {
        // An explicit partition list is an exact request: even a COMPLETE-method MV
        // (setUp default) must keep the PARTITIONS-only attempt instead of expanding
        // the refresh scope to a full refresh.
        MTMVTaskContext context = MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, Lists.newArrayList(poneName), RefreshMode.AUTO);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);
        Assertions.assertEquals(Lists.newArrayList("PARTITIONS"), toNames(attempts));
    }

    @Test
    public void testBuildAttemptsGoesStraightToCompleteWhenTheStreamIsUnusable() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(Collections.emptySet());
        Mockito.when(mtmv.getFullQualifiers()).thenReturn(Lists.newArrayList("internal", "db", "t1"));
        // The MV's database holds no stream for the base table.
        Mockito.when(mtmv.getDatabase()).thenReturn(Mockito.mock(Database.class));
        mtmvUtilStatic.when(() -> MTMVUtil.getTable(Mockito.any(BaseTableInfo.class))).thenReturn(mtmv);

        MTMVTask task = new MTMVTask(mtmv, relationWithOneBaseTable(), MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.INCREMENTAL, true, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);

        // Neither the incremental rewrite nor a partition refresh can read a stream that is not there,
        // and the IVM attempt would be rejected while a baseline barrier is pending, so the refresh goes
        // to the only attempt that reconciles the streams.
        Assertions.assertEquals(Lists.newArrayList("COMPLETE"), toNames(attempts));
        Assertions.assertEquals(IvmFailureReason.STREAM_UNSUPPORTED.name(),
                Deencapsulation.getField(task, "ivmFallbackReason"));
    }

    @Test
    public void testBuildAttemptsKeepsTheChainWhenTheStreamsAreUsable() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.getId()).thenReturn(7L);
        Mockito.when(mtmv.getFullQualifiers()).thenReturn(Lists.newArrayList("internal", "db", "t1"));
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(Collections.emptySet());
        OlapTableStream stream = Mockito.mock(OlapTableStream.class);
        Mockito.when(stream.getBaseTableFullQualifiers())
                .thenReturn(Lists.newArrayList("internal", "db", "t1"));
        Mockito.when(stream.isDisabled()).thenReturn(false);
        Mockito.when(stream.isStale()).thenReturn(false);
        Mockito.when(stream.getBaseTableNullable()).thenReturn(mtmv);
        Database mvDb = Mockito.mock(Database.class);
        Mockito.when(mvDb.getTableNullable(Mockito.anyString())).thenReturn(stream);
        Mockito.when(mtmv.getDatabase()).thenReturn(mvDb);
        mtmvUtilStatic.when(() -> MTMVUtil.getTable(Mockito.any(BaseTableInfo.class))).thenReturn(mtmv);

        MTMVTask task = new MTMVTask(mtmv, relationWithOneBaseTable(), MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.INCREMENTAL, true, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);

        // A usable stream is not a reason to refresh more than the request asked for.
        Assertions.assertEquals(Lists.newArrayList("IVM", "PARTITIONS", "COMPLETE"), toNames(attempts));
    }

    @Test
    public void testBuildAttemptsIgnoresAStreamOnlyTheClosureCarries() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.getId()).thenReturn(7L);
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(Collections.emptySet());
        // The MV of a chain reads the upstream MV, and the upstream's own base table is in the relation
        // only because the closure carries it: (t1) => upstream => mv.
        OlapTable upstream = Mockito.mock(OlapTable.class);
        Mockito.when(upstream.getFullQualifiers()).thenReturn(Lists.newArrayList("internal", "db", "upstream"));
        OlapTable grandParent = Mockito.mock(OlapTable.class);
        Mockito.when(grandParent.getFullQualifiers()).thenReturn(Lists.newArrayList("internal", "db", "t1"));
        BaseTableInfo upstreamInfo = Mockito.mock(BaseTableInfo.class);
        BaseTableInfo grandParentInfo = Mockito.mock(BaseTableInfo.class);
        mtmvUtilStatic.when(() -> MTMVUtil.getTable(upstreamInfo)).thenReturn(upstream);
        mtmvUtilStatic.when(() -> MTMVUtil.getTable(grandParentInfo)).thenReturn(grandParent);
        // The upstream's stream is there, the grandparent's is not.
        OlapTableStream stream = Mockito.mock(OlapTableStream.class);
        Mockito.when(stream.getBaseTableFullQualifiers())
                .thenReturn(Lists.newArrayList("internal", "db", "upstream"));
        Mockito.when(stream.isDisabled()).thenReturn(false);
        Mockito.when(stream.isStale()).thenReturn(false);
        Mockito.when(stream.getBaseTableNullable()).thenReturn(upstream);
        Database mvDb = Mockito.mock(Database.class);
        Mockito.when(mvDb.getTableNullable(IvmUtil.streamName(7L, upstream.getFullQualifiers())))
                .thenReturn(stream);
        Mockito.when(mtmv.getDatabase()).thenReturn(mvDb);
        MTMVRelation chainedRelation = new MTMVRelation(Sets.newHashSet(upstreamInfo, grandParentInfo),
                Sets.newHashSet(upstreamInfo), Sets.newHashSet(upstreamInfo), Sets.newHashSet(),
                Sets.newHashSet());

        MTMVTask task = new MTMVTask(mtmv, chainedRelation, MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.INCREMENTAL, true, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);

        // No rewrite reads the grandparent's stream, so its absence is no reason to rebuild the MV.
        Assertions.assertEquals(Lists.newArrayList("IVM", "PARTITIONS", "COMPLETE"), toNames(attempts));
        Assertions.assertNull(Deencapsulation.getField(task, "ivmFallbackReason"));
    }

    @Test
    public void testPartitionRefreshChecksOnlyTheStreamsItsPartitionsRead() throws Exception {
        // t1 UNION ALL t2, both PCT tables of the MV, each backing one of its partitions. Refreshing the
        // t1-backed partition reads t1's stream and leaves t2 to an ordinary scan, so t2's stream is not
        // part of this refresh and its absence must not send it to COMPLETE.
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.getId()).thenReturn(7L);
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(Collections.emptySet());
        OlapTable t1 = mockBaseTable("t1");
        OlapTable t2 = mockBaseTable("t2");
        BaseTableInfo t1Info = Mockito.mock(BaseTableInfo.class);
        BaseTableInfo t2Info = Mockito.mock(BaseTableInfo.class);
        mtmvUtilStatic.when(() -> MTMVUtil.getTable(t1Info)).thenReturn(t1);
        mtmvUtilStatic.when(() -> MTMVUtil.getTable(t2Info)).thenReturn(t2);
        OlapTableStream t1Stream = usableStreamFor(t1);
        Database mvDb = Mockito.mock(Database.class);
        Mockito.when(mvDb.getTableNullable(IvmUtil.streamName(7L, t1.getFullQualifiers())))
                .thenReturn(t1Stream);
        Mockito.when(mtmv.getDatabase()).thenReturn(mvDb);
        Mockito.when(mtmvPartitionInfo.getPctInfos()).thenReturn(Lists.newArrayList(
                new BaseColInfo("dt", t1Info), new BaseColInfo("dt", t2Info)));

        // The partition this refresh plans reads t1, and t2 keeps its place in the plan as a plain scan.
        Map<MTMVRelatedTableIf, Set<String>> mapping = Maps.newHashMap();
        mapping.put(t1, Sets.newHashSet("p1"));
        MTMVRefreshContext context = Mockito.mock(MTMVRefreshContext.class);
        Mockito.when(context.getByPartitionName(Mockito.anyString())).thenReturn(mapping);

        MTMVRelation relation = new MTMVRelation(Sets.newHashSet(t1Info, t2Info), Sets.newHashSet(t1Info, t2Info),
                Sets.newHashSet(t1Info, t2Info), Sets.newHashSet(), Sets.newHashSet());
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        Assertions.assertFalse((Boolean) Deencapsulation.invoke(task, "hasUnusableIvmStreamForPartitions",
                context, Lists.newArrayList("p_t1")));

        // Once a refreshed partition's mapping names t2, its stream is read, and its absence decides.
        mapping.put(t2, Sets.newHashSet("p2"));
        Assertions.assertTrue((Boolean) Deencapsulation.invoke(task, "hasUnusableIvmStreamForPartitions",
                context, Lists.newArrayList("p_t1")));
    }

    private OlapTable mockBaseTable(String name) {
        OlapTable baseTable = Mockito.mock(OlapTable.class);
        Mockito.when(baseTable.getName()).thenReturn(name);
        Mockito.when(baseTable.getFullQualifiers()).thenReturn(Lists.newArrayList("internal", "db", name));
        return baseTable;
    }

    /** A stream that {@code IvmUtil.isIvmStreamUsable} accepts for the given base table. */
    private OlapTableStream usableStreamFor(OlapTable baseTable) {
        List<String> qualifiers = baseTable.getFullQualifiers();
        OlapTableStream stream = Mockito.mock(OlapTableStream.class);
        Mockito.when(stream.getBaseTableFullQualifiers()).thenReturn(qualifiers);
        Mockito.when(stream.isDisabled()).thenReturn(false);
        Mockito.when(stream.isStale()).thenReturn(false);
        Mockito.when(stream.getBaseTableNullable()).thenReturn(baseTable);
        return stream;
    }

    private MTMVRelation relationWithOneBaseTable() {
        BaseTableInfo baseTable = Mockito.mock(BaseTableInfo.class);
        // A table of the query is in the plan, in the first level of the query, and in the closure.
        return new MTMVRelation(Sets.newHashSet(baseTable), Sets.newHashSet(baseTable),
                Sets.newHashSet(baseTable), Sets.newHashSet(), Sets.newHashSet());
    }

    private static List<String> toNames(List<?> attempts) {
        List<String> names = Lists.newArrayList();
        for (Object attempt : attempts) {
            names.add(String.valueOf(attempt));
        }
        return names;
    }

    @Test
    public void testPlanPartitionRefreshSelfManageWhenSync() throws Exception {
        Mockito.when(mtmvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.SELF_MANAGE);
        MTMVTask task = new MTMVTask(mtmv, relation,
                MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null, RefreshMode.AUTO));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");

        Object plan = Deencapsulation.invoke(task, "planPartitionRefresh",
                Mockito.mock(MTMVRefreshContext.class), request);

        Assertions.assertTrue((Boolean) Deencapsulation.getField(plan, "canRefreshByPartitions"));
        Assertions.assertTrue(CollectionUtils.isEmpty(Deencapsulation.getField(plan, "partitions")));
    }

    @Test
    public void testIncrementalFallbackOnNonIvmKeepsIvmAttempt() throws JobException {
        Mockito.when(mtmv.isIvm()).thenReturn(false);
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.INCREMENTAL, true, null);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = Deencapsulation.invoke(task, "buildAttempts", request, false);

        Assertions.assertEquals(Lists.newArrayList("IVM", "PARTITIONS", "COMPLETE"), attempts.stream()
                .map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testManualIvmWithOneRowRelationWithoutSnapshotUsesComplete() throws JobException {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(false);
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.INCREMENTAL, false, null);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = Deencapsulation.invoke(task, "buildAttempts", request, true);

        Assertions.assertEquals(Lists.newArrayList("COMPLETE"), attempts.stream()
                .map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testManualIvmWithOneRowRelationWithSnapshotUsesIncremental() throws JobException {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(true);
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.INCREMENTAL, false, null);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = Deencapsulation.invoke(task, "buildAttempts", request, true);

        Assertions.assertEquals(Lists.newArrayList("IVM"), attempts.stream()
                .map(Object::toString).collect(Collectors.toList()));
    }

    @Test
    public void testMvDefaultUnknownRefreshMethodRejected() {
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(null);
        MTMVTaskContext context = MTMVTaskContext.forMvDefault(MTMVTaskTriggerMode.SYSTEM);
        MTMVTask task = new MTMVTask(mtmv, relation, context);

        JobException exception = Assertions.assertThrows(JobException.class,
                () -> Deencapsulation.invoke(task, "resolveRefreshRequest"));

        Assertions.assertTrue(exception.getMessage().contains("unknown refresh method"));
    }

    @Test
    public void testTaskSchemaContainsComputeGroup() {
        Column computeGroupColumn = MTMVTask.SCHEMA.get(MTMVTask.SCHEMA.size() - 2);
        Column fallbackReasonColumn = MTMVTask.SCHEMA.get(MTMVTask.SCHEMA.size() - 1);
        Assertions.assertEquals(COMPUTE_GROUP, computeGroupColumn.getName());
        Assertions.assertEquals("IvmFallbackReason", fallbackReasonColumn.getName());
        Assertions.assertEquals(MTMVTask.SCHEMA.size() - 2,
                MTMVTask.COLUMN_TO_INDEX.get(COMPUTE_GROUP.toLowerCase()).intValue());
    }

    @Test
    public void testGetTvfInfoReturnsComputeGroup() {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Deencapsulation.setField(task, "computeGroup", "cg1");

        TRow row = task.getTvfInfo("job1");

        Assertions.assertEquals("cg1", row.getColumnValue()
                .get(MTMVTask.COLUMN_TO_INDEX.get(COMPUTE_GROUP.toLowerCase())).getStringVal());
    }

    @Test
    public void testSetupComputeGroupFromContext() {
        String originCloudUniqueId = Config.cloud_unique_id;
        try {
            Config.cloud_unique_id = "test_cloud";
            ConnectContext ctx = new ConnectContext();
            ctx.setCloudCluster("cg1");
            MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

            Deencapsulation.invoke(task, "setupComputeGroup", ctx);
            TRow row = task.getTvfInfo("job1");

            Assertions.assertEquals("cg1", row.getColumnValue()
                    .get(MTMVTask.COLUMN_TO_INDEX.get(COMPUTE_GROUP.toLowerCase())).getStringVal());
        } finally {
            Config.cloud_unique_id = originCloudUniqueId;
        }
    }

    @Test
    public void testSetupComputeGroupFromTaskContext() {
        String originCloudUniqueId = Config.cloud_unique_id;
        try {
            Config.cloud_unique_id = "test_cloud";
            ConnectContext ctx = new ConnectContext();
            MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                    RefreshMode.COMPLETE, true, "cg1");
            MTMVTask task = new MTMVTask(mtmv, relation, context);

            Deencapsulation.invoke(task, "setupComputeGroup", ctx);

            Assertions.assertEquals("cg1", ctx.getSessionVariable().getCloudCluster());
            TRow row = task.getTvfInfo("job1");
            Assertions.assertEquals("cg1", row.getColumnValue()
                    .get(MTMVTask.COLUMN_TO_INDEX.get(COMPUTE_GROUP.toLowerCase())).getStringVal());
        } finally {
            Config.cloud_unique_id = originCloudUniqueId;
        }
    }

    @Test
    public void testGetTvfInfoReturnsNullStringForMissingComputeGroup() {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        TRow row = task.getTvfInfo("job1");

        Assertions.assertEquals(FeConstants.null_string, row.getColumnValue()
                .get(MTMVTask.COLUMN_TO_INDEX.get(COMPUTE_GROUP.toLowerCase())).getStringVal());
    }

    @Test
    public void testDeserializeOldTaskWithoutComputeGroup() {
        MTMVTask task = GsonUtils.GSON.fromJson("{\"di\":1,\"mi\":2}", MTMVTask.class);

        TRow row = task.getTvfInfo("job1");

        Assertions.assertEquals(FeConstants.null_string, row.getColumnValue()
                .get(MTMVTask.COLUMN_TO_INDEX.get(COMPUTE_GROUP.toLowerCase())).getStringVal());
    }

    @Test
    public void testExecCarriesExcludedTriggerTablesIntoStatementContext() throws Exception {
        Set<TableNameInfo> excludedTriggerTables = Sets.newHashSet(
                new TableNameInfo("internal", "test_db", "excluded_agg"));
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(excludedTriggerTables);
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.getQuerySql()).thenReturn("select k1 from test_db.base_table");
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
                            Assertions.assertEquals(excludedTriggerTables, statementContext.getExcludedTriggerTables());
                            Assertions.assertEquals("select k1 from test_db.base_table",
                                    statementContext.getOriginStatement().originStmt);
                            return command;
                        }
                    });
            mtmvPlanUtilStatic.when(() -> MTMVPlanUtil.executeCommand(
                    Mockito.eq(mtmvCtx), Mockito.eq(command), Mockito.any(StatementContext.class),
                    Mockito.anyString(), Mockito.any(Consumer.class))).thenAnswer(new Answer<Void>() {
                        @Override
                        public Void answer(InvocationOnMock invocation) {
                            StatementContext statementContext = invocation.getArgument(2);
                            Assertions.assertEquals(excludedTriggerTables, statementContext.getExcludedTriggerTables());
                            Assertions.assertEquals("select k1 from test_db.base_table",
                                    statementContext.getOriginStatement().originStmt);
                            return null;
                        }
                    });

            Deencapsulation.invoke(task, "refreshPartitions", Sets.newHashSet(poneName), Collections.emptyMap(),
                    Optional.empty(), RefreshMode.PARTITIONS);
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
        Assertions.assertEquals(MTMVTask.SCHEMA.size(), cells.size());
        Assertions.assertEquals(IvmFailureReason.BINLOG_NOT_ENABLED.name(), cells.get(columnIndex).getStringVal());
    }

    @Test
    public void testCollectPctResetPartitionIdsSupportsMultiplePctTables() throws Exception {
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getFullName()).thenReturn("test_db");

        OlapTable firstPctTable = Mockito.mock(OlapTable.class);
        Mockito.when(firstPctTable.getName()).thenReturn("first_pct");
        Mockito.when(firstPctTable.getDatabase()).thenReturn(database);
        Partition firstPartition = Mockito.mock(Partition.class);
        Partition sharedPartition = Mockito.mock(Partition.class);
        Partition lastPartition = Mockito.mock(Partition.class);
        Mockito.when(firstPartition.getId()).thenReturn(11L);
        Mockito.when(sharedPartition.getId()).thenReturn(12L);
        Mockito.when(lastPartition.getId()).thenReturn(13L);
        Mockito.when(firstPctTable.getPartitionOrAnalysisException("first_p1")).thenReturn(firstPartition);
        Mockito.when(firstPctTable.getPartitionOrAnalysisException("first_shared")).thenReturn(sharedPartition);
        Mockito.when(firstPctTable.getPartitionOrAnalysisException("first_p3")).thenReturn(lastPartition);

        OlapTable secondPctTable = Mockito.mock(OlapTable.class);
        Mockito.when(secondPctTable.getName()).thenReturn("second_pct");
        Mockito.when(secondPctTable.getDatabase()).thenReturn(database);
        Partition secondFirstPartition = Mockito.mock(Partition.class);
        Partition secondLastPartition = Mockito.mock(Partition.class);
        Mockito.when(secondFirstPartition.getId()).thenReturn(21L);
        Mockito.when(secondLastPartition.getId()).thenReturn(22L);
        Mockito.when(secondPctTable.getPartitionOrAnalysisException("second_p1")).thenReturn(secondFirstPartition);
        Mockito.when(secondPctTable.getPartitionOrAnalysisException("second_p2")).thenReturn(secondLastPartition);

        MTMVRefreshContext refreshContext = Mockito.mock(MTMVRefreshContext.class);
        Mockito.when(refreshContext.getByPartitionName("mv_p1")).thenReturn(
                ImmutableMap.<MTMVRelatedTableIf, Set<String>>of(
                        firstPctTable, Sets.newHashSet("first_p1", "first_shared"),
                        secondPctTable, Sets.newHashSet("second_p1")));
        Mockito.when(refreshContext.getByPartitionName("mv_p2")).thenReturn(
                ImmutableMap.<MTMVRelatedTableIf, Set<String>>of(
                        firstPctTable, Sets.newHashSet("first_shared", "first_p3"),
                        secondPctTable, Sets.newHashSet("second_p2")));

        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Map<BaseTableInfo, Set<Long>> result = Deencapsulation.invoke(
                task, "collectPctResetPartitionIds", refreshContext, Sets.newHashSet("mv_p1", "mv_p2"));

        Assertions.assertEquals(Sets.newHashSet(11L, 12L, 13L), result.get(new BaseTableInfo(firstPctTable)));
        Assertions.assertEquals(Sets.newHashSet(21L, 22L), result.get(new BaseTableInfo(secondPctTable)));
    }

    @Test
    public void testExecuteIvmAttemptRecordsFallbackReason() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        MTMVRefreshContext refreshContext = mockIvmIncrRefreshContext();

        try (MockedConstruction<IvmIncrRefreshManager> ignored = Mockito.mockConstruction(IvmIncrRefreshManager.class,
                (mock, context) -> Mockito.when(mock.doRefresh(Mockito.any()))
                        .thenReturn(
                        IvmIncrRefreshResult.fallback(IvmFailureReason.BINLOG_NOT_ENABLED, "no_binlog")))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt", refreshContext, request,
                    new ConnectContext(), Lists.newArrayList());
            Assertions.assertEquals("FALLBACK_ALLOWED", result.toString());
            Mockito.verify(ignored.constructed().get(0)).doRefresh(Mockito.any());
        }

        Assertions.assertEquals(IvmFailureReason.BINLOG_NOT_ENABLED.name(),
                Deencapsulation.getField(task, "ivmFallbackReason"));
    }

    @Test
    public void testExecuteIvmAttemptRetriesRpcFailure() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        MTMVRefreshContext refreshContext = mockIvmIncrRefreshContext();
        int originalMaxQueryRetryTime = Config.max_query_retry_time;
        Config.max_query_retry_time = 1;
        try (MockedConstruction<IvmIncrRefreshManager> ignored = Mockito.mockConstruction(IvmIncrRefreshManager.class,
                (mock, context) -> Mockito.when(mock.doRefresh(Mockito.any()))
                        .thenThrow(
                                new RuntimeException(new RpcException("be", "rpc failed")))
                        .thenReturn(IvmIncrRefreshResult.success()))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt", refreshContext, request,
                    new ConnectContext(), Lists.newArrayList());

            Assertions.assertEquals("SUCCESS", result.toString());
            ArgumentCaptor<IvmIncrRefreshContext> refreshContextCaptor =
                    ArgumentCaptor.forClass(IvmIncrRefreshContext.class);
            Mockito.verify(ignored.constructed().get(0), Mockito.times(2))
                    .doRefresh(refreshContextCaptor.capture());
            Assertions.assertNotSame(refreshContextCaptor.getAllValues().get(0),
                    refreshContextCaptor.getAllValues().get(1));
        } finally {
            Config.max_query_retry_time = originalMaxQueryRetryTime;
        }
    }

    @Test
    public void testExecuteIvmAttemptRetriesMissingMvPartition() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        MTMVRefreshContext refreshContext = mockIvmIncrRefreshContext();
        int originalMaxQueryRetryTime = Config.max_query_retry_time;
        boolean originalEnableDebugPoints = Config.enable_debug_points;
        Config.max_query_retry_time = 1;
        Config.enable_debug_points = true;
        DebugPointUtil.clearDebugPoints();
        DebugPointUtil.addDebugPoint(MTMVTask.DEBUG_POINT_SKIP_PARTITION_SYNC);
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class)))
                .thenReturn(Lists.newArrayList(poneName));
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.generatePartitionSnapshots(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class), Mockito.nullable(Set.class)))
                .thenReturn(Collections.emptyMap());
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getBaseVersions(
                Mockito.same(mtmv), Mockito.anyMap()))
                .thenReturn(Mockito.mock(MTMVBaseVersions.class));
        AtomicInteger refreshCount = new AtomicInteger();
        try (MockedConstruction<IvmIncrRefreshManager> ignored = Mockito.mockConstruction(IvmIncrRefreshManager.class,
                (mock, context) -> Mockito.when(mock.doRefresh(Mockito.any())).thenAnswer(invocation -> {
                    if (refreshCount.getAndIncrement() == 0) {
                        return IvmIncrRefreshResult.fallback(IvmFailureReason.MV_PARTITION_NOT_FOUND,
                                "no partition for this tuple");
                    }
                    return IvmIncrRefreshResult.success();
                }))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt", refreshContext, request,
                    new ConnectContext(), Lists.newArrayList());

            Assertions.assertEquals("SUCCESS", result.toString());
            Assertions.assertEquals(2, refreshCount.get());
            Assertions.assertEquals(2, ignored.constructed().size());
        } finally {
            DebugPointUtil.clearDebugPoints();
            Config.enable_debug_points = originalEnableDebugPoints;
            Config.max_query_retry_time = originalMaxQueryRetryTime;
        }
    }

    @Test
    public void testExecuteIvmAttemptFallsBackToCompleteForMissingRefreshSnapshot() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(false);
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.INCREMENTAL, true, null);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        MTMVRefreshContext refreshContext = mockIvmIncrRefreshContext();

        try (MockedConstruction<IvmIncrRefreshManager> ignored = Mockito.mockConstruction(IvmIncrRefreshManager.class)) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt", refreshContext, request,
                    new ConnectContext(), Lists.newArrayList());

            Assertions.assertEquals("FALLBACK_TO_COMPLETE", result.toString());
            Assertions.assertTrue(ignored.constructed().isEmpty());
        }
        Assertions.assertEquals("INCOMPLETE_REFRESH_SNAPSHOT",
                Deencapsulation.getField(task, "ivmFallbackReason"));
    }

    @Test
    public void testExecuteIvmAttemptRunsWithoutRefreshSnapshotWhenFallbackDisabled() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.hasRefreshSnapshot()).thenReturn(false);
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.INCREMENTAL, false, null);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        MTMVRefreshContext refreshContext = mockIvmIncrRefreshContext();

        try (MockedConstruction<IvmIncrRefreshManager> ignored = Mockito.mockConstruction(IvmIncrRefreshManager.class,
                (mock, constructionContext) -> Mockito.when(
                        mock.doRefresh(Mockito.any()))
                        .thenReturn(IvmIncrRefreshResult.success()))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt", refreshContext, request,
                    new ConnectContext(), Lists.newArrayList());

            Assertions.assertEquals("SUCCESS", result.toString());
            Assertions.assertEquals(1, ignored.constructed().size());
            InOrder inOrder = Mockito.inOrder(mtmv, ignored.constructed().get(0));
            inOrder.verify(mtmv).validateIvmRefreshStart(0L);
            inOrder.verify(ignored.constructed().get(0)).doRefresh(Mockito.any());
        }
    }

    @Test
    public void testExecuteIvmAttemptFallsBackToCompleteForPlanSignatureMismatchInAutoMode() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        MTMVRefreshContext refreshContext = mockIvmIncrRefreshContext();
        try (MockedConstruction<IvmIncrRefreshManager> ignored = Mockito.mockConstruction(IvmIncrRefreshManager.class,
                (mock, context) -> Mockito.when(mock.doRefresh(Mockito.any()))
                        .thenReturn(
                        IvmIncrRefreshResult.fallback(IvmFailureReason.PLAN_SIGNATURE_MISMATCH, "layout drift")))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt", refreshContext, request,
                    new ConnectContext(), Lists.newArrayList());
            Assertions.assertEquals("FALLBACK_TO_COMPLETE", result.toString());
        }

        Assertions.assertEquals(IvmFailureReason.PLAN_SIGNATURE_MISMATCH.name(),
                Deencapsulation.getField(task, "ivmFallbackReason"));
    }

    @Test
    public void testExecuteIvmAttemptFallsBackToCompleteForBrokenBaseline() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        MTMVRefreshContext refreshContext = mockIvmIncrRefreshContext();

        try (MockedConstruction<IvmIncrRefreshManager> ignored = Mockito.mockConstruction(IvmIncrRefreshManager.class,
                (mock, context) -> Mockito.when(mock.doRefresh(Mockito.any()))
                        .thenReturn(
                        IvmIncrRefreshResult.fallback(IvmFailureReason.BINLOG_BROKEN, "broken baseline")))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt", refreshContext, request,
                    new ConnectContext(), Lists.newArrayList());
            Assertions.assertEquals("FALLBACK_TO_COMPLETE", result.toString());
        }
    }

    @Test
    public void testStrictIncrementalRejectsPendingBaselineBeforePartitionSync() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.requireCompleteBaselineRebuild();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
        Mockito.when(mtmv.getPartitionNames()).thenReturn(Collections.singleton(poneName));
        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.INCREMENTAL, false, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");

        JobException exception = Assertions.assertThrows(JobException.class,
                () -> Deencapsulation.invoke(task, "validateIvmBaselineBeforePartitionSync", request));

        Assertions.assertTrue(exception.getMessage().contains("run an AUTO or COMPLETE refresh first"));
        Assertions.assertEquals(IvmFailureReason.BINLOG_BROKEN.name(),
                Deencapsulation.getField(task, "ivmFallbackReason"));
    }

    @Test
    public void testBarePartitionsRejectsCompletePendingBaseline() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.requireCompleteBaselineRebuild();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.PARTITIONS, false, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");

        JobException exception = Assertions.assertThrows(JobException.class,
                () -> Deencapsulation.invoke(task, "validateIvmBaselineBeforePartitionSync", request));

        Assertions.assertTrue(exception.getMessage().contains("run a PARTITIONS FALLBACK, AUTO, or COMPLETE refresh"));
    }

    @Test
    public void testPartitionsFallbackRebuildsPendingBaselineWithComplete() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.requireCompleteBaselineRebuild();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
        Mockito.when(mtmv.getPartitionNames()).thenReturn(Collections.emptySet());
        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.PARTITIONS, true, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");

        Deencapsulation.invoke(task, "validateIvmBaselineBeforePartitionSync", request);
        List<Object> attempts = Lists.newArrayList();
        attempts.addAll(Deencapsulation.invoke(task, "buildAttempts", request, false));
        Assertions.assertEquals("[PARTITIONS, COMPLETE]", attempts.toString());

        Deencapsulation.invoke(task, "handlePendingIvmBaselineRebuild",
                Mockito.mock(MTMVRefreshContext.class), request, new ConnectContext(), attempts);

        // A pending COMPLETE rebuild reshapes the attempt list instead of rebuilding inline, so
        // PARTITIONS FALLBACK rebuilds the whole MV through the COMPLETE attempt it keeps.
        Assertions.assertEquals("[COMPLETE]", attempts.toString());
        Assertions.assertEquals(IvmFailureReason.BINLOG_BROKEN.name(),
                Deencapsulation.getField(task, "ivmFallbackReason"));
        // The barrier is released by the caller once the reshaped attempts have run.
        Mockito.verify(mtmv, Mockito.never()).releaseIvmBaselineRebuild(Mockito.anyLong());
    }

    @Test
    public void testPendingBaselineRebuildChecksTheStreamsItsPartitionsRead() throws Exception {
        // A partial barrier left by an earlier failed refresh. The pre-step rebuilds those partitions
        // before the attempts run, and that rebuild reads their streams, so a stream missing for them
        // decides the request just as it does for the partition attempt -- and PARTITIONS FALLBACK
        // reaches this pre-step without an IVM attempt for buildAttempts to have judged.
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.getId()).thenReturn(7L);
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(Collections.emptySet());
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.addPendingBaselineRebuildPartitions(Sets.newHashSet(poneName));
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
        OlapTable t1 = mockBaseTable("t1");
        BaseTableInfo t1Info = Mockito.mock(BaseTableInfo.class);
        mtmvUtilStatic.when(() -> MTMVUtil.getTable(t1Info)).thenReturn(t1);
        // t1 is not a PCT table, so every partition this rebuild refreshes reads it through its stream,
        // and the MV's database holds no stream for it.
        Mockito.when(mtmv.getDatabase()).thenReturn(Mockito.mock(Database.class));
        MTMVRefreshContext context = Mockito.mock(MTMVRefreshContext.class);
        Mockito.when(context.getByPartitionName(Mockito.anyString())).thenReturn(Maps.newHashMap());

        MTMVRelation relation = new MTMVRelation(Sets.newHashSet(t1Info), Sets.newHashSet(t1Info),
                Sets.newHashSet(t1Info), Sets.newHashSet(), Sets.newHashSet());
        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.PARTITIONS, true, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<Object> attempts = Lists.newArrayList();
        attempts.addAll(Deencapsulation.invoke(task, "buildAttempts", request, false));
        Assertions.assertEquals("[PARTITIONS, COMPLETE]", attempts.toString());

        try {
            Deencapsulation.invoke(task, "handlePendingIvmBaselineRebuild", context, request,
                    new ConnectContext(), attempts);
        } catch (Exception expected) {
            // Without the stream check the pre-step rebuilds inline, and how far that rebuild gets
            // against these mocks is not what this test is about; the attempts it leaves behind are.
        }

        // The rebuild that cannot read its streams is skipped rather than attempted: its own barrier
        // would have guarded nothing but the data it never wrote, and the COMPLETE attempt left in the
        // list reconciles the stream and clears the barrier that is already pending.
        Assertions.assertEquals("[COMPLETE]", attempts.toString());
        Assertions.assertEquals(IvmFailureReason.STREAM_UNSUPPORTED.name(),
                Deencapsulation.getField(task, "ivmFallbackReason"));
        Mockito.verify(mtmv, Mockito.never()).persistIvmBaselineGuard(Mockito.any(), Mockito.anySet(),
                Mockito.anyLong());
        Mockito.verify(mtmv, Mockito.never()).releaseIvmBaselineRebuild(Mockito.anyLong());

        // The same rebuild without fallback is not covered by a COMPLETE attempt, so it fails here
        // rather than starting a rebuild that cannot read its streams.
        MTMVTask strictTask = new MTMVTask(mtmv, relation, MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.PARTITIONS, false, null));
        Object strictRequest = Deencapsulation.invoke(strictTask, "resolveRefreshRequest");
        List<Object> strictAttempts = Lists.newArrayList();
        strictAttempts.addAll(Deencapsulation.invoke(strictTask, "buildAttempts", strictRequest, false));
        Assertions.assertEquals("[PARTITIONS]", strictAttempts.toString());

        JobException exception = Assertions.assertThrows(JobException.class,
                () -> Deencapsulation.invoke(strictTask, "handlePendingIvmBaselineRebuild", context,
                        strictRequest, new ConnectContext(), strictAttempts));

        Assertions.assertTrue(exception.getMessage().contains("IVM stream is unusable"));
    }

    @Test
    public void testCompleteAttemptWritesTheBarrierBeforeReconcilingStreams() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getPartitionNames()).thenReturn(Sets.newHashSet(poneName));
        // The reconcile starts from the MV's database, which is what makes it visible to the order check.
        Mockito.when(mtmv.getDatabase()).thenReturn(Mockito.mock(Database.class));
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        InOrder inOrder = Mockito.inOrder(mtmv);

        try {
            Deencapsulation.invoke(task, "executeCompleteAttempt",
                    Mockito.mock(MTMVRefreshContext.class), new ConnectContext());
        } catch (Exception expected) {
            // How far the rebuild itself gets is not what this test is about.
        }

        // A recreated stream starts from the base table's current rows, so the barrier that makes the
        // next refresh rebuild the MV has to be durable before the stream is replaced. The other order
        // loses those rows with no error anywhere.
        inOrder.verify(mtmv).persistIvmBaselineGuard(Mockito.any(), Mockito.anySet(), Mockito.anyLong());
        inOrder.verify(mtmv).getDatabase();
    }


    @Test
    public void testDroppedBaselinePartitionsReleaseBarrierWithoutRebuild() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.addPendingBaselineRebuildPartitions(Sets.newHashSet(poneName));
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
        // Partition sync already dropped the partition the barrier named, so nothing is left to
        // pre-rebuild and the surviving partitions catch up through the attempts themselves.
        Mockito.when(mtmv.getPartitionNames()).thenReturn(Sets.newHashSet(ptwoName));
        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.PARTITIONS, true, null));
        Deencapsulation.setField(task, "mtmvSchemaChangeVersion", 7L);
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");

        List<Object> attempts = Lists.newArrayList();
        attempts.addAll(Deencapsulation.invoke(task, "buildAttempts", request, false));
        Deencapsulation.invoke(task, "handlePendingIvmBaselineRebuild",
                Mockito.mock(MTMVRefreshContext.class), request, new ConnectContext(), attempts);

        Assertions.assertEquals("[PARTITIONS, COMPLETE]", attempts.toString());
        Assertions.assertNull(Deencapsulation.getField(task, "refreshMode"));
        Assertions.assertEquals(IvmFailureReason.BINLOG_BROKEN.name(),
                Deencapsulation.getField(task, "ivmFallbackReason"));
        Mockito.verify(mtmv).releaseIvmBaselineRebuild(7L);
    }

    @Test
    public void testExecuteIvmAttemptKeepsRefreshScopeForNonSignatureFallbackInAutoMode() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        MTMVRefreshContext refreshContext = mockIvmIncrRefreshContext();
        Deencapsulation.setField(task, "needRefreshPartitions", Lists.newArrayList(poneName));
        Deencapsulation.setField(task, "refreshMode", MTMVTask.MTMVTaskRefreshMode.PARTIAL);

        try (MockedConstruction<IvmIncrRefreshManager> ignored = Mockito.mockConstruction(IvmIncrRefreshManager.class,
                (mock, context) -> Mockito.when(mock.doRefresh(Mockito.any()))
                        .thenReturn(
                        IvmIncrRefreshResult.fallback(IvmFailureReason.BINLOG_NOT_ENABLED, "no_binlog")))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt", refreshContext, request,
                    new ConnectContext(), Lists.newArrayList());
            Assertions.assertEquals("FALLBACK_ALLOWED", result.toString());
        }

        Assertions.assertEquals(Lists.newArrayList(poneName),
                Deencapsulation.getField(task, "needRefreshPartitions"));
        Assertions.assertEquals(MTMVTask.MTMVTaskRefreshMode.PARTIAL,
                Deencapsulation.getField(task, "refreshMode"));
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
            Assertions.assertNotEquals(storedSignature.getSha256(), currentSignature.getSha256());

            IvmInfo ivmInfo = new IvmInfo();
            ivmInfo.setPlanSignature(storedSignature.getSha256());
            Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
            Mockito.when(mtmv.isIvm()).thenReturn(true);
            Mockito.when(mtmv.getName()).thenReturn("test_mv");
            Mockito.when(mtmv.getPartitionNames()).thenReturn(Sets.newHashSet(poneName, ptwoName));

            MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
            MTMVRefreshContext refreshContext = mockIvmIncrRefreshContext();
            Deencapsulation.setField(task, "needRefreshPartitions", Lists.newArrayList(poneName));
            Deencapsulation.setField(task, "refreshMode", MTMVTask.MTMVTaskRefreshMode.PARTIAL);

            try (MockedConstruction<IvmIncrRefreshManager> ignored = Mockito.mockConstruction(IvmIncrRefreshManager.class,
                    (mock, context) -> Mockito.when(mock.doRefresh(Mockito.any()))
                        .thenReturn(
                            IvmIncrRefreshResult.fallback(IvmFailureReason.PLAN_SIGNATURE_MISMATCH,
                                    "layout drift")))) {
                Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
                Object result = Deencapsulation.invoke(task, "executeIvmAttempt", refreshContext, request,
                        new ConnectContext(), Lists.newArrayList());
                Assertions.assertEquals("FALLBACK_TO_COMPLETE", result.toString());
            }

            Assertions.assertEquals(IvmFailureReason.PLAN_SIGNATURE_MISMATCH.name(),
                    Deencapsulation.getField(task, "ivmFallbackReason"));
        } finally {
            DebugPointUtil.clearDebugPoints();
            Config.enable_debug_points = originalEnableDebugPoints;
        }
    }

    @Test
    public void testSignatureMismatchCompleteCapturesConsistentPlanSignature() throws Exception {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Deencapsulation.setField(task, "ivmFallbackReason", IvmFailureReason.PLAN_SIGNATURE_MISMATCH.name());
        IvmPlanSignature signature = new IvmPlanSignature("layout_s2", "signature_s2");

        executeCompleteRefresh(task, signature, signature);

        Assertions.assertEquals("signature_s2", task.getRefreshedIvmPlanSignature());
    }

    @Test
    public void testSignatureMismatchCompleteRejectsInconsistentBatchSignatures() {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Deencapsulation.setField(task, "ivmFallbackReason", IvmFailureReason.PLAN_SIGNATURE_MISMATCH.name());

        JobException exception = Assertions.assertThrows(JobException.class, () -> executeCompleteRefresh(task,
                new IvmPlanSignature("layout_s2", "signature_s2"),
                new IvmPlanSignature("layout_s3", "signature_s3")));

        Assertions.assertTrue(exception.getMessage().contains("inconsistent plan signatures"));
        Assertions.assertNull(task.getRefreshedIvmPlanSignature());
    }

    @Test
    public void testOtherCompleteFallbackDoesNotCapturePlanSignature() throws Exception {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Deencapsulation.setField(task, "ivmFallbackReason", IvmFailureReason.BINLOG_BROKEN.name());
        IvmPlanSignature signature = new IvmPlanSignature("layout_s2", "signature_s2");

        executeCompleteRefresh(task, signature, signature);

        Assertions.assertNull(task.getRefreshedIvmPlanSignature());
    }

    @Test
    public void testIvmExecutionFailureFallsBackToPartitions() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        MTMVTaskContext context = MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.INCREMENTAL, true, null);
        MTMVTask task = new MTMVTask(mtmv, relation, context);
        MTMVRefreshContext refreshContext = mockIvmIncrRefreshContext();

        try (MockedConstruction<IvmIncrRefreshManager> ignored = Mockito.mockConstruction(IvmIncrRefreshManager.class,
                (mock, constructionContext) -> Mockito.when(
                        mock.doRefresh(Mockito.any())).thenReturn(
                        IvmIncrRefreshResult.fallback(IvmFailureReason.INCREMENTAL_EXECUTION_FAILED, "delta failed")))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt", refreshContext, request,
                    new ConnectContext(), Lists.newArrayList());
            Assertions.assertEquals("FALLBACK_ALLOWED", result.toString());
        }

        Assertions.assertEquals(IvmFailureReason.INCREMENTAL_EXECUTION_FAILED.name(),
                Deencapsulation.getField(task, "ivmFallbackReason"));
    }

    @Test
    public void testOldTaskJsonWithoutIvmFallbackReasonDeserializes() {
        String oldJson = "{\"di\":1,\"mi\":2,\"taskContext\":{\"triggerMode\":\"MANUAL\"}}";

        MTMVTask task = GsonUtils.GSON.fromJson(oldJson, MTMVTask.class);

        Assertions.assertNotNull(task);
        Assertions.assertNull(Deencapsulation.getField(task, "ivmFallbackReason"));
    }

    @Test
    public void testRefreshedIvmPlanSignatureIsNotSerialized() {
        MTMVTask task = new MTMVTask();
        Deencapsulation.setField(task, "refreshedIvmPlanSignature", "new_signature");

        Assertions.assertFalse(GsonUtils.GSON.toJson(task).contains("new_signature"));
    }

    private IvmPlanSignature signatureForDebugDriftTest() {
        IvmRewriteResult rewriteResult = new IvmRewriteResult();
        rewriteResult.setNormalizedPlan(buildSignaturePlan());
        return new IvmPlanSignatureGenerator().generate(rewriteResult.getNormalizedPlan());
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

    private void executeCompleteRefresh(MTMVTask task, IvmPlanSignature firstBatchSignature,
            IvmPlanSignature secondBatchSignature) throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.getRefreshPartitionNum()).thenReturn(1);
        Mockito.when(mtmvPartitionInfo.getPctInfos()).thenReturn(Collections.emptyList());
        Database database = Mockito.mock(Database.class);
        Mockito.when(database.getFullName()).thenReturn("test_db");
        Mockito.when(mtmv.getDatabase()).thenReturn(database);

        MTMVRefreshContext refreshContext = Mockito.mock(MTMVRefreshContext.class);
        Mockito.when(refreshContext.getByPartitionName(Mockito.anyString())).thenReturn(Collections.emptyMap());
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.generatePartitionSnapshots(
                Mockito.same(refreshContext), Mockito.anySet(), Mockito.anySet()))
                .thenReturn(Collections.emptyMap());
        Deencapsulation.setField(task, "needRefreshPartitions", Lists.newArrayList(poneName, ptwoName));

        ConnectContext mtmvCtx = new ConnectContext();
        mtmvCtx.setQueryId(new TUniqueId(1L, 2L));
        mtmvCtx.setThreadLocalInfo();
        UpdateMvByPartitionCommand command = Mockito.mock(UpdateMvByPartitionCommand.class);
        try (MockedStatic<MTMVPlanUtil> mtmvPlanUtilStatic = Mockito.mockStatic(MTMVPlanUtil.class);
                MockedStatic<UpdateMvByPartitionCommand> updateMvStatic
                        = Mockito.mockStatic(UpdateMvByPartitionCommand.class)) {
            mtmvPlanUtilStatic.when(() -> MTMVPlanUtil.createMTMVContext(Mockito.eq(mtmv), Mockito.anyList()))
                    .thenReturn(mtmvCtx);
            updateMvStatic.when(() -> UpdateMvByPartitionCommand.from(
                    Mockito.eq(mtmv), Mockito.anySet(), Mockito.anyMap(), Mockito.any(StatementContext.class)))
                    .thenReturn(command);
            // Build nested mocks before starting the static stubbing chain.
            List<StmtExecutor> batchExecutors = Lists.newArrayList(
                    executorWithPlanSignature(firstBatchSignature),
                    executorWithPlanSignature(secondBatchSignature));
            AtomicInteger batchIndex = new AtomicInteger();
            mtmvPlanUtilStatic.when(() -> MTMVPlanUtil.executeCommand(
                    Mockito.eq(mtmvCtx), Mockito.eq(command), Mockito.any(StatementContext.class),
                    Mockito.anyString(), Mockito.any(Consumer.class))).thenAnswer(invocation -> {
                        Consumer<StmtExecutor> executorConsumer = invocation.getArgument(4);
                        executorConsumer.accept(batchExecutors.get(batchIndex.getAndIncrement()));
                        executorConsumer.accept(null);
                        return null;
                    });

            Deencapsulation.invoke(task, "executePartitionBasedRefresh",
                    refreshContext, RefreshMode.COMPLETE, mtmvCtx);
        } finally {
            ConnectContext.remove();
        }
    }

    private StmtExecutor executorWithPlanSignature(IvmPlanSignature signature) {
        IvmRewriteResult rewriteResult = new IvmRewriteResult();
        rewriteResult.setPlanSignature(signature);
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        Mockito.when(cascadesContext.getIvmRewriteResult()).thenReturn(Optional.of(rewriteResult));
        NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        Mockito.when(planner.getCascadesContext()).thenReturn(cascadesContext);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.planner()).thenReturn(planner);
        return executor;
    }

    private MTMVRefreshContext mockIvmIncrRefreshContext() throws AnalysisException {
        MTMVRefreshContext refreshContext = Mockito.mock(MTMVRefreshContext.class);
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                Mockito.same(refreshContext), Mockito.nullable(Set.class))).thenReturn(Lists.newArrayList(poneName));
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.generatePartitionSnapshots(
                Mockito.same(refreshContext), Mockito.nullable(Set.class), Mockito.nullable(Set.class)))
                .thenReturn(Collections.emptyMap());
        return refreshContext;
    }

    @Test
    public void testRegisterExecutorRejectsCancelledTask() {
        MTMVTask task = new MTMVTask(mtmv, relation,
                MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null, RefreshMode.INCREMENTAL, true, null));
        task.setStatus(TaskStatus.CANCELED);
        org.apache.doris.qe.StmtExecutor executor = Mockito.mock(org.apache.doris.qe.StmtExecutor.class);

        boolean rejected = false;
        try {
            task.registerExecutor(executor);
        } catch (IllegalStateException e) {
            rejected = true;
        }
        Assertions.assertTrue(rejected, "registerExecutor must reject a cancelled task");
        // A cancelled task must not expose a registered executor to the cancel path.
        Assertions.assertNull(Deencapsulation.getField(task, "executor"));
    }

    @Test
    public void testRegisterExecutorAcceptsRunningTask() {
        MTMVTask task = new MTMVTask(mtmv, relation,
                MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null, RefreshMode.INCREMENTAL, true, null));
        task.setStatus(TaskStatus.RUNNING);
        org.apache.doris.qe.StmtExecutor executor = Mockito.mock(org.apache.doris.qe.StmtExecutor.class);

        task.registerExecutor(executor);
        Assertions.assertSame(executor, Deencapsulation.getField(task, "executor"));
        // Registering null clears the field (used by executeCommand's finally after the command finishes).
        task.registerExecutor(null);
        Assertions.assertNull(Deencapsulation.getField(task, "executor"));
    }
}
