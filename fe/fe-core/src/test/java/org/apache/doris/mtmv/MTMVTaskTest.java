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
import java.util.concurrent.ConcurrentSkipListSet;
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
        Mockito.when(mtmv.getStatus()).thenReturn(new MTMVStatus());
        // Sane defaults for the epoch state: no partition needs a rebuild unless a case says so. The
        // routing reads the states themselves, so a case that wants a rebuild gives it a state whose
        // requirement is ahead of what it holds. What the escalation reads is the MV's own verdict over
        // those states (MTMVTest covers it), so a case that wants it stubs the verdict.
        Mockito.when(mtmv.getPartitionStates()).thenReturn(Collections.emptyMap());
        Mockito.when(mtmv.allPartitionsNeedRebuild()).thenReturn(false);
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

    /**
     * An epoch a batch captures is clamped to the one the routing decision saw.
     *
     * <p>An invalidation that lands between that decision and the batch's read raises the partition's
     * requirement above what the batch is about to read; recording what it read would describe the data as
     * current, and the delta this refresh applies cannot remove the rows the invalidation made unusable.
     * Recording the planned value leaves the partition dirty, so the next refresh rebuilds it.
     */
    @Test
    public void testCapturedEpochIsClampedToTheOneTheRoutingDecisionSaw() {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        // The routing decided on p1 at epoch 2, and an invalidation raised it to 5 before the read.
        Deencapsulation.setField(task, "ivmPlannedEpochs", Maps.newHashMap(Map.of(poneName, 2L)));
        Deencapsulation.invoke(task, "commitCapturedEpochs", Maps.newHashMap(Map.of(poneName, 5L)));

        Assertions.assertEquals(Map.of(poneName, 2L), Deencapsulation.getField(task, "ivmCapturedEpochs"));

        // A partition no routing decision named keeps what the batch read: there is no ceiling to clamp to.
        MTMVTask unnamed = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Deencapsulation.invoke(unnamed, "commitCapturedEpochs", Maps.newHashMap(Map.of(ptwoName, 7L)));

        Assertions.assertEquals(Map.of(ptwoName, 7L), Deencapsulation.getField(unnamed, "ivmCapturedEpochs"));
    }

    /**
     * A PARTITIONS request that may not fall back is not widened by an invalidated baseline.
     *
     * <p>What the invalidation needs rebuilt is not what the request names -- it covers partitions partition
     * sync has not created yet -- so refreshing the named ones would leave the MV in SCHEMA_CHANGE with rows
     * nothing rebuilt, and widening to COMPLETE would rebuild partitions the caller deliberately kept out.
     * The forms whose scope already includes a whole-MV rebuild are the ones that can answer it.
     */
    @Test
    public void testAStrictPartitionsRefreshIsRefusedRatherThanWidened() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.getStatus()).thenReturn(new MTMVStatus(MTMVState.SCHEMA_CHANGE, "invalidated"));
        MTMVTask task = new MTMVTask(mtmv, relation,
                MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null, RefreshMode.PARTITIONS, false, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");

        JobException exception = Assertions.assertThrows(JobException.class,
                () -> Deencapsulation.invoke(task, "buildAttempts", request, false));

        Assertions.assertTrue(exception.getMessage().contains("PARTITIONS FALLBACK"), exception.getMessage());

        // The same request with fallback allowed reaches the COMPLETE its scope already carries.
        MTMVTask fallbackTask = new MTMVTask(mtmv, relation,
                MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null, RefreshMode.PARTITIONS, true, null));
        Object fallbackRequest = Deencapsulation.invoke(fallbackTask, "resolveRefreshRequest");

        Assertions.assertEquals(Lists.newArrayList("COMPLETE"),
                toNames((List<?>) Deencapsulation.invoke(fallbackTask, "buildAttempts", fallbackRequest, false)));
    }

    /**
     * The captured epochs are handed out as a copy: a caller writing through the getter would be editing
     * what the task publishes, and a STOP publishes while the executing worker may still be merging into
     * the map the caller would be iterating.
     */
    @Test
    public void testCapturedEpochsAreHandedOutAsACopy() {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        task.getIvmCapturedEpochs().put(poneName, 3L);

        Assertions.assertTrue(((Map<?, ?>) Deencapsulation.getField(task, "ivmCapturedEpochs")).isEmpty());
    }

    /**
     * A task read back from the journal carries no captured epochs: the field is transient, so gson leaves
     * it null and the constructor that would have initialized it never runs. A replay applies the states
     * the record carries instead, so the getter has to answer for that case rather than throw.
     */
    @Test
    public void testCapturedEpochsOfATaskReadBackFromTheJournalAreEmpty() {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Deencapsulation.setField(task, "ivmCapturedEpochs", null);

        Assertions.assertTrue(task.getIvmCapturedEpochs().isEmpty());
    }

    /**
     * The rebuilt-partition count is an IVM diagnostic, and a plain MV reaches the COMPLETE success path
     * through an ordinary AUTO refresh -- where rebuilding is what the refresh does, not a side effect of an
     * invalidated baseline.
     */
    @Test
    public void testANonIvmRefreshReportsNoRebuiltPartitions() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(false);
        MTMVTask task = new MTMVTask(mtmv, relation,
                MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null, RefreshMode.AUTO, false, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");

        Deencapsulation.invoke(task, "recordRebuiltPartitions", request);

        Assertions.assertEquals(0, (int) Deencapsulation.getField(task, "ivmRebuiltPartitions"));
    }

    /**
     * The count is what the refresh has replaced, so it is read from the same accumulator the task reports
     * its progress with. That accumulator is created by the first phase that reports one, which leaves the
     * attempt that reports before any phase has: a whole-MV refresh of an MV with nothing to refresh, and
     * one the stream reconciliation threw out of, both report on a task that has committed nothing. That is
     * no partitions, and it must read as zero where the refresh is reported rather than throw there.
     */
    @Test
    public void testARefreshThatReplacedNothingReportsNoRebuiltPartitions() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        MTMVTask task = new MTMVTask(mtmv, relation,
                MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null, RefreshMode.AUTO, true, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        Assertions.assertNull(Deencapsulation.getField(task, "completedPartitions"));

        Deencapsulation.invoke(task, "recordRebuiltPartitions", request);

        Assertions.assertEquals(0, (int) Deencapsulation.getField(task, "ivmRebuiltPartitions"));
    }

    /**
     * And the count is what committed, not what was planned: the accumulator grows as batches commit, so a
     * rebuild that replaced one of its partitions and failed on the next reports one.
     */
    @Test
    public void testTheRebuiltCountFollowsThePartitionsThatCommitted() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        MTMVTask task = new MTMVTask(mtmv, relation,
                MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null, RefreshMode.AUTO, true, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        Deencapsulation.invoke(task, "recordRefreshCompleted", Lists.newArrayList(poneName));

        Deencapsulation.invoke(task, "recordRebuiltPartitions", request);

        Assertions.assertEquals(1, (int) Deencapsulation.getField(task, "ivmRebuiltPartitions"));

        // A later attempt that replaced both keeps the larger count rather than the last one it read.
        Deencapsulation.invoke(task, "recordRefreshCompleted", Lists.newArrayList(poneName, ptwoName));
        Deencapsulation.invoke(task, "recordRebuiltPartitions", request);

        Assertions.assertEquals(2, (int) Deencapsulation.getField(task, "ivmRebuiltPartitions"));
    }

    /**
     * A retry that synchronized partitions has to be judged again: alignment gives a partition it creates
     * {@code {0, 1}} -- behind its requirement -- and the routing decision was taken before it existed.
     * Without the fresh read the retried attempt would hand it to the delta path with no ceiling to be
     * clamped against, and its capture is all that path can produce: the partition would be recorded as
     * caught up while it has never received a baseline.
     */
    @Test
    public void testRetryAdoptsThePartitionsAlignmentCreated() {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        // The routing saw p1 as caught up at epoch 2; p2 is what the retry's alignment added, and
        // alignment gives a partition it creates {0, 1}.
        Deencapsulation.setField(task, "ivmPlannedEpochs", Maps.newHashMap(Map.of(poneName, 2L)));
        Mockito.when(mtmv.getPartitionStates()).thenReturn(Maps.newHashMap(Map.of(
                poneName, new MTMVPartitionState(2, 2),
                ptwoName, MTMVPartitionState.initial())));
        Set<String> dirtyPartitions = Sets.newLinkedHashSet();

        Deencapsulation.invoke(task, "adoptPartitionsCreatedByTheRetry", dirtyPartitions);

        Assertions.assertEquals(Sets.newHashSet(ptwoName), dirtyPartitions);
        Assertions.assertEquals(Map.of(poneName, 2L, ptwoName, 1L),
                Deencapsulation.getField(task, "ivmPlannedEpochs"));
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

    @Test
    public void testBuildAttemptsEscalatesToCompleteWhenEveryPartitionNeedsARebuild() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        // Every partition either holds rows read before a change or was never filled: COMPLETE does exactly
        // what their routing branches would, in a single read of the MV. Which partition states make that
        // verdict true is MTMVTest's, next to the predicate that reads them.
        Mockito.when(mtmv.allPartitionsNeedRebuild()).thenReturn(true);

        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.INCREMENTAL, true, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);

        Assertions.assertEquals(Lists.newArrayList("COMPLETE"), toNames(attempts));
    }

    @Test
    public void testBuildAttemptsDoesNotEscalateWithoutAnInvalidatedPartition() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        // Nothing needs a rebuild, so COMPLETE would be a full recomputation for no reason. The verdict is
        // the default in setUp (false); asserted here so a change to it is a failure rather than a shrug.
        Assertions.assertFalse(mtmv.allPartitionsNeedRebuild());

        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.INCREMENTAL, true, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);

        Assertions.assertEquals(Lists.newArrayList("IVM", "PARTITIONS", "COMPLETE"), toNames(attempts));
    }

    @Test
    public void testBuildAttemptsEscalatesAnIvmMvInSchemaChangeToComplete() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        Mockito.when(mtmv.getStatus()).thenReturn(new MTMVStatus(
                MTMVState.SCHEMA_CHANGE, "the base table has been updated"));

        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.AUTO, true, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);

        // A schema-level invalidation is not a set of dirty partitions: it covers the partitions partition
        // sync has not created yet as well, so no per-partition branch can express it. The task says how
        // many partitions the request did not ask to rebuild, which is what the INCREMENTAL attempt would
        // have left out of the trace otherwise.
        Assertions.assertEquals(Lists.newArrayList("COMPLETE"), toNames(attempts));
        // Planning does not claim the work: the count is recorded once the rebuild has actually run, so a
        // refresh that fails before replacing anything reports nothing rather than the whole MV. What this
        // route owes the request is the escalation itself, which is the assertion above.
        Assertions.assertEquals(0, (int) Deencapsulation.getField(task, "ivmRebuiltPartitions"));
    }

    @Test
    public void testBuildAttemptsLeavesANonIvmMvInSchemaChangeOnItsOwnChain() throws Exception {
        // setUp stubs a non-IVM MV. Its state is not what its attempt chain is built from: the cleared
        // snapshot of a schema change already sends every partition to a rebuild, so the chain stays as it
        // was, which is what keeps this change from moving a non-IVM MV's behaviour.
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.AUTO);
        Mockito.when(mtmv.getStatus()).thenReturn(new MTMVStatus(
                MTMVState.SCHEMA_CHANGE, "the base table has been updated"));

        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);

        Assertions.assertEquals(Lists.newArrayList("PARTITIONS", "COMPLETE"), toNames(attempts));
        Assertions.assertEquals(0, (int) Deencapsulation.getField(task, "ivmRebuiltPartitions"));
    }

    @Test
    public void testBuildAttemptsKeepsAnExplicitPartitionListOutOfTheSchemaChangeEscalation() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        Mockito.when(mtmv.getStatus()).thenReturn(new MTMVStatus(
                MTMVState.SCHEMA_CHANGE, "the base table has been updated"));

        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, Lists.newArrayList(poneName), RefreshMode.AUTO));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);

        // An explicit partition list is an exact request and is never widened to COMPLETE. (An IVM MV
        // rejects one at analysis time, so this is the belt to that suspender.)
        Assertions.assertEquals(Lists.newArrayList("PARTITIONS"), toNames(attempts));
        Assertions.assertEquals(0, (int) Deencapsulation.getField(task, "ivmRebuiltPartitions"));
    }

    @Test
    public void testBuildAttemptsRebuildsTheWholeMvForAStrictIncrementalInSchemaChange() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmvRefreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        Mockito.when(mtmv.getStatus()).thenReturn(new MTMVStatus(
                MTMVState.SCHEMA_CHANGE, "the base table has been updated"));

        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(
                MTMVTaskTriggerMode.MANUAL, null, RefreshMode.INCREMENTAL, false, null));
        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        List<?> attempts = (List<?>) Deencapsulation.invoke(task, "buildAttempts", request, false);

        // Asking against the fallback does not refuse this one: the baseline the incremental attempt would
        // read is gone, while a COMPLETE refresh restores exactly it. Reporting the count is what keeps the
        // request honest -- the result says the partitions it did not ask for were rebuilt.
        Assertions.assertEquals(Lists.newArrayList("COMPLETE"), toNames(attempts));
        // The request's count is reported by the result, not decided here: recorded once the rebuild has
        // run, so a refresh that never replaced anything reports nothing.
        Assertions.assertEquals(0, (int) Deencapsulation.getField(task, "ivmRebuiltPartitions"));
    }

    @Test
    public void testIncrementalAttemptLeavesTheRebuiltPartitionsOutOfItsScope() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        MTMVRefreshContext refreshContext = Mockito.mock(MTMVRefreshContext.class);
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                Mockito.same(refreshContext), Mockito.nullable(Set.class)))
                .thenReturn(Lists.newArrayList(poneName, ptwoName));
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.generatePartitionSnapshots(
                Mockito.same(refreshContext), Mockito.nullable(Set.class), Mockito.nullable(Set.class)))
                .thenReturn(Collections.emptyMap());

        try (MockedConstruction<IvmIncrRefreshManager> ignored = Mockito.mockConstruction(IvmIncrRefreshManager.class,
                (mock, context) -> Mockito.when(mock.doRefresh(Mockito.any()))
                        .thenReturn(IvmIncrRefreshResult.success()))) {
            // poneName was rebuilt by the partition executor before this attempt, so the incremental
            // refresh must not treat it as needing a catch-up: it can only append, and it would record the
            // partition as current while its rows are exactly what the rebuild replaced.
            IvmIncrRefreshResult result = (IvmIncrRefreshResult) Deencapsulation.invoke(
                    task, "executeSingleIvmAttempt", refreshContext, Sets.newHashSet(poneName));
            Assertions.assertTrue(result.isSuccess());
        }

        Assertions.assertEquals(Sets.newHashSet(ptwoName),
                Deencapsulation.getField(task, "needRefreshPartitions"));
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

    /**
     * A refresh that rebuilt a partition and then fell back out of the incremental attempt must not plan
     * to refresh that partition again. The plan is computed from the snapshot the MV holds, which such a
     * refresh has not published yet, so without the rebuild's own record the partition still looks unsynced
     * and the fallback replaces the work the rebuild just did.
     */
    @Test
    public void testFallbackPlanLeavesTheRebuiltPartitionsOut() throws Exception {
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.isMTMVSync(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class),
                Mockito.nullable(Set.class))).thenReturn(false);
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class)))
                .thenReturn(Lists.newArrayList(poneName, ptwoName));
        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.INCREMENTAL, true, null));
        // poneName was rebuilt by the partition executor before this attempt, so the task's accumulator
        // already holds it.
        Map<String, MTMVRefreshPartitionSnapshot> accumulated = Maps.newConcurrentMap();
        accumulated.put(poneName, Mockito.mock(MTMVRefreshPartitionSnapshot.class));
        Deencapsulation.setField(task, "partitionSnapshots", accumulated);

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        Object plan = Deencapsulation.invoke(task, "planPartitionRefresh",
                Mockito.mock(MTMVRefreshContext.class), request);

        Assertions.assertTrue((Boolean) Deencapsulation.getField(plan, "canRefreshByPartitions"));
        Assertions.assertEquals(Lists.newArrayList(ptwoName),
                Deencapsulation.getField(plan, "partitions"));
    }

    /**
     * A partition the criterion says must be rebuilt is planned even though the snapshots say the MV is in
     * sync. The two answer different questions -- what a refresh last read, and what a later read cannot
     * catch up -- and they disagree in the one state a whole-MV rebuild that did not finish leaves behind:
     * every requirement raised, every snapshot kept. Planning by snapshots alone would report NOT_REFRESH
     * over partitions whose rows nothing repaired, and only the entry points that reach the incremental
     * attempt would ever recover them.
     */
    @Test
    public void testPlanPartitionRefreshPlansThePartitionsThatNeedARebuild() throws Exception {
        // setUp stubs isMTMVSync true and getMTMVNeedRefreshPartitions empty: by themselves they say there
        // is nothing to refresh, which is the early return this has to get past.
        Mockito.when(mtmv.getPartitionsNeedingRebuild())
                .thenReturn(Sets.newLinkedHashSet(Sets.newHashSet(poneName)));
        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.PARTITIONS, true, null));

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        Object plan = Deencapsulation.invoke(task, "planPartitionRefresh",
                Mockito.mock(MTMVRefreshContext.class), request);

        Assertions.assertTrue((Boolean) Deencapsulation.getField(plan, "canRefreshByPartitions"));
        Assertions.assertEquals(Lists.newArrayList(poneName),
                Deencapsulation.getField(plan, "partitions"));
    }

    /**
     * The two answers are both planned when they disagree in the other direction as well: a partition the
     * snapshots call unsynced joins the ones that need a rebuild, rather than replacing them.
     */
    @Test
    public void testPlanPartitionRefreshKeepsTheCriterionAlongsideTheSnapshots() throws Exception {
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.isMTMVSync(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class),
                Mockito.nullable(Set.class))).thenReturn(false);
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class)))
                .thenReturn(Lists.newArrayList(ptwoName));
        Mockito.when(mtmv.getPartitionsNeedingRebuild())
                .thenReturn(Sets.newLinkedHashSet(Sets.newHashSet(poneName)));
        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.PARTITIONS, true, null));

        Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
        Object plan = Deencapsulation.invoke(task, "planPartitionRefresh",
                Mockito.mock(MTMVRefreshContext.class), request);

        Assertions.assertEquals(Sets.newHashSet(poneName, ptwoName),
                Sets.newHashSet((List<?>) Deencapsulation.getField(plan, "partitions")));
    }

    /**
     * The partitions an earlier phase replaced survive the incremental attempt that falls back after them.
     * The task keeps one accumulator for the whole of it, because that is what the MV publishes: a phase
     * that started from empty would drop the work of the phases before it, and the next refresh would find
     * those partitions unsynced and replace them once more.
     */
    @Test
    public void testFallbackKeepsTheSnapshotsOfThePartitionsTheRebuildReplaced() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        MTMVRefreshPartitionSnapshot rebuiltSnapshot = Mockito.mock(MTMVRefreshPartitionSnapshot.class);
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class)))
                .thenReturn(Lists.newArrayList(ptwoName));
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.generatePartitionSnapshots(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class),
                Mockito.nullable(Set.class))).thenReturn(Collections.emptyMap());
        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.INCREMENTAL, true, null));
        Map<String, MTMVRefreshPartitionSnapshot> accumulated = Maps.newConcurrentMap();
        accumulated.put(poneName, rebuiltSnapshot);
        Deencapsulation.setField(task, "partitionSnapshots", accumulated);

        try (MockedConstruction<IvmIncrRefreshManager> ignored = Mockito.mockConstruction(
                IvmIncrRefreshManager.class, (mock, context) -> Mockito.when(mock.doRefresh(Mockito.any()))
                        .thenReturn(IvmIncrRefreshResult.fallback(
                                IvmFailureReason.INCREMENTAL_EXECUTION_FAILED, "forced")))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt",
                    Mockito.mock(MTMVRefreshContext.class), request, Mockito.mock(ConnectContext.class),
                    Lists.newArrayList());
            Assertions.assertEquals("FALLBACK_ALLOWED", result.toString());
        }

        Assertions.assertSame(rebuiltSnapshot,
                ((Map<?, ?>) Deencapsulation.getField(task, "partitionSnapshots")).get(poneName));
    }

    /**
     * The same holds for what the task reports: an attempt adds to it rather than replacing it. The
     * partition the rebuild phase committed is part of the refresh the user asked for -- and the MV has
     * published it, along with its epoch -- so a report that dropped it would describe work this task did
     * as work it never did.
     */
    @Test
    public void testALaterAttemptAddsToTheReportInsteadOfReplacingIt() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.getMTMVNeedRefreshPartitions(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class)))
                .thenReturn(Lists.newArrayList(ptwoName));
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.generatePartitionSnapshots(
                Mockito.nullable(MTMVRefreshContext.class), Mockito.nullable(Set.class),
                Mockito.nullable(Set.class))).thenReturn(Collections.emptyMap());
        MTMVTask task = new MTMVTask(mtmv, relation, MTMVTaskContext.of(MTMVTaskTriggerMode.MANUAL, null,
                RefreshMode.INCREMENTAL, true, null));
        // The rebuild phase ran first and reported poneName as both its scope and its result.
        Deencapsulation.setField(task, "needRefreshPartitions",
                new ConcurrentSkipListSet<>(Sets.newHashSet(poneName)));
        Deencapsulation.setField(task, "completedPartitions",
                new ConcurrentSkipListSet<>(Sets.newHashSet(poneName)));

        try (MockedConstruction<IvmIncrRefreshManager> ignored = Mockito.mockConstruction(
                IvmIncrRefreshManager.class, (mock, context) -> Mockito.when(mock.doRefresh(Mockito.any()))
                        .thenReturn(IvmIncrRefreshResult.fallback(
                                IvmFailureReason.INCREMENTAL_EXECUTION_FAILED, "forced")))) {
            Object request = Deencapsulation.invoke(task, "resolveRefreshRequest");
            Object result = Deencapsulation.invoke(task, "executeIvmAttempt",
                    Mockito.mock(MTMVRefreshContext.class), request, Mockito.mock(ConnectContext.class),
                    Lists.newArrayList());
            Assertions.assertEquals("FALLBACK_ALLOWED", result.toString());
        }

        // The incremental attempt took a scope of its own and committed nothing, which must leave the
        // rebuild's partition in both sets rather than replacing them with its own.
        Assertions.assertEquals(Sets.newHashSet(poneName, ptwoName),
                Deencapsulation.getField(task, "needRefreshPartitions"));
        Assertions.assertEquals(Sets.newHashSet(poneName),
                Deencapsulation.getField(task, "completedPartitions"));
    }

    /**
     * And a partition is reported once, however many attempts cover it: a whole-MV rebuild after a
     * per-partition one names the partitions the rebuild already reported, and counting them twice would
     * report more work than the MV has partitions.
     */
    @Test
    public void testTheReportCountsAPartitionOnce() {
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        Deencapsulation.invoke(task, "recordRefreshScope", Lists.newArrayList(poneName, ptwoName));
        Deencapsulation.invoke(task, "recordRefreshCompleted", Lists.newArrayList(poneName));
        Deencapsulation.invoke(task, "recordRefreshScope", Lists.newArrayList(ptwoName));
        Deencapsulation.invoke(task, "recordRefreshCompleted", Lists.newArrayList(poneName, ptwoName));

        Assertions.assertEquals(Sets.newHashSet(poneName, ptwoName),
                Deencapsulation.getField(task, "needRefreshPartitions"));
        Assertions.assertEquals(Sets.newHashSet(poneName, ptwoName),
                Deencapsulation.getField(task, "completedPartitions"));
    }

    /**
     * The two columns these sets feed are persisted, so what changes about them is the field's type and not
     * the record it reads: a task written by an older FE carries them as arrays of names, which is what a
     * set is written as, and reads back into either shape.
     */
    @Test
    public void testThePartitionColumnsAreStillArraysInTheJournal() {
        MTMVTask task = GsonUtils.GSON.fromJson("{\"di\":1,\"mi\":2}", MTMVTask.class);
        Deencapsulation.invoke(task, "recordRefreshScope", Lists.newArrayList(ptwoName, poneName));
        Deencapsulation.invoke(task, "recordRefreshCompleted", Lists.newArrayList(poneName));

        String json = GsonUtils.GSON.toJson(task);
        Assertions.assertTrue(
                json.contains("\"needRefreshPartitions\":[\"" + poneName + "\",\"" + ptwoName + "\"]"), json);
        Assertions.assertTrue(json.contains("\"completedPartitions\":[\"" + poneName + "\"]"), json);

        // And the reader gets a set back, whichever version wrote the record.
        MTMVTask readBack = GsonUtils.GSON.fromJson(json, MTMVTask.class);
        Assertions.assertEquals(Sets.newHashSet(poneName, ptwoName),
                Deencapsulation.getField(readBack, "needRefreshPartitions"));
        Assertions.assertEquals(Sets.newHashSet(poneName),
                Deencapsulation.getField(readBack, "completedPartitions"));

        // Which is the same record an older task carries, read the same way.
        MTMVTask older = GsonUtils.GSON.fromJson("{\"di\":1,\"mi\":2,\"needRefreshPartitions\":[\"p1\",\"p2\"],"
                + "\"completedPartitions\":[\"p1\"]}", MTMVTask.class);
        Assertions.assertEquals(Sets.newHashSet(poneName, ptwoName),
                Deencapsulation.getField(older, "needRefreshPartitions"));
        Assertions.assertEquals(Sets.newHashSet(poneName),
                Deencapsulation.getField(older, "completedPartitions"));
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
        Column computeGroupColumn = MTMVTask.SCHEMA.get(MTMVTask.COLUMN_TO_INDEX.get(COMPUTE_GROUP.toLowerCase()));
        Column fallbackReasonColumn = MTMVTask.SCHEMA.get(MTMVTask.COLUMN_TO_INDEX.get("ivmfallbackreason"));
        Column rebuiltPartitionsColumn = MTMVTask.SCHEMA.get(MTMVTask.COLUMN_TO_INDEX.get("ivmrebuiltpartitions"));
        Assertions.assertEquals(COMPUTE_GROUP, computeGroupColumn.getName());
        Assertions.assertEquals("IvmFallbackReason", fallbackReasonColumn.getName());
        Assertions.assertEquals("IvmRebuiltPartitions", rebuiltPartitionsColumn.getName());
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
    public void testExecuteIvmAttemptKeepsRefreshScopeForNonSignatureFallbackInAutoMode() throws Exception {
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));
        MTMVRefreshContext refreshContext = mockIvmIncrRefreshContext();
        // What an earlier phase had already put in the report: the fallback must not take it back out.
        Deencapsulation.setField(task, "needRefreshPartitions", new ConcurrentSkipListSet<>(Sets.newHashSet(poneName)));
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

        Assertions.assertEquals(Sets.newHashSet(poneName),
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
            Deencapsulation.setField(task, "needRefreshPartitions",
                    new ConcurrentSkipListSet<>(Sets.newHashSet(poneName)));
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
    public void testUnionPreloadFailurePreservesCompletedGroupProgress() throws Exception {
        Mockito.when(mtmv.getName()).thenReturn("test_mv");
        Mockito.when(mtmv.getRefreshPartitionNum()).thenReturn(1);
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(Collections.emptySet());
        MTMVTask task = new MTMVTask(mtmv, relation, new MTMVTaskContext(MTMVTaskTriggerMode.MANUAL));

        MTMVRefreshContext refreshContext = Mockito.mock(MTMVRefreshContext.class);
        Mockito.when(refreshContext.preparePartitionSnapshots(Sets.newHashSet(poneName, ptwoName)))
                .thenThrow(new AnalysisException("union preload failed"));
        MTMVRefreshPartitionSnapshot firstSnapshot = Mockito.mock(MTMVRefreshPartitionSnapshot.class);
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.generatePartitionSnapshots(
                Mockito.same(refreshContext), Mockito.anySet(), Mockito.eq(Sets.newHashSet(poneName))))
                .thenReturn(ImmutableMap.of(poneName, firstSnapshot));
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.generatePartitionSnapshots(
                Mockito.same(refreshContext), Mockito.anySet(), Mockito.eq(Sets.newHashSet(ptwoName))))
                .thenThrow(new AnalysisException("second group failed"));

        ConnectContext mtmvCtx = new ConnectContext();
        mtmvCtx.setQueryId(new TUniqueId(1L, 2L));
        UpdateMvByPartitionCommand command = Mockito.mock(UpdateMvByPartitionCommand.class);
        try (MockedStatic<MTMVPlanUtil> mtmvPlanUtilStatic = Mockito.mockStatic(MTMVPlanUtil.class);
                MockedStatic<UpdateMvByPartitionCommand> updateMvStatic
                        = Mockito.mockStatic(UpdateMvByPartitionCommand.class)) {
            mtmvPlanUtilStatic.when(() -> MTMVPlanUtil.createMTMVContext(
                    Mockito.eq(mtmv), Mockito.anyList())).thenReturn(mtmvCtx);
            updateMvStatic.when(() -> UpdateMvByPartitionCommand.from(
                    Mockito.eq(mtmv), Mockito.anySet(), Mockito.anyMap(), Mockito.any(StatementContext.class)))
                    .thenReturn(command);

            AnalysisException failure = Assertions.assertThrows(AnalysisException.class,
                    () -> Deencapsulation.invoke(task, "executePartitionBasedRefresh",
                            refreshContext, RefreshMode.COMPLETE, mtmvCtx,
                            Lists.newArrayList(poneName, ptwoName)));
            Assertions.assertTrue(failure.getMessage().contains("second group failed"));
        }

        Assertions.assertEquals(Sets.newHashSet(poneName),
                Deencapsulation.getField(task, "completedPartitions"));
        Map<String, MTMVRefreshPartitionSnapshot> snapshots = Deencapsulation.getField(task, "partitionSnapshots");
        Assertions.assertSame(firstSnapshot, snapshots.get(poneName));
        Assertions.assertFalse(snapshots.containsKey(ptwoName));
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
                    refreshContext, RefreshMode.COMPLETE, mtmvCtx, Lists.newArrayList(poneName, ptwoName));
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
