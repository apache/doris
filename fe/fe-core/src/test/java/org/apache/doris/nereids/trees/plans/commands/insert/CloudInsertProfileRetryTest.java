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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.profile.ProfileManager;
import org.apache.doris.common.profile.ProfileManager.ProfileType;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TDetailedReportParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryProfile;
import org.apache.doris.thrift.TRuntimeProfileNode;
import org.apache.doris.thrift.TRuntimeProfileTree;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CloudInsertProfileRetryTest extends TestWithFeService {
    @ParameterizedTest
    @ValueSource(strings = {"SUCCESS", "SUCCESS_WITH_SESSION_REVERT", "SUCCESS_EMPTY_WITH_SESSION_REVERT",
            "EMPTY_WITH_SESSION_REVERT", "PLANNING_FAILURE", "EXHAUSTED",
            "CANCELLED", "DIRECT", "RPC", "NON_REPLAN", "MERGE_SUCCESS",
            "MERGE_PLANNING_FAILURE", "MERGE_SUCCESS_WITH_SESSION_REVERT",
            "MERGE_SUCCESS_EMPTY_WITH_SESSION_REVERT"})
    @ResourceLock("global")
    public void testInsertProfileRemainsOpenUntilOuterRetryDecision(String caseName, @TempDir Path storage) throws Exception {
        boolean merge = caseName.startsWith("MERGE_");
        String outcome = merge ? caseName.substring("MERGE_".length()) : caseName;
        boolean realExecution = merge && "SUCCESS_EMPTY_WITH_SESSION_REVERT".equals(outcome);
        String oldCloudUniqueId = Config.cloud_unique_id;
        int oldRetryTime = Config.max_query_retry_time;
        SessionVariable oldSession = connectContext.getSessionVariable();
        SessionVariable session = new SessionVariable();
        session.enableProfile = true;
        session.autoProfileThresholdMs = 0;
        session.parallelPipelineTaskNum = 1;
        session.cloudCluster = "profile-insert-retry-test";
        ConnectContext context = Mockito.spy(connectContext);
        context.setSessionVariable(session);
        Mockito.doReturn(connectContext.getComputeGroup()).when(context).getComputeGroup();
        TUniqueId firstId = new TUniqueId(0x22040L, 31L);
        context.setQueryId(firstId);
        context.setStartTime();
        String sql = merge
                ? "merge into profile_retry_db.profile_retry_target t using (select 1 as id) s on t.id = s.id "
                    + "when not matched then insert (id) values (s.id)"
                : "insert into profile_retry_db.profile_retry_target select 1";
        StmtExecutor executor = Mockito.spy(new StmtExecutor(context, sql));
        Method parse = StmtExecutor.class.getDeclaredMethod("parseByNereids");
        parse.setAccessible(true);
        parse.invoke(executor);
        Assertions.assertTrue(executor.isProfileSafeStmt());
        if (merge) {
            Assertions.assertInstanceOf(MergeIntoCommand.class,
                    ((LogicalPlanAdapter) executor.getParsedStmt()).getLogicalPlan());
        } else {
            // Match the profile type set by InsertIntoTableCommand.
            executor.setProfileType(ProfileType.LOAD);
        }
        LogicalPlanAdapter adapter = Mockito.spy((LogicalPlanAdapter) executor.getParsedStmt());
        Command command = Mockito.spy((Command) adapter.getLogicalPlan());
        if (realExecution) {
            Mockito.doReturn(command).when(adapter).getLogicalPlan();
            executor.setParsedStmt(adapter);
        }
        Profile profile = executor.getProfile();
        ProfileManager manager = ProfileManager.getInstance();
        NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        Mockito.when(planner.getPhysicalRelations()).thenReturn(Collections.emptyList());
        OlapTable table = Mockito.mock(OlapTable.class);
        Database database = new Database(12345L, "profile_retry_db");
        Mockito.when(table.getDatabase()).thenReturn(database);
        EnvFactory factory = Mockito.mock(EnvFactory.class);
        List<ExecutionProfile> executions = new ArrayList<>();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger callbacks = new AtomicInteger();
        AtomicInteger spillChecks = new AtomicInteger();
        ExecutorService backgroundStorage = Executors.newSingleThreadExecutor();
        Mockito.when(factory.createCoordinator(Mockito.eq(context), Mockito.eq(planner), Mockito.any(),
                Mockito.anyLong())).thenAnswer(invocation -> {
                    TUniqueId queryId = context.queryId();
                    ExecutionProfile execution = new ExecutionProfile(queryId, Collections.singletonList(0));
                    boolean empty = "EMPTY_WITH_SESSION_REVERT".equals(outcome)
                            || ("SUCCESS_EMPTY_WITH_SESSION_REVERT".equals(outcome) && attempts.get() > 1);
                    if (!empty) {
                        executions.add(execution);
                    }
                    Coordinator coordinator = Mockito.mock(Coordinator.class);
                    TQueryOptions options = new TQueryOptions();
                    options.enable_profile = true;
                    Mockito.when(coordinator.getQueryOptions()).thenReturn(options);
                    Mockito.when(coordinator.getExecutionProfile()).thenReturn(execution);
                    Mockito.when(coordinator.getExecStatus()).thenReturn(Status.OK);
                    Mockito.when(coordinator.getBeToInstancesNum())
                            .thenReturn(empty ? Collections.emptyMap() : Collections.singletonMap(
                                    attempts.get() == 1 ? "abandoned-be:9060" : "final-be:9060",
                                    attempts.get() == 1 ? 7 : 2));
                    Mockito.when(coordinator.isDone()).thenReturn(true);
                    Mockito.when(coordinator.isQueryCancelled()).thenReturn("CANCELLED".equals(outcome));
                    Mockito.doAnswer(dispatch -> {
                        QeProcessorImpl.INSTANCE.registerQueryFinishCallback(DebugUtil.printId(queryId),
                                callbacks::incrementAndGet);
                        execution.addFragmentBackend(0, 1L);
                        Assertions.assertTrue(execution.updateProfile(completeBackendReport(queryId),
                                new TNetworkAddress("127.0.0.1", 9060), true).ok());
                        Assertions.assertTrue(execution.isCompleted());
                        if (attempts.get() == 1) {
                            if ("RPC".equals(outcome)) {
                                throw new RpcException("test-be", SystemInfoService.ERROR_E230);
                            }
                            throw new UserException("NON_REPLAN".equals(outcome)
                                    ? "terminal insert failure" : SystemInfoService.ERROR_E230);
                        }
                        return null;
                    }).when(coordinator).exec();
                    return coordinator;
                });
        Answer<Void> executeAttempt = invocation -> {
            TUniqueId queryId = realExecution ? context.queryId() : invocation.getArgument(0);
            if (!realExecution) {
                context.setQueryId(queryId);
                context.setStartTime();
            }
            if (outcome.endsWith("WITH_SESSION_REVERT")) {
                if (realExecution) {
                    Assertions.assertTrue(session.setVarOnce("parallel_pipeline_task_num", "3"));
                } else {
                    session.parallelPipelineTaskNum = 3;
                }
            }
            profile.getSummaryProfile().setQueryBeginTime(context.getStartTime());
            if (attempts.incrementAndGet() > 1) {
                Assertions.assertEquals(Long.MAX_VALUE, profile.getQueryFinishTimestamp());
                Assertions.assertNull(manager.getExecutionProfile(firstId));
                Assertions.assertNull(manager.findProfileElementObject(DebugUtil.printId(firstId)));
                Assertions.assertTrue(profile.getExecutionProfiles().isEmpty());
                Assertions.assertNull(profile.getPhysicalPlan());
                Assertions.assertNull(executor.getCoord());
                if ("PLANNING_FAILURE".equals(outcome)) {
                    context.getState().setError("terminal planning failure");
                    throw new UserException("terminal planning failure");
                }
            }
            PhysicalEmptyRelation plan = new PhysicalEmptyRelation(
                    new RelationId(attempts.get()), Collections.emptyList(), Optional.empty(), null,
                    PhysicalProperties.ANY, null);
            Mockito.when(planner.getPhysicalPlan()).thenReturn(plan);
            executor.setPlanner(planner);
            if (realExecution && attempts.get() > 1) {
                // RowLevelDmlCommand returns before executeSingleInsert for empty input.
                context.getState().setOk();
                return null;
            }
            AbstractInsertExecutor insert = new AbstractInsertExecutor(context, table, "retry_test", planner,
                    Optional.empty(), "EMPTY_WITH_SESSION_REVERT".equals(outcome)
                            || ("SUCCESS_EMPTY_WITH_SESSION_REVERT".equals(outcome) && attempts.get() > 1),
                    attempts.get()) {
                @Override
                public void beginTransaction() {
                }

                @Override
                protected void finalizeSink(PlanFragment fragment, DataSink sink, PhysicalSink physicalSink) {
                }

                @Override
                protected void beforeExec() {
                }

                @Override
                protected void onComplete() {
                    context.getState().setOk();
                }

                @Override
                protected void onFail(Throwable error) {
                    context.getState().setError(error.getMessage());
                }

                @Override
                protected void afterExec(StmtExecutor statementExecutor) {
                }
            };
            try {
                insert.executeSingleInsert(executor);
            } catch (UserException error) {
                if (!"DIRECT".equals(outcome)) {
                    // The attempt ended; the retry decision is still pending.
                    backgroundStorage.submit(() -> {
                        spillChecks.incrementAndGet();
                        if (profile.shouldStoreToStorage()) {
                            profile.writeToStorage(storage.toString());
                            profile.releaseMemory();
                        }
                    }).get(10, TimeUnit.SECONDS);
                    Assertions.assertFalse(profile.profileHasBeenStored(),
                            "a retryable INSERT must not become eligible for background spill");
                    Assertions.assertEquals(Long.MAX_VALUE, profile.getQueryFinishTimestamp());
                    Assertions.assertNotNull(profile.rowsProducedMap);
                }
                if ("CANCELLED".equals(outcome)) {
                    // Cancel before the outer retry decision.
                    Mockito.when(insert.getCoordinator().getExecStatus())
                            .thenReturn(new Status(TStatusCode.CANCELLED, "cancelled before outer decision"));
                }
                throw error;
            }
            if (outcome.endsWith("WITH_SESSION_REVERT") && !realExecution) {
                // Restore SET_VAR before the outer finalizer.
                session.parallelPipelineTaskNum = 1;
            }
            return null;
        };
        if (realExecution) {
            Mockito.doAnswer(executeAttempt).when(command).run(Mockito.eq(context), Mockito.eq(executor));
        } else {
            Mockito.doAnswer(executeAttempt).when(executor).execute(Mockito.any(TUniqueId.class));
        }
        try (MockedStatic<EnvFactory> factories = Mockito.mockStatic(EnvFactory.class)) {
            factories.when(EnvFactory::getInstance).thenReturn(factory);
            Config.cloud_unique_id = "profile-insert-retry-test";
            Config.max_query_retry_time = "EXHAUSTED".equals(outcome) ? 0 : 1;
            context.setThreadLocalInfo();
            if (outcome.startsWith("SUCCESS") || "EMPTY_WITH_SESSION_REVERT".equals(outcome)
                    || "NON_REPLAN".equals(outcome)) {
                executor.queryRetry(firstId);
            } else if ("DIRECT".equals(outcome)) {
                Assertions.assertThrows(UserException.class, () -> executor.execute(firstId));
            } else {
                Class<? extends Exception> errorType = "RPC".equals(outcome) ? RpcException.class : UserException.class;
                Assertions.assertThrows(errorType,
                        () -> executor.queryRetry(firstId));
            }
            int expectedAttempts = outcome.startsWith("SUCCESS") || "PLANNING_FAILURE".equals(outcome) ? 2 : 1;
            Assertions.assertEquals(expectedAttempts, attempts.get());
            if (realExecution) {
                Assertions.assertEquals(1, session.parallelPipelineTaskNum);
            }
            Assertions.assertNotEquals(Long.MAX_VALUE, profile.getQueryFinishTimestamp());
            Assertions.assertEquals(DebugUtil.printId(context.queryId()), profile.getId());
            Assertions.assertEquals(merge ? "QUERY" : "LOAD", profile.getSummaryProfile().getSummary()
                    .getInfoString(SummaryProfile.TASK_TYPE));
            Assertions.assertNotNull(manager.findProfileElementObject(profile.getId()));
            Assertions.assertEquals(executions.size(), callbacks.get());
            if (outcome.contains("EMPTY")) {
                Assertions.assertTrue(profile.getExecutionProfiles().isEmpty());
                if (realExecution) {
                    Assertions.assertNull(executor.getCoord());
                    Assertions.assertEquals(1, executions.size());
                    Mockito.verify(command, Mockito.times(2)).run(context, executor);
                } else {
                    Mockito.verify(executor.getCoord(), Mockito.never()).exec();
                }
            } else if (outcome.startsWith("SUCCESS")) {
                Assertions.assertEquals(Collections.singletonList(executions.get(1)), profile.getExecutionProfiles());
            } else if ("PLANNING_FAILURE".equals(outcome)) {
                Assertions.assertTrue(profile.getExecutionProfiles().isEmpty());
                Assertions.assertNull(profile.getPhysicalPlan());
                Assertions.assertEquals("ERR", profile.getSummaryProfile().getSummary()
                        .getInfoString(SummaryProfile.TASK_STATE));
                Assertions.assertNull(executor.getCoord());
                Assertions.assertEquals("0", profile.getSummaryProfile().getExecutionSummary()
                        .getInfoString(SummaryProfile.TOTAL_INSTANCES_NUM));
                Assertions.assertEquals("", profile.getSummaryProfile().getExecutionSummary()
                        .getInfoString(SummaryProfile.INSTANCES_NUM_PER_BE));
                Assertions.assertFalse(profile.getProfileByLevel().contains("abandoned-be:9060"));
            }
            Assertions.assertEquals("DIRECT".equals(outcome) || "RPC".equals(outcome)
                    || "NON_REPLAN".equals(outcome) || "EMPTY_WITH_SESSION_REVERT".equals(outcome)
                    ? 0 : 1, spillChecks.get());
            for (ExecutionProfile execution : executions) {
                Assertions.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(execution.getQueryId()));
            }
            if ("CANCELLED".equals(outcome)) {
                Assertions.assertEquals("CANCELLED", profile.getSummaryProfile().getSummary()
                        .getInfoString(SummaryProfile.TASK_STATE));
            }
            if (outcome.endsWith("WITH_SESSION_REVERT")) {
                Assertions.assertEquals("3", profile.getSummaryProfile().getExecutionSummary()
                        .getInfoString(SummaryProfile.PARALLEL_FRAGMENT_EXEC_INSTANCE));
            }
            if (outcome.startsWith("SUCCESS") || outcome.contains("EMPTY")) {
                Assertions.assertEquals("OK", profile.getSummaryProfile().getSummary()
                        .getInfoString(SummaryProfile.TASK_STATE));
                Assertions.assertNotNull(profile.getSummaryProfile().getSummary()
                        .getInfoString(SummaryProfile.END_TIME));
                Assertions.assertNotNull(profile.getSummaryProfile().getSummary()
                        .getInfoString(SummaryProfile.TOTAL_TIME));
            }
            if (outcome.startsWith("SUCCESS")) {
                Assertions.assertFalse(profile.getProfileByLevel().contains("abandoned-be:9060"));
            }
            // Store only after the statement finishes.
            Assertions.assertTrue(profile.shouldStoreToStorage());
            profile.writeToStorage(storage.toString());
            Assertions.assertTrue(profile.profileHasBeenStored());
            Profile stored = Profile.read(profile.getProfileStoragePath());
            Assertions.assertNotNull(stored);
            if ("PLANNING_FAILURE".equals(outcome)) {
                Assertions.assertEquals("ERR", stored.getSummaryProfile().getSummary()
                        .getInfoString(SummaryProfile.TASK_STATE));
            }
            if ("PLANNING_FAILURE".equals(outcome) || outcome.startsWith("SUCCESS")) {
                Assertions.assertFalse(stored.getProfileByLevel().contains("abandoned-be:9060"));
            }
            if (outcome.endsWith("WITH_SESSION_REVERT")) {
                Assertions.assertEquals("3", stored.getSummaryProfile().getExecutionSummary()
                        .getInfoString(SummaryProfile.PARALLEL_FRAGMENT_EXEC_INSTANCE));
            }
            profile.releaseMemory();
        } finally {
            backgroundStorage.shutdownNow();
            for (ExecutionProfile execution : executions) {
                QeProcessorImpl.INSTANCE.unregisterQuery(execution.getQueryId());
            }
            Profile cleanup = new Profile(true, 1, -1);
            executions.forEach(cleanup::addExecutionProfile);
            manager.removeProfile(cleanup);
            manager.removeProfile(profile);
            Config.cloud_unique_id = oldCloudUniqueId;
            Config.max_query_retry_time = oldRetryTime;
            connectContext.setSessionVariable(oldSession);
            connectContext.setThreadLocalInfo();
        }
    }

    private static TQueryProfile completeBackendReport(TUniqueId queryId) {
        TRuntimeProfileNode pipeline = new TRuntimeProfileNode();
        pipeline.setName("Pipeline 0");
        pipeline.setNumChildren(0);
        pipeline.setCounters(Collections.emptyList());
        pipeline.setMetadata(0);
        pipeline.setIndent(true);
        pipeline.setInfoStrings(Collections.emptyMap());
        pipeline.setInfoStringsDisplayOrder(Collections.emptyList());
        pipeline.setChildCountersMap(Collections.emptyMap());
        pipeline.setTimestamp(0);
        TRuntimeProfileTree tree = new TRuntimeProfileTree();
        tree.setNodes(Collections.singletonList(pipeline));
        TDetailedReportParams report = new TDetailedReportParams();
        report.setProfile(tree);
        report.setIsFragmentLevel(false);
        TQueryProfile queryProfile = new TQueryProfile();
        queryProfile.setQueryId(queryId);
        queryProfile.putToFragmentIdToProfile(0, Collections.singletonList(report));
        return queryProfile;
    }
}
