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

package org.apache.doris.job.extensions.insert;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.executor.TaskProcessor;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class InsertTaskTest {
    @Test
    public void testMakeConnection() {
        ConnectContext previousContext = new ConnectContext();
        try (AutoCloseConnectContext parent = new AutoCloseConnectContext(previousContext)) {
            ConnectContext ctx = InsertTask.makeConnectContext(null, null);
            Assertions.assertSame(previousContext, ConnectContext.get());
            try (AutoCloseConnectContext scope = new AutoCloseConnectContext(ctx)) {
                Assertions.assertTrue(ctx.getState().isNereids());
                Assertions.assertSame(ctx, ConnectContext.get());
            }
            Assertions.assertTrue(ctx.isClosingOrClosed());
            Assertions.assertSame(previousContext, ConnectContext.get());
            Assertions.assertFalse(previousContext.isClosingOrClosed());
        }
    }

    @Test
    void planTasksUseTheSavedNamespaceAndAcceptOlderJobMetadata() throws IOException {
        ConnectContext submitter = Mockito.mock(ConnectContext.class);
        Mockito.when(submitter.getDefaultCatalog()).thenReturn("external");
        Mockito.when(submitter.getDatabase()).thenReturn("submitted_db");
        Mockito.when(submitter.getCurrentDbId()).thenReturn(21L);
        InsertJob job = new InsertJob(submitter, "load", List.of(), Set.of(), Map.of(), "", null);
        JsonObject metadata = GsonUtils.GSON.toJsonTree(job).getAsJsonObject();
        Assertions.assertEquals("external", metadata.get("catalog").getAsString());
        Assertions.assertEquals("submitted_db", metadata.get("cdb").getAsString());

        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        InternalCatalog internal = Mockito.mock(InternalCatalog.class);
        Database oldDb = Mockito.mock(Database.class);
        Mockito.when(internal.getDb(21L)).thenReturn(Optional.of(oldDb));
        Mockito.when(oldDb.getFullName()).thenReturn("historical_db");
        try (MockedStatic<Env> environment = Mockito.mockStatic(Env.class)) {
            environment.when(Env::getCurrentEnv).thenReturn(env);
            environment.when(Env::getCurrentInternalCatalog).thenReturn(internal);
            NameSpaceContext current = taskNamespaceFromMetadata(metadata);
            Assertions.assertEquals("external", current.getDefaultCatalog());
            Assertions.assertEquals("submitted_db", current.getCurrentDb());
            Assertions.assertEquals(21L, current.getCurrentDbId());
            Mockito.verifyNoInteractions(internal);

            metadata.remove("catalog");
            metadata.remove("cdb");
            NameSpaceContext historical = taskNamespaceFromMetadata(metadata);
            Assertions.assertEquals(InternalCatalog.INTERNAL_CATALOG_NAME, historical.getDefaultCatalog());
            Assertions.assertEquals("historical_db", historical.getCurrentDb());
            Assertions.assertEquals(21L, historical.getCurrentDbId());
        }
    }

    private NameSpaceContext taskNamespaceFromMetadata(JsonObject metadata) throws IOException {
        ByteArrayOutputStream saved = new ByteArrayOutputStream();
        Text.writeString(new DataOutputStream(saved), metadata.toString());
        InsertJob restored = (InsertJob) AbstractJob.readFields(new DataInputStream(
                new ByteArrayInputStream(saved.toByteArray())));
        // This test exercises persisted namespace selection with an available executable plan.
        InsertIntoTableCommand plan = Mockito.mock(InsertIntoTableCommand.class);
        Mockito.when(plan.getLabelName()).thenReturn(Optional.of("load"));
        restored.setPlans(List.of(plan));
        InsertTask task = restored.createTasks(TaskType.SCHEDULED, Map.of()).get(0);
        return Deencapsulation.getField(task, "planNamespace");
    }

    @Test
    public void testFailedDatabaseSetupClosesOnlyNewContext() {
        ConnectContext parent = new ConnectContext();
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(parent);
                MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class,
                        (context, construction) -> {
                            Mockito.when(context.getState()).thenReturn(new QueryState());
                            Mockito.doThrow(new IllegalStateException("database setup failed"))
                                    .when(context).setDatabase("db");
                        })) {
            Assertions.assertThrows(IllegalStateException.class,
                    () -> InsertTask.makeConnectContext(null, "db"));
            Assertions.assertEquals(1, contexts.constructed().size());
            Mockito.verify(contexts.constructed().get(0)).cleanup();
            Assertions.assertSame(parent, ConnectContext.get());
            Assertions.assertFalse(parent.isClosingOrClosed());
        }
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void executionOwnsItsContextThroughTheLastCallback(boolean scheduled, boolean failInPreparation) throws Exception {
        AtomicReference<ConnectContext> child = new AtomicReference<>();
        List<String> events = new ArrayList<>();
        InsertIntoTableCommand plan = Mockito.mock(InsertIntoTableCommand.class);
        Mockito.when(plan.getLabelName()).thenReturn(Optional.of("load"));
        NameSpaceContext namespace = Mockito.mock(NameSpaceContext.class);
        Mockito.when(namespace.getDefaultCatalog()).thenAnswer(call -> {
            ConnectContext ctx = ConnectContext.get();
            child.set(ctx);
            ctx.getSessionVariable().setEnableSqlCache(false);
            QeProcessorImpl.INSTANCE.registerQueryFinishCallback(DebugUtil.printId(ctx.queryId()), () -> {
                Assertions.assertSame(ctx, ConnectContext.get());
                Assertions.assertFalse(ctx.isClosingOrClosed());
                events.add("query finished");
            });
            if (failInPreparation) {
                throw new IllegalStateException("injected namespace failure");
            }
            return InternalCatalog.INTERNAL_CATALOG_NAME;
        });
        Mockito.doAnswer(call -> {
            ConnectContext ctx = call.getArgument(0);
            ctx.getState().setOk();
            return null;
        }).when(plan).runWithUpdateInfo(Mockito.any(), Mockito.any(), Mockito.any());
        InsertTask task = new InsertTask(plan, UserIdentity.ROOT, namespace, new LoadStatistic()) {
            @Override
            public boolean onSuccess() {
                Assertions.assertSame(child.get(), ConnectContext.get());
                return events.add("success");
            }

            @Override
            public boolean onFail() {
                Assertions.assertSame(child.get(), ConnectContext.get());
                return events.add("failure");
            }
        };
        ConnectContext parent = new ConnectContext();
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(parent)) {
            if (scheduled) {
                task.runTask();
                Assertions.assertEquals(List.of(failInPreparation ? "failure" : "success", "query finished"), events);
            } else {
                if (failInPreparation) {
                    Assertions.assertThrows(JobException.class, task::run);
                } else {
                    task.run();
                }
                Assertions.assertEquals(List.of("query finished"), events);
            }
            Assertions.assertTrue(child.get().isClosingOrClosed());
            Assertions.assertSame(parent, ConnectContext.get());
            Assertions.assertFalse(parent.isClosingOrClosed());
        }
    }

    @Test
    public void canceledPlanTaskKeepsItsWorkerContextUntilTheCommandReturns() throws Exception {
        InsertIntoTableCommand plan = Mockito.mock(InsertIntoTableCommand.class);
        Mockito.when(plan.getLabelName()).thenReturn(Optional.of("load_plan"));
        LoadStatistic statistic = new LoadStatistic();
        InsertTask task = new InsertTask(plan, UserIdentity.ROOT, new NameSpaceContext("ctl", "load_db", 5L), statistic);
        task.setStatus(TaskStatus.PENDING);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicReference<ConnectContext> running = new AtomicReference<>();
        Mockito.doAnswer(call -> {
            ConnectContext ctx = call.getArgument(0);
            StmtExecutor executor = call.getArgument(1);
            Assertions.assertEquals("ctl", ctx.getDefaultCatalog());
            Assertions.assertEquals("load_db", ctx.getDatabase());
            Assertions.assertEquals(5L, ctx.getCurrentDbId());
            Assertions.assertSame(ctx, executor.getContext());
            Assertions.assertSame(ctx, ConnectContext.get());
            running.set(ctx);
            started.countDown();
            Assertions.assertTrue(finish.await(10, TimeUnit.SECONDS));
            ctx.getState().setOk();
            return null;
        }).when(plan).runWithUpdateInfo(Mockito.any(), Mockito.any(), Mockito.same(statistic));

        TaskProcessor worker = new TaskProcessor(1, 1, Thread::new);
        try {
            Assertions.assertTrue(worker.addTask(task));
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            task.cancel(false);
            Assertions.assertFalse(running.get().isClosingOrClosed());
            Mockito.verify(plan).cancel();
        } finally {
            finish.countDown();
            worker.shutdown();
        }
        Assertions.assertTrue(running.get().isClosingOrClosed());
        Assertions.assertEquals(TaskStatus.CANCELED, task.getStatus());
        Assertions.assertNull(task.getErrMsg());
    }

}
