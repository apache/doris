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

package org.apache.doris.load;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.fileformat.ParquetFileFormatProperties;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ExportJobTest {

    @Test
    public void exportBindsThePlanToItsWorkerSession() throws Exception {
        ExportJob job = Mockito.spy(new ExportJob(1));
        job.setTableName(new TableNameInfo("external", "db", "t"));
        job.setExportTable(Mockito.mock(TableIf.class));
        job.setFormat("csv");
        job.setMaxFileSize("");
        job.setBrokerDesc(new BrokerDesc("local"));
        job.setWhereExpression(Optional.empty());
        Mockito.doNothing().when(job).updateExportJobState(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any());
        Env env = workerEnvironment();
        Mockito.when(env.getCatalogMgr().getCatalog("external").getType()).thenReturn("external");
        ConnectContext caller = new ConnectContext();
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(caller);
                MockedStatic<Env> environment = Mockito.mockStatic(Env.class)) {
            environment.when(Env::getCurrentEnv).thenReturn(env);
            int originalTimeout = caller.getSessionVariable().getQueryTimeoutS();
            job.setSessionVariables(caller.getSessionVariable());
            job.setTimeoutSecond(originalTimeout + 1);
            caller.setStatementContext(new StatementContext(caller, null));
            StatementScopeIdGenerator.newRelationId();
            try (MockedConstruction<StatementContext> statements = Mockito.mockConstruction(StatementContext.class)) {
                job.generateOutfileLogicalPlans(List.of("external", "db", "t"));
                Assertions.assertTrue(statements.constructed().isEmpty());
            }
            AtomicReference<ConnectContext> worker = new AtomicReference<>();
            try (MockedConstruction<StmtExecutor> executions = Mockito.mockConstruction(StmtExecutor.class,
                    (executor, construction) -> {
                        ConnectContext ctx = (ConnectContext) construction.arguments().get(0);
                        LogicalPlanAdapter plan = (LogicalPlanAdapter) construction.arguments().get(1);
                        Assertions.assertSame(ctx, plan.getStatementContext().getConnectContext());
                        Assertions.assertSame(ctx, ConnectContext.get());
                        Assertions.assertEquals(originalTimeout + 1, ctx.getSessionVariable().getQueryTimeoutS());
                        worker.set(ctx);
                    })) {
                TransientTaskExecutor task = job.getCopiedTaskExecutors().get(0);
                task.execute();
                Assertions.assertNull(Deencapsulation.getField(task, "stmtExecutor"));
                Assertions.assertEquals(1, executions.constructed().size());
            }
            Assertions.assertEquals(originalTimeout, job.getSessionVariables().getQueryTimeoutS());
            Assertions.assertNotSame(caller, worker.get());
            Assertions.assertTrue(worker.get().isClosingOrClosed());
            Assertions.assertSame(caller, ConnectContext.get());
            Assertions.assertFalse(caller.isClosingOrClosed());
        }
    }

    @Test
    void cancellationBeforeAndAfterExecutorPublication() throws Exception {
        ExportJob job = Mockito.mock(ExportJob.class);
        Mockito.when(job.getExportTable()).thenReturn(Mockito.mock(TableIf.class));
        Mockito.when(job.getTableName()).thenReturn(new TableNameInfo("internal", "db", "t"));
        Mockito.when(job.getSessionVariables()).thenReturn(VariableMgr.newSessionVariable());
        Optional<LogicalPlan> plan = Optional.of(Mockito.mock(LogicalPlan.class));
        Env env = workerEnvironment();
        try (MockedStatic<Env> environment = Mockito.mockStatic(Env.class)) {
            environment.when(Env::getCurrentEnv).thenReturn(env);
            ExportTaskExecutor before = new ExportTaskExecutor(plan, job);
            try (MockedConstruction<StmtExecutor> executions = Mockito.mockConstruction(StmtExecutor.class,
                    (executor, construction) -> before.cancel())) {
                Assertions.assertThrows(JobException.class, before::execute);
                Mockito.verify(executions.constructed().get(0), Mockito.never()).execute();
                Assertions.assertNull(Deencapsulation.getField(before, "stmtExecutor"));
            }

            ExportTaskExecutor running = new ExportTaskExecutor(plan, job);
            try (MockedConstruction<StmtExecutor> executions = Mockito.mockConstruction(StmtExecutor.class,
                    (executor, construction) -> Mockito.doAnswer(invocation -> {
                        FutureTask<Void> cancellation = new FutureTask<>(() -> {
                            running.cancel();
                            return null;
                        });
                        new Thread(cancellation).start();
                        cancellation.get(5, TimeUnit.SECONDS);
                        throw new IllegalStateException("export interrupted");
                    }).when(executor).execute())) {
                Assertions.assertThrows(JobException.class, running::execute);
                running.cancel();
                Mockito.verify(executions.constructed().get(0)).cancel(Mockito.any(Status.class));
                Assertions.assertNull(Deencapsulation.getField(running, "stmtExecutor"));
            }
        }
    }

    private Env workerEnvironment() {
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        InternalCatalog internal = Mockito.mock(InternalCatalog.class);
        Mockito.when(internal.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getInternalCatalog()).thenReturn(internal);
        Mockito.when(env.getCatalogMgr().getCatalog(InternalCatalog.INTERNAL_CATALOG_NAME)).thenReturn(internal);
        return env;
    }

    @Test
    public void testEnableInt96TimestampsOutfileProperty() {
        ExportJob exportJob = new ExportJob(1);
        exportJob.setFormat("parquet");
        exportJob.setMaxFileSize("");
        exportJob.setBrokerDesc(new BrokerDesc("local"));

        Map<String, String> outfileProperties =
                Deencapsulation.invoke(exportJob, "convertOutfileProperties");
        Assertions.assertFalse(outfileProperties.containsKey(
                ParquetFileFormatProperties.ENABLE_INT96_TIMESTAMPS));

        exportJob.setEnableInt96Timestamps("false");
        outfileProperties = Deencapsulation.invoke(exportJob, "convertOutfileProperties");
        Assertions.assertEquals("false", outfileProperties.get(
                ParquetFileFormatProperties.ENABLE_INT96_TIMESTAMPS));
    }
}
