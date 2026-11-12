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

package org.apache.doris.catalog;

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.master.MasterImpl;
import org.apache.doris.master.ReportHandler;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.trees.plans.commands.DropFunctionCommand;
import org.apache.doris.nereids.trees.plans.commands.info.FunctionArgTypesInfo;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskCleanupDaemon;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CleanUDFCacheTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TReportRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTaskType;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class UdfCacheCleanupRecoveryTest {
    private static final long DB_ID = 970001;
    private static final long FUNCTION_ID = 970002;
    private static final long BACKEND_ID = 970003;
    private static final String DB_NAME = "udf_cleanup_recovery";

    @TempDir
    Path journalDirectory;

    private Env env;
    private Backend backend;
    private EditLog editLog;
    private EditLog previousEditLog;
    private String previousImageDir;
    private String previousEditLogType;
    private boolean previousBatchEditLog;
    private byte[] previousRecycleBin;

    @BeforeEach
    void setUp() throws Exception {
        env = Env.getCurrentEnv();
        MetricRepo.init();
        previousEditLog = env.getEditLog();
        previousImageDir = env.imageDir;
        previousEditLogType = Config.edit_log_type;
        previousBatchEditLog = Config.enable_batch_editlog;
        previousRecycleBin = saveRecycleBin(Env.getCurrentRecycleBin());
        loadRecycleBin(saveRecycleBin(new CatalogRecycleBin()));
        AgentTaskQueue.clearAllTasks();
        env.imageDir = journalDirectory.toString();
        Config.edit_log_type = "local";
        Config.enable_batch_editlog = false;
        editLog = new EditLog("udf-cleanup-test");
        editLog.open();
        env.setEditLog(editLog);
        backend = new Backend(BACKEND_ID, "127.0.0.1", 19050);
        backend.setBePort(19060);
        backend.setAlive(false);
        Env.getCurrentSystemInfo().addBackend(backend);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (Env.getCurrentInternalCatalog().getDbNullable(DB_ID) != null) {
            Env.getCurrentInternalCatalog().replayDropDb(DB_NAME, true, 0L);
        }
        editLog.close();
        env.setEditLog(previousEditLog);
        env.imageDir = previousImageDir;
        Config.edit_log_type = previousEditLogType;
        Config.enable_batch_editlog = previousBatchEditLog;
        Env.getCurrentSystemInfo().replayDropBackend(backend);
        env.getFunctionRegistry().dropUdfByDb(DB_NAME);
        loadRecycleBin(previousRecycleBin);
        AgentTaskQueue.clearAllTasks();
    }

    @Test
    void systemDatabaseFunctionsAreRetainedBeforeAndAfterCatalogLoad() throws Exception {
        CatalogMgr original = env.getCatalogMgr();
        try {
            assertSystemDatabaseFunctionsAreRetained();
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                new CatalogMgr().write(out);
            }
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                env.loadCatalog(in, 0);
            }
            Assertions.assertNotSame(original, env.getCatalogMgr());
            assertSystemDatabaseFunctionsAreRetained();
        } finally {
            Field catalogMgr = Env.class.getDeclaredField("catalogMgr");
            catalogMgr.setAccessible(true);
            catalogMgr.set(env, original);
        }
    }

    private void assertSystemDatabaseFunctionsAreRetained() throws Exception {
        for (long databaseId : List.of(InfoSchemaDb.DATABASE_ID, MysqlDb.DATABASE_ID)) {
            Database db = env.getInternalCatalog().getDbNullable(databaseId);
            Function function = pythonFunction("system_database_evaluate", Type.INT);
            function.getFunctionName().setDb(db.getFullName());
            db.replayAddFunction(function);
            try {
                report(Set.of(FUNCTION_ID));
                Assertions.assertNull(queuedTask());
                Assertions.assertSame(function, env.getFunctionById(FUNCTION_ID));
            } finally {
                db.replayDropFunction(new FunctionSearchDesc(function.getFunctionName(), function.getArgs(),
                        function.hasVarArgs()));
            }
        }
    }

    @Test
    void taskReportCarriesFunctionIdsAndAcceptsOlderBackends() throws Exception {
        ReportHandler handler = new ReportHandler();
        TReportRequest request = new TReportRequest();
        request.setBackend(new TBackend(backend.getHost(), backend.getBePort(), 0));
        request.setTasks(Map.of());
        executeReport(handler, request);
        Assertions.assertNull(queuedTask());
        request.setPythonUdfIds(Set.of(FUNCTION_ID));
        executeReport(handler, request);
        Assertions.assertNotNull(queuedTask());
    }

    private void executeReport(ReportHandler handler, TReportRequest request) throws Exception {
        Assertions.assertEquals(TStatusCode.OK, handler.handleReport(request).getStatus().getStatusCode());
        Method take = ReportHandler.class.getDeclaredMethod("takeReportTask");
        take.setAccessible(true);
        Object task = take.invoke(handler);
        Method execute = task.getClass().getDeclaredMethod("exec");
        execute.setAccessible(true);
        execute.invoke(task);
    }

    @Test
    void taskReportPreservesPendingPythonFunctionIds() throws Exception {
        ReportHandler handler = new ReportHandler();
        TReportRequest request = new TReportRequest();
        request.setBackend(new TBackend(backend.getHost(), backend.getBePort(), 0));
        request.setTasks(Map.of());
        request.setPythonUdfIds(Set.of(FUNCTION_ID));
        Assertions.assertEquals(TStatusCode.OK, handler.handleReport(request).getStatus().getStatusCode());
        request.unsetPythonUdfIds();
        executeReport(handler, request);
        Assertions.assertNotNull(queuedTask());
    }

    @Test
    void emptyPythonReportReplacesPendingFunctionIds() throws Exception {
        ReportHandler handler = new ReportHandler();
        TReportRequest request = new TReportRequest();
        request.setBackend(new TBackend(backend.getHost(), backend.getBePort(), 0));
        request.setTasks(Map.of());
        request.setPythonUdfIds(Set.of(FUNCTION_ID));
        Assertions.assertEquals(TStatusCode.OK, handler.handleReport(request).getStatus().getStatusCode());
        request.setPythonUdfIds(Set.of());
        executeReport(handler, request);
        Assertions.assertNull(queuedTask());
    }

    @Test
    void emptyPythonReportDoesNotWaitForDatabaseFunctions() throws Exception {
        Database db = databaseWithPythonFunction();
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        synchronized (db) {
            CompletableFuture<Void> finished = CompletableFuture.runAsync(() -> {
                try {
                    report(Set.of());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            finished.get(5, TimeUnit.SECONDS);
        }
        Assertions.assertNull(queuedTask());
    }

    @Test
    void pythonReportDoesNotWaitForDatabaseFunctions() throws Exception {
        Database db = databaseWithPythonFunction();
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        synchronized (db) {
            CompletableFuture<Void> finished = CompletableFuture.runAsync(() -> {
                try {
                    report(Set.of(FUNCTION_ID, FUNCTION_ID + 1));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            finished.get(5, TimeUnit.SECONDS);
        }
        Assertions.assertNull(queuedTask());
        Assertions.assertNotNull(AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID + 1));
    }

    @Test
    void functionIndexTracksCreateAndDrop() throws Exception {
        Database db = new Database(DB_ID, DB_NAME);
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        ScalarFunction function = pythonFunction("created", Type.INT);
        db.addFunction(function, false);
        Assertions.assertSame(function, env.getFunctionById(function.getId()));
        db.dropFunction(new FunctionSearchDesc(function.getFunctionName(), function.getArgs(), false), false);
        Assertions.assertNull(env.getFunctionById(function.getId()));
    }

    @Test
    void failedFunctionRegistrationRemovesIndexEntry() throws Exception {
        Database db = new Database(DB_ID, DB_NAME);
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        ScalarFunction function = pythonFunction("invalid_return_type", ScalarType.createDatetimeV2Type(7));
        function.setId(0);
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> db.addFunction(function, false));
        Assertions.assertTrue(function.getId() > 0);
        Assertions.assertNull(env.getFunctionById(function.getId()));
        Assertions.assertTrue(db.getFunctions().isEmpty());
    }

    @Test
    void globalFunctionIndexTracksCreateFailureAndDrop() throws Exception {
        ScalarFunction function = pythonFunction("global_index", Type.INT);
        FunctionSearchDesc description = new FunctionSearchDesc(function.getFunctionName(), function.getArgs(), false);
        GlobalFunctionMgr manager = env.getGlobalFunctionMgr();
        try {
            manager.addFunction(function, false);
            Assertions.assertSame(function, env.getFunctionById(function.getId()));
            ScalarFunction invalid = pythonFunction("invalid_global_index", ScalarType.createDatetimeV2Type(7));
            invalid.setId(0);
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> manager.addFunction(invalid, false));
            Assertions.assertTrue(invalid.getId() > 0);
            Assertions.assertNull(env.getFunctionById(invalid.getId()));
            Assertions.assertSame(function, env.getFunctionById(function.getId()));
        } finally {
            manager.dropFunction(description, true);
        }
        Assertions.assertNull(env.getFunctionById(function.getId()));
    }

    @Test
    void databaseImageRebuildsFunctionIndex() throws Exception {
        Database db = databaseWithPythonFunction();
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(1);
            db.write(out);
        }
        Assertions.assertFalse(GsonUtils.GSON.toJsonTree(db).getAsJsonObject().has("id2Function"));
        Env.getCurrentInternalCatalog().replayDropDb(DB_NAME, true, 0L);
        Assertions.assertNull(env.getFunctionById(FUNCTION_ID));
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            env.loadDb(in, 0);
        }
        Database restored = Env.getCurrentInternalCatalog().getDbNullable(DB_ID);
        Assertions.assertSame(restored.getFunctions().get(0), env.getFunctionById(FUNCTION_ID));
        report(Set.of(FUNCTION_ID));
        Assertions.assertNull(queuedTask());
    }

    @Test
    void globalFunctionImageRebuildsFunctionIndex() throws Exception {
        ByteArrayOutputStream original = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(original)) {
            env.getGlobalFunctionMgr().write(out);
        }
        try {
            Function function = pythonFunction("global_image_index", Type.INT);
            env.getGlobalFunctionMgr().replayAddFunction(function);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                env.getGlobalFunctionMgr().write(out);
            }
            Assertions.assertFalse(GsonUtils.GSON.toJsonTree(env.getGlobalFunctionMgr())
                    .getAsJsonObject().has("id2Function"));
            FunctionSearchDesc description = new FunctionSearchDesc(function.getFunctionName(),
                    function.getArgs(), false);
            env.getGlobalFunctionMgr().replayDropFunction(description);
            Assertions.assertNull(env.getFunctionById(FUNCTION_ID));
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                env.loadGlobalFunction(in, 0);
            }
            Function restored = env.getGlobalFunctionMgr().getFunction(description);
            Assertions.assertNotSame(function, restored);
            Assertions.assertSame(restored, env.getFunctionById(FUNCTION_ID));
            report(Set.of(FUNCTION_ID));
            Assertions.assertNull(queuedTask());
            env.getGlobalFunctionMgr().replayDropFunction(description);
        } finally {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(original.toByteArray()))) {
                env.loadGlobalFunction(in, 0);
            }
        }
    }

    @Test
    void recycleBinImageRebuildsFunctionIndex() throws Exception {
        Database db = databaseWithPythonFunction();
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        Env.getCurrentInternalCatalog().dropDb(DB_NAME, false, false);
        byte[] image = saveRecycleBin(Env.getCurrentRecycleBin());
        loadRecycleBin(image);
        Assertions.assertNotNull(env.getFunctionById(FUNCTION_ID));
        Assertions.assertNotSame(db.getFunctions().get(0), env.getFunctionById(FUNCTION_ID));
        report(Set.of(FUNCTION_ID));
        Assertions.assertNull(queuedTask());
        Env.getCurrentInternalCatalog().recoverDatabase(DB_NAME, DB_ID, "");
        Function recovered = Env.getCurrentInternalCatalog().getDbNullable(DB_ID).getFunctions().get(0);
        Assertions.assertEquals(FUNCTION_ID, recovered.getId());
        Assertions.assertSame(recovered, env.getFunctionById(recovered.getId()));
    }

    @Test
    void forceDroppedDatabaseImageDoesNotRegisterFunctions() throws Exception {
        Env.getCurrentInternalCatalog().replayCreateDb(databaseWithPythonFunction(), "");
        Env.getCurrentInternalCatalog().dropDb(DB_NAME, false, true);
        loadRecycleBin(saveRecycleBin(Env.getCurrentRecycleBin()));
        Assertions.assertNull(env.getFunctionById(FUNCTION_ID));
    }

    @Test
    void detachedDatabaseCannotRegisterNewFunctionIds() throws Exception {
        Database db = databaseWithPythonFunction();
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        Env.getCurrentInternalCatalog().replayDropDb(DB_NAME, true, 0L);
        ScalarFunction late = pythonFunction("late_registration", Type.INT);
        late.setId(FUNCTION_ID + 1);
        db.replayAddFunction(late);
        Assertions.assertNull(env.getFunctionById(FUNCTION_ID));
        Assertions.assertNull(env.getFunctionById(late.getId()));
    }

    @Test
    void concurrentDatabaseDropRemovesFunctionRegistrations() throws Exception {
        Database db = databaseWithPythonFunction();
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        ScalarFunction added = pythonFunction("concurrent_registration", Type.INT);
        added.setId(FUNCTION_ID + 1);
        CountDownLatch start = new CountDownLatch(1);
        CompletableFuture<Void> registration = CompletableFuture.runAsync(() -> {
            try {
                start.await();
                db.replayAddFunction(added);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        CompletableFuture<Void> removal = CompletableFuture.runAsync(() -> {
            try {
                start.await();
                Env.getCurrentInternalCatalog().replayDropDb(DB_NAME, true, 0L);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        start.countDown();
        CompletableFuture.allOf(registration, removal).get(5, TimeUnit.SECONDS);
        Assertions.assertNull(env.getFunctionById(FUNCTION_ID));
        Assertions.assertNull(env.getFunctionById(added.getId()));
    }

    @Test
    void localDropIsReconciledAfterJournalReplay() throws Exception {
        dropReplayAndReport(SetType.DEFAULT);
    }

    @Test
    void globalDropIsReconciledAfterJournalReplay() throws Exception {
        dropReplayAndReport(SetType.GLOBAL);
    }

    private void dropReplayAndReport(SetType scope) throws Exception {
        Database db = databaseWithPythonFunction();
        Function function = db.getFunctions().get(0);
        Assertions.assertEquals(-1, function.getAutoSuspend());
        if (scope == SetType.GLOBAL) {
            env.getGlobalFunctionMgr().replayAddFunction(function);
        } else {
            Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        }
        report(Set.of(FUNCTION_ID));
        Assertions.assertNull(queuedTask());
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setThreadLocalInfo();
        try {
            new DropFunctionCommand(scope, false, new FunctionName(DB_NAME, "evaluate"),
                    new FunctionArgTypesInfo(List.of(IntegerType.INSTANCE), false)).run(context, null);
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
        Assertions.assertNotNull(queuedTask());
        JournalEntity drop = readJournal(1);
        Assertions.assertEquals(scope == SetType.GLOBAL ? OperationType.OP_DROP_GLOBAL_FUNCTION
                : OperationType.OP_DROP_FUNCTION, drop.getOpCode());
        AgentTaskQueue.clearAllTasks();
        if (scope == SetType.GLOBAL) {
            env.getGlobalFunctionMgr().replayAddFunction(function);
        } else {
            db.replayAddFunction(function);
        }
        EditLog.loadJournal(env, 1L, drop);
        Assertions.assertNull(queuedTask());
        backend.setAlive(true);
        report(Set.of(FUNCTION_ID));
        Assertions.assertNotNull(queuedTask());
        finish(TStatusCode.OK);
        Assertions.assertNull(queuedTask());
    }

    @Test
    void databaseRecoveryPreservesFunctionIdWithoutCleanup() throws Exception {
        Database db = databaseWithPythonFunction();
        Function original = db.getFunctions().get(0);
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        Env.getCurrentInternalCatalog().dropDb(DB_NAME, false, false);
        Assertions.assertNull(queuedTask());
        Assertions.assertSame(original, env.getFunctionById(FUNCTION_ID));
        report(Set.of(FUNCTION_ID));
        Assertions.assertNull(queuedTask());
        Env.getCurrentInternalCatalog().recoverDatabase(DB_NAME, DB_ID, "");
        Function recovered = db.getFunctions().get(0);
        Assertions.assertEquals(FUNCTION_ID, recovered.getId());
        Assertions.assertSame(original, recovered);
        Assertions.assertSame(recovered, env.getFunctionById(FUNCTION_ID));
        Assertions.assertEquals(-1, recovered.getAutoSuspend());
        report(Set.of(FUNCTION_ID));
        Assertions.assertNull(queuedTask());
    }

    @Test
    void recoverableDatabaseDropReplayPreservesFunctionIndex() throws Exception {
        Database db = databaseWithPythonFunction();
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        Env.getCurrentInternalCatalog().dropDb(DB_NAME, false, false);
        JournalEntity drop = readJournal(1);
        Assertions.assertEquals(OperationType.OP_DROP_DB, drop.getOpCode());
        loadRecycleBin(saveRecycleBin(new CatalogRecycleBin()));
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        EditLog.loadJournal(env, 1L, drop);
        Assertions.assertSame(db.getFunctions().get(0), env.getFunctionById(FUNCTION_ID));
        report(Set.of(FUNCTION_ID));
        Assertions.assertNull(queuedTask());
    }

    @Test
    void databaseRecoveryJournalAndImagePreserveFunctionId() throws Exception {
        Database db = databaseWithPythonFunction();
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        Env.getCurrentInternalCatalog().dropDb(DB_NAME, false, false);
        byte[] recycled = saveRecycleBin(Env.getCurrentRecycleBin());
        Env.getCurrentInternalCatalog().recoverDatabase(DB_NAME, DB_ID, "");
        Assertions.assertEquals(FUNCTION_ID, db.getFunctions().get(0).getId());
        long journalId = 2L;
        JournalEntity recovery = readJournal(journalId);
        Assertions.assertEquals(OperationType.OP_RECOVER_DB, recovery.getOpCode());

        Env.getCurrentInternalCatalog().replayDropDb(DB_NAME, true, 0L);
        loadRecycleBin(recycled);
        EditLog.loadJournal(env, journalId, recovery);
        Database replayed = Env.getCurrentInternalCatalog().getDbNullable(DB_ID);
        Assertions.assertEquals(FUNCTION_ID, replayed.getFunctions().get(0).getId());
        Assertions.assertSame(replayed.getFunctions().get(0), env.getFunctionById(FUNCTION_ID));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            replayed.write(out);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Assertions.assertEquals(FUNCTION_ID, Database.read(in).getFunctions().get(0).getId());
        }
    }

    @Test
    void databaseDropReplayRebuildsCleanupFromBackendReport() throws Exception {
        Database db = databaseWithPythonFunction();
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        Env.getCurrentInternalCatalog().dropDb(DB_NAME, false, true);
        JournalEntity drop = readJournal(1);
        Assertions.assertEquals(OperationType.OP_DROP_DB, drop.getOpCode());
        AgentTaskQueue.clearAllTasks();
        loadRecycleBin(saveRecycleBin(new CatalogRecycleBin()));
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        EditLog.loadJournal(env, 1L, drop);
        Assertions.assertNull(queuedTask());
        report(Set.of(FUNCTION_ID));
        Assertions.assertNotNull(queuedTask());
    }

    @Test
    void erasedDatabaseReplayRebuildsCleanupFromBackendReport() throws Exception {
        Database db = databaseWithPythonFunction();
        Env.getCurrentInternalCatalog().replayCreateDb(db, "");
        Env.getCurrentInternalCatalog().dropDb(DB_NAME, false, false);
        byte[] recycled = saveRecycleBin(Env.getCurrentRecycleBin());
        Env.getCurrentRecycleBin().eraseDatabaseInstantly(DB_ID);
        Assertions.assertNull(env.getFunctionById(FUNCTION_ID));
        Assertions.assertNotNull(queuedTask());
        JournalEntity erase = readJournal(2);
        Assertions.assertEquals(OperationType.OP_ERASE_DB, erase.getOpCode());
        AgentTaskQueue.clearAllTasks();
        loadRecycleBin(recycled);
        Assertions.assertNotNull(env.getFunctionById(FUNCTION_ID));
        report(Set.of(FUNCTION_ID));
        Assertions.assertNull(queuedTask());
        EditLog.loadJournal(env, 2L, erase);
        Assertions.assertNull(env.getFunctionById(FUNCTION_ID));
        Assertions.assertNull(queuedTask());
        backend.setAlive(true);
        report(Set.of(FUNCTION_ID));
        Assertions.assertNotNull(queuedTask());
        finish(TStatusCode.OK);
        Assertions.assertNull(queuedTask());
    }

    @Test
    void removedBackendCleanupExpiresAndReportsCannotRecreateIt() throws Exception {
        report(Set.of(FUNCTION_ID));
        AgentTask task = queuedTask();
        Assertions.assertNotNull(task);
        Env.getCurrentSystemInfo().dropBackend(BACKEND_ID);
        Assertions.assertSame(task, queuedTask());
        expireTask(task);
        runTaskCleanup();
        Assertions.assertNull(queuedTask());
        report(Set.of(FUNCTION_ID));
        Assertions.assertNull(queuedTask());
    }

    @Test
    void cloudScaleInAndGroupRemovalCleanupExpires() throws Exception {
        CloudSystemInfoService service = new CloudSystemInfoService();
        Backend second = new Backend(BACKEND_ID + 1, "127.0.0.1", 19051);
        Map<String, String> tags = Map.of(Tag.TYPE_LOCATION, "default",
                Tag.CLOUD_CLUSTER_ID, "cleanup-group", Tag.CLOUD_CLUSTER_NAME, "cleanup-group");
        backend.setTagMap(tags);
        second.setTagMap(tags);
        service.updateCloudBackends(List.of(backend, second), List.of());
        AgentTask firstTask = new CleanUDFCacheTask(BACKEND_ID, "", FUNCTION_ID, true);
        AgentTask secondTask = new CleanUDFCacheTask(second.getId(), "", FUNCTION_ID, true);
        AgentTaskQueue.addTask(firstTask);
        AgentTaskQueue.addTask(secondTask);

        service.updateCloudBackends(List.of(), List.of(backend));
        Assertions.assertNull(service.getBackend(BACKEND_ID));
        Assertions.assertSame(firstTask, queuedTask());
        expireTask(firstTask);
        runTaskCleanup();
        Assertions.assertNull(queuedTask());
        Assertions.assertSame(secondTask,
                AgentTaskQueue.getTask(second.getId(), TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID));
        Assertions.assertEquals(1, AgentTaskQueue.getTaskNum());

        service.updateCloudBackends(List.of(), List.of(second));
        Assertions.assertNull(service.getBackend(second.getId()));
        expireTask(secondTask);
        runTaskCleanup();
        Assertions.assertNull(AgentTaskQueue.getTask(second.getId(), TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID));
        Assertions.assertEquals(0, AgentTaskQueue.getTaskNum());
    }

    @Test
    void cleanupSubmittedAfterBackendRemovalExpires() throws Exception {
        CountDownLatch prepared = new CountDownLatch(1);
        CountDownLatch submit = new CountDownLatch(1);
        CompletableFuture<Void> submission = CompletableFuture.runAsync(() -> {
            Assertions.assertSame(backend, Env.getCurrentSystemInfo().getBackend(BACKEND_ID));
            CleanUDFCacheTask task = new CleanUDFCacheTask(BACKEND_ID, "", FUNCTION_ID, true);
            prepared.countDown();
            try {
                Assertions.assertTrue(submit.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            FunctionUtil.submitUdfCacheCleanupTasks(List.of(task));
        });
        try {
            Assertions.assertTrue(prepared.await(10, TimeUnit.SECONDS));
            Env.getCurrentSystemInfo().dropBackend(BACKEND_ID);
        } finally {
            submit.countDown();
        }
        submission.get(10, TimeUnit.SECONDS);
        AgentTask task = queuedTask();
        Assertions.assertNotNull(task);
        runTaskCleanup();
        Assertions.assertSame(task, queuedTask());
        expireTask(task);
        runTaskCleanup();
        Assertions.assertNull(queuedTask());
        Assertions.assertEquals(0, AgentTaskQueue.getTaskNum());
    }

    @Test
    void backendReportRecreatesExpiredCleanup() throws Exception {
        report(Set.of(FUNCTION_ID));
        AgentTask task = queuedTask();
        expireTask(task);
        runTaskCleanup();
        Assertions.assertNull(queuedTask());
        backend.setAlive(true);
        report(Set.of(FUNCTION_ID));
        Assertions.assertNotNull(queuedTask());
        Assertions.assertNotSame(task, queuedTask());
        finish(TStatusCode.OK);
        Assertions.assertNull(queuedTask());
    }

    private void expireTask(AgentTask task) throws Exception {
        Field createTime = AgentTask.class.getDeclaredField("createTime");
        createTime.setAccessible(true);
        createTime.setLong(task, System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(6));
    }

    private void runTaskCleanup() throws Exception {
        Method method = AgentTaskCleanupDaemon.class.getDeclaredMethod("runAfterCatalogReady");
        method.setAccessible(true);
        method.invoke(new AgentTaskCleanupDaemon());
    }

    @Test
    void currentFunctionsAreRetainedAndOtherIdsAreCleaned() throws Exception {
        Env.getCurrentInternalCatalog().replayCreateDb(databaseWithPythonFunction(), "");
        report(Set.of(FUNCTION_ID, FUNCTION_ID + 1));
        Assertions.assertNull(queuedTask());
        Assertions.assertNotNull(AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID + 1));
    }

    @Test
    void repeatedReportsAndFailedAcknowledgementKeepOneTask() throws Exception {
        report(Set.of(FUNCTION_ID));
        AgentTask task = queuedTask();
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> task.getFailedTimes() > 0);
        report(Set.of(FUNCTION_ID));
        Assertions.assertSame(task, queuedTask());
        finish(TStatusCode.INTERNAL_ERROR);
        Assertions.assertSame(task, queuedTask());
        finish(TStatusCode.OK);
        Assertions.assertNull(queuedTask());
        // A late query can recreate resources; the next report must clean them again.
        report(Set.of(FUNCTION_ID));
        Assertions.assertNotNull(queuedTask());
        Assertions.assertNotSame(task, queuedTask());
    }

    private void report(Set<Long> ids) throws Exception {
        Method method = ReportHandler.class.getDeclaredMethod("pythonUdfReport", long.class, Set.class);
        method.setAccessible(true);
        method.invoke(null, BACKEND_ID, ids);
    }

    private void finish(TStatusCode status) {
        TFinishTaskRequest request = new TFinishTaskRequest(new TBackend(backend.getHost(), backend.getBePort(), 0),
                TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID, new TStatus(status));
        Assertions.assertEquals(TStatusCode.OK, new MasterImpl().finishTask(request).getStatus().getStatusCode());
    }

    private Database databaseWithPythonFunction() {
        Database db = new Database(DB_ID, DB_NAME);
        db.replayAddFunction(pythonFunction("evaluate", Type.INT));
        return db;
    }

    private ScalarFunction pythonFunction(String name, Type returnType) {
        ScalarFunction function = new ScalarFunction(new FunctionName(DB_NAME, name),
                List.of(Type.INT), returnType, false, Function.BinaryType.PYTHON_UDF, true, false);
        function.setId(FUNCTION_ID);
        function.setAutoSuspend(-1);
        function.setRuntimeVersion("3.12.11");
        function.setSymbolName("evaluate");
        return function;
    }

    private AgentTask queuedTask() {
        return AgentTaskQueue.getTask(BACKEND_ID, TTaskType.CLEAN_UDF_CACHE, FUNCTION_ID);
    }

    private JournalEntity readJournal(long id) {
        JournalCursor cursor = editLog.read(id, id);
        try {
            return cursor.next().second;
        } finally {
            cursor.close();
        }
    }

    private byte[] saveRecycleBin(CatalogRecycleBin bin) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            bin.write(out);
        }
        return bytes.toByteArray();
    }

    private void loadRecycleBin(byte[] image) throws Exception {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(image))) {
            env.loadRecycleBin(in, 0);
        }
    }
}
