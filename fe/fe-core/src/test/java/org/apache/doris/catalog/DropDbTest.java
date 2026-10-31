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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.RecoverDatabaseCommand;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.CleanUDFCacheTask;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DropDbTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseWithSql("create database test1;");
        createDatabaseWithSql("create database test2;");
        createDatabaseWithSql("create database test3;");
        createTable("create table test1.tbl1(k1 int, k2 bigint) duplicate key(k1) "
                + "distributed by hash(k2) buckets 1" + " properties('replication_num' = '1');");
        createTable("create table test2.tbl1" + "(k1 int, k2 bigint)" + " duplicate key(k1) "
                + "distributed by hash(k2) buckets 1 " + "properties('replication_num' = '1');");
    }

    @Test
    public void testDropDbPreservesFunctionsForRecovery() throws Exception {
        Database db = createDatabaseWithPythonFunctions("drop_function_cache_db");
        Set<Long> functionIds = functionIds(db);
        try (MockedStatic<AgentTaskExecutor> executor = Mockito.mockStatic(AgentTaskExecutor.class)) {
            dropDatabaseWithSql("drop database drop_function_cache_db");
            RecoverDatabaseCommand recover = (RecoverDatabaseCommand) new NereidsParser()
                    .parseSingle("recover database drop_function_cache_db");
            recover.run(connectContext, null);
            Database recovered = Env.getCurrentInternalCatalog().getDbOrMetaException("drop_function_cache_db");
            Assertions.assertEquals(functionIds, functionIds(recovered));
            executor.verifyNoInteractions();
        }
    }

    @Test
    public void testForceDropDbSubmitsFunctionCacheCleanup() throws Exception {
        Database db = createDatabaseWithPythonFunctions("force_drop_function_cache_db");
        // Java database cleanup is unchanged; only the new Python pools need this notification.
        db.getFunctions().stream()
                .filter(function -> function.getFunctionName().getFunction().equals("py_scalar"))
                .findFirst().orElseThrow().setBinaryType(Function.BinaryType.JAVA_UDF);
        try (MockedStatic<AgentTaskExecutor> executor = Mockito.mockStatic(AgentTaskExecutor.class)) {
            dropDatabaseWithSql("drop database force_drop_function_cache_db force");
            assertFunctionCacheCleanupSubmitted(executor, db);
        }
        Assertions.assertNull(Env.getCurrentInternalCatalog().getDbNullable(db.getFullName()));
    }

    @Test
    public void testExpiredDbSubmitsFunctionCacheCleanup() throws Exception {
        assertErasedDbSubmitsFunctionCacheCleanup("expired");
    }

    @Test
    public void testSameNameErasedDbSubmitsFunctionCacheCleanup() throws Exception {
        assertErasedDbSubmitsFunctionCacheCleanup("same_name");
    }

    @Test
    public void testInstantlyErasedDbSubmitsFunctionCacheCleanup() throws Exception {
        assertErasedDbSubmitsFunctionCacheCleanup("instantly");
    }

    @Test
    public void testDropDbWithoutFunctionsDoesNotSubmitCacheCleanup() throws Exception {
        createDatabaseWithSql("create database drop_empty_function_cache_db");
        try (MockedStatic<AgentTaskExecutor> executor = Mockito.mockStatic(AgentTaskExecutor.class)) {
            dropDatabaseWithSql("drop database drop_empty_function_cache_db force");
            executor.verifyNoInteractions();
        }
    }

    private void assertErasedDbSubmitsFunctionCacheCleanup(String kind) throws Exception {
        Database db = createDatabaseWithPythonFunctions("erase_" + kind + "_function_cache_db");
        dropDatabaseWithSql("drop database " + db.getFullName());
        CatalogRecycleBin recycleBin = Env.getCurrentRecycleBin();
        try (MockedStatic<AgentTaskExecutor> executor = Mockito.mockStatic(AgentTaskExecutor.class)) {
            switch (kind) {
                case "expired":
                    recycleBin.setRecycleTimeByIdForReplay(db.getId(), 0L);
                    Deencapsulation.invoke(recycleBin, "eraseDatabase", System.currentTimeMillis(), -1);
                    break;
                case "same_name":
                    recycleBin.setRecycleTimeByIdForReplay(db.getId(), 1L);
                    Deencapsulation.invoke(recycleBin, "eraseDatabaseWithSameName", db.getFullName(),
                            System.currentTimeMillis(), 0, new ArrayList<>(List.of(db.getId())));
                    break;
                case "instantly":
                    recycleBin.eraseDatabaseInstantly(db.getId());
                    break;
                default:
                    throw new AssertionError(kind);
            }
            Assertions.assertFalse(recycleBin.isRecycleDatabase(db.getId()));
            assertFunctionCacheCleanupSubmitted(executor, db);
        }
    }

    private Database createDatabaseWithPythonFunctions(String dbName) throws Exception {
        createDatabaseWithSql("create database " + dbName);
        createFunction("create function " + dbName + ".py_scalar(int) returns int properties("
                + "'type'='PYTHON_UDF', 'symbol'='evaluate', 'runtime_version'='3.10.2')");
        createFunction("create aggregate function " + dbName + ".py_agg(int) returns int properties("
                + "'type'='PYTHON_UDF', 'symbol'='Agg', 'runtime_version'='3.10.2')");
        createFunction("create tables function " + dbName + ".py_table(int) returns array<int> properties("
                + "'type'='PYTHON_UDF', 'symbol'='evaluate', 'runtime_version'='3.10.2')");
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbName);
        Assertions.assertEquals(4, functionIds(db).size());
        Assertions.assertTrue(db.getFunctions().stream()
                .anyMatch(function -> function.getFunctionName().getFunction().equals("py_table_outer")));
        return db;
    }

    private Set<Long> functionIds(Database db) {
        return db.getFunctions().stream().map(Function::getId).collect(Collectors.toSet());
    }

    private void assertFunctionCacheCleanupSubmitted(MockedStatic<AgentTaskExecutor> executor, Database db)
            throws Exception {
        Map<Long, String> functions = db.getFunctions().stream()
                .filter(function -> function.getBinaryType() == Function.BinaryType.PYTHON_UDF)
                .collect(Collectors.toMap(Function::getId, Function::signatureString));
        Set<Long> databaseFunctionIds = functionIds(db);
        Set<Long> backendIds = Env.getCurrentSystemInfo().getAllBackendsByAllCluster().keySet();
        Assertions.assertFalse(backendIds.isEmpty());
        ArgumentCaptor<AgentBatchTask> submitted = ArgumentCaptor.forClass(AgentBatchTask.class);
        executor.verify(() -> AgentTaskExecutor.submit(submitted.capture()), Mockito.atLeastOnce());
        Map<Long, Map<Long, String>> cleanedFunctions = new HashMap<>();
        int taskCount = 0;
        for (AgentBatchTask batch : submitted.getAllValues()) {
            for (AgentTask task : batch.getAllTasks()) {
                if (task instanceof CleanUDFCacheTask) {
                    CleanUDFCacheTask cleanup = (CleanUDFCacheTask) task;
                    long functionId = cleanup.toThrift().getFunctionId();
                    // Recycle-bin scans may also erase other databases in the same test environment.
                    if (databaseFunctionIds.contains(functionId)) {
                        Assertions.assertTrue(functions.containsKey(functionId), "Only Python pools need cleanup");
                        cleanedFunctions.computeIfAbsent(task.getBackendId(), ignored -> new HashMap<>())
                                .put(functionId, cleanup.toThrift().getFunctionSignature());
                        taskCount++;
                    }
                }
            }
        }
        Assertions.assertEquals(backendIds, cleanedFunctions.keySet());
        for (Map<Long, String> cleaned : cleanedFunctions.values()) {
            Assertions.assertEquals(functions, cleaned);
        }
        Assertions.assertEquals(backendIds.size() * functions.size(), taskCount);
    }

    @Test
    public void testNormalDropDb() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test1");
        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1");
        Partition partition = table.getAllPartitions().iterator().next();
        long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        String dropDbSql = "drop database test1";
        dropDatabaseWithSql(dropDbSql);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assertions.assertNull(db);
        List<Replica> replicaList = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        Assertions.assertEquals(1, replicaList.size());
        String recoverDbSql = "recover database test1";
        NereidsParser nereidsParser = new NereidsParser();
        RecoverDatabaseCommand command = (RecoverDatabaseCommand) nereidsParser.parseSingle(recoverDbSql);
        command.run(connectContext, null);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assertions.assertNotNull(db);
        Assertions.assertEquals("test1", db.getFullName());
        table = (OlapTable) db.getTableOrMetaException("tbl1");
        Assertions.assertNotNull(table);
        Assertions.assertEquals("tbl1", table.getName());

        dropDbSql = "drop schema test1";
        dropDatabaseWithSql(dropDbSql);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assertions.assertNull(db);
        command.run(connectContext, null);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assertions.assertNotNull(db);

        dropDbSql = "drop schema if exists test1";
        dropDatabaseWithSql(dropDbSql);
        db = Env.getCurrentInternalCatalog().getDbNullable("test1");
        Assertions.assertNull(db);
    }

    @Test
    public void testForceDropDb() throws Exception {
        String dropDbSql = "drop database test2 force";
        dropDatabaseWithSql(dropDbSql);
        Database db = Env.getCurrentInternalCatalog().getDbNullable("test2");
        Assertions.assertNull(db);
        // After unify force and non-force drop db, the replicas will be recycled eventually.
        //
        // Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test2");
        // OlapTable table = (OlapTable) db.getTableOrMetaException("tbl1");
        // Partition partition = table.getAllPartitions().iterator().next();
        // long tabletId = partition.getBaseIndex().getTablets().get(0).getId();
        // ...
        // List<Replica> replicaList = Env.getCurrentEnv().getTabletInvertedIndex().getReplicasByTabletId(tabletId);
        // Assert.assertTrue(replicaList.isEmpty());
        String recoverDbSql = "recover database test2";
        NereidsParser nereidsParser = new NereidsParser();
        RecoverDatabaseCommand command = (RecoverDatabaseCommand) nereidsParser.parseSingle(recoverDbSql);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Unknown database 'test2' or database id '-1'",
                () -> command.run(connectContext, null));
        dropDbSql = "drop schema test3 force";
        db = Env.getCurrentInternalCatalog().getDbOrMetaException("test3");
        Assertions.assertNotNull(db);
        dropDatabaseWithSql(dropDbSql);
        db = Env.getCurrentInternalCatalog().getDbNullable("test3");
        Assertions.assertNull(db);
        recoverDbSql = "recover database test3";
        RecoverDatabaseCommand command2 = (RecoverDatabaseCommand) nereidsParser.parseSingle(recoverDbSql);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Unknown database 'test3'",
                () -> command2.run(connectContext, null));

        dropDbSql = "drop schema if exists test3 force";
        dropDatabaseWithSql(dropDbSql);
    }
}
