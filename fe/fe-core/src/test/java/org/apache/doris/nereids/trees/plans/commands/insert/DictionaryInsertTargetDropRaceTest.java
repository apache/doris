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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugPointUtil.DebugPoint;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

class DictionaryInsertTargetDropRaceTest extends TestWithFeService {
    private static final String BLOCK_BEFORE_PLAN = "DictionaryManager.dataLoad.blockBeforePlan";

    private boolean debugPointsEnabled;

    @BeforeEach
    void saveDebugPointConfig() {
        debugPointsEnabled = Config.enable_debug_points;
    }

    @AfterEach
    void clearDebugPoints() {
        DebugPointUtil.clearDebugPoints();
        Config.enable_debug_points = debugPointsEnabled;
    }

    @Test
    void rejectsDroppedDictionaryAfterResolvingItsOwner() throws Exception {
        Config.enable_debug_points = true;
        DebugPoint blockPoint = new DebugPoint();
        blockPoint.executeLimit = Integer.MAX_VALUE;
        DebugPointUtil.addDebugPoint(BLOCK_BEFORE_PLAN, blockPoint);

        String sourceDbName = "dictionary_insert_drop_race_source";
        createDatabaseAndUse(sourceDbName);
        createSourceTable();

        String dbName = "dictionary_insert_drop_race";
        createDatabaseAndUse(dbName);
        executeNereidsSql("CREATE DICTIONARY dic1 USING internal." + sourceDbName
                + ".source_table (city KEY, id VALUE) "
                + "LAYOUT(HASH_MAP) PROPERTIES ('data_lifetime' = '600')");

        Dictionary dictionary = Env.getCurrentEnv().getDictionaryManager().getDictionary(dbName, "dic1");
        Database database = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        String sql = "INSERT INTO " + dbName + ".dic1 SELECT * FROM "
                + dictionary.getSourceCtlName() + "." + dictionary.getSourceDbName() + "."
                + dictionary.getSourceTableName();
        InsertIntoTableCommand baseCommand = (InsertIntoTableCommand) new NereidsParser().parseSingle(sql);
        CountDownLatch targetValidated = new CountDownLatch(1);
        CountDownLatch resumePlanning = new CountDownLatch(1);
        AtomicBoolean blockOnce = new AtomicBoolean(true);
        InsertIntoDictionaryCommand command = new InsertIntoDictionaryCommand(
                baseCommand, database, dictionary, false) {
            @Override
            protected TableIf getTargetTableIf(ConnectContext ctx, List<String> qualifiedTargetTableName) {
                TableIf target = super.getTargetTableIf(ctx, qualifiedTargetTableName);
                if (blockOnce.compareAndSet(true, false)) {
                    targetValidated.countDown();
                    await(resumePlanning);
                }
                return target;
            }
        };

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            await(() -> blockPoint.executeNum.get() > 0);
            Future<Throwable> result = executorService.submit(() -> runInitPlan(command, sql, dbName));
            Assertions.assertTrue(targetValidated.await(10, TimeUnit.SECONDS));
            Env.getCurrentInternalCatalog().dropDb(dbName, false, true);
            resumePlanning.countDown();

            Throwable failure = result.get(10, TimeUnit.SECONDS);
            Assertions.assertNotNull(failure);
            Assertions.assertTrue(hasCause(failure, org.apache.doris.nereids.exceptions.AnalysisException.class),
                    failure.toString());
            Assertions.assertTrue(failure.toString().contains("Dictionary dic1 has been dropped"),
                    failure.toString());
            Assertions.assertFalse(hasCause(failure, NullPointerException.class), failure.toString());
            Assertions.assertNull(Env.getCurrentEnv().getDictionaryManager().getDictionary(dictionary.getId()));

            createDatabaseAndUse(dbName);
            executeNereidsSql("CREATE DICTIONARY dic1 USING internal." + sourceDbName
                    + ".source_table (city KEY, id VALUE) "
                    + "LAYOUT(HASH_MAP) PROPERTIES ('data_lifetime' = '600')");
            Dictionary replacement = Env.getCurrentEnv().getDictionaryManager().getDictionary(dbName, "dic1");
            Assertions.assertNotEquals(dictionary.getId(), replacement.getId());
        } finally {
            resumePlanning.countDown();
            DebugPointUtil.removeDebugPoint(BLOCK_BEFORE_PLAN);
            executorService.shutdownNow();
        }

        await(() -> !dictionary.getLastUpdateResult().isEmpty());
        Assertions.assertTrue(dictionary.getLastUpdateResult().contains("has been dropped"),
                dictionary.getLastUpdateResult());
        Assertions.assertFalse(dictionary.getLastUpdateResult().contains("Cannot invoke"),
                dictionary.getLastUpdateResult());
        Env.getCurrentInternalCatalog().dropDb(dbName, false, true);
        Env.getCurrentInternalCatalog().dropDb(sourceDbName, false, true);
    }

    private void createSourceTable() throws Exception {
        createTable("CREATE TABLE source_table (id INT NOT NULL, city VARCHAR(32) NOT NULL) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
    }

    private Throwable runInitPlan(InsertIntoDictionaryCommand command, String sql, String dbName) {
        try {
            ConnectContext ctx = createCtx(UserIdentity.ROOT, "127.0.0.1");
            ctx.setDatabase(dbName);
            ctx.setQueryId(new TUniqueId(1, 2));
            ctx.setStatementContext(new StatementContext(ctx, new OriginStatement(sql, 0)));
            command.initPlan(ctx, new StmtExecutor(ctx, sql));
            return null;
        } catch (Throwable t) {
            return t;
        }
    }

    private static boolean hasCause(Throwable throwable, Class<? extends Throwable> causeClass) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (causeClass.isInstance(cause)) {
                return true;
            }
        }
        return false;
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting to resume dictionary planning");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        Assertions.assertTrue(condition.getAsBoolean());
    }
}
