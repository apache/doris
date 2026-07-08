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

package org.apache.doris.datasource.lowercase;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.DropCatalogCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ExternalDatabaseRefreshConcurrencyTest extends TestWithFeService {
    private static final String CATALOG_NAME = "test_refresh_concurrency";
    private static final String DB_NAME = "db1";
    // Mixed-case remote name vs. lower-case lookup forces the lowerCaseToTableName path.
    private static final String REMOTE_TABLE_NAME = "TABLE1";
    private static final String LOOKUP_TABLE_NAME = "table1";

    private static Env env;
    private ConnectContext rootCtx;

    @Override
    protected void runBeforeAll() throws Exception {
        rootCtx = createDefaultCtx();
        env = Env.getCurrentEnv();
        String createStmt = "create catalog " + CATALOG_NAME + " properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.datasource.lowercase."
                + "ExternalDatabaseRefreshConcurrencyTest$RefreshConcurrencyProvider\"\n"
                + ");";

        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(createStmt);
        if (logicalPlan instanceof CreateCatalogCommand) {
            ((CreateCatalogCommand) logicalPlan).run(rootCtx, null);
        }
    }

    @Override
    protected void beforeCluster() {
        // mode 2 is the case-insensitive path patched by the fix.
        Config.lower_case_table_names = 2;
        Config.enable_debug_points = true;
        FeConstants.runningUnitTest = true;
    }

    @Override
    protected void runAfterAll() throws Exception {
        super.runAfterAll();
        rootCtx.setThreadLocalInfo();
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle("drop catalog " + CATALOG_NAME);
        if (logicalPlan instanceof DropCatalogCommand) {
            ((DropCatalogCommand) logicalPlan).run(rootCtx, null);
        }
    }

    private static void enableListSleep(long sleepMs) {
        Map<String, String> params = new HashMap<>();
        params.put("sleepMs", String.valueOf(sleepMs));
        DebugPointUtil.addDebugPointWithParams("ExternalDatabase.listTableNames.sleep", params);
    }

    @AfterEach
    public void clearDebugPoints() {
        DebugPointUtil.clearDebugPoints();
    }

    private ExternalDatabase<?> getDb() {
        ExternalDatabase<?> db = (ExternalDatabase<?>) env.getCatalogMgr()
                .getCatalog(CATALOG_NAME).getDbNullable(DB_NAME);
        Assertions.assertNotNull(db, "test database should be created");
        // Trigger first init so meta cache is built.
        db.getTableNamesWithLock();
        return db;
    }

    /** Sanity check: getTableNullable still works right after a reset. */
    @Test
    public void testGetTableAfterReset() {
        ExternalDatabase<?> db = getDb();
        db.resetMetaToUninitialized();
        TableIf tbl = db.getTableNullable(LOOKUP_TABLE_NAME);
        Assertions.assertNotNull(tbl,
                "after reset, getTableNullable(\"" + LOOKUP_TABLE_NAME + "\") must still find the table");
        Assertions.assertEquals(REMOTE_TABLE_NAME, tbl.getName());
    }

    /**
     * One refresher + many readers; getTableNullable must never return null.
     * The debug point widens the clear/refill window to make the race reproducible.
     */
    @Test
    public void testGetTableNotNullUnderFrequentRefresh() throws Exception {
        ExternalDatabase<?> db = getDb();

        // Widen the clear/refill window inside listTableNames() so the race becomes reproducible.
        enableListSleep(20L);

        try {
            final int readerCount = 8;
            final long durationMs = 2000L;
            ExecutorService pool = Executors.newFixedThreadPool(readerCount + 1);
            AtomicBoolean stop = new AtomicBoolean(false);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger nullCount = new AtomicInteger();
            AtomicInteger lookupCount = new AtomicInteger();
            AtomicReference<Throwable> firstError = new AtomicReference<>();

            // Refresher: keep wiping lowerCaseToTableName.
            pool.submit(() -> {
                try {
                    start.await();
                    rootCtx.setThreadLocalInfo();
                    while (!stop.get()) {
                        db.resetMetaToUninitialized();
                        Thread.yield();
                    }
                } catch (Throwable t) {
                    firstError.compareAndSet(null, t);
                }
            });

            // Readers: getTableNullable must always find the table.
            for (int i = 0; i < readerCount; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        rootCtx.setThreadLocalInfo();
                        while (!stop.get()) {
                            TableIf tbl = db.getTableNullable(LOOKUP_TABLE_NAME);
                            lookupCount.incrementAndGet();
                            if (tbl == null) {
                                nullCount.incrementAndGet();
                            }
                        }
                    } catch (Throwable t) {
                        firstError.compareAndSet(null, t);
                    }
                });
            }

            start.countDown();
            Thread.sleep(durationMs);
            stop.set(true);
            pool.shutdown();
            Assertions.assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS),
                    "concurrent refresh+getTable test timed out");

            Assertions.assertNull(firstError.get(),
                    "unexpected error during concurrent run: " + firstError.get());
            Assertions.assertTrue(lookupCount.get() > 0,
                    "expected readers to perform at least one lookup");
            Assertions.assertEquals(0, nullCount.get(),
                    "regression: db.getTableNullable returned null " + nullCount.get()
                            + " out of " + lookupCount.get()
                            + " times under frequent refresh (lowerCaseToTableName was raced)");
        } finally {
            DebugPointUtil.removeDebugPoint("ExternalDatabase.listTableNames.sleep");
        }
    }

    /**
     * Cold-start race: many readers hit a freshly-reset db at once. Before the fix
     * they could clear each other's refill and return null.
     */
    @Test
    public void testGetTableNotNullOnConcurrentColdStart() throws Exception {
        ExternalDatabase<?> db = getDb();

        enableListSleep(20L);

        try {
            final int threadCount = 16;
            final int rounds = 30;
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            try {
                for (int round = 0; round < rounds; round++) {
                    db.resetMetaToUninitialized();

                    CountDownLatch start = new CountDownLatch(1);
                    CountDownLatch done = new CountDownLatch(threadCount);
                    AtomicInteger nullCount = new AtomicInteger();
                    AtomicReference<Throwable> firstError = new AtomicReference<>();

                    for (int i = 0; i < threadCount; i++) {
                        pool.submit(() -> {
                            try {
                                start.await();
                                rootCtx.setThreadLocalInfo();
                                TableIf tbl = db.getTableNullable(LOOKUP_TABLE_NAME);
                                if (tbl == null) {
                                    nullCount.incrementAndGet();
                                }
                            } catch (Throwable t) {
                                firstError.compareAndSet(null, t);
                            } finally {
                                done.countDown();
                            }
                        });
                    }

                    start.countDown();
                    Assertions.assertTrue(done.await(30, TimeUnit.SECONDS),
                            "round " + round + " timed out");
                    Assertions.assertNull(firstError.get(),
                            "round " + round + " unexpected error: " + firstError.get());
                    Assertions.assertEquals(0, nullCount.get(),
                            "round " + round + ": db.getTableNullable returned null on cold start"
                                    + " (lowerCaseToTableName was raced)");
                }
            } finally {
                pool.shutdownNow();
                Assertions.assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS),
                        "executor failed to terminate");
            }
        } finally {
            DebugPointUtil.removeDebugPoint("ExternalDatabase.listTableNames.sleep");
        }
    }

    /** Mocked provider exposing a single mixed-case table. */
    public static class RefreshConcurrencyProvider implements TestExternalCatalog.TestCatalogProvider {
        public static final Map<String, Map<String, List<Column>>> MOCKED_META;

        static {
            MOCKED_META = Maps.newHashMap();
            Map<String, List<Column>> tblSchemaMap1 = Maps.newHashMap();
            tblSchemaMap1.put(REMOTE_TABLE_NAME, Lists.newArrayList(
                    new Column("k1", PrimitiveType.INT),
                    new Column("k2", PrimitiveType.VARCHAR)));
            MOCKED_META.put(DB_NAME, tblSchemaMap1);
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }
}
