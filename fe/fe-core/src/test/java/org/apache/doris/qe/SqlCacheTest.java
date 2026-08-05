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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.proto.Types.PUniqueId;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SqlCacheTest extends TestWithFeService {
    @Override
    public void runBeforeAll() throws Exception {
        createDatabase("sql_cache_constraint_test");
        createTable("create table sql_cache_constraint_test.t (k int) "
                + "distributed by hash(k) buckets 1 properties('replication_num'='1')");
    }

    @Test
    public void testCacheKey() {
        TUniqueId queryId = new TUniqueId();
        UUID uuid = UUID.randomUUID();
        queryId.setHi(uuid.getMostSignificantBits());
        queryId.setLo(uuid.getLeastSignificantBits());
        UserIdentity admin = new UserIdentity("admin", "127.0.0.1");

        SessionVariable sessionVariable = new SessionVariable();
        SqlCacheContext cacheContext = new SqlCacheContext(admin);
        cacheContext.setOriginSql("SELECT * FROM tbl");
        PUniqueId key1 = cacheContext.doComputeCacheKeyMd5(ImmutableSet.of(), sessionVariable);

        SqlCacheContext cacheContext2 = new SqlCacheContext(admin);
        cacheContext2.setOriginSql(
                "-- Same query with comments and extra spaces\n"
                    + "/* Comment */  SELECT   *   FROM   tbl  "
        );
        PUniqueId key2 = cacheContext2.doComputeCacheKeyMd5(ImmutableSet.of(), sessionVariable);
        Assertions.assertEquals(key1, key2);

        SqlCacheContext cacheContext3 = new SqlCacheContext(admin);
        cacheContext3.setOriginSql(
                "-- Same query with comments and extra spaces\n"
                        + "/* Comment */  SELeCT   *   FROM   tbl  "
        );
        PUniqueId key3 = cacheContext3.doComputeCacheKeyMd5(ImmutableSet.of(), sessionVariable);
        Assertions.assertNotEquals(key1, key3);
    }

    @Test
    public void testSqlCache() throws Exception {
        connectContext.getSessionVariable().setEnableSqlCache(true);
        executeNereidsSql("select 100");
        executeNereidsSql("select 200");

        Env currentEnv = Env.getCurrentEnv();
        NereidsSqlCacheManager sqlCacheManager = currentEnv.getSqlCacheManager();
        Assertions.assertEquals(2, sqlCacheManager.getSqlCaches().asMap().size());

        executeNereidsSql("admin set frontend config ('sql_cache_manage_num'='1')");
        Assertions.assertEquals(1, sqlCacheManager.getSqlCaches().asMap().size());
    }

    @Test
    public void testInvalidateSqlCacheByPersistedTableName() throws Exception {
        TableIf table = Env.getCurrentInternalCatalog()
                .getDbOrDdlException("sql_cache_constraint_test").getTableOrDdlException("t");
        SqlCacheContext cacheContext = new SqlCacheContext(new UserIdentity("admin", "127.0.0.1"));
        cacheContext.addUsedTable(table);
        NereidsSqlCacheManager sqlCacheManager = Env.getCurrentEnv().getSqlCacheManager();
        sqlCacheManager.getSqlCaches().put("mapping_constraint_cache", cacheContext);
        long initialEpoch = sqlCacheManager.getInvalidationEpoch();

        sqlCacheManager.invalidateAboutTableAndFencePublication(TableNameInfoUtils.fromTableOrNull(table));

        Assertions.assertNull(sqlCacheManager.getSqlCaches().getIfPresent("mapping_constraint_cache"));
        Assertions.assertEquals(initialEpoch + 1, sqlCacheManager.getInvalidationEpoch());
    }

    @Test
    public void testReplayInvalidationRevokesLookupValue() throws Exception {
        connectContext.getSessionVariable().setEnableSqlCache(true);
        NereidsSqlCacheManager sqlCacheManager = Env.getCurrentEnv().getSqlCacheManager();
        sqlCacheManager.invalidateAll();
        String sql = "select 300";
        prepareFeCacheContext(sql);
        sqlCacheManager.tryAddFeSqlCache(connectContext, sql);
        Assertions.assertEquals(1, sqlCacheManager.getSqlCaches().asMap().size());

        try (MockedStatic<StmtExecutor> stmtExecutor = Mockito.mockStatic(StmtExecutor.class)) {
            stmtExecutor.when(() -> StmtExecutor.syncJournalIfNeeded(connectContext))
                    .thenAnswer(invocation -> {
                        sqlCacheManager.invalidateAll();
                        return null;
                    });
            Assertions.assertFalse(sqlCacheManager.tryParseSql(connectContext, sql).isPresent());
        }
    }

    @Test
    public void testTableInvalidationDoesNotRejectUnrelatedCachePublication() throws Exception {
        connectContext.getSessionVariable().setEnableSqlCache(true);
        NereidsSqlCacheManager sqlCacheManager = Env.getCurrentEnv().getSqlCacheManager();
        sqlCacheManager.invalidateAll();
        String sql = "select 350";
        prepareFeCacheContext(sql);
        long initialEpoch = sqlCacheManager.getInvalidationEpoch();
        TableIf table = Env.getCurrentInternalCatalog()
                .getDbOrDdlException("sql_cache_constraint_test").getTableOrDdlException("t");

        sqlCacheManager.invalidateAboutTable(table);
        sqlCacheManager.tryAddFeSqlCache(connectContext, sql);

        Assertions.assertEquals(initialEpoch, sqlCacheManager.getInvalidationEpoch());
        Assertions.assertEquals(1, sqlCacheManager.getSqlCaches().asMap().size());
    }

    @Test
    public void testInvalidationEpochRejectsLateCachePublication() throws Exception {
        connectContext.getSessionVariable().setEnableSqlCache(true);
        NereidsSqlCacheManager sqlCacheManager = Env.getCurrentEnv().getSqlCacheManager();
        sqlCacheManager.invalidateAll();
        String sql = "select 400";
        prepareFeCacheContext(sql);
        TableIf table = Env.getCurrentInternalCatalog()
                .getDbOrDdlException("sql_cache_constraint_test").getTableOrDdlException("t");

        CountDownLatch publicationReady = new CountDownLatch(1);
        CountDownLatch invalidationFinished = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> publication = executor.submit(() -> {
                publicationReady.countDown();
                Assertions.assertTrue(invalidationFinished.await(30, TimeUnit.SECONDS));
                sqlCacheManager.tryAddFeSqlCache(connectContext, sql);
                return null;
            });
            Future<?> invalidation = executor.submit(() -> {
                Assertions.assertTrue(publicationReady.await(30, TimeUnit.SECONDS));
                try {
                    sqlCacheManager.invalidateAboutTableAndFencePublication(table);
                } finally {
                    invalidationFinished.countDown();
                }
                return null;
            });
            publication.get(30, TimeUnit.SECONDS);
            invalidation.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
        Assertions.assertTrue(sqlCacheManager.getSqlCaches().asMap().isEmpty());

        prepareFeCacheContext(sql);
        sqlCacheManager.tryAddFeSqlCache(connectContext, sql);
        Assertions.assertEquals(1, sqlCacheManager.getSqlCaches().asMap().size());
    }

    private void prepareFeCacheContext(String sql) {
        StatementContext statementContext =
                new StatementContext(connectContext, new OriginStatement(sql, 0));
        connectContext.setStatementContext(statementContext);
        SqlCacheContext sqlCacheContext = statementContext.getSqlCacheContext().get();
        sqlCacheContext.setResultSetInFe(Mockito.mock(ResultSet.class));
    }
}
