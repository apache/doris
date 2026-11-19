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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class ShowCharsetCommandTest extends TestWithFeService {
    private static final List<List<String>> EXPECTED_ROWS = ImmutableList.of(
            ImmutableList.of("utf8mb4", "UTF-8 Unicode", "utf8mb4_0900_bin", "4"));

    private ShowResultSet execute(String sql) throws Exception {
        connectContext.getState().reset();
        StmtExecutor executor = new StmtExecutor(connectContext, sql);
        ShowCharsetCommand command = (ShowCharsetCommand) new NereidsParser().parseSingle(sql);
        StatementContext statementContext = connectContext.getStatementContext();
        TUniqueId queryId = new TUniqueId(1, 2);
        connectContext.setQueryId(queryId);
        try {
            return command.doRun(connectContext, executor);
        } finally {
            Assertions.assertSame(connectContext, ConnectContext.get());
            Assertions.assertSame(executor, connectContext.getExecutor());
            Assertions.assertSame(statementContext, connectContext.getStatementContext());
            Assertions.assertSame(queryId, connectContext.queryId());
            Assertions.assertFalse(connectContext.getState().isInternal());
            Assertions.assertFalse(connectContext.getState().isQuery());
        }
    }

    @Test
    void testUnfiltered() throws Exception {
        Assertions.assertEquals(EXPECTED_ROWS, execute("SHOW CHARSET").getResultRows());
        ShowResultSet result = execute("SHOW CHARACTER SET");
        Assertions.assertEquals(EXPECTED_ROWS, result.getResultRows());
        Assertions.assertEquals("Charset", result.getMetaData().getColumn(0).getName());
        Assertions.assertEquals("Default collation", result.getMetaData().getColumn(2).getName());
    }

    @Test
    void testLike() throws Exception {
        for (String pattern : ImmutableList.of("utf8mb4", "utf8%", "utf8mb_", "UTF8%", "%")) {
            Assertions.assertEquals(EXPECTED_ROWS,
                    execute("SHOW CHARACTER SET LIKE '" + pattern + "'").getResultRows(), pattern);
        }
        Assertions.assertTrue(execute("SHOW CHARSET LIKE 'latin%'").getResultRows().isEmpty());
        Assertions.assertTrue(execute("SHOW CHARSET LIKE ''").getResultRows().isEmpty());
    }

    @Test
    void testWhere() throws Exception {
        Assertions.assertEquals(EXPECTED_ROWS,
                execute("SHOW CHARACTER SET WHERE charset = 'utf8mb4'").getResultRows());
        Assertions.assertEquals(EXPECTED_ROWS,
                execute("SHOW CHARSET WHERE CHARSET = 'utf8mb4' AND Maxlen >= 4").getResultRows());
        Assertions.assertEquals(EXPECTED_ROWS,
                execute("SHOW CHARSET WHERE Charset = 'latin1' OR Description = 'UTF-8 Unicode'").getResultRows());
        Assertions.assertEquals(EXPECTED_ROWS,
                execute("SHOW CHARSET WHERE `Default collation` = 'utf8mb4_0900_bin'").getResultRows());
        Assertions.assertTrue(execute("SHOW CHARSET WHERE Charset = 'latin1'").getResultRows().isEmpty());
        Assertions.assertTrue(execute("SHOW CHARSET WHERE Maxlen > 4").getResultRows().isEmpty());
        Assertions.assertEquals(EXPECTED_ROWS,
                execute("SHOW CHARSET WHERE upper(Charset) = 'UTF8MB4' AND Maxlen + 1 = 5").getResultRows());
    }

    @Test
    void testNullAndConstantPredicates() throws Exception {
        Assertions.assertEquals(EXPECTED_ROWS, execute("SHOW CHARSET WHERE TRUE").getResultRows());
        Assertions.assertTrue(execute("SHOW CHARSET WHERE FALSE").getResultRows().isEmpty());
        Assertions.assertTrue(execute("SHOW CHARSET WHERE NULL").getResultRows().isEmpty());
        Assertions.assertTrue(execute("SHOW CHARSET WHERE Charset = NULL").getResultRows().isEmpty());
        Assertions.assertEquals(EXPECTED_ROWS,
                execute("SHOW CHARSET WHERE Charset IS NOT NULL").getResultRows());
    }

    @Test
    void testCancelledBeforeFilterStarts() {
        String sql = "SHOW CHARSET WHERE TRUE";
        StmtExecutor executor = new StmtExecutor(connectContext, sql);
        ShowCharsetCommand command = (ShowCharsetCommand) new NereidsParser().parseSingle(sql);
        Status reason = new Status(TStatusCode.CANCELLED, "cancelled before SHOW filter starts");
        executor.cancel(reason, false);

        UserException exception = Assertions.assertThrows(UserException.class,
                () -> command.doRun(connectContext, executor));
        Assertions.assertEquals(reason.getErrorMsg(), exception.getDetailMessage());
        Assertions.assertSame(connectContext, ConnectContext.get());
    }

    @Test
    void testUnknownColumn() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> execute("SHOW CHARSET WHERE unknown_column = 'utf8mb4'"));
        Assertions.assertTrue(exception.getMessage().contains("unknown_column"));
    }

    @Test
    void testSessionValues() throws Exception {
        boolean originalEnableSqlCache = connectContext.getSessionVariable().isEnableSqlCache();
        String originalCharset = connectContext.getSessionVariable().charsetServer;
        String originalCollation = connectContext.getSessionVariable().collationConnection;
        try {
            connectContext.getSessionVariable().setEnableSqlCache(true);
            Assertions.assertEquals(EXPECTED_ROWS, execute("SHOW CHARSET WHERE TRUE").getResultRows());
            connectContext.getSessionVariable().charsetServer = "test_charset";
            connectContext.getSessionVariable().collationConnection = "test_collation";
            List<List<String>> expected = execute("SHOW CHARSET").getResultRows();
            Assertions.assertEquals(expected, execute("SHOW CHARSET LIKE 'test_%'").getResultRows());
            Assertions.assertEquals(expected, execute("SHOW CHARSET WHERE TRUE").getResultRows());
            Assertions.assertTrue(connectContext.getSessionVariable().isEnableSqlCache());
            Assertions.assertEquals(expected, execute("SHOW CHARSET WHERE Charset = 'test_charset' "
                    + "AND `Default collation` = 'test_collation'").getResultRows());
            Assertions.assertTrue(execute("SHOW CHARSET WHERE Charset = 'utf8mb4'").getResultRows().isEmpty());
        } finally {
            connectContext.getSessionVariable().setEnableSqlCache(originalEnableSqlCache);
            connectContext.getSessionVariable().charsetServer = originalCharset;
            connectContext.getSessionVariable().collationConnection = originalCollation;
        }
    }

    @Test
    void testWherePreservesDatabaseAndCatalogNamespace() throws Exception {
        String originalCatalog = connectContext.getDefaultCatalog();
        String originalDb = connectContext.getDatabase();
        try {
            connectContext.changeDefaultCatalog("internal");
            connectContext.setDatabase("test_namespace_db");
            Assertions.assertEquals(EXPECTED_ROWS,
                    execute("SHOW CHARSET WHERE DATABASE() = 'test_namespace_db' AND CURRENT_CATALOG() = 'internal'")
                            .getResultRows());
            Assertions.assertTrue(
                    execute("SHOW CHARSET WHERE DATABASE() = 'other_db'").getResultRows().isEmpty());
        } finally {
            connectContext.changeDefaultCatalog(originalCatalog);
            connectContext.setDatabase(originalDb);
        }
    }

    @Test
    void testInternalQueryUpdatesReturnRowsOnFeShortcut() {
        ConnectContext internalContext = connectContext.createInternalQueryContext();
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(internalContext)) {
            StmtExecutor internalExecutor = new StmtExecutor(internalContext, "SELECT 1");
            Assertions.assertEquals(1, internalExecutor.executeInternalQuery().size());
            Assertions.assertEquals(1L, internalContext.getReturnRows());
        }
    }
}
