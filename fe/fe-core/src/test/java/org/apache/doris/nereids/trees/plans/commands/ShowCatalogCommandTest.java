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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ShowCatalogCommandTest extends TestWithFeService {
    @Test
    void testWhereClauseIsNotTreatedAsLikePattern() {
        LogicalPlan plan = new NereidsParser().parseSingle(
                "SHOW CATALOGS WHERE CatalogName = 'internal'");

        Assertions.assertInstanceOf(ShowCatalogCommand.class, plan);
        Assertions.assertTrue(plan.toString().startsWith("SHOW CATALOGS WHERE "));
        Assertions.assertFalse(plan.toString().contains(" LIKE "));
    }

    @Test
    void testWhereClauseFiltersCatalogRows() throws Exception {
        String sql = "SHOW CATALOGS WHERE CatalogName = 'internal'";
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        StmtExecutor executor = new StmtExecutor(connectContext, sql);

        ShowResultSet resultSet = ((ShowCatalogCommand) plan).doRun(connectContext, executor);
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        Assertions.assertEquals("internal", resultSet.getResultRows().get(0).get(1));

        String nonMatchingSql = "SHOW CATALOGS WHERE CatalogName = 'missing'";
        LogicalPlan nonMatchingPlan = new NereidsParser().parseSingle(nonMatchingSql);
        StmtExecutor nonMatchingExecutor = new StmtExecutor(connectContext, nonMatchingSql);
        ShowResultSet emptyResultSet = ((ShowCatalogCommand) nonMatchingPlan)
                .doRun(connectContext, nonMatchingExecutor);
        Assertions.assertTrue(emptyResultSet.getResultRows().isEmpty());
    }

    @Test
    void testWhereClauseDoesNotMutateOuterExecutionState() throws Exception {
        String sql = "SHOW CATALOGS WHERE CatalogName = 'internal'";
        ShowCatalogCommand command = (ShowCatalogCommand) new NereidsParser().parseSingle(sql);
        StmtExecutor executor = new StmtExecutor(connectContext, sql);
        TUniqueId queryId = new TUniqueId(123, 456);
        connectContext.setQueryId(queryId);
        connectContext.getState().setInternal(false);

        command.doRun(connectContext, executor);

        Assertions.assertEquals(queryId, connectContext.queryId());
        Assertions.assertFalse(connectContext.getState().isInternal());
        Assertions.assertNull(executor.getParsedStmt());
    }

    @Test
    void testWhereClauseIsAnalyzedForEmptyCatalogRows() throws Exception {
        String sql = "SHOW CATALOGS WHERE MissingColumn = 'value'";

        Assertions.assertThrows(AnalysisException.class,
                () -> executeWithCatalogRows(sql, Collections.emptyList()));
    }

    @Test
    void testWhereClausePreservesCatalogManagerOrdering() throws Exception {
        String supplementaryName = "catalog_" + Character.toString(0x10400);
        String bmpName = "catalog_" + Character.toString(0xF900);
        List<List<String>> catalogRows = List.of(
                catalogRow("1", supplementaryName, "comment"),
                catalogRow("2", bmpName, "comment"));
        String sql = "SHOW CATALOGS WHERE CatalogName IN ('" + supplementaryName + "', '" + bmpName + "')";

        ShowResultSet resultSet = executeWithCatalogRows(sql, catalogRows);

        Assertions.assertEquals(supplementaryName, resultSet.getResultRows().get(0).get(1));
        Assertions.assertEquals(bmpName, resultSet.getResultRows().get(1).get(1));
    }

    @Test
    void testWhereClauseUsesSqlNullSemantics() throws Exception {
        String defaultTimeSql = "SHOW CATALOGS WHERE CreateTime IS NULL AND LastUpdateTime IS NULL";
        ShowResultSet defaultTimeResult = ((ShowCatalogCommand) new NereidsParser().parseSingle(defaultTimeSql))
                .doRun(connectContext, new StmtExecutor(connectContext, defaultTimeSql));
        Assertions.assertEquals("internal", defaultTimeResult.getResultRows().get(0).get(1));

        List<List<String>> restoredRows = List.of(catalogRow("2", "restored_catalog", null));
        ShowResultSet restoredResult = executeWithCatalogRows(
                "SHOW CATALOGS WHERE Comment IS NULL", restoredRows);
        Assertions.assertEquals("restored_catalog", restoredResult.getResultRows().get(0).get(1));
    }

    private ShowResultSet executeWithCatalogRows(String sql, List<List<String>> catalogRows) throws Exception {
        Env realEnv = Env.getCurrentEnv();
        Env mockedCurrentEnv = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.when(mockedCurrentEnv.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.showCatalogs(Mockito.nullable(String.class), Mockito.nullable(String.class),
                Mockito.nullable(String.class))).thenReturn(catalogRows);
        AtomicInteger calls = new AtomicInteger();

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentEnv)
                    .thenAnswer(invocation -> calls.getAndIncrement() == 0 ? mockedCurrentEnv : realEnv);
            ShowCatalogCommand command = (ShowCatalogCommand) new NereidsParser().parseSingle(sql);
            return command.doRun(connectContext, new StmtExecutor(connectContext, sql));
        }
    }

    private List<String> catalogRow(String id, String name, String comment) {
        return Arrays.asList(id, name, "test", "No", FeConstants.null_string,
                FeConstants.null_string, comment, "");
    }
}
