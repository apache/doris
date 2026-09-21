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

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
}
