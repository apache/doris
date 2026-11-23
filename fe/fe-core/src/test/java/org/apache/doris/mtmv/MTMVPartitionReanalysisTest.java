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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.CreateMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.MTMVPartitionDefinition;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class MTMVPartitionReanalysisTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("partition_reanalysis");
        connectContext.setDatabase("partition_reanalysis");
    }

    @Override
    protected void runBeforeEach() {
        createStatementCtx("");
        connectContext.setThreadLocalInfo();
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testReanalyzePartitionAlias(boolean union, boolean explicitExpression) throws Exception {
        String suffix = union + "_" + explicitExpression;
        String name = "mv_alias_" + suffix;
        for (int i = 1; i <= 2; i++) {
            createTable("CREATE TABLE source" + i + "_" + suffix
                    + " (dt" + i + " DATETIME NOT NULL, v VARCHAR(10)) "
                    + "DUPLICATE KEY(dt" + i + ") PARTITION BY RANGE(dt" + i + ") "
                    + "(PARTITION p1 VALUES [('2024-01-01'), ('2024-01-02'))) "
                    + "DISTRIBUTED BY HASH(dt" + i + ") BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        }
        String query = "SELECT date_trunc(dt1, 'day') AS trx_date, v FROM source1_" + suffix;
        if (union) {
            query += " UNION ALL SELECT date_trunc(dt2, 'day') AS trx_date, v FROM source2_" + suffix;
            query = "SELECT trx_date, v, count(*) AS cnt FROM (" + query + ") t GROUP BY trx_date, v";
        }
        createMvByNereids("CREATE MATERIALIZED VIEW " + name
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL PARTITION BY ("
                + (explicitExpression ? "date_trunc(trx_date, 'day')" : "trx_date") + ") "
                + "DISTRIBUTED BY RANDOM BUCKETS 1 PROPERTIES ('replication_num' = '1') AS " + query);
        MTMV mv = (MTMV) Env.getCurrentInternalCatalog().getDbOrAnalysisException("partition_reanalysis")
                .getTableOrAnalysisException(name);
        MTMVPartitionInfo original = mv.getMvPartitionInfo();
        Assertions.assertEquals(MTMVPartitionType.EXPR, original.getPartitionType());
        Assertions.assertEquals("trx_date", original.getPartitionCol());
        String storedExpression = original.getExpr().accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE);
        Assertions.assertFalse(storedExpression.contains("trx_date"), storedExpression);

        alterTableSync("ALTER TABLE source1_" + suffix + " MODIFY COLUMN v VARCHAR(30)");
        if (union) {
            alterTableSync("ALTER TABLE source2_" + suffix + " MODIFY COLUMN v VARCHAR(30)");
        }

        // Exercise the same compatibility checks used when refreshing an existing MV after schema change.
        MTMVAnalyzeQueryInfo analyzed = MTMVPlanUtil.ensureMTMVQueryUsable(mv,
                MTMVPlanUtil.createMTMVContext(mv, MTMVPlanUtil.DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE));
        Assertions.assertEquals(original.getPartitionCol(), analyzed.getMvPartitionInfo().getPartitionCol());
        Assertions.assertEquals(original.getPctInfos().size(), analyzed.getMvPartitionInfo().getPctInfos().size());
        Assertions.assertTrue(analyzed.getMvPartitionInfo().getPctInfos().containsAll(original.getPctInfos()));
        Assertions.assertEquals(union ? 2 : 1, original.getPctInfos().size());
        Assertions.assertEquals(storedExpression,
                original.getExpr().accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
    }

    @ParameterizedTest
    @CsvSource({"week,q,true", "quarter,q,true", "day,q`day,true",
            "week,q,false", "quarter,q,false", "day,q`day,false"})
    void testReanalyzePartitionBoundary(String timeUnit, String alias, boolean projectBaseColumn) throws Exception {
        String suffix = timeUnit + "_" + alias.replace(".", "_dot_").replace("`", "_tick_")
                + "_" + projectBaseColumn;
        String table = "source_boundary_" + suffix;
        String name = "mv_boundary_" + suffix;
        String quotedAlias = "`" + alias.replace("`", "``") + "`";
        createTable("CREATE TABLE " + table + " (dt DATETIME NOT NULL, v VARCHAR(10)) "
                + "DUPLICATE KEY(dt) PARTITION BY RANGE(dt) "
                + "(PARTITION p1 VALUES [('2024-01-01'), ('2024-01-02'))) "
                + "DISTRIBUTED BY HASH(dt) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        createMvByNereids("CREATE MATERIALIZED VIEW " + name
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL PARTITION BY (" + quotedAlias + ") "
                + "DISTRIBUTED BY RANDOM BUCKETS 1 PROPERTIES ('replication_num' = '1') AS SELECT "
                + (projectBaseColumn ? "dt, " : "") + "date_trunc(dt, '" + timeUnit + "') AS "
                + quotedAlias + ", v FROM " + table);
        MTMV mv = (MTMV) Env.getCurrentInternalCatalog().getDbOrAnalysisException("partition_reanalysis")
                .getTableOrAnalysisException(name);
        MTMVPartitionInfo original = mv.getMvPartitionInfo();
        Assertions.assertEquals(alias, original.getPartitionCol());
        String storedExpression = original.getExpr().accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE);

        alterTableSync("ALTER TABLE " + table + " MODIFY COLUMN v VARCHAR(30)");
        MTMVAnalyzeQueryInfo analyzed = MTMVPlanUtil.ensureMTMVQueryUsable(mv,
                MTMVPlanUtil.createMTMVContext(mv, MTMVPlanUtil.DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE));
        Assertions.assertEquals(alias, analyzed.getMvPartitionInfo().getPartitionCol());
        Assertions.assertEquals(original.getPctInfos(), analyzed.getMvPartitionInfo().getPctInfos());
        Assertions.assertEquals(storedExpression,
                original.getExpr().accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE));
    }

    @ParameterizedTest
    @ValueSource(strings = {"q.day", "q`day", "q.day`value"})
    void testQuotedPartitionDefinition(String alias) throws Exception {
        String table = "quoted_source_" + alias.replace(".", "_dot_").replace("`", "_tick_");
        createTable("CREATE TABLE " + table + " (dt DATETIME NOT NULL, v VARCHAR(10)) "
                + "DUPLICATE KEY(dt) PARTITION BY RANGE(dt) "
                + "(PARTITION p1 VALUES [('2024-01-01'), ('2024-01-02'))) "
                + "DISTRIBUTED BY HASH(dt) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        MTMVPartitionInfo stored = new MTMVPartitionInfo(MTMVPartitionType.EXPR);
        stored.setPartitionCol(alias);
        stored.setExpr(new FunctionCallExpr("date_trunc",
                ImmutableList.of(new SlotRef(null, "dt"), new StringLiteral("day")), false));
        MTMVPartitionDefinition definition = new MTMVPartitionDefinition();
        definition.setPartitionType(stored.getPartitionType());
        definition.setFunctionCallExpression(new NereidsParser().parseExpression(
                MTMVPartitionExprFactory.getExprService(stored.getExpr()).toSql(stored)));
        // Exercise the reconstruction independently of physical table partition-description conversion.
        String previousRules = String.join(",", connectContext.getSessionVariable().getDisableNereidsRuleNames());
        connectContext.getSessionVariable().setDisableNereidsRules(CreateMTMVInfo.MTMV_PLANER_DISABLE_RULES);
        try {
            PlanChecker.from(connectContext).checkExplain("SELECT dt, date_trunc(dt, 'day') AS `"
                    + alias.replace("`", "``") + "`, v FROM " + table, planner -> {
                        MTMVPartitionInfo analyzed = definition.analyzeAndTransferToMTMVPartitionInfo(planner);
                        Assertions.assertEquals(alias, analyzed.getPartitionCol());
                        Assertions.assertEquals(1, analyzed.getPctInfos().size());
                        Assertions.assertEquals("dt", analyzed.getPctInfos().get(0).getColName());
                    });
        } finally {
            connectContext.getSessionVariable().setDisableNereidsRules(previousRules);
        }
    }
}
