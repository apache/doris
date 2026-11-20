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
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class MTMVPartitionReanalysisTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("partition_reanalysis");
        connectContext.setDatabase("partition_reanalysis");
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
}
