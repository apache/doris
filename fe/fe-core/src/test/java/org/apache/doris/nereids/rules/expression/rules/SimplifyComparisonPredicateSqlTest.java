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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SimplifyComparisonPredicateSqlTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
        createTables(
                "CREATE TABLE IF NOT EXISTS `log_items_test` (\n"
                        + "            a DATETIME(0) NOT NULL,\n"
                        + "            b decimal(10,2)\n"
                        + "            ) ENGINE=OLAP\n"
                        + "            UNIQUE KEY (`a`)\n"
                        + "            DISTRIBUTED BY HASH(`a`) BUCKETS 120\n"
                        + "            PROPERTIES (\n"
                        + "            \"replication_num\" = \"1\",\n"
                        + "            \"in_memory\" = \"false\",\n"
                        + "            \"compression\" = \"LZ4\",\n"
                        + "            \"storage_cooldown_time\" = \"9999-12-31 23:59:59\",\n"
                        + "            \"enable_unique_key_merge_on_write\" = \"true\"\n"
                        + "            );"
        );
    }

    @Test
    void testSql() {
        PlanChecker.from(connectContext)
                .analyze("select * from log_items_test where a < '2023-06-15 23:59:59.999' and b < 111.111;")
                .rewrite()
                .matches(
                    logicalFilter()
                        .when(f -> f.getConjuncts().stream().anyMatch(e -> e.toSql().equals("(a < '2023-06-16 00:00:00')")))
                        .when(f -> f.getConjuncts().stream().anyMatch(e -> e.toSql().equals("(b < 111.12)")))
                );

        PlanChecker.from(connectContext)
                .analyze("select * from log_items_test where a <= '2023-06-15 23:59:59.999' and b <= 111.111;")
                .rewrite()
                .matches(
                        logicalFilter()
                                .when(f -> f.getConjuncts().stream().anyMatch(e -> e.toSql().equals("(a <= '2023-06-16 00:00:00')")))
                                .when(f -> f.getConjuncts().stream().anyMatch(e -> e.toSql().equals("(b <= 111.11)")))
                );

        PlanChecker.from(connectContext)
                .analyze("select * from log_items_test where a = '2023-06-15 23:59:59.999' and b = 111.111;")
                .rewrite()
                .matches(
                        logicalEmptyRelation()
                );

        PlanChecker.from(connectContext)
                .analyze("select * from log_items_test where a > '2023-06-15 23:59:59.999' and b > 111.111;")
                .rewrite()
                .matches(
                        logicalFilter()
                                .when(f -> f.getConjuncts().stream().anyMatch(e -> e.toSql().equals("(a > '2023-06-16 00:00:00')")))
                                .when(f -> f.getConjuncts().stream().anyMatch(e -> e.toSql().equals("(b > 111.11)")))
                );

        PlanChecker.from(connectContext)
                .analyze("select * from log_items_test where a >= '2023-06-15 23:59:59.999' and b >= 111.111;")
                .rewrite()
                .matches(
                        logicalFilter()
                                .when(f -> f.getConjuncts().stream().anyMatch(e -> e.toSql().equals("(a >= '2023-06-16 00:00:00')")))
                                .when(f -> f.getConjuncts().stream().anyMatch(e -> e.toSql().equals("(b >= 111.12)")))
                );
    }

    @Test
    void dateLikeOverflow() {
        PlanChecker.from(connectContext)
                .analyze("select CAST('2021-01-32 00:00:00' AS DATETIME(6))")
                .rewrite()
                .matches(
                        logicalResultSink(
                                logicalOneRowRelation().when(p -> p.getProjects().get(0).child(0).equals(new NullLiteral(DateTimeV2Type.of(6))))
                        )
                );

        PlanChecker.from(connectContext)
                .analyze("select CONVERT_TZ('2021-01-32 00:00:00', '+08:00', 'America/London') = '2021-01-30'")
                .rewrite()
                .matches(
                        logicalResultSink(
                                logicalOneRowRelation().when(p -> p.getProjects().get(0).child(0) instanceof NullLiteral)
                        )
                );

        PlanChecker.from(connectContext)
                .analyze("select CONVERT_TZ('2021-01-32 00:00:00', '+08:00', 'America/London')")
                .rewrite()
                .matches(
                        logicalResultSink(
                                logicalOneRowRelation().when(p -> p.getProjects().get(0).child(0) instanceof NullLiteral)
                        )
                );

        PlanChecker.from(connectContext)
                .analyze("select CONVERT_TZ('2021-01-32 00:00:00.0000001', '+08:00', 'America/London')")
                .rewrite()
                .matches(
                        logicalResultSink(
                                logicalOneRowRelation().when(p -> p.getProjects().get(0).child(0) instanceof NullLiteral)
                        )
                );

        PlanChecker.from(connectContext)
                .analyze("select CONVERT_TZ('2021-01-32 00:00:00.001', '+08:00', 'America/London') = '2021-01-30'")
                .rewrite()
                .matches(
                        logicalResultSink(
                                logicalOneRowRelation().when(p -> p.getProjects().get(0).child(0) instanceof NullLiteral)
                        )
                );

        Assertions.assertThrows(AnalysisException.class, () -> PlanChecker.from(connectContext)
                .analyze("select CAST('2021-01-32 00:00:00' AS DATETIME(6)) = '2021-01-32 00:00:00'")
                .rewrite()
        );
        Assertions.assertThrows(AnalysisException.class, () -> PlanChecker.from(connectContext)
                .analyze("select CAST('2021-01-32 00:00:00' AS DATETIME(6)) = '2021-01-32 23:00:00'")
                .rewrite()
        );
        Assertions.assertThrows(AnalysisException.class, () -> PlanChecker.from(connectContext)
                .analyze("select CAST('2021-01-32 00:00:00' AS DATETIME(6)) = '1000'")
                .rewrite()
        );
    }
}
