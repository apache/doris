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

package org.apache.doris.nereids.sqltest;

import org.apache.doris.nereids.rules.rewrite.logical.ReorderJoin;
import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

public class MultiJoinTest extends TestWithFeService implements PatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");

        createTables(
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T2 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T3 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
    }

    @Override
    protected void runBeforeEach() throws Exception {
        NamedExpressionUtil.clear();
    }

    @Test
    void testMultiJoinEliminateCross() {
        List<String> sqls = ImmutableList.<String>builder()
                .add("SELECT * FROM T1, T2 LEFT JOIN T3 ON T2.id = T3.id WHERE T1.id = T2.id")
                .add("SELECT * FROM T2 LEFT JOIN T3 ON T2.id = T3.id, T1 WHERE T1.id = T2.id")
                .build();

        for (String sql : sqls) {
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .applyBottomUp(new ReorderJoin())
                    .matches(
                            logicalJoin(
                                    logicalJoin().whenNot(join -> join.getJoinType().isCrossJoin()),
                                    leafPlan()
                            ).whenNot(join -> join.getJoinType().isCrossJoin())
                    )
                    .printlnTree();
        }
    }

    @Test
    void testMultiJoinExistCross() {
        List<String> sqls = ImmutableList.<String>builder()
                .add("SELECT * FROM T2 LEFT SEMI JOIN T3 ON T2.id = T3.id, T1 WHERE T1.id > T2.id")
                .build();

        for (String sql : sqls) {
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .applyBottomUp(new ReorderJoin())
                    .matches(
                            logicalJoin(
                                    logicalJoin().whenNot(join -> join.getJoinType().isCrossJoin()),
                                    leafPlan()
                            ).when(join -> join.getJoinType().isCrossJoin())
                                    .whenNot(join -> join.getOtherJoinConjuncts().isEmpty())
                    )
                    .printlnTree();
        }
    }
}
