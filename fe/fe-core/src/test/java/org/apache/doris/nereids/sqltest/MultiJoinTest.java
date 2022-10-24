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
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

public class MultiJoinTest extends SqlTestBase {
    @Test
    void testMultiJoinEliminateCross() {
        List<String> sqls = ImmutableList.<String>builder()
                .add("SELECT * FROM T2 LEFT JOIN T3 ON T2.id = T3.id, T1 WHERE T1.id = T2.id")
                .add("SELECT * FROM T2 LEFT JOIN T3 ON T2.id = T3.id, T1 WHERE T1.id = T2.id AND T1.score > 0")
                .add("SELECT * FROM T2 LEFT JOIN T3 ON T2.id = T3.id, T1 WHERE T1.id = T2.id AND T1.score > 0 AND T1.id + T2.id + T3.id > 0")
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
    @Disabled
    // TODO: MultiJoin And EliminateOuter
    void testEliminateBelowOuter() {
        String sql = "SELECT * FROM T1, T2 LEFT JOIN T3 ON T2.id = T3.id WHERE T1.id = T2.id";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new ReorderJoin())
                .printlnTree();
    }

    @Test
    void testPushdownAndEliminateOuter() {
        String sql = "SELECT * FROM T1 LEFT JOIN T2 ON T1.id = T2.id WHERE T2.score > 0";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .printlnTree()
                .matches(
                        logicalJoin().when(join -> join.getJoinType().isInnerJoin())
                );

        String sql1 = "SELECT * FROM T1, T2 LEFT JOIN T3 ON T2.id = T3.id WHERE T1.id = T2.id AND T3.score > 0";
        PlanChecker.from(connectContext)
                .analyze(sql1)
                .rewrite()
                .printlnTree()
                .matches(
                        logicalJoin(
                                logicalJoin().when(join -> join.getJoinType().isInnerJoin()),
                                any()
                        ).when(join -> join.getJoinType().isInnerJoin())
                );
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
