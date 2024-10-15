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

import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Test;

public class InferTest extends SqlTestBase {
    @Test
    void testInferNotNullAndInferPredicates() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        // Test InferNotNull, EliminateOuter, InferPredicate together
        String sql = "select * from T1 left outer join T2 on T1.id = T2.id where T2.id = 4";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        innerLogicalJoin(
                            logicalFilter().when(f -> f.getPredicate().toString().equals("(id#0 = 4)")),
                            logicalFilter().when(f -> f.getPredicate().toString().equals("(id#2 = 4)"))
                        )
                );
    }

    @Test
    void testInferNotNullFromFilterAndEliminateOuter2() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql
                = "select * from T1 right outer join T2 on T1.id = T2.id where T1.id = 4 OR (T1.id > 4 AND T2.score IS NULL)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .printlnTree()
                .matches(
                    innerLogicalJoin(
                        logicalOlapScan(),
                        logicalFilter().when(
                                f -> f.getPredicate().toString().equals("((id#0 = 4) OR (id#0 > 4))"))
                    )

                );
    }

    @Test
    void testInferNotNullFromFilterAndEliminateOuter3() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql
                = "select * from T1 full outer join T2 on T1.id = T2.id where T1.id = 4 OR (T1.id > 4 AND T2.score IS NULL)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalFilter(
                            leftOuterLogicalJoin(
                                logicalFilter().when(
                                        f -> f.getPredicate().toString().equals("((id#0 = 4) OR (id#0 > 4))")),
                                logicalOlapScan()
                            )
                        ).when(f -> f.getPredicate().toString()
                                .equals("((id#0 = 4) OR ((id#0 > 4) AND score#3 IS NULL))"))
                );
    }

    @Test
    void testInferNotNullFromJoinAndEliminateOuter() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        // Is Not Null will infer from semi join, so right outer join can be eliminated.
        String sql
                = "select * from (select T1.id from T1 right outer join T2 on T1.id = T2.id) T1 left semi join T3 on T1.id = T3.id";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        innerLogicalJoin(
                                logicalProject(),
                                logicalProject(leftSemiLogicalJoin())
                        )
                );
    }

    @Test
    void aggEliminateOuterJoin() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select count(T2.score) from T1 left Join T2 on T1.id = T2.id";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalAggregate(
                               logicalProject(
                                       innerLogicalJoin()
                               )
                        )
                );
    }
}
