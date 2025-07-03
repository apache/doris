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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Test;

public class EliminateConstHashJoinConditionTest extends SqlTestBase {

    @Test
    void testEliminate() {
        CascadesContext c1 = createCascadesContext(
                "select * from T1 join T2 on T1.id = T2.id and T1.id = 1",
                connectContext
        );
        PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .matches(
                        logicalJoin().when(join ->
                                join.getHashJoinConjuncts().isEmpty() && join.getOtherJoinConjuncts().isEmpty())
                );
    }

    @Test
    void testNotEliminateNonInnerJoin() {
        CascadesContext c1 = createCascadesContext(
                "select * from T1 left join T2 on T1.id = T2.id where T1.id = 1",
                connectContext
        );
        PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .matches(
                        logicalJoin().when(join ->
                                !join.getHashJoinConjuncts().isEmpty())
                );
    }

}
