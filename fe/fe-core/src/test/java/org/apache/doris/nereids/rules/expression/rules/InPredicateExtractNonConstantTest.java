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

import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Test;

class InPredicateExtractNonConstantTest extends SqlTestBase {
    @Test
    public void testExtractNonConstant() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select * from T1 where id in (score, score, score + 100)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalFilter().when(f -> f.getPredicate().toString().equals(
                                "OR[(id#0 = score#1),(id#0 = (score#1 + 100))]"
                        )));

        sql = "select * from T1 where id in (score,  score + 10, score + score, score, 10, 20, 30, 100 + 200)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalFilter().when(f -> f.getPredicate().toString().equals(
                                "OR[id#0 IN (10, 20, 30, 300),(id#0 = score#1),(id#0 = (score#1 + 10)),(id#0 = (score#1 + score#1))]"
                )));
    }
}
