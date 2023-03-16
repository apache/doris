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

import org.apache.doris.nereids.rules.exploration.join.InnerJoinLAsscom;
import org.apache.doris.nereids.rules.exploration.join.JoinCommute;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Paper: Measuring the Complexity of Join Enumeration in Query Optimization
 * <pre>
 * Star query without products
 * Join tree Number：
 *   left-deep: 2(n-1)! * 1
 *   zig-zag: (n-1)! * 2^(n-1)
 *   bushy: star graph can't be a bushy, it can only form a zig-zag (because the center must be joined first)
 * </pre>
 */
public class CascadesJoinReorderTest extends SqlTestBase {
    @Test
    void testStartThreeJoin() {
        // Three join
        // (n-1)! * 2^(n-1) = 8
        String sql = "SELECT * FROM T1 "
                + "JOIN T2 ON T1.id = T2.id "
                + "JOIN T3 ON T1.id = T3.id";

        int plansNumber = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .applyExploration(JoinCommute.ZIG_ZAG.build())
                .applyExploration(InnerJoinLAsscom.INSTANCE.build())
                .applyExploration(JoinCommute.ZIG_ZAG.build())
                .plansNumber();

        Assertions.assertEquals(8, plansNumber);
    }

    @Test
    void testStarFourJoin() {
        // Four join
        // (n-1)! * 2^(n-1) = 48
        String sql = "SELECT * FROM T1 "
                + "JOIN T2 ON T1.id = T2.id "
                + "JOIN T3 ON T1.id = T3.id "
                + "JOIN T4 ON T1.id = T4.id ";

        int plansNumber = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .applyExploration(JoinCommute.ZIG_ZAG.build())
                .applyExploration(InnerJoinLAsscom.INSTANCE.build())
                .applyExploration(JoinCommute.ZIG_ZAG.build())
                .applyExploration(InnerJoinLAsscom.INSTANCE.build())
                .applyExploration(JoinCommute.ZIG_ZAG.build())
                .applyExploration(InnerJoinLAsscom.INSTANCE.build())
                .applyExploration(JoinCommute.ZIG_ZAG.build())
                .applyExploration(InnerJoinLAsscom.INSTANCE.build())
                .plansNumber();

        Assertions.assertEquals(48, plansNumber);
    }
}
