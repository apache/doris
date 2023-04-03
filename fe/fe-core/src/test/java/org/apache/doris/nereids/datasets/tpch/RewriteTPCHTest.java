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

package org.apache.doris.nereids.datasets.tpch;

import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Test;

public class RewriteTPCHTest extends TPCHTestBase {
    @Test
    void testQ1() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q1)
                .rewrite();
    }

    @Test
    void testQ2() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q2)
                .rewrite();
    }

    @Test
    void testQ3() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q3)
                .rewrite();
    }

    @Test
    void testQ4() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q4)
                .rewrite();
    }

    @Test
    void testQ5() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q5)
                .rewrite();
    }

    @Test
    void testQ6() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q6)
                .rewrite();
    }

    @Test
    void testQ7() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q7)
                .rewrite();
    }

    @Test
    void testQ8() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q8)
                .rewrite();
    }

    @Test
    void testQ9() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q9)
                .rewrite();
    }

    @Test
    void testQ10() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q10)
                .rewrite();
    }

    @Test
    void testQ11() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q11)
                .rewrite();
    }

    @Test
    void testQ12() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q12)
                .rewrite();
    }

    @Test
    void testQ13() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q13)
                .rewrite();
    }

    @Test
    void testQ14() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q14)
                .rewrite();
    }

    @Test
    void testQ15() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q15)
                .rewrite();
    }

    @Test
    void testQ16() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q16)
                .rewrite();
    }

    @Test
    void testQ17() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q17)
                .rewrite();
    }

    @Test
    void testQ18() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q18)
                .rewrite();
    }

    @Test
    void testQ19() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q19)
                .rewrite();
    }

    @Test
    void testQ20() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q20)
                .rewrite();
    }

    @Test
    void testQ21() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q21)
                .rewrite();
    }

    @Test
    void testQ22() {
        PlanChecker.from(connectContext)
                .analyze(TPCHUtils.Q22)
                .rewrite();
    }
}
