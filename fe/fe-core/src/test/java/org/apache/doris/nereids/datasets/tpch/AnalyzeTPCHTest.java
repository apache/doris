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

import org.junit.jupiter.api.Test;

/**
 * There are still many functions that have not been implemented,
 * so the tpch cannot be fully parsed, and the interface is only reserved here.
 * When the related functions of tpch are supported, the comments will be deleted and the analyze of tpch will be verified.
 */
public class AnalyzeTPCHTest extends TPCHTestBase {

    @Test
    public void q1() {
        checkAnalyze(TPCHUtils.Q1);
    }

    @Test
    public void q2() {
        checkAnalyze(TPCHUtils.Q2);
    }

    @Test
    public void q2_rewrite() {
        checkAnalyze(TPCHUtils.Q2_rewrite);
    }

    @Test
    public void q3() {
        checkAnalyze(TPCHUtils.Q3);
    }

    @Test
    public void q3_rewrite() {
        checkAnalyze(TPCHUtils.Q3_rewrite);
    }

    @Test
    public void q4() {
        checkAnalyze(TPCHUtils.Q4);
    }

    @Test
    public void q4_rewrite() {
        checkAnalyze(TPCHUtils.Q4_rewrite);
    }

    @Test
    public void q5() {
        checkAnalyze(TPCHUtils.Q5);
    }

    @Test
    public void q6() {
        checkAnalyze(TPCHUtils.Q6);
    }

    @Test
    public void q7() {
        checkAnalyze(TPCHUtils.Q7);
    }

    @Test
    public void q8() {
        checkAnalyze(TPCHUtils.Q8);
    }

    @Test
    public void q9() {
        checkAnalyze(TPCHUtils.Q9);
    }

    @Test
    public void q10() {
        checkAnalyze(TPCHUtils.Q10);
    }

    @Test
    public void q11() {
        checkAnalyze(TPCHUtils.Q11);
    }

    @Test
    public void q12() {
        checkAnalyze(TPCHUtils.Q12);
    }

    @Test
    public void q12_rewrite() {
        checkAnalyze(TPCHUtils.Q12_rewrite);
    }

    @Test
    public void q13() {
        checkAnalyze(TPCHUtils.Q13);
    }

    @Test
    public void q14() {
        checkAnalyze(TPCHUtils.Q14);
    }

    @Test
    public void q14_rewrite() {
        checkAnalyze(TPCHUtils.Q14_rewrite);
    }

    @Test
    public void q15() {
        checkAnalyze(TPCHUtils.Q15);
    }

    @Test
    public void q15_rewrite() {
        checkAnalyze(TPCHUtils.Q15_rewrite);
    }

    @Test
    public void q16() {
        checkAnalyze(TPCHUtils.Q16);
    }

    @Test
    public void q17() {
        checkAnalyze(TPCHUtils.Q17);
    }

    // TODO: support [broadcast] hint
    /*@Test
    public void q17_rewrite() {
        checkAnalyze(TPCHUtils.Q17_rewrite);
    }*/

    @Test
    public void q18() {
        checkAnalyze(TPCHUtils.Q18);
    }

    @Test
    public void q18_rewrite() {
        checkAnalyze(TPCHUtils.Q18_rewrite);
    }

    @Test
    public void q19() {
        checkAnalyze(TPCHUtils.Q19);
    }

    @Test
    public void q20() {
        checkAnalyze(TPCHUtils.Q20);
    }

    @Test
    public void q20_rewrite() {
        checkAnalyze(TPCHUtils.Q20_rewrite);
    }

    @Test
    public void q21() {
        checkAnalyze(TPCHUtils.Q21);
    }

    @Test
    public void q21_rewrite() {
        checkAnalyze(TPCHUtils.Q21_rewrite);
    }

    @Test
    public void q22() {
        checkAnalyze(TPCHUtils.Q22);
    }

    @Test
    public void q22_rewrite() {
        checkAnalyze(TPCHUtils.Q22_rewrite);
    }
}
