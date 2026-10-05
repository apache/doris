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

package org.apache.doris.nereids.datasets.ssb;

import org.junit.jupiter.api.Test;

public class AnalyzeSSBTest extends SSBTestBase {
    /**
     * TODO: check bound plan and expression details.
     */
    @Test
    public void q1_1() {
        checkAnalyze(SSBUtils.Q1_1);
    }

    @Test
    public void q1_2() {
        checkAnalyze(SSBUtils.Q1_2);
    }

    @Test
    public void q1_3() {
        checkAnalyze(SSBUtils.Q1_3);
    }

    @Test
    public void q2_1() {
        checkAnalyze(SSBUtils.Q2_1);
    }

    @Test
    public void q2_2() {
        checkAnalyze(SSBUtils.Q2_2);
    }

    @Test
    public void q2_3() {
        checkAnalyze(SSBUtils.Q2_3);
    }

    @Test
    public void q3_1() {
        checkAnalyze(SSBUtils.Q3_1);
    }

    @Test
    public void q3_2() {
        checkAnalyze(SSBUtils.Q3_2);
    }

    @Test
    public void q3_3() {
        checkAnalyze(SSBUtils.Q3_3);
    }

    @Test
    public void q3_4() {
        checkAnalyze(SSBUtils.Q3_4);
    }

    @Test
    public void q4_1() {
        checkAnalyze(SSBUtils.Q4_1);
    }

    @Test
    public void q4_2() {
        checkAnalyze(SSBUtils.Q4_2);
    }

    @Test
    public void q4_3() {
        checkAnalyze(SSBUtils.Q4_3);
    }
}
