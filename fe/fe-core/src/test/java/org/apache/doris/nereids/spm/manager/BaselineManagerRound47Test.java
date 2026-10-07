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

package org.apache.doris.nereids.spm.manager;

import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.BaselineStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * The forwarded-CREATE confirmation identity carries BOTH digests: the bind
 * digest identifies the binding, but two baselines may share it while carrying
 * different plan texts (one baseline per plan), so a row of the OTHER plan must
 * not confirm this statement. A missing plan digest (a pre-column row / a
 * caller without one) keeps the historical bind-only match.
 */
public class BaselineManagerRound47Test {

    private static BaselinePlan row(long id, String bindDigest, String planDigest) {
        BaselinePlan plan = new BaselinePlan();
        plan.setId(id);
        plan.setBindSqlDigest(bindDigest);
        plan.setPlanSqlDigest(planDigest);
        plan.setStatus(BaselineStatus.ENABLED);
        return plan;
    }

    @Test
    public void testForwardedCreateConfirmationRequiresBothDigests() {
        BaselineManager.ForwardedDdlExpectation expectation = BaselineManager.ForwardedDdlExpectation
                .created("SELECT k FROM t", "SELECT k FROM t", "qid", "bind-a", "plan-a");
        Assertions.assertTrue(expectation.isSatisfiedBy(
                        Map.of(1L, row(1, "bind-a", "plan-a"))),
                "the same bind AND plan digest confirms the statement");
        Assertions.assertFalse(expectation.isSatisfiedBy(
                        Map.of(1L, row(1, "bind-a", "plan-b"))),
                "a row of the same binding but ANOTHER plan must not confirm it");
    }

    @Test
    public void testForwardedCreateConfirmationFallsBackToTheBindDigestAlone() {
        BaselineManager.ForwardedDdlExpectation expectation = BaselineManager.ForwardedDdlExpectation
                .created("SELECT k FROM t", "SELECT k FROM t", "qid", "bind-a");
        Assertions.assertTrue(expectation.isSatisfiedBy(
                        Map.of(1L, row(1, "bind-a", "plan-b"))),
                "without a plan digest the historical bind-only match stays");
        Assertions.assertTrue(expectation.isSatisfiedBy(
                        Map.of(1L, row(1, "bind-a", null))),
                "a pre-column row keeps matching on the bind digest");
    }
}
