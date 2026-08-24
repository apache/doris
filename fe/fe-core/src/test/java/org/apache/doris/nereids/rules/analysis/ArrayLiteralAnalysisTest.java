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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Test;

/**
 * Tests that bracket array literals validate constantness of their items after
 * function binding, since unbound function calls look constant at parse time.
 */
public class ArrayLiteralAnalysisTest {

    @Test
    public void testArrayLiteralWithVolatileFunctionFails() {
        // [random()] looks constant at parse time because an unbound function defaults to
        // deterministic, but binding turns it into a volatile function, so the bracket array
        // literal must be rejected during analysis
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "constant", () -> {
            PlanChecker.from(connectContext).analyze("SELECT [random()]");
        });
    }

    @Test
    public void testArrayLiteralWithAggregateFunctionFails() {
        // [sum(1)] looks constant at parse time because an unbound function is neither an
        // aggregate nor a table generating function, but binding turns it into an aggregate,
        // so the bracket array literal must be rejected during analysis
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "constant", () -> {
            PlanChecker.from(connectContext).analyze("SELECT [sum(1)]");
        });
    }

    @Test
    public void testArrayLiteralWithColumnReferenceFails() {
        // a column reference is not constant and must be rejected during analysis as well
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "constant", () -> {
            PlanChecker.from(connectContext).analyze(
                    "SELECT [id] FROM (SELECT 1 AS id) t");
        });
    }

    @Test
    public void testArrayLiteralWithConstantExpressionAnalyzes() {
        // genuinely constant expressions (arithmetic, cast) stay constant after binding,
        // so the bracket array literal is lowered to the array function and analyzed fine
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        PlanChecker.from(connectContext).analyze("SELECT [1 + 2, 3 + 4]");
    }
}
