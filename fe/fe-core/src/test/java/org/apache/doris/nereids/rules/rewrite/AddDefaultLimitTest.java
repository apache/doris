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

import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import org.junit.jupiter.api.Test;

class AddDefaultLimitTest implements MemoPatternMatchSupported {

    @Test
    void testSqlSelectLimitOnCteResult() throws Exception {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        VariableMgr.setVar(connectContext.getSessionVariable(),
                new SetVar(SessionVariable.SQL_SELECT_LIMIT, new StringLiteral("1")));

        String sql = "WITH cte AS (SELECT 1 AS k) "
                + "SELECT k FROM cte UNION ALL SELECT k FROM cte";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .setIsQuery()
                .applyCustom(new AddDefaultLimit())
                .matchesFromRoot(logicalResultSink(
                        logicalCTEAnchor(
                                logicalCTEProducer(),
                                logicalLimit().when(limit -> limit.getLimit() == 1
                                        && limit.getOffset() == 0
                                        && limit.getPhase() == LimitPhase.ORIGIN))))
                .nonMatch(logicalCTEProducer(logicalLimit()));
    }
}
