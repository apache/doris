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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Under only_full_group_by, a non-aggregated select expression that is constant for every input row
 * (a uniform slot) is allowed, matching MySQL functional-dependency behavior. Non-constant
 * non-aggregated expressions are still rejected.
 */
public class OnlyFullGroupByConstantTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        // columns 'a','b' deliberately collide with the output aliases used below
        createTable("CREATE TABLE test.t_ab (a INT, b INT) ENGINE=OLAP\n"
                + "DUPLICATE KEY(a) DISTRIBUTED BY HASH(a) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1');");
    }

    @Test
    public void aliasCollidesWithConstantColumn() {
        // alias 'b' collides with the derived-table column 'b', so column-first binding groups by the
        // column 'b' and leaves the constant column 'a' ungrouped. 'a' is constant -> allowed.
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext)
                .analyze("SELECT a as b, b as c FROM (SELECT 1 as a, 2 as b) t1 GROUP BY b, c"));
    }

    @Test
    public void constantColumnNotInGroupBy() {
        // 'b' is constant and not grouped -> allowed even though it is neither grouped nor aggregated
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext)
                .analyze("SELECT a, b FROM (SELECT 1 as a, 2 as b) t1 GROUP BY a"));
    }

    @Test
    public void nonConstantColumnStillRejected() {
        // same shape over a real table: 'a' is non-constant and ungrouped -> still rejected (MySQL parity)
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze("SELECT a as b, b as c FROM test.t_ab GROUP BY b, c"));
        Assertions.assertTrue(ex.getMessage().contains("must appear in the GROUP BY"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void mixedMissingSlotsReportOnlyNonConstant() {
        // group by b2; both 'k' (constant 1) and 'a' (non-constant) are ungrouped. Only 'a' must be
        // reported; the constant 'k' is allowed.
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT x.a, x.k, x.b2 FROM (SELECT a, 1 as k, b as b2 FROM test.t_ab) x GROUP BY x.b2"));
        Assertions.assertTrue(ex.getMessage().contains("'a'"), "should report 'a': " + ex.getMessage());
        Assertions.assertFalse(ex.getMessage().contains("'k'"), "should NOT report constant 'k': " + ex.getMessage());
    }
}
