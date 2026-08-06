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
 * Under only_full_group_by, a non-grouped, non-aggregated output column that is constant for every input
 * row (uniform and not null) is accepted (MySQL functional dependency): NormalizeAggregate adds it to the
 * group-by keys. Non-constant columns, and uniform-but-nullable columns from the nullable side of an outer
 * join, are still rejected. All cases are verified through rewrite() (not only analyze()).
 */
public class OnlyFullGroupByConstantTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        // columns 'a','b' deliberately collide with the output aliases used below
        createTable("CREATE TABLE test.t_ab (a INT, b INT) ENGINE=OLAP\n"
                + "DUPLICATE KEY(a) DISTRIBUTED BY HASH(a) BUCKETS 1 PROPERTIES ('replication_num' = '1');");
        createTable("CREATE TABLE test.t_v (k INT, v INT) ENGINE=OLAP\n"
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1');");
    }

    private void rewriteOk(String sql) {
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan(),
                "should plan without error: " + sql);
    }

    private void rejected(String sql) {
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql).rewrite(), "should be rejected: " + sql);
        Assertions.assertTrue(ex.getMessage().contains("must appear in the GROUP BY"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void aliasCollidesWithConstantColumn() {
        // alias 'b' collides with the derived-table column 'b', so column-first binding groups by column 'b'
        // and leaves the constant column 'a' ungrouped. 'a' is constant -> allowed.
        rewriteOk("SELECT a as b, b as c FROM (SELECT 1 as a, 2 as b) t1 GROUP BY b, c");
    }

    @Test
    public void constantColumnNotInGroupBy() {
        // 'b' is constant and not grouped (a bare output column) -> allowed
        rewriteOk("SELECT a, b FROM (SELECT 1 as a, 2 as b) t1 GROUP BY a");
    }

    @Test
    public void constantGroupByLiteral() {
        // group-by key is itself a constant and gets eliminated; constant outputs a, b stay valid
        rewriteOk("SELECT a, b FROM (SELECT 1 as a, 2 as b) t1 GROUP BY 'g'");
    }

    @Test
    public void nonConstantColumnStillRejected() {
        // same shape over a real table: 'a' is non-constant and ungrouped -> still rejected (MySQL parity)
        rejected("SELECT a as b, b as c FROM test.t_ab GROUP BY b, c");
    }

    @Test
    public void outerJoinNullableUniformRejected() {
        // r.v is uniform (LIMIT 1) but propagated through the nullable side of a LEFT JOIN, so it is the
        // uniform value on matched rows and NULL on unmatched rows of the same group -> must stay rejected.
        rejected("SELECT r.v FROM (SELECT 1 AS g, 1 AS k UNION ALL SELECT 1 AS g, 2 AS k) l "
                + "LEFT JOIN (SELECT k, v FROM test.t_v LIMIT 1) r ON l.k = r.k GROUP BY l.g");
    }

    @Test
    public void mixedReportOnlyNonConstant() {
        // group by b2; 'k' (constant 1) is allowed, 'a' (non-constant) must be reported and 'k' must not.
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT x.a, x.k, x.b2 FROM (SELECT a, 1 as k, b as b2 FROM test.t_ab) x GROUP BY x.b2")
                        .rewrite());
        Assertions.assertTrue(ex.getMessage().contains("'a'"), "should report 'a': " + ex.getMessage());
        Assertions.assertFalse(ex.getMessage().contains("'k'"), "should NOT report constant 'k': " + ex.getMessage());
    }
}
