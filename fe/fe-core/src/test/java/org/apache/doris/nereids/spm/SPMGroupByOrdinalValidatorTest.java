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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

/** Structural counterexamples that a replan using only the captured value cannot detect. */
class SPMGroupByOrdinalValidatorTest {
    @BeforeEach
    void setUp() {
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        ctx.setStatementContext(new StatementContext(ctx, null));
        ctx.setThreadLocalInfo();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void testRealGroupingColumnWithReorderedOutputsAndKeys() {
        String frozen = "SELECT q.b AS d, q.a AS c, count(*) AS n FROM"
                + " (SELECT k % _spm_const_var(1) AS a, _spm_const_var(2) AS b FROM t) q GROUP BY q.a, q.b";
        Assertions.assertTrue(preserves(frozen, Set.of(Set.of(1L), Set.of(2L))));
        Assertions.assertFalse(preserves(frozen, Set.of(Set.of(1L, 2L))));
        Assertions.assertFalse(preserves(frozen, Set.of(Set.of(1L), Set.of(3L))));
    }

    @Test
    void testRejectsLostGroupingOrWrongProducer() {
        for (String sql : List.of(
                "SELECT _spm_const_var(1) AS c, count(*) AS n FROM t GROUP BY _spm_const_var(1)",
                "SELECT _spm_const_var(1) AS c, count(*) AS n FROM t GROUP BY 1",
                "SELECT _spm_const_var(1) AS c, count(*) AS n FROM t",
                "SELECT c, count(*) AS n FROM (SELECT _spm_const_var(1) AS c FROM t) q GROUP BY 1",
                "SELECT c, count(*) AS n FROM (SELECT _spm_const_var(2) AS c FROM t) q GROUP BY c",
                "SELECT _spm_const_var(1) AS c, count(*) AS n FROM (SELECT k AS g FROM t) q GROUP BY g",
                "SELECT c, count(*) AS n FROM (SELECT _spm_const_var(1) AS c, k AS c FROM t) q GROUP BY c",
                "SELECT c, count(*) AS n FROM (SELECT *, _spm_const_var(1) AS c FROM t) q GROUP BY c",
                "SELECT c, count(*) AS n FROM (SELECT _spm_const_var(1) AS c FROM t) q GROUP BY other.c",
                "SELECT c, count(*) AS c FROM (SELECT _spm_const_var(1) AS c FROM t) q GROUP BY c")) {
            Assertions.assertFalse(preserves(sql, Set.of(Set.of(1L))), sql);
        }
    }

    @Test
    void testRepeatContractPreservesMembershipMultiplicityAndGroupingArgumentOrder() {
        String source = "SELECT k % 2 AS a, v % 3 AS b, grouping_id(v % 3, k % 2) AS g, count(*) AS n FROM t"
                + " GROUP BY GROUPING SETS((1, 2), (1), (1), ())";
        String projection = " FROM (SELECT k % _spm_const_var(1) AS x, v % _spm_const_var(2) AS y FROM t) q";
        String output = "SELECT x, y, grouping_id(y, x) AS g, count(*) AS n";
        String sets = " GROUP BY GROUPING SETS((x, y), (x), (x), ())";
        Assertions.assertTrue(preservesRepeat(source, output + projection + sets));
        Assertions.assertTrue(preservesRepeat(source,
                output + projection + " GROUP BY GROUPING SETS((), (x), (y, x), (x))"));
        for (String frozen : List.of(
                output + projection + " GROUP BY GROUPING SETS((x, y), (x), (x))",
                output + projection + " GROUP BY GROUPING SETS((x, y), (x), ())",
                output + projection + " GROUP BY GROUPING SETS((x, y), (x), (y), ())",
                output + projection + " GROUP BY GROUPING SETS((1, y), (1), (1), ())",
                output.replace("grouping_id(y, x)", "grouping_id(x, y)") + projection + sets,
                output.replace("grouping_id(y, x)", "grouping(y)") + projection + sets,
                output.replace("grouping_id(y, x)", "0") + projection + sets,
                output + projection.replace("_spm_const_var(2)", "_spm_const_var(3)") + sets,
                output + projection + " GROUP BY x, y")) {
            Assertions.assertFalse(preservesRepeat(source, frozen), frozen);
        }
    }

    @Test
    void testRepeatContractPreservesConcreteMemberAndConsumerGroupingCall() {
        String source = "SELECT k % 2 AS a, v, count(*) AS n FROM t GROUP BY ROLLUP(1, 2)"
                + " HAVING grouping(k % 2) = 0";
        String frozen = "SELECT x, v, grouping(x) AS hidden, count(*) AS n FROM"
                + " (SELECT k % _spm_const_var(1) AS x, v FROM t) q GROUP BY GROUPING SETS((x,v),(x),())";
        Assertions.assertTrue(preservesRepeat(source, frozen));
        Assertions.assertFalse(preservesRepeat(source, frozen.replace("grouping(x)", "grouping(v)")));
        Assertions.assertFalse(preservesRepeat(source, frozen.replace("AS x, v FROM", "AS x, k AS v FROM")));
    }

    private boolean preservesRepeat(String source, String frozen) {
        NereidsParser parser = new NereidsParser();
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan parameterized = SPMPlanTreeSupport.transform(parser.parseSingle(source), builder);
        return SPMGroupByOrdinalValidator.preservesTargets(parser.parseSingle(frozen), parameterized,
                builder.getGroupingColumnTargets());
    }

    private boolean preserves(String sql, Set<Set<Long>> targets) {
        NereidsParser parser = new NereidsParser();
        return SPMGroupByOrdinalValidator.preservesTargets(parser.parseSingle(sql),
                parser.parseSingle("SELECT 1"), targets);
    }
}
