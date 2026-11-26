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

    private boolean preserves(String sql, Set<Set<Long>> targets) {
        return SPMGroupByOrdinalValidator.preservesTargets(new NereidsParser().parseSingle(sql), targets);
    }
}
