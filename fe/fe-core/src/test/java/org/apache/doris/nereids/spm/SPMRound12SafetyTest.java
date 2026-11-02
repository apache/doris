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

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.spm.ShowBaselinePlansCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

/**
 * Matching / parser safety.
 *
 * - SELECT-hint payloads are part of the baseline match. LogicalSelectHint has no
 *   expressions and its toDigest() drops the hint list, so the class + child comparison
 *   accepted two query blocks that differ ONLY in a hint value. A SET_VAR hint however
 *   changes how the query is analyzed / planned - time_zone / sql_mode
 *   change the RESULT - so the match must include the complete hint list of every block.
 * - SHOW BASELINE PLANS LIKE must decode the raw SQL string literal (doubled quotes /
 *   backslash escapes) before PatternMatcher; only stripping the surrounding quotes
 *   handed the matcher the ESCAPED characters, so a pattern containing an apostrophe
 *   could never match the stored SQL.
 */
public class SPMRound12SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** Two trees match only when their ONLY difference is a placeholder-able value. */
    private static boolean matches(String bindSql, String userSql) {
        return SPMPlanTreeSupport.check(parse(bindSql), parse(userSql),
                new HashMap<Long, Expression>());
    }

    // ==================== R12-2: SELECT-hint payloads are part of the match ====================

    @Test
    public void testSetVarPayloadIsPartOfTheMatch() {
        String plus = "SELECT /*+ SET_VAR(time_zone='+08:00') */ k FROM spm_r12_t WHERE k = 1";
        String minus = "SELECT /*+ SET_VAR(time_zone='-08:00') */ k FROM spm_r12_t WHERE k = 1";
        Assertions.assertTrue(matches(plus, plus),
                "the same query with the same hint must still match its own baseline");
        Assertions.assertFalse(matches(plus, minus),
                "time_zone changes the RESULT: replaying the creator's frozen expression /"
                        + " plan under the caller's -08:00 setting is neither query's semantics");
        Assertions.assertFalse(matches(plus, "SELECT k FROM spm_r12_t WHERE k = 1"),
                "an unhinted variant must not match a hinted baseline");
        Assertions.assertFalse(matches("SELECT k FROM spm_r12_t WHERE k = 1", plus),
                "a hinted variant must not match an unhinted baseline either");
    }

    @Test
    public void testSqlModePayloadIsPartOfTheMatch() {
        String pipes = "SELECT /*+ SET_VAR(sql_mode='PIPES_AS_CONCAT') */ k FROM spm_r12_t";
        String fullGroupBy = "SELECT /*+ SET_VAR(sql_mode='ONLY_FULL_GROUP_BY') */ k FROM spm_r12_t";
        Assertions.assertTrue(matches(pipes, pipes));
        Assertions.assertFalse(matches(pipes, fullGroupBy),
                "sql_mode takes part in the analysis of the replayed text: a different mode"
                        + " must not reuse this baseline");
    }

    @Test
    public void testSetVarKeyOrderIsNotSemantic() {
        String first = "SELECT /*+ SET_VAR(time_zone='+08:00', query_timeout=100) */"
                + " k FROM spm_r12_t WHERE k = 1";
        String reversed = "SELECT /*+ SET_VAR(query_timeout=100, time_zone='+08:00') */"
                + " k FROM spm_r12_t WHERE k = 1";
        Assertions.assertTrue(matches(first, reversed),
                "the pairs of ONE SET_VAR hint are a map: writing them in another order is"
                        + " the same setting and must not lose the match");
    }

    @Test
    public void testHintListShapeAndPayloadsArePartOfTheMatch() {
        String one = "SELECT /*+ SET_VAR(time_zone='+08:00') */ k FROM spm_r12_t WHERE k = 1";
        String extra = "SELECT /*+ SET_VAR(time_zone='+08:00') USE_MV(spm_r12_t) */"
                + " k FROM spm_r12_t WHERE k = 1";
        Assertions.assertFalse(matches(one, extra),
                "a hint LIST difference [] vs [SET_VAR, USE_MV] must not match");
        String mvA = "SELECT /*+ USE_MV(spm_r12_t) */ k FROM spm_r12_t WHERE k = 1";
        String mvB = "SELECT /*+ USE_MV(spm_r12_other) */ k FROM spm_r12_t WHERE k = 1";
        Assertions.assertTrue(matches(mvA, mvA));
        Assertions.assertFalse(matches(mvA, mvB),
                "USE_MV references concrete tables: the payload must take part in the match");
    }

    @Test
    public void testNestedBlockHintPayloadIsPartOfTheMatch() {
        String plus = "SELECT s.k FROM (SELECT /*+ SET_VAR(time_zone='+08:00') */ k"
                + " FROM spm_r12_t) s WHERE s.k = 1";
        String minus = "SELECT s.k FROM (SELECT /*+ SET_VAR(time_zone='-08:00') */ k"
                + " FROM spm_r12_t) s WHERE s.k = 1";
        Assertions.assertTrue(matches(plus, plus));
        Assertions.assertFalse(matches(plus, minus),
                "nested query blocks carry their own hint list: the inner block's payload"
                        + " must be compared exactly like the top-level one");
    }

    // ==================== R12-3: SHOW BASELINE PLANS LIKE literal decoding ====================

    @Test
    public void testShowBaselinePlansLikeDecodesSqlLiteral() {
        ShowBaselinePlansCommand apostrophe = (ShowBaselinePlansCommand) parse(
                "SHOW BASELINE PLANS LIKE '%a''b%'");
        Assertions.assertEquals("%a'b%", apostrophe.getPattern(),
                "the doubled quote of the literal must be decoded ONCE: the matcher needs"
                        + " the character the stored SQL contains, not the escape");

        ShowBaselinePlansCommand backslash = (ShowBaselinePlansCommand) parse(
                "SHOW BASELINE PLANS LIKE '%a\\\\b%'");
        Assertions.assertEquals("%a\\b%", backslash.getPattern(),
                "a backslash escape of the literal must be decoded as well");

        ShowBaselinePlansCommand plain = (ShowBaselinePlansCommand) parse(
                "SHOW BASELINE PLANS LIKE '%lineitem%'");
        Assertions.assertEquals("%lineitem%", plain.getPattern(),
                "a pattern without escapes must decode to itself");

        ShowBaselinePlansCommand noPattern = (ShowBaselinePlansCommand) parse(
                "SHOW BASELINE PLANS");
        Assertions.assertNull(noPattern.getPattern(),
                "without a LIKE clause there is no pattern");
    }
}
