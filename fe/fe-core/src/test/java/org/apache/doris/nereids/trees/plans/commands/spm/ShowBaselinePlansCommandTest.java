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

package org.apache.doris.nereids.trees.plans.commands.spm;

import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.BaselineScope;
import org.apache.doris.nereids.spm.BaselineSource;
import org.apache.doris.nereids.spm.BaselineStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * SHOW BASELINE PLANS LIKE operand semantics.
 *
 * Only an OMITTED LIKE is "no filter": an empty pattern operand (LIKE '') is a real
 * pattern that matches only empty values - treating it as absent admitted every baseline
 * although none of the searched SQL / status / source fields is empty.
 */
public class ShowBaselinePlansCommandTest {

    @Test
    public void testEmptyLikeIsARealPattern() throws Exception {
        Assertions.assertNull(ShowBaselinePlansCommand.buildLikeMatcher(null),
                "an omitted LIKE means no filter");
        java.util.regex.Pattern matcher = ShowBaselinePlansCommand.buildLikeMatcher("");
        Assertions.assertNotNull(matcher, "LIKE '' is a real pattern, not an omitted filter");
        Assertions.assertFalse(matcher.matcher("select 1").matches(),
                "LIKE '' matches only empty values, so no baseline row passes it");
        Assertions.assertTrue(matcher.matcher("").matches(),
                "the empty pattern matches the empty value");
    }

    /**
     * The LIKE chain must cover every column the WHERE form filters on. A
     * SESSION baseline whose SQL text never spells "SESSION" was missing from
     * SHOW BASELINE PLANS LIKE 'SESSION' because only the SQL text, source and
     * status were searched.
     */
    @Test
    public void testLikeAlsoMatchesTheMetadataColumns() throws Exception {
        BaselinePlan baseline = new BaselinePlan();
        baseline.setBindSql("SELECT a FROM t1");
        baseline.setPlanSql("SELECT a FROM t1");
        baseline.setSource(BaselineSource.CAPTURE);
        baseline.setStatus(BaselineStatus.ENABLED);
        baseline.setScope(BaselineScope.SESSION);

        Assertions.assertTrue(ShowBaselinePlansCommand.matchesLike(baseline,
                ShowBaselinePlansCommand.buildLikeMatcher("SESSION")),
                "LIKE 'SESSION' must find a session baseline whose SQL does not mention it");
        Assertions.assertTrue(ShowBaselinePlansCommand.matchesLike(baseline,
                ShowBaselinePlansCommand.buildLikeMatcher("cap%")),
                "the source is searched");
        Assertions.assertTrue(ShowBaselinePlansCommand.matchesLike(baseline,
                ShowBaselinePlansCommand.buildLikeMatcher("%t1%")),
                "the stored SQL text stays searchable");
        Assertions.assertFalse(ShowBaselinePlansCommand.matchesLike(baseline,
                ShowBaselinePlansCommand.buildLikeMatcher("GLOBAL")),
                "a GLOBAL operand must not match a SESSION baseline");
    }

    /**
     * #9: the LIKE operand searches STORED SQL TEXT. The shared MySQL-pattern helper
     * (PatternMatcher) REJECTED the literal characters every statement contains ('*',
     * '=', '('), so the most natural operand LIKE '%SELECT * FROM%' failed with an
     * analysis error, and its '%' compiled to a non-DOTALL '.' that could not span the
     * newlines the stored SQL is printed with.
     */
    @Test
    public void testSqlShapedPatternsAreMatchable() throws Exception {
        java.util.regex.Pattern sqlPattern =
                ShowBaselinePlansCommand.buildLikeMatcher("%select * from%");
        Assertions.assertTrue(sqlPattern.matcher("SELECT * FROM t1\nWHERE k = 1").matches(),
                "'%' must span the newlines of the stored SQL text");
        Assertions.assertTrue(sqlPattern.matcher(" SELECT * FROM t1").matches(),
                "the pattern must match case-insensitively across the whole value");
        Assertions.assertFalse(sqlPattern.matcher("SELECT k FROM t1").matches());

        // regex metacharacters stay LITERAL: a broken operator's "(a)+" must not turn
        // into a quantified group that matches "aa"
        java.util.regex.Pattern literal = ShowBaselinePlansCommand.buildLikeMatcher("sum(a)+1");
        Assertions.assertTrue(literal.matcher("sum(a)+1").matches());
        Assertions.assertFalse(literal.matcher("sumaa1").matches(),
                "the parentheses / plus must be escaped, not interpreted");
        Assertions.assertFalse(literal.matcher("sum a 1").matches());
    }

    @Test
    public void testUnderscoreMatchesExactlyOneCharacter() throws Exception {
        java.util.regex.Pattern pattern = ShowBaselinePlansCommand.buildLikeMatcher("ab_d");
        Assertions.assertTrue(pattern.matcher("abxd").matches());
        Assertions.assertTrue(pattern.matcher("ABYD").matches(),
                "matching stays case-insensitive");
        Assertions.assertFalse(pattern.matcher("abd").matches(),
                "'_' is exactly one character");
        Assertions.assertFalse(pattern.matcher("abxyd").matches());
    }

    /**
     * The SQL literal parser preserves \_ and \%: the matcher must consume the escape
     * together with the following character (as a LITERAL), otherwise a search for a
     * stored my_table via '%my\_table%' missed that row and could match unrelated text
     * carrying a backslash.
     */
    @Test
    public void testEscapedWildcardsAreLiteral() throws Exception {
        java.util.regex.Pattern escapedUnderscore =
                ShowBaselinePlansCommand.buildLikeMatcher("%my\\_table%");
        Assertions.assertTrue(escapedUnderscore.matcher("SELECT * FROM my_table").matches(),
                "\\_ is a literal underscore");
        Assertions.assertFalse(escapedUnderscore.matcher("SELECT * FROM myXtable").matches(),
                "\\_ must not act as a single-character wildcard");
        Assertions.assertFalse(
                escapedUnderscore.matcher("SELECT * FROM my\\_table").matches(),
                "the backslash is an escape, not part of the searched text");

        java.util.regex.Pattern escapedPercent =
                ShowBaselinePlansCommand.buildLikeMatcher("%100\\%");
        Assertions.assertTrue(escapedPercent.matcher("SELECT 100%").matches(),
                "\\% is a literal percent");
        Assertions.assertFalse(escapedPercent.matcher("SELECT 100x").matches(),
                "\\% must not act as a wildcard");
    }

    /**
     * The internal DATETIME columns are stored and parsed in UTC
     * (BaselineManager.toTs / fromTs), so SHOW must render them in UTC as well - with the
     * host zone a row stored as 12:00:00 showed 20:00:00 on an Asia/Shanghai FE and
     * 12:00:00 on a UTC one, i.e. the SAME baseline had two different create / update
     * times depending on which FE answered.
     */
    @Test
    public void testBaselineTimesAreRenderedInThePersistedZone() {
        long noonUtc = java.time.LocalDateTime.of(2026, 1, 1, 12, 0, 0)
                .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
        Assertions.assertEquals("2026-01-01 12:00:00",
                ShowBaselinePlansCommand.formatTime(noonUtc),
                "the stored DATETIME is UTC: SHOW must not apply the FE host zone");
    }
}
