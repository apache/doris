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

package org.apache.doris.policy;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.CreatePolicyCommand;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A built-in row policy is stored as a statement but handed to the planner as the SQL text of its predicate.
 * The recovered text has to be faithful: whatever the planner parses back must be the same predicate the
 * administrator wrote, or the policy silently filters different rows than it says it does.
 *
 * <p>The corpus below is what row policies actually contain - comparisons, set membership, pattern matches,
 * null tests, boolean structure with mixed precedence, function calls and the literal shapes that are easiest
 * to mangle (quotes inside strings, negative numbers, dates). Boolean structure is the case that matters
 * most: {@code toSql()} on a compound predicate yields the diagnostic form {@code AND[a,b]}, so recovering
 * the text from the stored statement rather than re-rendering the tree is what keeps these working.</p>
 */
public class RowPolicyFilterSqlTest {

    private static final NereidsParser PARSER = new NereidsParser();

    @ParameterizedTest
    @ValueSource(strings = {
            "k1 = 1",
            "k1 <> 1",
            "k1 >= 1 and k2 < 10",
            "k1 = 1 or k2 = 2",
            "not (k1 = 1)",
            "k1 = 1 and (k2 = 2 or k2 = 3)",
            "(k1 = 1 or k2 = 2) and k3 = 3",
            "k1 in (1, 2, 3)",
            "k1 not in (1, 2)",
            "region = 'cn'",
            "region = 'it''s'",
            "name like 'a%'",
            "name not like '%b'",
            "k1 is null",
            "k1 is not null",
            "k1 between 1 and 10",
            "k1 = -1",
            "amount > 1.5",
            "dt = date '2024-01-01'",
            "dt > '2024-01-01 10:00:00'",
            "upper(region) = 'CN'",
            "concat(a, b) = 'ab'",
            "substr(phone, 1, 3) = '138'",
            "k1 + 1 > 2",
            "k1 = 1 and region in ('cn', 'us') and name like 'x%'"
    })
    public void testPredicateSurvivesTheRoundTripToThePlanner(String original) throws AnalysisException {
        RowPolicy policy = policyOver(original);

        Expression asHandedToThePlanner = PARSER.parseExpression(policy.getFilterSql());

        Assertions.assertEquals(PARSER.parseExpression(original), asHandedToThePlanner,
                "the predicate the planner receives is not the one the policy was created with: " + original
                        + " became " + policy.getFilterSql());
    }

    /**
     * A policy whose statement no longer parses (an upgrade dropped a function, say) has no predicate to
     * render. It must fail the query, never quietly disappear - a vanished row filter exposes the whole
     * table.
     */
    @Test
    public void testUnparseablePolicyFailsInsteadOfVanishing() {
        RowPolicy broken = new RowPolicy(1L, "p1", "internal", "db1", "t1", UserIdentity.ROOT, null,
                "CREATE ROW POLICY p1 ON db1.t1 AS RESTRICTIVE TO root USING (gone_function(k1))", 0,
                FilterType.RESTRICTIVE, null);

        AnalysisException thrown = Assertions.assertThrows(AnalysisException.class, broken::getFilterSql);
        Assertions.assertTrue(thrown.getMessage().contains("Invalid row policy"),
                "the error must name the broken policy so an operator can find it: " + thrown.getMessage());
    }

    /**
     * The same, for a policy that does have a predicate.
     *
     * <p>The case above stops at the null check and never reaches the recovery of the text, so it cannot see
     * whether recovery fails safely. This one does reach it: the predicate is there, the statement it came
     * from no longer parses, and the query has to be refused rather than read unfiltered.
     */
    @Test
    public void testAPolicyWhoseStatementNoLongerParsesFailsEvenWithAPredicate() {
        RowPolicy broken = new RowPolicy(1L, "p1", "internal", "db1", "t1", UserIdentity.ROOT, null,
                "SELECT 1", 0, FilterType.RESTRICTIVE, PARSER.parseExpression("k1 = 1"));

        AnalysisException thrown = Assertions.assertThrows(AnalysisException.class, broken::getFilterSql);
        Assertions.assertTrue(thrown.getMessage().contains("Invalid row policy"),
                "the error must name the broken policy so an operator can find it: " + thrown.getMessage());
    }

    /**
     * A policy created inside a multi-statement request.
     *
     * <p>{@code originStmt} is the whole request, and {@code stmtIdx} says which statement of it this policy
     * is; parsing the text as a single statement recovers the wrong policy or nothing at all. The creating
     * path avoids the question by carrying the predicate text along, and the replay path has to honour the
     * index.
     */
    @Test
    public void testAPolicyFromAMultiStatementRequestRecoversItsOwnPredicate() throws Exception {
        String request = "CREATE ROW POLICY p0 ON db1.t1 AS RESTRICTIVE TO root USING (k1 = 1);"
                + "CREATE ROW POLICY p1 ON db1.t1 AS RESTRICTIVE TO root USING (k2 = 2)";

        // As CREATE ROW POLICY builds it: the text is known, nothing is recovered.
        RowPolicy created = new RowPolicy(1L, "p1", "internal", "db1", "t1", UserIdentity.ROOT, null,
                request, 1, FilterType.RESTRICTIVE, PARSER.parseExpression("k2 = 2"), "k2 = 2");
        Assertions.assertEquals(PARSER.parseExpression("k2 = 2"),
                PARSER.parseExpression(created.getFilterSql()));

        // As it comes back out of an image: no predicate and no text, both recovered from the statement.
        RowPolicy replayed = new RowPolicy(1L, "p1", "internal", "db1", "t1", UserIdentity.ROOT, null,
                request, 1, FilterType.RESTRICTIVE, null);
        replayed.gsonPostProcess();
        Assertions.assertFalse(replayed.isInvalid(), "the replayed policy lost its predicate");
        Assertions.assertEquals(PARSER.parseExpression("k2 = 2"),
                PARSER.parseExpression(replayed.getFilterSql()));
    }

    /** Builds the policy the way CREATE ROW POLICY does: statement text plus the predicate parsed from it. */
    private RowPolicy policyOver(String predicate) {
        String statement = "CREATE ROW POLICY p1 ON db1.t1 AS RESTRICTIVE TO root USING (" + predicate + ")";
        CreatePolicyCommand command = (CreatePolicyCommand) PARSER.parseSingle(statement);
        return new RowPolicy(1L, "p1", "internal", "db1", "t1", UserIdentity.ROOT, null,
                statement, 0, FilterType.RESTRICTIVE, command.getWherePredicate().get());
    }
}
