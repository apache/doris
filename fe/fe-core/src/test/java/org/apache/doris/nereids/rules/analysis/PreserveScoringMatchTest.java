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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class PreserveScoringMatchTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("duplicate_score");
        connectContext.setDatabase("duplicate_score");
        createTable("CREATE TABLE documents (id INT, content STRING) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testScoredDuplicatesSurviveAnalysisAndRewrite() {
        for (String operator : new String[] {" OR ", " AND "}) {
            for (int count : new int[] {2, 3}) {
                String predicate = String.join(operator,
                        Collections.nCopies(count, "(content MATCH_ANY 'spider')"));
                PlanChecker checker = PlanChecker.from(connectContext).analyze(scoredQuery(predicate));
                Assertions.assertEquals(count, matchCount(checker.getPlan()));
                Assertions.assertEquals(count, matchCount(checker.rewrite().getPlan()));
            }
        }
    }

    @Test
    void testDuplicateScoredBranchesRetainEachContribution() {
        String branch = "(content MATCH_ANY 'spider' AND content MATCH_ANY 'web')";
        Plan plan = PlanChecker.from(connectContext).analyze(scoredQuery(branch + " OR " + branch))
                .rewrite().getPlan();
        Assertions.assertEquals(4, matchCount(plan));
    }

    @Test
    void testScoreOnlyInOrderByRetainsOccurrences() {
        Plan plan = PlanChecker.from(connectContext).analyze(
                "SELECT id FROM documents WHERE content MATCH_ANY 'spider' OR content MATCH_ANY 'spider' "
                        + "ORDER BY score() DESC LIMIT 100").rewrite().getPlan();
        Assertions.assertEquals(2, matchCount(plan));
    }

    @Test
    void testScoreAboveDerivedTableRetainsOccurrences() {
        String predicate = "content MATCH_ANY 'spider' OR content MATCH_ANY 'spider'";
        Plan plan = PlanChecker.from(connectContext).analyze(
                "SELECT id, score() AS s FROM (SELECT id, content FROM documents WHERE " + predicate
                        + ") t ORDER BY s DESC LIMIT 100").rewrite().getPlan();
        Assertions.assertEquals(2, matchCount(plan));
    }

    @Test
    void testScoreAboveCteRetainsOccurrences() {
        Plan plan = PlanChecker.from(connectContext).analyze(
                "WITH t AS (SELECT id, content FROM documents WHERE content MATCH_ANY 'spider' "
                        + "AND content MATCH_ANY 'spider') SELECT id, score() AS s FROM t "
                        + "ORDER BY s DESC LIMIT 100").rewrite().getPlan();
        Assertions.assertEquals(2, matchCount(plan));
    }

    @Test
    void testScoreInLaterCteRetainsEarlierOccurrences() {
        Plan plan = PlanChecker.from(connectContext).analyze(
                "WITH base AS (SELECT id, content FROM documents WHERE content MATCH_ANY 'spider' "
                        + "AND content MATCH_ANY 'spider'), ranked AS (SELECT id, score() AS s FROM base "
                        + "ORDER BY s DESC LIMIT 100) SELECT * FROM ranked").rewrite().getPlan();
        Assertions.assertEquals(2, matchCount(plan));
    }

    @Test
    void testUnscoredPredicatesStillDeduplicate() {
        for (String operator : new String[] {" OR ", " AND "}) {
            String predicate = String.join(operator,
                    Collections.nCopies(3, "(content MATCH_ANY 'spider')"));
            Plan plan = PlanChecker.from(connectContext).analyze("SELECT id FROM documents WHERE " + predicate)
                    .rewrite().getPlan();
            Assertions.assertEquals(1, matchCount(plan));
        }
    }

    private static String scoredQuery(String predicate) {
        return "SELECT id, score() AS bm25_score FROM documents WHERE " + predicate
                + " ORDER BY bm25_score DESC LIMIT 100";
    }

    private static int matchCount(Plan plan) {
        int count = 0;
        for (Plan node : plan.<Plan>collectToList(p -> p instanceof LogicalFilter)) {
            for (Expression expression : node.getExpressions()) {
                count += expression.collectToList(e -> e instanceof Match).size();
            }
        }
        return count;
    }
}
