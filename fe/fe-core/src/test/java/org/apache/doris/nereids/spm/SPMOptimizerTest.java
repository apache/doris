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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SPMOptimizer tests: the SPM excluded-rule set and the whitelist builder.
 *
 * Critical invariant: every rule name in SPM_EXCLUDED_RULE_NAMES and in the enumerated
 * MATERIALIZE_VIEW family must be a valid RuleType name - the whitelist is written into
 * enable_nereids_rules, which the engine resolves with RuleType.valueOf() and throws on
 * unknown names, which would break baseline planning.
 */
public class SPMOptimizerTest {

    @Test
    public void testAllExplicitRuleNamesValid() {
        for (String ruleName : SPMOptimizer.SPM_EXCLUDED_RULE_NAMES) {
            Assertions.assertDoesNotThrow(() -> RuleType.valueOf(ruleName),
                    "SPM excluded rule must be a valid RuleType name: " + ruleName);
        }
    }

    @Test
    public void testMaterializedViewRulesEnumerated() {
        List<String> mvRules = SPMOptimizer.getMaterializedViewRuleNames();
        // The whole MATERIALIZE_VIEW family must be covered, e.g. at least one
        // representative rule and no empty set
        Assertions.assertFalse(mvRules.isEmpty());
        Assertions.assertTrue(mvRules.contains("MATERIALIZED_VIEW_PROJECT_JOIN"));
        for (String ruleName : mvRules) {
            Assertions.assertDoesNotThrow(() -> RuleType.valueOf(ruleName),
                    "Enumerated MV rule must be a valid RuleType name: " + ruleName);
        }
        // every enumerated name really is a materialized view rule
        for (String ruleName : mvRules) {
            Assertions.assertTrue(RuleType.valueOf(ruleName).isMaterializedViewRule(),
                    ruleName + " should be a materialized view rule");
        }
    }

    @Test
    public void testGetSpmExcludedRuleNamesDeduplicates() {
        List<String> all = SPMOptimizer.getSpmExcludedRuleNames();
        Set<String> unique = new HashSet<>(all);
        Assertions.assertEquals(all.size(), unique.size(),
                "getSpmExcludedRuleNames must be deduplicated");
        // MV family is included
        Assertions.assertTrue(all.contains("MATERIALIZED_VIEW_PROJECT_JOIN"));
    }

    @Test
    public void testBuildSpmEnabledRulesIsWhitelist() throws Exception {
        String enabled = SPMOptimizer.buildSpmEnabledRules("");
        Set<String> names = new HashSet<>(List.of(enabled.split(",")));
        // every SPM-excluded rule is outside the whitelist
        for (String excluded : SPMOptimizer.getSpmExcludedRuleNames()) {
            Assertions.assertFalse(names.contains(excluded),
                    "excluded rule must not be whitelisted: " + excluded);
        }
        // every other RuleType is whitelisted, including the never-gated checks
        Assertions.assertEquals(
                RuleType.values().length - SPMOptimizer.getSpmExcludedRuleNames().size(),
                names.size());
        Assertions.assertTrue(names.contains("CHECK_PRIVILEGES"));
        Assertions.assertTrue(names.contains("CHECK_ROW_POLICY"));
    }

    @Test
    public void testBuildSpmEnabledRulesIntersectsOriginal() throws Exception {
        // the session whitelist is preserved: intersection, normalized (trim / upper case)
        String enabled = SPMOptimizer.buildSpmEnabledRules(" check_privileges , infer_predicates ");
        // INFER_PREDICATES is SPM-excluded, CHECK_PRIVILEGES is allowed
        Assertions.assertEquals("CHECK_PRIVILEGES", enabled);
    }

    @Test
    public void testBuildSpmEnabledRulesRejectsUnknownAndEmptyIntersection() {
        // unknown rule name -> explicit failure (the whitelist is parsed with RuleType.valueOf)
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMOptimizer.buildSpmEnabledRules("NOT_A_RULE"));
        // a session whitelist of only SPM-excluded rules leaves no rule to run
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMOptimizer.buildSpmEnabledRules("SALT_JOIN"));
    }

    @Test
    public void testWhitelistMaskEqualsExcludedRuleMask() throws Exception {
        // The SPM mask (installed on the private statement context of a baseline-creation
        // statement) vs the equivalent disable list: the statement-level forbidden mask
        // must be IDENTICAL, i.e. baseline creation is semantically unchanged.
        SessionVariable disableBased = new SessionVariable();
        disableBased.setDisableNereidsRules(String.join(",", SPMOptimizer.getSpmExcludedRuleNames()));
        BitSet disableMask = new StatementContext().getOrCacheDisableRules(disableBased);

        StatementContext spmContext = new StatementContext();
        spmContext.setSpmExcludedRules(SPMOptimizer.buildSpmExcludedRuleMask(""));
        BitSet whitelistMask = spmContext.getOrCacheDisableRules(new SessionVariable());

        Assertions.assertEquals(disableMask, whitelistMask,
                "the SPM mask must forbid exactly the same rules as the disable list");

        // the engine-essential checks are never gated, even when the whitelist is tiny
        StatementContext tinyContext = new StatementContext();
        tinyContext.setSpmExcludedRules(SPMOptimizer.buildSpmExcludedRuleMask(
                RuleType.CHECK_PRIVILEGES.name()));
        BitSet tinyMask = tinyContext.getOrCacheDisableRules(new SessionVariable());
        Assertions.assertFalse(tinyMask.get(RuleType.CHECK_PRIVILEGES.ordinal()));
        Assertions.assertFalse(tinyMask.get(RuleType.CHECK_ROW_POLICY.ordinal()));
        Assertions.assertTrue(tinyMask.get(RuleType.INFER_PREDICATES.ordinal()));
    }

    // ==================== the public variable keeps its inert planning behavior ====================

    /**
     * The SPM rule mask lives on the baseline-creation statement's private context only.
     * enable_nereids_rules must keep its established behavior: setting it does NOT forbid
     * the rules outside the list, otherwise an existing session such as
     * set enable_nereids_rules='ELIMINATE_GROUP_BY_KEY_BY_UNIFORM' would disable every
     * binding and physical implementation rule for ordinary SELECTs (even BINDING_RELATION).
     */
    @Test
    public void testEnableNereidsRulesDoesNotMaskOrdinaryStatements() {
        SessionVariable session = new SessionVariable();
        session.setEnableNereidsRules("ELIMINATE_GROUP_BY_KEY_BY_UNIFORM");
        BitSet mask = new StatementContext().getOrCacheDisableRules(session);
        Assertions.assertFalse(mask.get(RuleType.BINDING_RELATION.ordinal()),
                "binding must stay enabled for an ordinary statement");
        Assertions.assertFalse(mask.get(RuleType.LOGICAL_PROJECT_TO_PHYSICAL_PROJECT_RULE.ordinal()),
                "implementation rules must stay enabled for an ordinary statement");
        Assertions.assertEquals(session.getDisableNereidsRules(), mask,
                "the statement mask is exactly the disable list again");
    }
}
