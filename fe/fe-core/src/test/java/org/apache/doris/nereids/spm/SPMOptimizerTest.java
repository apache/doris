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
        // New mechanism (enable_nereids_rules whitelist, installed during CREATE) vs old
        // mechanism (disable list containing exactly the excluded rules): the statement-level
        // forbidden mask must be IDENTICAL, i.e. baseline creation is semantically unchanged.
        SessionVariable disableBased = new SessionVariable();
        disableBased.setDisableNereidsRules(String.join(",", SPMOptimizer.getSpmExcludedRuleNames()));
        BitSet disableMask = new StatementContext().getOrCacheDisableRules(disableBased);

        SessionVariable whitelistBased = new SessionVariable();
        whitelistBased.setEnableNereidsRules(SPMOptimizer.buildSpmEnabledRules(""));
        BitSet whitelistMask = new StatementContext().getOrCacheDisableRules(whitelistBased);

        Assertions.assertEquals(disableMask, whitelistMask,
                "whitelist mode must forbid exactly the same rules as the disable list");

        // a narrow session whitelist still never gates the engine-essential checks
        SessionVariable tinyWhitelist = new SessionVariable();
        tinyWhitelist.setEnableNereidsRules(RuleType.CHECK_PRIVILEGES.name());
        BitSet tinyMask = new StatementContext().getOrCacheDisableRules(tinyWhitelist);
        Assertions.assertFalse(tinyMask.get(RuleType.CHECK_PRIVILEGES.ordinal()));
        Assertions.assertFalse(tinyMask.get(RuleType.CHECK_ROW_POLICY.ordinal()));
        Assertions.assertTrue(tinyMask.get(RuleType.INFER_PREDICATES.ordinal()));
    }
}
