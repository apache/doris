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

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Round-54 #2: an explicit {@code FROM t INDEX mv} pin is a SEMANTIC choice of the user,
 * not an optimizer decision - on an aggregate-key table the pinned (coarser) rollup
 * returns aggregated rows where the base table returns one row per record. The frozen
 * text cannot carry it: the physical scan only knows the SELECTED index id, which every
 * optimizer-side rollup / MV choice sets as well, while the matching key DOES carry the
 * pin (the bind digest renders INDEX &lt;name&gt;). A frozen "FROM t" would therefore read
 * the base table for a caller whose pinned query matched the baseline - the replan probe
 * does not catch it (the text is valid) and the schema fingerprint does not hash the
 * selection. The freeze is declined for such a statement instead, so the rewrite replays
 * the parameterized tree, whose own analysis re-applies the pin.
 */
public class SPMRound54SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** The pinned relation of a parsed statement (the only one in these statements). */
    private static UnboundRelation relationOf(LogicalPlan plan) {
        UnboundRelation[] holder = new UnboundRelation[1];
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
            if (node instanceof UnboundRelation) {
                holder[0] = (UnboundRelation) node;
            }
        });
        return holder[0];
    }

    /**
     * The detection the freeze guard is built on: an explicit INDEX clause is reported on
     * the parsed statement - including one that only appears inside a CTE body or a
     * subquery (the whole statement is inspected, not just the top-level children).
     */
    @Test
    public void testExplicitIndexPinIsDetected() {
        LogicalPlan pinned = parse("SELECT k, v FROM t INDEX mv");
        Assertions.assertEquals("mv", relationOf(pinned).getIndexName().orElse(null),
                "the parsed relation must carry the user's INDEX pin");
        Assertions.assertTrue(SPMPlanTreeSupport.pinsExplicitIndex(pinned),
                "the freeze guard must see the explicit INDEX pin");

        // the same query WITHOUT the pin must stay freezable
        Assertions.assertFalse(SPMPlanTreeSupport.pinsExplicitIndex(parse("SELECT k, v FROM t")),
                "a plain scan carries no pin and may be frozen");
        Assertions.assertFalse(
                SPMPlanTreeSupport.pinsExplicitIndex(parse("SELECT k FROM (SELECT k FROM t) s")),
                "a derived-table wrapper adds no pin");

        // a pin inside a CTE body / subquery counts as well: the frozen text of the WHOLE
        // statement would drop it, and the caller's identical statement still matches
        Assertions.assertTrue(SPMPlanTreeSupport.pinsExplicitIndex(parse(
                        "WITH c AS (SELECT k FROM t INDEX mv) SELECT k FROM c")),
                "a pin inside a CTE body must be detected");
        Assertions.assertTrue(SPMPlanTreeSupport.pinsExplicitIndex(parse(
                        "SELECT k FROM (SELECT k FROM t INDEX mv) s")),
                "a pin inside a derived table must be detected");
        // a NULL plan (no statement) is never reported as pinned
        Assertions.assertFalse(SPMPlanTreeSupport.pinsExplicitIndex(null));
    }

    /**
     * The pin also takes part in the BIND-side scan identity the matcher compares, which
     * is what makes a frozen base-table plan so dangerous: only a caller carrying that very
     * INDEX clause can match the baseline, and the selector guard already refuses a
     * CREATE whose bind and plan texts pin different selectors.
     */
    @Test
    public void testIndexPinIsPartOfTheScanIdentity() {
        Assertions.assertTrue(SPMPlanTreeSupport.sameScanIdentityForTest(
                        relationOf(parse("SELECT k, v FROM t INDEX mv")),
                        relationOf(parse("SELECT k, v FROM t INDEX mv"))),
                "the same INDEX pin must match");
        Assertions.assertFalse(SPMPlanTreeSupport.sameScanIdentityForTest(
                        relationOf(parse("SELECT k, v FROM t INDEX mv")),
                        relationOf(parse("SELECT k, v FROM t"))),
                "a caller WITHOUT the pin must not match a pinned baseline");
        Assertions.assertFalse(SPMPlanTreeSupport.sameScanIdentityForTest(
                        relationOf(parse("SELECT k, v FROM t INDEX mv")),
                        relationOf(parse("SELECT k, v FROM t INDEX mv2"))),
                "a different pinned index must not match");
    }
}
