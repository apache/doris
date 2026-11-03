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
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Round-51 review fixes without their own regression suite:
 *
 * - #4 (LogicalSelectHint#toSpmDigest): the SET_VAR payload is ORDER-INSENSITIVE by
 *   construction - the parser stores the assignments in TEXT order and toString()
 *   preserves it, so the same two settings written in reverse order produced different
 *   digests. The candidate lookup then never reached the order-insensitive
 *   sameSelectHints comparison and the equivalent reordered caller missed the baseline.
 */
public class SPMRound51SafetyTest {

    private static LogicalPlan relation() {
        return (LogicalPlan) new NereidsParser().parseSingle("SELECT k FROM t");
    }

    private static LogicalPlan withSetVar(LogicalPlan child, String... keysAndValues) {
        Map<String, Optional<String>> parameters = new LinkedHashMap<>();
        for (int i = 0; i + 1 < keysAndValues.length; i += 2) {
            parameters.put(keysAndValues[i], Optional.of(keysAndValues[i + 1]));
        }
        SelectHint hint = new SelectHintSetVar("SET_VAR", parameters);
        return new LogicalSelectHint<>(ImmutableList.of(hint), child);
    }

    /**
     * The same two SET_VAR settings in either order produce the SAME SPM digest (the
     * payload is canonicalized), while the hint itself stays part of the digest (a
     * hint-free variant must not collide).
     */
    @Test
    public void testSetVarDigestIsOrderInsensitive() {
        LogicalPlan first = withSetVar(relation(), "time_zone", "+08:00", "query_timeout", "100");
        LogicalPlan reordered = withSetVar(relation(), "query_timeout", "100", "time_zone", "+08:00");
        LogicalPlan noHint = relation();
        Assertions.assertEquals(first.toSpmDigest(), reordered.toSpmDigest(),
                "the assignment order must not change the SPM digest");
        Assertions.assertNotEquals(first.toSpmDigest(), noHint.toSpmDigest(),
                "the hint payload stays part of the digest");
        // the KEY spelling is case-insensitive as well (Doris variable names are)
        LogicalPlan upperKeys = withSetVar(relation(), "TIME_ZONE", "+08:00", "QUERY_TIMEOUT", "100");
        Assertions.assertEquals(first.toSpmDigest().toLowerCase(Locale.ROOT),
                upperKeys.toSpmDigest().toLowerCase(Locale.ROOT),
                "variable-name case must not change the SPM digest: "
                        + first.toSpmDigest() + " vs " + upperKeys.toSpmDigest());
        // ... but a different VALUE still differs (the payload is not dropped)
        LogicalPlan otherValue = withSetVar(relation(), "time_zone", "+08:00", "query_timeout", "101");
        Assertions.assertNotEquals(first.toSpmDigest(), otherValue.toSpmDigest(),
                "a different assignment value must change the SPM digest");
    }
}
