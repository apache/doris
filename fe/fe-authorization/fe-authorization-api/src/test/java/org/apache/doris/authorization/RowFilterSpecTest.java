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

package org.apache.doris.authorization;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The equality contract of {@link RowFilterSpec} is what the SQL result cache uses to answer "did this
 * table's row policies change since we planned this query?". Both directions of that answer are a real
 * defect when wrong, so both are asserted here: equal-when-unchanged (else every lookup evicts the cache and
 * a Ranger user never gets a cache hit) and unequal-when-updated (else an edited policy keeps serving rows
 * the subject may no longer see).
 */
public class RowFilterSpecTest {

    @Test
    public void testSpecsWithSameContentAreEqual() {
        RowFilterSpec planned = new RowFilterSpec("7:3", "region = 'cn'", RowFilterMergeType.RESTRICTIVE);
        RowFilterSpec reevaluated = new RowFilterSpec("7:3", "region = 'cn'", RowFilterMergeType.RESTRICTIVE);

        Assertions.assertEquals(planned, reevaluated,
                "an unchanged policy must re-evaluate to an equal spec, or the cache is evicted every lookup");
        Assertions.assertEquals(planned.hashCode(), reevaluated.hashCode());
    }

    @Test
    public void testUpdatedPolicyIsNotEqual() {
        RowFilterSpec planned = new RowFilterSpec("7:3", "region = 'cn'", RowFilterMergeType.RESTRICTIVE);

        Assertions.assertNotEquals(planned,
                new RowFilterSpec("7:4", "region = 'cn'", RowFilterMergeType.RESTRICTIVE),
                "a new policy version must compare unequal so the cache invalidates");
        Assertions.assertNotEquals(planned,
                new RowFilterSpec("7:3", "region = 'us'", RowFilterMergeType.RESTRICTIVE),
                "a rewritten predicate must compare unequal so the cache invalidates");
        Assertions.assertNotEquals(planned,
                new RowFilterSpec("7:3", "region = 'cn'", RowFilterMergeType.PERMISSIVE),
                "flipping restrictive to permissive changes which rows are visible");
    }

    @Test
    public void testRestrictiveIsTheDefaultShorthand() {
        Assertions.assertEquals(RowFilterMergeType.RESTRICTIVE,
                RowFilterSpec.restrictive("7:3", "region = 'cn'").getMergeType());
    }

    @Test
    public void testBlankFilterIsRejected() {
        // A blank predicate parses to nothing, which would widen access to the whole table instead of
        // restricting it - the failure mode that must never be silent.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> RowFilterSpec.restrictive("7:3", "   "));
        Assertions.assertThrows(NullPointerException.class,
                () -> RowFilterSpec.restrictive("7:3", null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> RowFilterSpec.restrictive("", "region = 'cn'"));
    }
}
