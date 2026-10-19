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

package org.apache.doris.nereids.stats;

import org.apache.doris.nereids.trees.plans.JoinType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test of the upper bound an injected join fan-out is capped with: a full outer join also keeps the
 * rows of both sides which found no partner, so it can produce more rows than the cartesian product of
 * its inputs - an entry measured on such a join must not be capped below the row count it measured.
 */
public class HboJoinExpansionTest {

    @Test
    public void testAFullOuterJoinKeepsItsUnmatchedRows() {
        // one row on each side which do not match: the full outer join produces two rows although the
        // cartesian product is one, so an injected fan-out of 2 is not capped to 1
        Assertions.assertEquals(2.0,
                HboStatsCalculator.expansionUpperBound(JoinType.FULL_OUTER_JOIN, 1, 1));
        // an inner join can never exceed the cartesian product
        Assertions.assertEquals(1.0,
                HboStatsCalculator.expansionUpperBound(JoinType.INNER_JOIN, 1, 1));
        Assertions.assertEquals(6.0,
                HboStatsCalculator.expansionUpperBound(JoinType.INNER_JOIN, 2, 3));
        Assertions.assertEquals(6.0,
                HboStatsCalculator.expansionUpperBound(JoinType.FULL_OUTER_JOIN, 2, 3));
        // when the sum is larger than the product, the sum is the bound of a full outer join
        Assertions.assertEquals(101.0,
                HboStatsCalculator.expansionUpperBound(JoinType.FULL_OUTER_JOIN, 1, 100));
    }
}
