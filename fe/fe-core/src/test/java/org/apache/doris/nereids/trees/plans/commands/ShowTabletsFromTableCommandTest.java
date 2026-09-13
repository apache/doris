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

package org.apache.doris.nereids.trees.plans.commands;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Covers the mapping from the parsed LIMIT/OFFSET pair onto the number of rows SHOW TABLETS has
 * to keep. The end-to-end row selection is covered by the show_p0/test_show_tablet regression
 * suite; this test pins down the arithmetic, including the two cases that are easy to get wrong:
 * "no LIMIT clause" vs "LIMIT 0", and the overflow of LIMIT + OFFSET.
 */
public class ShowTabletsFromTableCommandTest {

    @Test
    public void testNoLimitClauseIsUnbounded() {
        // the parser passes -1 when the statement carries no LIMIT clause at all
        Assertions.assertEquals(Optional.empty(), ShowTabletsFromTableCommand.computeSizeLimit(-1, 0));
    }

    @Test
    public void testExplicitZeroLimitKeepsNoRow() {
        Assertions.assertEquals(Optional.of(0), ShowTabletsFromTableCommand.computeSizeLimit(0, 0));
    }

    @Test
    public void testZeroLimitWithOffsetStillKeepsNoRow() {
        // LIMIT 5, 0 keeps 5 rows here, but all of them are dropped by the OFFSET afterwards
        Assertions.assertEquals(Optional.of(5), ShowTabletsFromTableCommand.computeSizeLimit(0, 5));
    }

    @Test
    public void testLimitWithoutOffset() {
        Assertions.assertEquals(Optional.of(10), ShowTabletsFromTableCommand.computeSizeLimit(10, 0));
    }

    @Test
    public void testLimitAndOffsetAreAddedUp() {
        Assertions.assertEquals(Optional.of(13), ShowTabletsFromTableCommand.computeSizeLimit(3, 10));
    }

    @Test
    public void testLargeLimitIsClampedToIntRange() {
        Assertions.assertEquals(Optional.of(Integer.MAX_VALUE),
                ShowTabletsFromTableCommand.computeSizeLimit(3000000000L, 0));
    }

    @Test
    public void testHugeLimitAndOffsetDoNotOverflow() {
        // both operands come from Long.parseLong, so adding them before clamping would wrap
        // around into a negative size and later blow up in List#subList
        Assertions.assertEquals(Optional.of(Integer.MAX_VALUE),
                ShowTabletsFromTableCommand.computeSizeLimit(Long.MAX_VALUE, Long.MAX_VALUE));
    }
}
