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

package org.apache.doris.common.proc;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for CurrentQueryStatisticsProcDir progress formatting.
 * Tests the formatProgress method directly — no mocking or reflection required.
 */
public class CurrentQueryStatisticsProcDirTest {

    @Test
    public void testProgressNormal() {
        // 7 out of 20 tasks finished = 35.0%
        Assert.assertEquals("35.0%", CurrentQueryStatisticsProcDir.formatProgress(20, 7));
    }

    @Test
    public void testProgressAllFinished() {
        // 8 out of 8 = 100.0%
        Assert.assertEquals("100.0%", CurrentQueryStatisticsProcDir.formatProgress(8, 8));
    }

    @Test
    public void testProgressOneThird() {
        // 1 out of 3 = 33.3%
        Assert.assertEquals("33.3%", CurrentQueryStatisticsProcDir.formatProgress(3, 1));
    }

    @Test
    public void testProgressTwoThirds() {
        // 2 out of 3 = 66.7%
        Assert.assertEquals("66.7%", CurrentQueryStatisticsProcDir.formatProgress(3, 2));
    }

    @Test
    public void testProgressZeroPercent() {
        // 0 out of 5 = 0.0%
        Assert.assertEquals("0.0%", CurrentQueryStatisticsProcDir.formatProgress(5, 0));
    }

    @Test
    public void testProgressZeroTotal() {
        // total = 0, finished = 0 → "0.0%" (no division by zero)
        Assert.assertEquals("0.0%", CurrentQueryStatisticsProcDir.formatProgress(0, 0));
    }

    @Test
    public void testProgressFinishedExceedsTotal() {
        // Defensive: if finished > total, still returns a percentage (may exceed 100%)
        Assert.assertEquals("200.0%", CurrentQueryStatisticsProcDir.formatProgress(5, 10));
    }

    @Test
    public void testProgressNegativeTotal() {
        // total < 0 → returns "0.0%"
        Assert.assertEquals("0.0%", CurrentQueryStatisticsProcDir.formatProgress(-1, 5));
    }

    @Test
    public void testProgressLargeValues() {
        // Verify no overflow with large numbers
        Assert.assertEquals("50.0%",
                CurrentQueryStatisticsProcDir.formatProgress(Integer.MAX_VALUE, Integer.MAX_VALUE / 2));
    }

    @Test
    public void testProgressFractional() {
        // 1 out of 7 = 14.3% (14.2857... rounds to 14.3)
        Assert.assertEquals("14.3%", CurrentQueryStatisticsProcDir.formatProgress(7, 1));
    }
}
