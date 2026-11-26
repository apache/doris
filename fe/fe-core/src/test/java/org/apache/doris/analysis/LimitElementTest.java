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

package org.apache.doris.analysis;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class LimitElementTest {

    private static List<Integer> rows(int n) {
        List<Integer> rows = Lists.newArrayList();
        for (int i = 0; i < n; i++) {
            rows.add(i);
        }
        return rows;
    }

    @Test
    public void testWindowWithinIntRange() {
        Assertions.assertEquals(Lists.newArrayList(0, 1, 2), new LimitElement(0, 3).applyTo(rows(5)));
        Assertions.assertEquals(Lists.newArrayList(2, 3), new LimitElement(2, 2).applyTo(rows(5)));
        Assertions.assertEquals(Lists.newArrayList(3, 4), new LimitElement(3, 100).applyTo(rows(5)));
    }

    // A limit larger than Integer.MAX_VALUE must select every remaining row. Narrowing the
    // offset+limit sum to int first truncates 4294967296 to 0, which silently returned nothing.
    @Test
    public void testLimitAboveIntMaxReturnsAllRows() {
        Assertions.assertEquals(rows(5), new LimitElement(0, 4294967296L).applyTo(rows(5)));
        Assertions.assertEquals(rows(5), new LimitElement(0, Long.MAX_VALUE).applyTo(rows(5)));
    }

    // 3000000000 narrows to a negative int, which made subList throw IndexOutOfBoundsException.
    @Test
    public void testLimitTruncatingToNegativeIntReturnsAllRows() {
        Assertions.assertEquals(rows(5), new LimitElement(0, 3000000000L).applyTo(rows(5)));
    }

    // The offset+limit addition itself can overflow long; the window must still be clamped.
    @Test
    public void testOffsetPlusLimitOverflowIsClamped() {
        Assertions.assertEquals(Lists.newArrayList(1, 2, 3, 4),
                new LimitElement(1, Long.MAX_VALUE).applyTo(rows(5)));
    }

    // An offset past the end selects nothing, whatever its magnitude, and never throws.
    @Test
    public void testOffsetBeyondEndReturnsEmpty() {
        Assertions.assertTrue(new LimitElement(5, 10).applyTo(rows(5)).isEmpty());
        Assertions.assertTrue(new LimitElement(3000000000L, 10).applyTo(rows(5)).isEmpty());
        Assertions.assertTrue(new LimitElement(Long.MAX_VALUE, 10).applyTo(rows(5)).isEmpty());
    }

    // Without a limit the window runs from the offset to the end.
    @Test
    public void testNoLimitRunsToEnd() {
        LimitElement noLimit = new LimitElement(2, -1);
        Assertions.assertFalse(noLimit.hasLimit());
        Assertions.assertEquals(Lists.newArrayList(2, 3, 4), noLimit.applyTo(rows(5)));
    }

    @Test
    public void testEmptyInput() {
        Assertions.assertTrue(new LimitElement(0, 10).applyTo(rows(0)).isEmpty());
        Assertions.assertTrue(new LimitElement(3000000000L, 10).applyTo(rows(0)).isEmpty());
    }
}
