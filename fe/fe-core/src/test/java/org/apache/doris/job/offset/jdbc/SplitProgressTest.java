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

package org.apache.doris.job.offset.jdbc;

import org.apache.doris.job.offset.jdbc.JdbcSourceOffsetProvider.SplitProgress;

import org.junit.Assert;
import org.junit.Test;

public class SplitProgressTest {

    @Test
    public void testDefaultStateIsAllNull() {
        SplitProgress p = new SplitProgress();
        Assert.assertNull(p.getCurrentSplittingTable());
        Assert.assertNull(p.getNextSplitStart());
        Assert.assertNull(p.getNextSplitId());
    }

    @Test
    public void testCopyDeepClonesNextSplitStart() {
        SplitProgress original = new SplitProgress();
        original.setCurrentSplittingTable("db.tbl_a");
        original.setNextSplitStart(new Object[]{100L});
        original.setNextSplitId(5);

        SplitProgress copy = original.copy();
        Assert.assertEquals("db.tbl_a", copy.getCurrentSplittingTable());
        Assert.assertArrayEquals(new Object[]{100L}, copy.getNextSplitStart());
        Assert.assertEquals(Integer.valueOf(5), copy.getNextSplitId());

        // Mutating copy.nextSplitStart must not affect the original (deep copy).
        copy.getNextSplitStart()[0] = 999L;
        Assert.assertEquals(100L, original.getNextSplitStart()[0]);
    }

    @Test
    public void testCopyHandlesNullFields() {
        SplitProgress original = new SplitProgress();
        SplitProgress copy = original.copy();
        Assert.assertNull(copy.getCurrentSplittingTable());
        Assert.assertNull(copy.getNextSplitStart());
        Assert.assertNull(copy.getNextSplitId());
    }
}
