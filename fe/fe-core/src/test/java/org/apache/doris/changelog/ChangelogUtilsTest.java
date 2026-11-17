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

package org.apache.doris.changelog;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.tso.TSOTimestamp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ChangelogUtilsTest {
    @Test
    void testEffectiveStartTso() {
        Assertions.assertEquals(101L, ChangelogUtils.effectiveStartTso(null, 100L, false));
        Assertions.assertEquals(101L, ChangelogUtils.effectiveStartTso(99L, 100L, false));
        Assertions.assertEquals(101L, ChangelogUtils.effectiveStartTso(100L, 100L, false));
        Assertions.assertEquals(101L, ChangelogUtils.effectiveStartTso(101L, 100L, true));
        Assertions.assertEquals(102L, ChangelogUtils.effectiveStartTso(102L, 100L, true));
        Assertions.assertEquals(TSOTimestamp.UNBOUNDED_TSO,
                ChangelogUtils.effectiveStartTso(null, TSOTimestamp.MAX_REAL_TSO, false));
    }

    @Test
    void testRejectExpiredMinDeltaOffset() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> ChangelogUtils.effectiveStartTso(99L, 100L, true));
        Assertions.assertTrue(exception.getMessage().contains(ChangelogUtils.CHANGELOG_OFFSET_EXPIRED));
        Assertions.assertThrows(AnalysisException.class,
                () -> ChangelogUtils.effectiveStartTso(100L, 100L, true));
    }
}
