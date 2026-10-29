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

import org.apache.doris.common.Config;
import org.apache.doris.nereids.stats.HboPlanStatisticsManager.PinnedHboStatistics;
import org.apache.doris.nereids.stats.HboPlanStatisticsManager.PinnedType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test of the removal an admin statement performs: two sessions can interleave, and a removal by
 * fingerprint alone would drop an entry which another session replaced with a fresh baseline in the
 * meantime - the entry the read side is about to apply.
 */
public class HboPinnedStatisticsRaceTest {

    private static final String STRUCT = "F{EqualTo(col(internal.db.t.b),lit(*))}(S{internal.db.t})";

    @Test
    public void testAReplacedEntryIsNotRemoved() {
        boolean persist = Config.hbo_persist_pinned_to_internal_db;
        Config.hbo_persist_pinned_to_internal_db = false;
        try {
            HboPlanStatisticsManager manager = new HboPlanStatisticsManager();
            manager.putPinnedPlanStatistics("fp", 10, PinnedType.EXACT, STRUCT,
                    HboPlanStatisticsManager.LiteralMode.NO_LITERAL);
            PinnedHboStatistics judged = manager.getAllPinnedPlanStatistics().get("fp");
            Assertions.assertNotNull(judged);
            Assertions.assertEquals(10, judged.getRows());

            // a second admin session publishes a fresh baseline for the same fingerprint
            manager.putPinnedPlanStatistics("fp", 20, PinnedType.EXACT, STRUCT,
                    HboPlanStatisticsManager.LiteralMode.NO_LITERAL);
            Assertions.assertFalse(manager.removePinnedPlanStatisticsIfUnchanged("fp", judged),
                    "the removal of an obsolete entry must not touch a fresh baseline");
            Assertions.assertEquals(20, manager.getAllPinnedPlanStatistics().get("fp").getRows());

            // the entry which is still stored is removed
            PinnedHboStatistics current = manager.getAllPinnedPlanStatistics().get("fp");
            Assertions.assertTrue(manager.removePinnedPlanStatisticsIfUnchanged("fp", current));
            Assertions.assertTrue(manager.getAllPinnedPlanStatistics().isEmpty());
        } finally {
            Config.hbo_persist_pinned_to_internal_db = persist;
        }
    }

}
