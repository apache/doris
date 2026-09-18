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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test of the parts of the hbo data state verdict which need no catalog: how a scan token is
 * built and parsed, that the baseline of a scan token does not take part in the fingerprint, and how
 * the recorded baseline of an entry is compared with the state of a query (the read side decision).
 *
 * <p>{@link HboStructFreshness#of} (the verdict of {@code HBO SHOW STATISTICS} /
 * {@code HBO DELETE STALE STATISTICS}, which reads the catalog) and the end to end behaviour are
 * covered by the regression suites {@code hbo_row_count_drift_test} and
 * {@code hbo_delete_stale_statistics_test}.
 */
public class HboStructFreshnessTest {
    private static final String FILTER_WITH_LITERAL =
            "F{EqualTo(col(internal.db.t.b),lit(1:INT))}(";
    private static final String FILTER_NO_LITERAL =
            "F{EqualTo(col(internal.db.t.b),lit(*))}(";

    @Test
    public void testScanTokenRoundTrip() {
        HboScanDescriptor descriptor = HboScanDescriptor.parse("internal.db.t,v3,r1000,p1/5");
        Assertions.assertEquals("internal.db.t", descriptor.getTable());
        Assertions.assertEquals(3, descriptor.getVisibleVersion());
        Assertions.assertEquals(1000, descriptor.getScanRows());
        Assertions.assertEquals(1, descriptor.getSelectedPartitions());
        Assertions.assertEquals(5, descriptor.getTotalPartitions());
        Assertions.assertFalse(descriptor.isPartitionSelectionComplete());
        Assertions.assertEquals("internal.db.t,v3,r1000,p1/5", descriptor.render());

        // an unknown number, and a selection which covers every partition, are not printed
        HboScanDescriptor partial = HboScanDescriptor.parse("internal.db.t,v3");
        Assertions.assertEquals(3, partial.getVisibleVersion());
        Assertions.assertEquals(HboScanDescriptor.UNKNOWN, partial.getScanRows());
        Assertions.assertTrue(partial.isPartitionSelectionComplete());
        Assertions.assertEquals("internal.db.t,v3", partial.render());

        // a hand written token has neither version nor rows and selects everything
        HboScanDescriptor bare = HboScanDescriptor.parse("internal.db.t");
        Assertions.assertFalse(bare.hasVisibleVersion());
        Assertions.assertFalse(bare.hasScanRows());
        Assertions.assertTrue(bare.isPartitionSelectionComplete());
        Assertions.assertEquals("internal.db.t", bare.render());
    }

    @Test
    public void testScanBaselineIsNotPartOfTheFingerprint() {
        // the same plan pattern on the same table hashes the same whatever the data state is
        String before = FILTER_WITH_LITERAL + "S{internal.db.t,v2,r1000})";
        String after = FILTER_WITH_LITERAL + "S{internal.db.t,v7,r1080,p1/5})";
        Assertions.assertEquals(GroupStructInfo.stripScanBaseline(before),
                GroupStructInfo.stripScanBaseline(after));
        Assertions.assertEquals(FILTER_WITH_LITERAL + "S{internal.db.t})", GroupStructInfo.stripScanBaseline(after));
        // the literals of the predicates are untouched
        Assertions.assertFalse(GroupStructInfo.stripScanBaseline(after).contains("lit(*)"));
        // a canonical string without any baseline is returned as it is
        String bare = FILTER_WITH_LITERAL + "S{internal.db.t})";
        Assertions.assertEquals(bare, GroupStructInfo.stripScanBaseline(bare));
    }

    @Test
    public void testVerdictOfTheReadSide() {
        // the recorded state is still the current one
        assertEquals(HboStructFreshness.STATE_LIVE,
                verdict(FILTER_WITH_LITERAL, "S{internal.db.t,v2,r1000})", "S{internal.db.t,v2,r1000})"));
        // a data change within the tolerance (10% by default) is a drift: the entry is applied
        HboStructFreshness drifted = HboStructFreshness.between(
                FILTER_WITH_LITERAL + "S{internal.db.t,v2,r1000})",
                FILTER_WITH_LITERAL + "S{internal.db.t,v3,r1050})");
        Assertions.assertEquals(HboStructFreshness.STATE_DRIFTED, drifted.getState());
        Assertions.assertTrue(drifted.isDrifted());
        Assertions.assertFalse(drifted.isStale());
        Assertions.assertEquals("drifted(rows=1050,rec=1000,+5.0%)", drifted.getSummary());
        Assertions.assertEquals("internal.db.t:v2,r1000", drifted.getRecorded());
        Assertions.assertEquals("internal.db.t:v3,r1050", drifted.getLive());
        Assertions.assertEquals("internal.db.t:v3,r1050,+5.0%", drifted.getLiveDetail());
        // a deletion is judged the same way (the size of the data is compared, not its direction)
        assertEquals(HboStructFreshness.STATE_DRIFTED,
                verdict(FILTER_WITH_LITERAL, "S{internal.db.t,v2,r1000})", "S{internal.db.t,v3,r950})"));
        // the tolerance is a ratio of the recorded row count, never an absolute number
        assertEquals(HboStructFreshness.STATE_DRIFTED,
                verdict(FILTER_WITH_LITERAL, "S{internal.db.t,v2,r400})", "S{internal.db.t,v3,r432})"));
        assertEquals(HboStructFreshness.STATE_STALE,
                verdict(FILTER_WITH_LITERAL, "S{internal.db.t,v2,r400})", "S{internal.db.t,v3,r450})"));
        // beyond the tolerance the recorded row count is not applied at all
        HboStructFreshness stale = HboStructFreshness.between(
                FILTER_WITH_LITERAL + "S{internal.db.t,v2,r1000})",
                FILTER_WITH_LITERAL + "S{internal.db.t,v3,r1510})");
        Assertions.assertEquals(HboStructFreshness.STATE_STALE, stale.getState());
        Assertions.assertTrue(stale.isStale());
        Assertions.assertEquals("stale(rows=1510,rec=1000,+51.0%)", stale.getSummary());
    }

    @Test
    public void testVerdictOfAnUnjudgeableEntry() {
        // an entry which records no data state at all (a hand written struct literal) is applied as
        // it always was: there is nothing to compare
        assertEquals(HboStructFreshness.STATE_UNKNOWN,
                verdict(FILTER_WITH_LITERAL, "S{internal.db.t})", "S{internal.db.t,v3,r1000})"));
        // a version, but no row count: the version check is the only signal left
        assertEquals(HboStructFreshness.STATE_LIVE,
                verdict(FILTER_WITH_LITERAL, "S{internal.db.t,v3})", "S{internal.db.t,v3,r1000})"));
        assertEquals(HboStructFreshness.STATE_STALE,
                verdict(FILTER_WITH_LITERAL, "S{internal.db.t,v3})", "S{internal.db.t,v4,r1000})"));
        // the row count of the current scan cannot be read (e.g. an empty table without statistics)
        assertEquals(HboStructFreshness.STATE_STALE,
                verdict(FILTER_WITH_LITERAL, "S{internal.db.t,v2,r1000})", "S{internal.db.t,v3})"));
        // an entry which describes another scan than the group cannot be judged
        assertEquals(HboStructFreshness.STATE_UNKNOWN, verdict(FILTER_WITH_LITERAL,
                "S{internal.db.other,v2,r1000})", "S{internal.db.t,v2,r1000})"));
        assertEquals(HboStructFreshness.STATE_UNKNOWN, verdict(FILTER_WITH_LITERAL,
                "S{internal.db.t,v2,r1000};S{internal.db.other,v2,r1000})", "S{internal.db.t,v2,r1000})"));
    }

    @Test
    public void testStrictModeDisablesTheTolerance() {
        double previous = Config.hbo_row_count_change_ratio;
        try {
            Config.hbo_row_count_change_ratio = 0;
            // the same data state is still applied ...
            assertEquals(HboStructFreshness.STATE_LIVE,
                    verdict(FILTER_WITH_LITERAL, "S{internal.db.t,v2,r1000})", "S{internal.db.t,v2,r1000})"));
            // ... but any data change is a miss, even a change of one row
            assertEquals(HboStructFreshness.STATE_STALE,
                    verdict(FILTER_WITH_LITERAL, "S{internal.db.t,v2,r1000})", "S{internal.db.t,v3,r1001})"));
            assertEquals(HboStructFreshness.STATE_STALE,
                    verdict(FILTER_WITH_LITERAL, "S{internal.db.t,v2,r1000})", "S{internal.db.t,v3,r999})"));
        } finally {
            Config.hbo_row_count_change_ratio = previous;
        }
        assertEquals(HboStructFreshness.STATE_DRIFTED,
                verdict(FILTER_WITH_LITERAL, "S{internal.db.t,v2,r1000})", "S{internal.db.t,v3,r1001})"));
    }

    @Test
    public void testRowCountOfAnEmptyTable() {
        // nothing was recorded: any row is a change far beyond the tolerance
        assertEquals(HboStructFreshness.STATE_DRIFTED,
                verdict(FILTER_WITH_LITERAL, "S{internal.db.t,v2,r0})", "S{internal.db.t,v3,r0})"));
        assertEquals(HboStructFreshness.STATE_STALE,
                verdict(FILTER_WITH_LITERAL, "S{internal.db.t,v2,r0})", "S{internal.db.t,v3,r1})"));
    }

    @Test
    public void testTheLiteralModeDoesNotAffectTheVerdict() {
        // the scan tokens of both literal modes of a filter node describe the same scan, so the
        // verdict of the constant agnostic entry is the same as the one of the exact entry
        assertEquals(HboStructFreshness.STATE_DRIFTED, verdict(FILTER_NO_LITERAL,
                "S{internal.db.t,v2,r1000})", "S{internal.db.t,v3,r1050})"));
    }

    private static String verdict(String prefix, String recordedScan, String liveScan) {
        return HboStructFreshness.between(prefix + recordedScan, prefix + liveScan).getState();
    }

    private static void assertEquals(String expected, String actual) {
        Assertions.assertEquals(expected, actual, actual);
    }
}
