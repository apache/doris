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

package org.apache.doris.plugin.audit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

/**
 * Round-39 #3: the audit WRITER's zone history - every zone an audit row was actually
 * RENDERED in, sampled at the render - is what lets the SPM capture require a window pass
 * in zones it never observed itself (two zone changes between two cycles).
 *
 * Round-40 #4: eviction is tied to the DURABLE CAPTURE PROGRESS
 * ({@code spm_capture_checkpoint.last_scan_timestamp}), not to an age TTL: a zone that
 * still owns a row of an uncompleted window must stay in the required set however old it
 * is, and may only be dropped once the capture watermark has passed its last use.
 *
 * Round-41 #6: covered-through ALONE is not enough - a zone enters the set when a row is
 * rendered in it, but the shared report that lets every node count on the zone happens
 * one REPORTER TICK later. A capture cycle that sampled the previous shared set and then
 * checkpointed past the render instant used to drop the zone, so a later window no longer
 * required a pass in it and the row was never captured. The zone stays required until a
 * CONFIRMED shared report included it.
 */
public class AuditWriterZonesTest {

    /**
     * round-43 #5: the zone an audit row of one instant was RENDERED in is the registered
     * zone whose last use is the FIRST one at or after that instant. The publish-fence
     * probe renders its bound in exactly that zone: the audit table stores the writer's
     * LOCAL wall clock, so after a {@code SET GLOBAL time_zone} a bound rendered in the
     * current zone is hours away from the stored one and the probe can never confirm a
     * (perfectly visible) row.
     */
    @Test
    public void testZoneOfRenderTimePicksTheZoneInEffectAtTheInstant() {
        AuditWriterZones.resetForTest();
        try {
            long t0 = 1_780_000_000_000L;
            AuditWriterZones.note("UTC", t0 + 10_000L);            // rendered until t0+10s
            AuditWriterZones.note("Asia/Tokyo", t0 + 40_000L);     // ... then in Tokyo
            AuditWriterZones.note("America/New_York", t0 + 90_000L);

            Assertions.assertEquals("UTC", AuditWriterZones.zoneOfRenderTime(t0 + 9_000L),
                    "a row of t0+9s was rendered under UTC (the first use reaching past it)");
            Assertions.assertEquals("Asia/Tokyo",
                    AuditWriterZones.zoneOfRenderTime(t0 + 15_000L),
                    "after the UTC epoch ended, the earliest still-reaching use is Tokyo's");
            Assertions.assertEquals("America/New_York",
                    AuditWriterZones.zoneOfRenderTime(t0 + 50_000L),
                    "a row between the Tokyo and New York epochs belongs to New York");
            Assertions.assertNull(AuditWriterZones.zoneOfRenderTime(0),
                    "no instant = no answer (the caller falls back to the default zone)");
        } finally {
            AuditWriterZones.resetForTest();
        }
    }

    @Test
    public void testZoneHistoryKeepsTheLastUsePerZoneAndDropsCoveredOnes() {
        AuditWriterZones.resetForTest();
        try {
            long now = System.currentTimeMillis();
            // the durable capture watermark is 90s old: it has passed the oldest epoch
            // but neither of the two live ones
            AuditWriterZones.captureCoveredThroughForTest = () -> now - 90_000L;
            AuditWriterZones.note("UTC", now);
            AuditWriterZones.note("Asia/Tokyo", now - 60_000L);
            AuditWriterZones.note("UTC", now + 1_000L);
            // an epoch whose rows a COMPLETED window already consumed: it can own no
            // uncompleted row any more - but it is only evictable once a CONFIRMED
            // shared report (round-41 #6) has carried it to the other nodes
            AuditWriterZones.note("America/New_York", now - 120_000L);
            Assertions.assertTrue(AuditWriterZones.snapshot().containsKey("America/New_York"),
                    "an uncovered report obligation keeps the covered zone in the set");

            // a CONFIRMED shared report carrying the covered epoch retires it (round-41 #6)
            AuditWriterZones.markReported(Set.of("UTC", "Asia/Tokyo", "America/New_York"));

            Map<String, Long> zones = AuditWriterZones.snapshot();
            Assertions.assertEquals(Set.of("UTC", "Asia/Tokyo"), zones.keySet(),
                    "zones the capture has not passed stay, the covered epoch is dropped: "
                            + zones);
            Assertions.assertEquals(now + 1_000L, zones.get("UTC"),
                    "the LATEST use wins per zone");
            Assertions.assertEquals(Set.of("UTC", "Asia/Tokyo"), AuditWriterZones.zones());

            // empty / null zone ids are ignored
            AuditWriterZones.note(null, now);
            AuditWriterZones.note("", now);
            Assertions.assertEquals(2, AuditWriterZones.snapshot().size());
        } finally {
            AuditWriterZones.captureCoveredThroughForTest = null;
            AuditWriterZones.resetForTest();
        }
    }

    /**
     * round-41 #6: a FRESH zone whose render instant the capture has already checkpointed
     * past must still be listed. The zone set enters the report the moment a row renders
     * in it, but the CONFIRMED shared report that lets every node count on the zone comes
     * one reporter tick later; a capture cycle that sampled the previous shared set,
     * scanned (nothing required in the new zone), and checkpointed past the render used to
     * drop the zone before it ever surfaced - later windows then never required a pass in
     * it and the row stayed invisible.
     */
    @Test
    public void testFreshZoneIsKeptUntilItsFirstConfirmedReport() {
        AuditWriterZones.resetForTest();
        try {
            long now = System.currentTimeMillis();
            // the capture watermark is already PAST the render instant below
            AuditWriterZones.captureCoveredThroughForTest = () -> now + 1_000L;
            AuditWriterZones.note("America/New_York", now - 5_000L);
            Assertions.assertEquals(Set.of("America/New_York"), AuditWriterZones.zones(),
                    "an unreported zone must survive the covered filter");

            AuditWriterZones.markReported(Set.of("America/New_York"));
            Assertions.assertEquals(Set.of(), AuditWriterZones.zones(),
                    "once its confirmed report carried it, the covered zone is covered");

            // registry pressure may evict a covered+reported zone; a LATER render
            // re-registers it as fresh and the report obligation starts over
            for (int i = 0; i < AuditWriterZones.MAX_ZONES + 5; i++) {
                AuditWriterZones.note("zone-" + i, now - 3_000L);
                AuditWriterZones.markReported(Set.of("zone-" + i));
            }
            AuditWriterZones.note("America/New_York", now - 6_000L);
            Assertions.assertEquals(Set.of("America/New_York"), AuditWriterZones.zones(),
                    "a re-registered zone owes a fresh report although it is covered: "
                            + AuditWriterZones.zones());
        } finally {
            AuditWriterZones.captureCoveredThroughForTest = null;
            AuditWriterZones.resetForTest();
        }
    }

    /**
     * Round-40 #4: an OLD zone that still owns rows of an uncompleted window is not
     * dropped. A pending capture window can survive scanner failures / withheld
     * publications for many hours, so the previous 24h TTL could prune a zone whose rows
     * the window still scans - the follower then reported the pruned set and a leader
     * scanning only its own zones exhausted the window and checkpointed past the row.
     */
    @Test
    public void testZoneStillOwningUnconsumedRowsSurvivesItsAge() {
        AuditWriterZones.resetForTest();
        try {
            long now = System.currentTimeMillis();
            // the capture watermark is far BEHIND the zone's last use (the window owning
            // its rows has not completed)
            AuditWriterZones.captureCoveredThroughForTest = () -> now - 30 * 60 * 60 * 1000L;
            long lastUse = now - 25 * 60 * 60 * 1000L; // 25 hours ago: older than any TTL
            AuditWriterZones.note("America/New_York", lastUse);
            Assertions.assertEquals(Set.of("America/New_York"), AuditWriterZones.zones(),
                    "a zone the capture has not passed must stay required, however old it is");

            // round-41 #6: the covered filter alone must NOT drop it - the shared report
            // that lets every node settle the zone may still be in flight
            AuditWriterZones.captureCoveredThroughForTest = () -> now;
            Assertions.assertEquals(Set.of("America/New_York"), AuditWriterZones.zones(),
                    "a covered but UNREPORTED zone still owes its report");

            // once the durable progress passes the zone's last use AND a confirmed report
            // carried it, it is dropped
            AuditWriterZones.markReported(Set.of("America/New_York"));
            Assertions.assertEquals(Set.of(), AuditWriterZones.zones(),
                    "a zone covered by completed windows stops being required");
        } finally {
            AuditWriterZones.captureCoveredThroughForTest = null;
            AuditWriterZones.resetForTest();
        }
    }

    @Test
    public void testEncodeDecodeRoundTripToleratesUnparsableEntries() {
        Assertions.assertEquals(Set.of(), AuditWriterZones.decode(null));
        Assertions.assertEquals(Set.of(), AuditWriterZones.decode(""));
        Assertions.assertEquals(Set.of("UTC", "Asia/Tokyo"),
                AuditWriterZones.decode("UTC=1000,Asia/Tokyo=2000"));
        // a legacy / NULL rendering never fails the read
        Assertions.assertEquals(Set.of("UTC"),
                AuditWriterZones.decode("NULL,UTC=1000,garbage,=5,x="));
    }

    /**
     * The soft bound only drops zones the capture has already passed; a zone that still
     * owns an unconsumed row is never evicted, even when the cap would want it (round-40
     * #4 - the old LRU dropped the very zone the pending window needed).
     */
    @Test
    public void testRegistryBoundNeverDropsAZoneTheCaptureStillNeeds() {
        AuditWriterZones.resetForTest();
        try {
            long now = System.currentTimeMillis();
            long covered = now - 5_000L;
            AuditWriterZones.captureCoveredThroughForTest = () -> covered;
            // covered epochs beyond the cap are evictable ...
            for (int i = 0; i < 10; i++) {
                AuditWriterZones.note("covered-" + i, covered - 1_000L - i);
            }
            // ... but only AFTER a confirmed shared report named them (round-41 #6): the
            // report obligation of a just-registered zone outlives the covered filter
            for (int i = 0; i < 10; i++) {
                AuditWriterZones.markReported(Set.of("covered-" + i));
            }
            // a zone the capture has NOT passed is never dropped, even when it is the
            // least recently used one of the surviving set
            AuditWriterZones.note("needed-old", covered + 1_000L);
            for (int i = 0; i < 30; i++) {
                AuditWriterZones.note("zone-" + i, now + i);
            }
            Set<String> zones = AuditWriterZones.zones();
            Assertions.assertTrue(zones.contains("needed-old"),
                    "the uncovered zone survives the eviction sweep: " + zones);
            Assertions.assertFalse(zones.contains("covered-0"),
                    "covered zones are not part of the required set any more");
            Assertions.assertTrue(zones.size() <= AuditWriterZones.MAX_ZONES,
                    "the covered entries beyond the cap are dropped, the uncovered ones"
                            + " stay: " + zones.size());

            // once the capture has passed them too, no zone is required any more
            AuditWriterZones.markReported(AuditWriterZones.zones());
            AuditWriterZones.captureCoveredThroughForTest = () -> now + 1_000L;
            Assertions.assertEquals(Set.of(), AuditWriterZones.zones());
        } finally {
            AuditWriterZones.captureCoveredThroughForTest = null;
            AuditWriterZones.resetForTest();
        }
    }

    /**
     * round-44 #12: the eviction floor must be ROLLBACK-SAFE. The checkpoint is
     * append-only and the capture can REWIND to an earlier pending window at any time, so
     * the floor is the MINIMUM of the token-greatest progress and the most-behind pending
     * window's start: evicting by a merely-newest watermark dropped a zone whose render
     * instant sits inside the rewound window - the reviewer's -05:00 zone for a 09:05 row
     * was omitted from the follower's next report exactly when the leader adopted the
     * [09:00,12:00) window and no longer knew it had to scan -05:00.
     */
    @Test
    public void testEvictionFloorIsRollbackSafe() {
        // no durable progress (empty store): the previous floor is kept
        Assertions.assertEquals(-1L, AuditWriterZones.resolvedCoveredThrough(null, null));
        Assertions.assertEquals(-1L, AuditWriterZones.resolvedCoveredThrough(null, 42L));
        // no pending window: the resolved progress stands alone
        Assertions.assertEquals(99L, AuditWriterZones.resolvedCoveredThrough(99L, null));
        Assertions.assertEquals(99L, AuditWriterZones.resolvedCoveredThrough(99L, 0L));
        // a readable pending window CAPS the floor at its start, even when a newer
        // progress row exists (a stale-but-readable progress row never proves the pending
        // window was consumed)
        Assertions.assertEquals(50L, AuditWriterZones.resolvedCoveredThrough(99L, 50L));
        // a candidate at/after the progress cannot RAISE the floor
        Assertions.assertEquals(99L, AuditWriterZones.resolvedCoveredThrough(99L, 120L));
    }

    /**
     * The consequence of the capped floor: while the pending window [09:00, 12:00) is
     * unconsumed, the zone that rendered the 09:05 row (last use 09:05) must NOT be
     * evicted although the stale progress row claims 12:10 - the rewound pass still has
     * to visit it.
     */
    @Test
    public void testAZoneInsideARewindableWindowIsNotEvicted() {
        AuditWriterZones.resetForTest();
        try {
            long staleProgress = 12 * 3_600_000L;
            long pendingWindowStart = 9 * 3_600_000L;
            AuditWriterZones.captureCoveredThroughForTest =
                    () -> AuditWriterZones.resolvedCoveredThrough(staleProgress,
                            pendingWindowStart);
            AuditWriterZones.note("America/New_York", 9 * 3_600_000L + 300_000L); // 09:05
            AuditWriterZones.markReported(Set.of("America/New_York"));
            Assertions.assertEquals(Set.of("America/New_York"), AuditWriterZones.zones(),
                    "a zone whose last render sits inside the rewindable window stays"
                            + " required: " + AuditWriterZones.zones());

            // once the pending window completed AND the progress passed the last use, the
            // zone is evicted normally
            AuditWriterZones.captureCoveredThroughForTest =
                    () -> AuditWriterZones.resolvedCoveredThrough(staleProgress, null);
            Assertions.assertEquals(Set.of(), AuditWriterZones.zones(),
                    "with no pending window the resolved progress evicts the covered zone");
        } finally {
            AuditWriterZones.captureCoveredThroughForTest = null;
            AuditWriterZones.resetForTest();
        }
    }
}
