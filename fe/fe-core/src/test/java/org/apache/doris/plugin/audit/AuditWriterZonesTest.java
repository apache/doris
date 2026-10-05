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
 */
public class AuditWriterZonesTest {

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
            // uncompleted row any more, however recently it was pruned
            AuditWriterZones.note("America/New_York", now - 120_000L);

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

            // once the durable progress passes the zone's last use, it is dropped
            AuditWriterZones.captureCoveredThroughForTest = () -> now;
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
            // ... but a zone the capture has NOT passed is never dropped, even when it is
            // the least recently used one of the surviving set
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
            AuditWriterZones.captureCoveredThroughForTest = () -> now + 1_000L;
            Assertions.assertEquals(Set.of(), AuditWriterZones.zones());
        } finally {
            AuditWriterZones.captureCoveredThroughForTest = null;
            AuditWriterZones.resetForTest();
        }
    }
}
