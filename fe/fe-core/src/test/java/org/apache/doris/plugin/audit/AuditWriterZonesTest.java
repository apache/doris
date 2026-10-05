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
 */
public class AuditWriterZonesTest {

    @Test
    public void testZoneHistoryKeepsTheLastUsePerZoneAndPrunesTheRetained() {
        AuditWriterZones.resetForTest();
        try {
            long now = System.currentTimeMillis();
            AuditWriterZones.note("UTC", now);
            AuditWriterZones.note("Asia/Tokyo", now - 60_000L);
            AuditWriterZones.note("UTC", now + 1_000L);
            // an epoch longer than the retention cannot render anything the capture still
            // scans: it is pruned from the required set
            AuditWriterZones.note("America/New_York", now - AuditWriterZones.RETAIN_MILLIS - 1);

            Map<String, Long> zones = AuditWriterZones.snapshot();
            Assertions.assertEquals(Set.of("UTC", "Asia/Tokyo"), zones.keySet(),
                    "live zones stay, the pruned epoch is dropped: " + zones);
            Assertions.assertEquals(now + 1_000L, zones.get("UTC"),
                    "the LATEST use wins per zone");
            Assertions.assertEquals(Set.of("UTC", "Asia/Tokyo"), AuditWriterZones.zones());

            // empty / null zone ids are ignored
            AuditWriterZones.note(null, now);
            AuditWriterZones.note("", now);
            Assertions.assertEquals(2, AuditWriterZones.snapshot().size());
        } finally {
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

    @Test
    public void testRegistryBoundDropsTheLeastRecentlyUsedZone() {
        AuditWriterZones.resetForTest();
        try {
            long now = System.currentTimeMillis();
            for (int i = 0; i < AuditWriterZones.MAX_ZONES + 1; i++) {
                AuditWriterZones.note("zone-" + i, now + i);
            }
            Set<String> zones = AuditWriterZones.zones();
            Assertions.assertEquals(AuditWriterZones.MAX_ZONES, zones.size(),
                    "the registry stays bounded: " + zones.size());
            Assertions.assertFalse(zones.contains("zone-0"),
                    "the least recently used zone is the one dropped");
            Assertions.assertTrue(zones.contains("zone-" + AuditWriterZones.MAX_ZONES),
                    "the newest zone is retained");
        } finally {
            AuditWriterZones.resetForTest();
        }
    }
}
