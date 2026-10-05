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

import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.VariableMgr;

import com.google.common.annotations.VisibleForTesting;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The zone history of the audit WRITER (mirrors
 * {@code AuditLogScanner#auditWriteZone}): every zone an audit row has actually been
 * RENDERED in, with the last instant that happened.
 *
 * <p>{@code audit_log.time} stores the writer's local wall clock, so a row written under
 * a zone that is no longer current is invisible to scan bounds rendered in the current
 * zone. The SPM capture therefore has to scan a window in EVERY zone that can own rows
 * before completing it - and a zone change BETWEEN two capture cycles is invisible to the
 * capture's own comparisons (round-39 #3: UTC -> -05:00 -> +08:00 between cycles, with a
 * short 12:00Z event stored as 07:00 outside both the UTC and the +08:00 renderings).
 * Sampling the writer HERE - at the moment a row is rendered - is what records the
 * intermediate zone; the per-FE snapshot travels to the leader through the
 * publication-horizon table's {@code writer_zones} column.
 *
 * <p>The registry is in-memory: after a restart the zones are re-recorded by whichever
 * FE still renders audit rows, and the cluster table's per-FE row keeps the previous
 * knowledge while it is being refreshed. Entries unused for longer than
 * {@link #RETAIN_MILLIS} are pruned: a zone can only render rows WHILE it is current,
 * publication lag is bounded by the publication fences, and the capture's own
 * late-completion lookback is a day - a zone silent for longer cannot render anything
 * the capture still scans.
 */
public final class AuditWriterZones {

    /** How long a silent zone stays part of the required scan set (see the javadoc). */
    public static final long RETAIN_MILLIS = 24 * 60 * 60 * 1000L;

    /** Hard bound of the registry: real deployments change the global time_zone rarely. */
    static final int MAX_ZONES = 32;

    private static final ConcurrentHashMap<String, Long> WRITER_ZONES = new ConcurrentHashMap<>();

    private AuditWriterZones() {
    }

    /**
     * Records one zone the audit writer rendered a row with (the last-use instant is kept
     * per zone). Beyond {@link #MAX_ZONES} the LEAST recently used zone is dropped: it is
     * the farthest from owning a row.
     *
     * @param zoneId   the zone ID the row was rendered in
     * @param atMillis the render instant (epoch millis)
     */
    @VisibleForTesting
    static void note(String zoneId, long atMillis) {
        if (zoneId == null || zoneId.isEmpty()) {
            return;
        }
        WRITER_ZONES.merge(zoneId, atMillis, Math::max);
        if (WRITER_ZONES.size() > MAX_ZONES) {
            String oldestZone = null;
            long oldestUsed = Long.MAX_VALUE;
            for (Map.Entry<String, Long> entry : WRITER_ZONES.entrySet()) {
                if (entry.getValue() < oldestUsed) {
                    oldestUsed = entry.getValue();
                    oldestZone = entry.getKey();
                }
            }
            if (oldestZone != null && !oldestZone.equals(zoneId)) {
                WRITER_ZONES.remove(oldestZone);
            }
        }
    }

    /** Records the zone the audit writer renders timestamps with RIGHT NOW. */
    public static void noteCurrentWriterZone() {
        note(currentWriterZoneId(), System.currentTimeMillis());
    }

    /**
     * The zone ID the audit writer renders timestamps with: the global session
     * time_zone (the context-less loader thread falls back to it, see
     * {@code AuditLogScanner#auditWriteZone} - keep the two in sync).
     */
    static String currentWriterZoneId() {
        return TimeUtils.getOrSystemTimeZone(
                VariableMgr.getDefaultSessionVariable().getTimeZone()).toZoneId().getId();
    }

    /** The zones used within {@link #RETAIN_MILLIS}, with their last-use instants. */
    public static Map<String, Long> snapshot() {
        long cutoff = System.currentTimeMillis() - RETAIN_MILLIS;
        Map<String, Long> zones = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : WRITER_ZONES.entrySet()) {
            if (entry.getValue() >= cutoff) {
                zones.put(entry.getKey(), entry.getValue());
            }
        }
        return zones;
    }

    /** {@link #snapshot()} as a plain zone-ID set (the capture's required-set input). */
    public static Set<String> zones() {
        return new LinkedHashSet<>(snapshot().keySet());
    }

    /**
     * Encodes the live registry for the per-FE horizon row: {@code zone=millis} pairs,
     * comma-separated. Zone IDs contain '/' and '_' but neither ',' nor '='.
     *
     * @return the encoded registry (empty when nothing was rendered yet)
     */
    public static String encode() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> entry : snapshot().entrySet()) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(entry.getKey()).append('=').append(entry.getValue());
        }
        return sb.toString();
    }

    /**
     * Parses an encoded registry (see {@link #encode}); every unparsable entry (a legacy
     * row, a NULL rendering) is skipped instead of failing the read.
     *
     * @param encoded the encoded registry, may be null / empty
     * @return the zone IDs found
     */
    public static Set<String> decode(String encoded) {
        Set<String> zones = new LinkedHashSet<>();
        if (encoded == null || encoded.isEmpty()) {
            return zones;
        }
        for (String entry : encoded.split(",")) {
            int eq = entry.lastIndexOf('=');
            if (eq <= 0) {
                continue;
            }
            String zoneId = entry.substring(0, eq).trim();
            if (zoneId.isEmpty()) {
                continue;
            }
            try {
                Long.parseLong(entry.substring(eq + 1).trim());
            } catch (NumberFormatException e) {
                continue;
            }
            zones.add(zoneId);
        }
        return zones;
    }

    /** For tests: forget every recorded zone. */
    @VisibleForTesting
    public static void resetForTest() {
        WRITER_ZONES.clear();
    }
}
