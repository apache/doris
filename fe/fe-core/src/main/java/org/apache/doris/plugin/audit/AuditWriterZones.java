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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
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
 * knowledge while it is being refreshed. A zone is evicted ONLY once the DURABLE CAPTURE
 * PROGRESS has passed its last use (round-40 #4): the capture may keep a window pending
 * for many hours (scanner failures, withheld publications), and a zone that still owns a
 * row inside such a window must stay part of the required scan set until the window
 * completed - the previous 24h time TTL (and the 32-zone cap) removed exactly that
 * knowledge, after which the leader could exhaust the window scanning only its own zones
 * and checkpoint past the pruned zone's row.
 */
public final class AuditWriterZones {

    /**
     * Soft bound of the registry: beyond it the least recently used zone the capture has
     * already passed is dropped. A zone the capture has NOT passed yet is never dropped
     * (see {@link #evictCoveredZones}) - the registry then grows by the number of zones
     * the cluster really rendered in, an inherently small set (the engine's time_zone
     * values).
     */
    static final int MAX_ZONES = 32;

    private static final Logger LOG = LogManager.getLogger(AuditWriterZones.class);

    private static final ConcurrentHashMap<String, Long> WRITER_ZONES = new ConcurrentHashMap<>();

    /**
     * The zones that have been part of at least one CONFIRMED shared report (round-41
     * #6). A freshly registered zone must not be evicted by the covered-through filter
     * before the capture ever LEARNED it: the row renderer can register and publish a
     * row under a new zone -05:00, a capture cycle can sample the previous shared zone
     * set and checkpoint past the render time, and only then does the reporter tick
     * refresh {@code coveredThrough} and encode the registry - which used to drop -05:00
     * before it was ever reported, so later windows no longer knew the row's rendering
     * and missed it permanently. Eviction (and the covered filter) therefore requires
     * BOTH: the zone was reported at least once AND capture progress passed its last
     * use. Reporting keeps the zone visible in every later shared snapshot until that
     * point.
     */
    private static final java.util.Set<String> REPORTED_ZONES = ConcurrentHashMap.newKeySet();

    private static final String SELECT_CAPTURE_WATERMARK_SQL =
            "SELECT `last_scan_timestamp` FROM `__internal_schema`.`spm_capture_checkpoint`"
                    + " WHERE `id` = 1";
    private static final int WATERMARK_READ_TIMEOUT_SECONDS = 10;

    /**
     * How often the durable capture watermark is re-read (see
     * {@link #refreshCaptureCoveredThrough}). The value only gates EVICTION, so a stale
     * (older) value merely keeps a zone longer - never drops one too early.
     */
    private static final long COVERED_THROUGH_REFRESH_MILLIS = 30_000L;

    /**
     * The start of the next window the SPM capture will scan
     * ({@code spm_capture_checkpoint.last_scan_timestamp}): every audit row RENDERED
     * before this instant belongs to a COMPLETED window. 0 = no progress known (nothing
     * is evicted / dropped).
     */
    private static volatile long coveredThrough = 0;
    private static volatile long coveredThroughReadAt = 0;

    /**
     * Test seam for the durable capture watermark (null in production): a scripted test
     * drives eviction deterministically without an internal table.
     */
    @VisibleForTesting
    static volatile java.util.function.LongSupplier captureCoveredThroughForTest;

    /**
     * Test seam for the zone the writer is about to render in (null in production): lets
     * a test switch the zone between a row's registration and its rendering.
     */
    @VisibleForTesting
    static volatile java.util.function.Supplier<String> currentWriterZoneForTest;

    private AuditWriterZones() {
    }

    /**
     * Records one zone the audit writer rendered a row with (the last-use instant is kept
     * per zone). A zone entering the registry is NOT yet "reported": it stays outside the
     * covered-through filter until {@link #markReported} confirms its first shared
     * snapshot (round-41 #6).
     *
     * @param zoneId   the zone ID the row was rendered in
     * @param atMillis the render instant (epoch millis)
     */
    @VisibleForTesting
    static void note(String zoneId, long atMillis) {
        if (zoneId == null || zoneId.isEmpty()) {
            return;
        }
        Long previous = WRITER_ZONES.get(zoneId);
        WRITER_ZONES.merge(zoneId, atMillis, Math::max);
        if (previous == null) {
            // fresh registration: its first shared report is still owed
            REPORTED_ZONES.remove(zoneId);
        }
        evictCoveredZones(zoneId);
    }

    /**
     * Marks the zones that were part of a CONFIRMED shared report (the horizon row was
     * read back with exactly this set, see AuditPublicationHorizon#reportLocalHorizon).
     * Only reported zones become eligible for covered-through eviction (round-41 #6).
     *
     * @param reportedZoneIds the zone IDs the confirmed report carried
     */
    public static void markReported(java.util.Collection<String> reportedZoneIds) {
        if (reportedZoneIds == null) {
            return;
        }
        for (String zoneId : reportedZoneIds) {
            if (zoneId != null && WRITER_ZONES.containsKey(zoneId)) {
                REPORTED_ZONES.add(zoneId);
            }
        }
    }

    /**
     * Whether a zone may be dropped once capture progress passed it: it must have been
     * part of a confirmed shared report first (round-41 #6) - otherwise the capture was
     * never told about its rendering.
     */
    private static boolean isCoveredAndReported(String zoneId, long lastUse, long covered) {
        return lastUse < covered && REPORTED_ZONES.contains(zoneId);
    }

    /**
     * Drops the least recently used zone(s) the durable capture has already passed
     * (round-40 #4) while the registry is over {@link #MAX_ZONES}. A zone whose last use
     * is NOT covered by capture progress is left in place: it can still own a row of an
     * uncompleted window, and reporting it is what makes the capture scan that window in
     * the zone before advancing the watermark.
     */
    private static void evictCoveredZones(String protectedZone) {
        long covered = captureCoveredThrough();
        while (WRITER_ZONES.size() > MAX_ZONES) {
            String candidate = null;
            long oldest = Long.MAX_VALUE;
            for (Map.Entry<String, Long> entry : WRITER_ZONES.entrySet()) {
                if (isCoveredAndReported(entry.getKey(), entry.getValue(), covered)
                        && entry.getValue() < oldest
                        && !entry.getKey().equals(protectedZone)) {
                    oldest = entry.getValue();
                    candidate = entry.getKey();
                }
            }
            if (candidate == null) {
                LOG.debug("audit writer zones: {} zones registered and none of them is"
                        + " covered by capture progress yet; keeping every zone that may"
                        + " still own an unconsumed row", WRITER_ZONES.size());
                return;
            }
            WRITER_ZONES.remove(candidate);
            REPORTED_ZONES.remove(candidate);
        }
    }

    /**
     * The zone an audit row of {@code atMillis} was most likely RENDERED in (round-43
     * #5): the registered zone whose last use is the FIRST one at or after the row's
     * instant. The registry records ONE last-use per zone, so the zone in effect when the
     * row was rendered is the earliest zone whose usage reaches that instant. Used by the
     * publish-fence visibility probe, which must ask for the audit time the row was
     * actually written with - rendering the bound in the CURRENT global zone turned an
     * observable row into an unconfirmable one after a `SET GLOBAL time_zone`.
     *
     * @param atMillis the row's event instant (epoch millis)
     * @return the rendering zone, or null when the registry cannot answer
     */
    static String zoneOfRenderTime(long atMillis) {
        if (atMillis <= 0) {
            return null;
        }
        String bestZone = null;
        long bestLastUse = Long.MAX_VALUE;
        for (Map.Entry<String, Long> entry : WRITER_ZONES.entrySet()) {
            long lastUse = entry.getValue() == null ? 0L : entry.getValue();
            if (lastUse >= atMillis && lastUse < bestLastUse) {
                bestLastUse = lastUse;
                bestZone = entry.getKey();
            }
        }
        return bestZone;
    }

    /**
     * The zone ID the audit writer renders timestamps with: the global session
     * time_zone (the context-less loader thread falls back to it, see
     * {@code AuditLogScanner#auditWriteZone} - keep the two in sync).
     */
    static String currentWriterZoneId() {
        java.util.function.Supplier<String> seam = currentWriterZoneForTest;
        if (seam != null) {
            return seam.get();
        }
        return TimeUtils.getOrSystemTimeZone(
                VariableMgr.getDefaultSessionVariable().getTimeZone()).toZoneId().getId();
    }

    /**
     * Re-reads {@link #coveredThrough} at most once per
     * {@link #COVERED_THROUGH_REFRESH_MILLIS} (called from the horizon reporter's tick -
     * never from the per-row render path). A failed read keeps the previous value: an
     * older watermark can only retain a zone too long, never drop one too early.
     */
    static void refreshCaptureCoveredThrough() {
        if (captureCoveredThroughForTest != null) {
            return; // scripted tests own the value
        }
        long now = System.currentTimeMillis();
        if (now - coveredThroughReadAt < COVERED_THROUGH_REFRESH_MILLIS) {
            return;
        }
        coveredThroughReadAt = now;
        try {
            Env env = Env.getCurrentEnv();
            if (env == null || !env.isReady()) {
                return; // no live internal table (unit tests / startup): keep the value
            }
            List<ResultRow> rows = StatisticsUtil.executeQuery(SELECT_CAPTURE_WATERMARK_SQL,
                    Collections.emptyMap(), WATERMARK_READ_TIMEOUT_SECONDS);
            if (rows == null || rows.isEmpty()) {
                return;
            }
            String text = rows.get(0).getWithDefault(0, "");
            if (text == null || text.isEmpty()) {
                return;
            }
            coveredThrough = Math.max(0L, Long.parseLong(text.trim()));
        } catch (Throwable t) {
            // keep the previous value: an unreadable watermark must not free zones whose
            // rows the capture may still owe a scan for
            LOG.debug("audit writer zones: cannot read the capture watermark: {}",
                    t.getMessage());
        }
    }

    /**
     * The durable capture watermark (see {@link #coveredThrough}): 0 when nothing was
     * captured yet. Cheap; the hot render path reads the cached value.
     */
    public static long captureCoveredThrough() {
        java.util.function.LongSupplier seam = captureCoveredThroughForTest;
        if (seam != null) {
            return Math.max(0L, seam.getAsLong());
        }
        return coveredThrough;
    }

    /** The zones whose last use is NOT covered by capture progress, with their last use. */
    public static Map<String, Long> snapshot() {
        long covered = captureCoveredThrough();
        Map<String, Long> zones = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : WRITER_ZONES.entrySet()) {
            if (isCoveredAndReported(entry.getKey(), entry.getValue(), covered)) {
                continue;
            }
            zones.put(entry.getKey(), entry.getValue());
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

    /** For tests: forget every recorded zone and the cached capture watermark. */
    @VisibleForTesting
    public static void resetForTest() {
        WRITER_ZONES.clear();
        REPORTED_ZONES.clear();
        coveredThrough = 0;
        coveredThroughReadAt = 0;
    }
}
