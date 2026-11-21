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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.lance.Version;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.regex.Pattern;

/** Resolves Doris time-travel selectors on the main chain to immutable Lance version IDs. */
final class LanceSnapshotResolver {
    /** The time formats {@link #parseTimestamp} accepts, for error messages. */
    static final String TIMESTAMP_FORMATS = "'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd HH:mm:ss.SSS'";

    private static final DateTimeFormatter SECOND_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter MILLISECOND_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final Pattern VERSION_NUMBER = Pattern.compile("[+-]?[0-9]+");

    /**
     * Doris's session time-zone aliases: {@link ZoneId#SHORT_IDS} overlaid with the four Doris overrides,
     * matched case-insensitively because {@code SET time_zone} stores an alias as written. The connector
     * cannot read the engine's map, so it is rebuilt here the way the Paimon connector rebuilds it.
     */
    private static final Map<String, String> SESSION_TIME_ZONE_ALIASES;

    static {
        Map<String, String> aliases = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        aliases.putAll(ZoneId.SHORT_IDS);
        aliases.put("CST", "Asia/Shanghai");
        aliases.put("PRC", "Asia/Shanghai");
        aliases.put("UTC", "UTC");
        aliases.put("GMT", "UTC");
        SESSION_TIME_ZONE_ALIASES = Collections.unmodifiableMap(aliases);
    }

    private LanceSnapshotResolver() {
    }

    static long parseVersion(String value) {
        final long version;
        try {
            version = Long.parseLong(value);
        } catch (NumberFormatException e) {
            // Deliberately not chained: the NumberFormatException text would replace this one.
            throw new DorisConnectorException(isVersionNumber(value)
                    ? "Lance FOR VERSION AS OF version " + value + " is out of range"
                    : "Lance FOR VERSION AS OF requires a numeric version, but was '" + value + "'");
        }
        if (version <= 0) {
            throw new DorisConnectorException(
                    "Lance FOR VERSION AS OF requires a positive version, but was " + version);
        }
        return version;
    }

    /**
     * Whether a {@code FOR VERSION AS OF} value is a version number. A signed number counts as one, so
     * that {@code '-1'} is reported as an invalid version.
     */
    private static boolean isVersionNumber(String value) {
        return VERSION_NUMBER.matcher(value).matches();
    }

    /** The session's time zone, resolved the way the engine resolves {@code time_zone}. */
    static ZoneId sessionZone(String sessionTimeZone) {
        try {
            return ZoneId.of(sessionTimeZone, SESSION_TIME_ZONE_ALIASES);
        } catch (DateTimeException e) {
            throw new DorisConnectorException("Session time zone '" + sessionTimeZone
                    + "' is not a standard zone id and cannot be used for Lance FOR TIME AS OF; use a"
                    + " standard IANA zone id such as 'Asia/Shanghai' or 'UTC', or FOR VERSION AS OF", e);
        }
    }

    /**
     * Parses a time-travel timestamp in {@code zone}, with second or millisecond precision, into epoch
     * milliseconds; empty if the value is in neither format. Commit times are compared in full, so a commit
     * later within the requested millisecond is not selected. As Doris has always parsed this value, a time
     * before 1970 reads as unparsable, and so does a year too large for epoch milliseconds.
     */
    static OptionalLong parseTimestamp(String value, ZoneId zone) {
        long timestamp;
        try {
            timestamp = toEpochMilli(value, SECOND_FORMAT, zone);
            if (timestamp < 0) {
                timestamp = toEpochMilli(value, MILLISECOND_FORMAT, zone);
            }
        } catch (ArithmeticException e) {
            return OptionalLong.empty();
        }
        return timestamp < 0 ? OptionalLong.empty() : OptionalLong.of(timestamp);
    }

    private static long toEpochMilli(String value, DateTimeFormatter format, ZoneId zone) {
        LocalDateTime time;
        try {
            time = LocalDateTime.parse(value, format);
        } catch (DateTimeParseException e) {
            return -1;
        }
        return time.atZone(zone).toInstant().toEpochMilli();
    }

    /** No version of the chain was committed at or before the requested {@code FOR TIME AS OF} time. */
    static final class NoVersionAtOrBeforeException extends DorisConnectorException {
        private NoVersionAtOrBeforeException(String requestedText) {
            super("Lance dataset has no version at or before '" + requestedText + "'");
        }
    }

    /**
     * Cleanup removed a version newer than every version committed at or before the requested time. Its
     * commit time went with it, so it may have been the answer.
     */
    static final class HistoryRemovedException extends DorisConnectorException {
        private final long version;

        private HistoryRemovedException(long version) {
            super("Lance version " + version + " no longer exists");
            this.version = version;
        }

        long getVersion() {
            return version;
        }
    }

    /** The commit time a manifest records, at the precision it records it in (below a millisecond). */
    private static Instant commitTime(Version version) {
        return version.getDataTime().toInstant();
    }

    /**
     * Resolves {@code FOR TIME AS OF} on the main chain from the commit times its manifests record.
     *
     * <p>Selects the version committed last at or before the requested time, compared in full: a commit
     * later within the requested millisecond is after it. Of versions committed at the same instant, the
     * newest. As in Iceberg, commit times are compared as recorded, without assuming they grow with
     * version numbers.
     *
     * <p>Lance numbers a chain's commits consecutively, so a number the listing skips is a version cleanup
     * removed, and it took its commit time with it. As Iceberg drops its snapshot log before a removed
     * snapshot, only the versions newer than the newest removed one are candidates; a time none of them
     * covers fails rather than reading an older snapshot.
     *
     * @param listed the chain's versions as a storage listing shows them
     * @param requestedText the user's {@code FOR TIME AS OF} text, echoed in the error message
     * @throws HistoryRemovedException if no candidate qualifies and a removed version cut the history
     * @throws NoVersionAtOrBeforeException if no candidate qualifies otherwise
     */
    static long versionAtOrBefore(Collection<Version> listed, long timestampMillis, String requestedText) {
        NavigableMap<Long, Version> byId = new TreeMap<>();
        for (Version version : listed) {
            byId.put(version.getId(), version);
        }
        List<Version> history = new ArrayList<>();
        Long removed = null;
        Long expected = byId.isEmpty() ? null : byId.lastKey();
        for (Version version : byId.descendingMap().values()) {
            if (version.getId() != expected) {
                removed = expected;
                break;
            }
            history.add(version);
            expected = version.getId() - 1;
        }
        Instant requested = Instant.ofEpochMilli(timestampMillis);
        Long removedVersion = removed;
        return history.stream()
                .filter(version -> !commitTime(version).isAfter(requested))
                .max(Comparator.comparing(LanceSnapshotResolver::commitTime).thenComparingLong(Version::getId))
                .map(Version::getId)
                .orElseThrow(() -> removedVersion != null
                        ? new HistoryRemovedException(removedVersion)
                        : new NoVersionAtOrBeforeException(requestedText));
    }
}
