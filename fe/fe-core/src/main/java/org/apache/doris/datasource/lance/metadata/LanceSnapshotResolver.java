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

package org.apache.doris.datasource.lance.metadata;

import org.lance.Version;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.regex.Pattern;

/** Resolves Doris time-travel selectors to immutable Lance version IDs. */
public final class LanceSnapshotResolver {
    private LanceSnapshotResolver() {
    }

    public static long parseVersion(String value) {
        final long version;
        try {
            version = Long.parseLong(value);
        } catch (NumberFormatException e) {
            // Deliberately not chained: the catalog reports the root cause message, and the
            // NumberFormatException text would replace this one.
            throw new IllegalArgumentException(isVersionNumber(value)
                    ? "Lance FOR VERSION AS OF version " + value + " is out of range"
                    : "Lance FOR VERSION AS OF requires a numeric version, but was '" + value + "'");
        }
        if (version <= 0) {
            throw new IllegalArgumentException(
                    "Lance FOR VERSION AS OF requires a positive version, but was " + version);
        }
        return version;
    }

    private static final Pattern VERSION_NUMBER = Pattern.compile("[+-]?[0-9]+");

    /**
     * Whether a {@code FOR VERSION AS OF} value is a version number rather than a tag name. A
     * signed number counts as one, so that {@code '-1'} is reported as an invalid version.
     */
    public static boolean isVersionNumber(String value) {
        return VERSION_NUMBER.matcher(value).matches();
    }

    /** No version of a chain was committed at or before the requested {@code FOR TIME AS OF} time. */
    public static final class NoVersionAtOrBeforeException extends IllegalArgumentException {
        private NoVersionAtOrBeforeException(String requestedText) {
            super("Lance dataset has no version at or before '" + requestedText + "'");
        }
    }

    /**
     * Cleanup removed a version newer than every version committed at or before the requested
     * time. Its commit time went with it, so it may have been the answer.
     */
    public static final class HistoryRemovedException extends IllegalArgumentException {
        private final long version;

        private HistoryRemovedException(long version) {
            super("Lance version " + version + " no longer exists");
            this.version = version;
        }

        public long getVersion() {
            return version;
        }
    }

    /** The commit time a manifest records, at the precision it records it in (below a millisecond). */
    private static Instant commitTime(Version version) {
        return version.getDataTime().toInstant();
    }

    static long versionAtOrBefore(List<Version> versions, long timestampMillis) {
        return versionAtOrBefore(versions, timestampMillis, String.valueOf(timestampMillis));
    }

    /**
     * Selects the version committed last at or before the requested timestamp, from the commit
     * times the manifests record, compared in full: a commit later within the requested
     * millisecond is after it. Of versions committed at the same instant, the newest (Iceberg
     * keeps the first of equal times instead). As in Iceberg, commit times are compared as
     * recorded, without assuming they grow with version numbers.
     *
     * @param requestedText the user's {@code FOR TIME AS OF} text, echoed in the error message
     * @throws NoVersionAtOrBeforeException if every version was committed after the timestamp
     */
    public static long versionAtOrBefore(Collection<Version> versions, long timestampMillis, String requestedText) {
        Instant requested = Instant.ofEpochMilli(timestampMillis);
        return versions.stream()
                .filter(version -> !commitTime(version).isAfter(requested))
                .max(Comparator.comparing(LanceSnapshotResolver::commitTime).thenComparingLong(Version::getId))
                .orElseThrow(() -> new NoVersionAtOrBeforeException(requestedText))
                .getId();
    }

    /**
     * Resolves {@code FOR TIME AS OF} on one manifest chain with {@link #versionAtOrBefore}.
     *
     * <p>Commit times need not grow with version numbers, and a version removed by cleanup takes
     * its commit time with it, so it could have been the answer. As Iceberg drops its snapshot log
     * before a removed snapshot, only the versions newer than the newest removed one are
     * candidates; a time none of them covers fails rather than reading an older snapshot.
     *
     * @param listed the chain's versions a storage listing shows
     * @param recorded the versions a namespace records for a managed chain, which are the chain;
     *     null when the chain is what storage holds. Lance numbers a chain's commits consecutively,
     *     under a namespace too (DirectoryNamespace accepts only the next version), so a number the
     *     listing skips is a removed version, and on a managed chain so is a number between its
     *     oldest and newest recorded version that the namespace lacks, and a recorded version the
     *     listing lacks
     * @throws HistoryRemovedException if no candidate qualifies and a removed version cut the history
     * @throws NoVersionAtOrBeforeException if no candidate qualifies otherwise
     */
    public static long versionAtOrBefore(Collection<Version> listed, NavigableSet<Long> recorded,
            long timestampMillis, String requestedText) {
        NavigableMap<Long, Version> byId = new TreeMap<>();
        for (Version version : listed) {
            if (recorded == null || recorded.contains(version.getId())) {
                byId.put(version.getId(), version);
            }
        }
        List<Version> history = new ArrayList<>();
        Long removed = null;
        if (recorded == null) {
            Long expected = byId.isEmpty() ? null : byId.lastKey();
            for (Version version : byId.descendingMap().values()) {
                if (version.getId() != expected) {
                    removed = expected;
                    break;
                }
                history.add(version);
                expected = version.getId() - 1;
            }
        } else if (!recorded.isEmpty()) {
            for (long id = recorded.last(); id >= recorded.first(); id--) {
                Version version = recorded.contains(id) ? byId.get(id) : null;
                if (version == null) {
                    removed = id;
                    break;
                }
                history.add(version);
            }
        }
        try {
            return versionAtOrBefore(history, timestampMillis, requestedText);
        } catch (NoVersionAtOrBeforeException e) {
            if (removed != null) {
                throw new HistoryRemovedException(removed);
            }
            throw e;
        }
    }
}
