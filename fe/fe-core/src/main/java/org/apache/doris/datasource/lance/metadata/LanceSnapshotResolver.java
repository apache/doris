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

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
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

    /** The commit time a manifest records, at millisecond precision. */
    public static long commitMillis(Version version) {
        return version.getDataTime().toInstant().toEpochMilli();
    }

    static long versionAtOrBefore(List<Version> versions, long timestampMillis) {
        return versionAtOrBefore(versions, timestampMillis, String.valueOf(timestampMillis));
    }

    /**
     * Selects the latest version whose manifest commit time does not exceed the requested
     * timestamp, compared at millisecond precision, as Lance resolves {@code asof}.
     *
     * @param requestedText the user's {@code FOR TIME AS OF} text, echoed in the error message
     * @throws NoVersionAtOrBeforeException if every version was committed after the timestamp
     */
    public static long versionAtOrBefore(Collection<Version> versions, long timestampMillis, String requestedText) {
        return versions.stream()
                .filter(version -> commitMillis(version) <= timestampMillis)
                .max(Comparator.comparingLong(LanceSnapshotResolver::commitMillis).thenComparingLong(Version::getId))
                .orElseThrow(() -> new NoVersionAtOrBeforeException(requestedText))
                .getId();
    }
}
