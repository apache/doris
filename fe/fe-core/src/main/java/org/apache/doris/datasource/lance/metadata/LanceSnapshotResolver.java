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
import org.lance.namespace.model.TableVersion;

import java.util.Comparator;
import java.util.List;
import java.util.OptionalLong;
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

    /**
     * Selects the latest namespace-recorded version whose commit time does not exceed the
     * requested timestamp. Returns empty when the namespace lists no versions or omits a commit
     * time, in which case the caller resolves from storage instead.
     *
     * @param requestedText the user's {@code FOR TIME AS OF} text, echoed in the error message
     */
    public static OptionalLong namespaceVersionAtOrBefore(List<TableVersion> versions, long timestampMillis,
            String requestedText) {
        if (versions.isEmpty() || versions.stream().anyMatch(
                version -> version.getVersion() == null || version.getTimestampMillis() == null)) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(versions.stream()
                .filter(version -> version.getTimestampMillis() <= timestampMillis)
                .max(Comparator.comparingLong(TableVersion::getTimestampMillis)
                        .thenComparingLong(TableVersion::getVersion))
                .orElseThrow(() -> new IllegalArgumentException(
                        "Lance dataset has no version at or before '" + requestedText + "'"))
                .getVersion());
    }

    private static long commitMillis(Version version) {
        return version.getDataTime().toInstant().toEpochMilli();
    }

    static long versionAtOrBefore(List<Version> versions, long timestampMillis) {
        return versionAtOrBefore(versions, timestampMillis, String.valueOf(timestampMillis));
    }

    /**
     * Selects the latest version from a dataset's own version list whose commit time does not
     * exceed the requested timestamp. Resolves {@code FOR TIME AS OF} for storage-versioned
     * datasets, and for namespace-managed datasets whose namespace reports no commit times.
     *
     * @param requestedText the user's {@code FOR TIME AS OF} text, echoed in the error message
     */
    public static long versionAtOrBefore(List<Version> versions, long timestampMillis, String requestedText) {
        // Compare at millisecond precision, the precision a namespace reports commit times in,
        // so storage and namespace resolution agree on the millisecond a commit lands in.
        return versions.stream()
                .filter(version -> commitMillis(version) <= timestampMillis)
                .max(Comparator.comparingLong(LanceSnapshotResolver::commitMillis).thenComparingLong(Version::getId))
                .orElseThrow(() -> new IllegalArgumentException(
                        "Lance dataset has no version at or before '" + requestedText + "'"))
                .getId();
    }
}
