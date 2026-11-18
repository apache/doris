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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.Version;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.OptionalLong;
import java.util.TreeMap;

class LanceSnapshotResolverTest {

    @Test
    void versionSelectorRequiresPositiveNumericVersion() {
        Assertions.assertEquals(7, LanceSnapshotResolver.parseVersion("7"));
        Assertions.assertEquals("Lance FOR VERSION AS OF requires a numeric version, but was 'tag_name'",
                Assertions.assertThrows(DorisConnectorException.class,
                        () -> LanceSnapshotResolver.parseVersion("tag_name")).getMessage());
        Assertions.assertEquals("Lance FOR VERSION AS OF requires a positive version, but was 0",
                Assertions.assertThrows(DorisConnectorException.class,
                        () -> LanceSnapshotResolver.parseVersion("0")).getMessage());
        Assertions.assertEquals("Lance FOR VERSION AS OF requires a positive version, but was -1",
                Assertions.assertThrows(DorisConnectorException.class,
                        () -> LanceSnapshotResolver.parseVersion("-1")).getMessage());
        Assertions.assertEquals("Lance FOR VERSION AS OF version 99999999999999999999 is out of range",
                Assertions.assertThrows(DorisConnectorException.class,
                        () -> LanceSnapshotResolver.parseVersion("99999999999999999999")).getMessage());
    }

    @Test
    void timeSelectorResolvesLatestVersionNotAfterTimestamp() {
        ZonedDateTime first = ZonedDateTime.of(2026, 8, 1, 10, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime second = first.plusHours(1);
        ZonedDateTime third = second.plusHours(1);
        Version version1 = version(1, first);
        Version version2 = version(2, second);
        Version version3 = version(3, third);

        Assertions.assertEquals(2, LanceSnapshotResolver.versionAtOrBefore(
                Arrays.asList(version3, version1, version2), millis(second.plusMinutes(30)), "t"));
        Assertions.assertEquals(3, LanceSnapshotResolver.versionAtOrBefore(
                Arrays.asList(version1, version2, version3), millis(third), "t"));
        Assertions.assertThrows(LanceSnapshotResolver.NoVersionAtOrBeforeException.class,
                () -> LanceSnapshotResolver.versionAtOrBefore(
                        Arrays.asList(version1, version2, version3), millis(first) - 1, "t"));
    }

    /** Lance records commit times below a millisecond; a commit later within the requested one is after it. */
    @Test
    void commitTimesAreComparedBelowAMillisecond() {
        ZonedDateTime base = ZonedDateTime.of(2026, 7, 20, 3, 53, 50, 752_000_000, ZoneOffset.UTC);
        Version early = version(1, base.plusNanos(131_204));
        Version late = version(2, base.plusNanos(900_000));
        Assertions.assertThrows(LanceSnapshotResolver.NoVersionAtOrBeforeException.class,
                () -> LanceSnapshotResolver.versionAtOrBefore(Arrays.asList(early, late), millis(base), "t"));
        Assertions.assertEquals(2, LanceSnapshotResolver.versionAtOrBefore(
                Arrays.asList(early, late), millis(base) + 1, "t"));
        // Equal commit times: the newest version wins.
        Assertions.assertEquals(4, LanceSnapshotResolver.versionAtOrBefore(
                Arrays.asList(version(3, base), version(4, base)), millis(base), "t"));
    }

    /**
     * A version cleanup removed may have been the answer, so the history before it is not searched: a time
     * none of the newer versions covers fails rather than reading an older snapshot.
     */
    @Test
    void removedVersionCutsTheHistoryItCouldHaveAnswered() {
        ZonedDateTime first = ZonedDateTime.of(2026, 8, 1, 10, 0, 0, 0, ZoneOffset.UTC);
        Version version1 = version(1, first);
        Version version3 = version(3, first.plusHours(2));
        Version version4 = version(4, first.plusHours(3));
        Assertions.assertEquals(3, LanceSnapshotResolver.versionAtOrBefore(
                Arrays.asList(version1, version3, version4), millis(first.plusHours(2)), "t"));
        LanceSnapshotResolver.HistoryRemovedException removed = Assertions.assertThrows(
                LanceSnapshotResolver.HistoryRemovedException.class,
                () -> LanceSnapshotResolver.versionAtOrBefore(
                        Arrays.asList(version1, version3, version4), millis(first.plusMinutes(30)), "t"));
        Assertions.assertEquals(2, removed.getVersion());
    }

    @Test
    void timestampsParseInTheSessionZoneAtSecondOrMillisecondPrecision() {
        ZoneId utc = ZoneId.of("UTC");
        Assertions.assertEquals(OptionalLong.of(millis(ZonedDateTime.of(2026, 7, 20, 3, 53, 50, 0, ZoneOffset.UTC))),
                LanceSnapshotResolver.parseTimestamp("2026-07-20 03:53:50", utc));
        Assertions.assertEquals(
                OptionalLong.of(millis(ZonedDateTime.of(2026, 7, 20, 3, 53, 50, 753_000_000, ZoneOffset.UTC))),
                LanceSnapshotResolver.parseTimestamp("2026-07-20 03:53:50.753", utc));
        Assertions.assertEquals(
                OptionalLong.of(millis(ZonedDateTime.of(2026, 7, 20, 3, 53, 50, 0, ZoneOffset.UTC))),
                LanceSnapshotResolver.parseTimestamp("2026-07-20 11:53:50",
                        LanceSnapshotResolver.sessionZone("Asia/Shanghai")));
        // Doris's session aliases resolve as the engine resolves them.
        Assertions.assertEquals(ZoneId.of("Asia/Shanghai"), LanceSnapshotResolver.sessionZone("cst"));
        Assertions.assertEquals(ZoneId.of("+08:00"), LanceSnapshotResolver.sessionZone("+08:00"));
        Assertions.assertThrows(DorisConnectorException.class, () -> LanceSnapshotResolver.sessionZone("NOPE/ZZZ"));

        Assertions.assertFalse(LanceSnapshotResolver.parseTimestamp("1690000000000", utc).isPresent());
        Assertions.assertFalse(LanceSnapshotResolver.parseTimestamp("2026-07-20", utc).isPresent());
        Assertions.assertFalse(LanceSnapshotResolver.parseTimestamp("2026-07-20 03:53:50.7531", utc).isPresent());
        // A year too large for epoch milliseconds is unparsable rather than an overflow.
        Assertions.assertFalse(LanceSnapshotResolver.parseTimestamp("+999999999-12-31 23:59:59", utc).isPresent());
    }

    private static Version version(long id, ZonedDateTime commitTime) {
        return new Version(id, commitTime, new TreeMap<>());
    }

    private static long millis(ZonedDateTime time) {
        return time.toInstant().toEpochMilli();
    }
}
