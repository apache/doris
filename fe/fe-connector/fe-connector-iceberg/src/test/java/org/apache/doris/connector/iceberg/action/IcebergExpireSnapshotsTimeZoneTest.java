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

package org.apache.doris.connector.iceberg.action;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Stream;

/** Tests the snapshot sets retained by local datetime and epoch cutoff arguments. */
@ResourceLock(Resources.TIME_ZONE)
public class IcebergExpireSnapshotsTimeZoneTest {
    private TimeZone originalTimeZone;

    @BeforeEach
    public void rememberDefaultTimeZone() {
        originalTimeZone = TimeZone.getDefault();
    }

    @AfterEach
    public void restoreDefaultTimeZone() {
        TimeZone.setDefault(originalTimeZone);
    }

    @ParameterizedTest(name = "JVM {0}, session {1}, cutoff equals second snapshot: {3}")
    @MethodSource("timeZonesAndBoundaries")
    public void isoAndEpochCutoffsRetainTheSameSnapshotIds(String jvmZone, String sessionZone,
            String expectedZone, boolean cutoffAtSecondSnapshot) throws InterruptedException {
        TimeZone.setDefault(TimeZone.getTimeZone(jvmZone));
        Table table = threeSnapshots();
        List<Snapshot> history = snapshotHistory(table);
        long cutoff = cutoffAtSecondSnapshot
                ? history.get(1).timestampMillis() : history.get(0).timestampMillis() + 1;
        String localCutoff = localCutoff(cutoff, expectedZone);
        Set<Long> expected = ImmutableSet.of(history.get(1).snapshotId(), history.get(2).snapshotId());

        expire(table, localCutoff, ActionTestTables.session(sessionZone));
        Assertions.assertEquals(expected, snapshotIds(table),
                "only snapshots strictly before the session-local cutoff may expire");

        // Applying the equivalent instant must preserve exactly the same surviving snapshot IDs.
        expire(table, String.valueOf(cutoff), ActionTestTables.session(sessionZone));
        Assertions.assertEquals(expected, snapshotIds(table));
    }

    private static Stream<Arguments> timeZonesAndBoundaries() {
        return Stream.of(
                new String[] {"UTC", "Asia/Shanghai", "Asia/Shanghai"},
                new String[] {"UTC", "CST", "Asia/Shanghai"},
                new String[] {"UTC", "+08:00", "+08:00"},
                new String[] {"Asia/Shanghai", "UTC", "UTC"},
                new String[] {"UTC", "-08:00", "-08:00"},
                new String[] {"UTC", "UTC", "UTC"},
                new String[] {"Asia/Shanghai", "Asia/Shanghai", "Asia/Shanghai"})
                .flatMap(zones -> Stream.of(
                        Arguments.of(zones[0], zones[1], zones[2], false),
                        Arguments.of(zones[0], zones[1], zones[2], true)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Asia/Shanghai", "CST", "+08:00", "-08:00", "UTC", "America/Los_Angeles"})
    public void epochCutoffIsIndependentOfTheSessionTimeZone(String sessionZone) throws InterruptedException {
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        Table table = threeSnapshots();
        List<Snapshot> history = snapshotHistory(table);
        long cutoff = history.get(0).timestampMillis() + 1;

        expire(table, String.valueOf(cutoff), ActionTestTables.session(sessionZone));

        Assertions.assertEquals(ImmutableSet.of(history.get(1).snapshotId(), history.get(2).snapshotId()),
                snapshotIds(table));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "Unknown/Zone"})
    public void absentOrInvalidSessionTimeZoneUsesUtc(String sessionZone) throws InterruptedException {
        assertUtcFallback(ActionTestTables.session(sessionZone));
    }

    @Test
    public void absentSessionUsesUtc() throws InterruptedException {
        assertUtcFallback(null);
    }

    private static void assertUtcFallback(ConnectorSession session) throws InterruptedException {
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        Table table = threeSnapshots();
        List<Snapshot> history = snapshotHistory(table);
        long cutoff = history.get(0).timestampMillis() + 1;

        expire(table, localCutoff(cutoff, "UTC"), session);

        Assertions.assertEquals(ImmutableSet.of(history.get(1).snapshotId(), history.get(2).snapshotId()),
                snapshotIds(table));
    }

    @ParameterizedTest
    @ValueSource(strings = {"not-a-time", "2026-11-10 10:00:00", "2026-11-10T10:00:00Z", "9223372036854775808"})
    public void rejectsInvalidOrUnsupportedCutoffFormats(String cutoff) {
        IcebergExpireSnapshotsAction action = action(cutoff);
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, action::validate);
        Assertions.assertEquals("Invalid older_than format. Expected ISO datetime "
                + "(yyyy-MM-ddTHH:mm:ss) or timestamp in milliseconds: " + cutoff, e.getMessage());
    }

    private static IcebergExpireSnapshotsAction action(String cutoff) {
        return new IcebergExpireSnapshotsAction(ImmutableMap.of("older_than", cutoff, "retain_last", "1"),
                Collections.emptyList(), null);
    }

    private static void expire(Table table, String cutoff, ConnectorSession session) {
        IcebergExpireSnapshotsAction action = action(cutoff);
        action.validate();
        action.execute(table, session);
        table.refresh();
    }

    private static String localCutoff(long cutoff, String zone) {
        return Instant.ofEpochMilli(cutoff).atZone(ZoneId.of(zone)).toLocalDateTime()
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    private static Table threeSnapshots() throws InterruptedException {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        ActionTestTables.createTable(catalog, "cutoff");
        ActionTestTables.appendSnapshot(catalog, "cutoff", "first.parquet", 1L);
        Thread.sleep(5);
        ActionTestTables.appendSnapshot(catalog, "cutoff", "second.parquet", 1L);
        Thread.sleep(5);
        ActionTestTables.appendSnapshot(catalog, "cutoff", "third.parquet", 1L);
        Table table = catalog.loadTable(ActionTestTables.id("cutoff"));
        List<Snapshot> history = snapshotHistory(table);
        Assertions.assertEquals(3, history.size());
        Assertions.assertTrue(history.get(0).timestampMillis() + 1 < history.get(1).timestampMillis(),
                "the fixture needs a cutoff strictly between the first two snapshots");
        Assertions.assertTrue(history.get(1).timestampMillis() < history.get(2).timestampMillis());
        return table;
    }

    private static List<Snapshot> snapshotHistory(Table table) {
        List<Snapshot> history = new ArrayList<>();
        table.snapshots().forEach(history::add);
        history.sort(Comparator.comparingLong(Snapshot::timestampMillis));
        return history;
    }

    private static Set<Long> snapshotIds(Table table) {
        Set<Long> ids = new HashSet<>();
        table.snapshots().forEach(snapshot -> ids.add(snapshot.snapshotId()));
        return ids;
    }
}
