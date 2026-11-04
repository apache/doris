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

import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.mvcc.MvccTable;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.Version;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

public class LanceSnapshotTest {

    @Test
    public void testVersionSelectorRequiresPositiveNumericVersion() {
        Assertions.assertEquals(7, LanceSnapshotResolver.parseVersion("7"));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> LanceSnapshotResolver.parseVersion("tag_name"));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> LanceSnapshotResolver.parseVersion("0"));
    }

    @Test
    public void testTimeSelectorResolvesLatestVersionNotAfterTimestamp() {
        ZonedDateTime first = ZonedDateTime.of(2026, 8, 1, 10, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime second = first.plusHours(1);
        ZonedDateTime third = second.plusHours(1);
        Version version1 = new Version(1, first, new TreeMap<>());
        Version version2 = new Version(2, second, new TreeMap<>());
        Version version3 = new Version(3, third, new TreeMap<>());

        Assertions.assertEquals(2, LanceSnapshotResolver.versionAtOrBefore(
                Arrays.asList(version3, version1, version2), second.plusMinutes(30).toInstant().toEpochMilli()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceSnapshotResolver.versionAtOrBefore(
                        Arrays.asList(version1, version2, version3), first.minusNanos(1).toInstant().toEpochMilli()));
    }

    /**
     * Lance records commit times below a millisecond. A commit later within the requested
     * millisecond is after it, and two commits within one millisecond keep their order.
     */
    @Test
    public void testTimeSelectorComparesCommitTimesInFull() {
        Instant commit2 = Instant.parse("2026-09-19T13:06:09.113997Z");
        Version version1 = versionAt(1, Instant.parse("2026-09-19T13:06:07.597965Z"));
        Version version2 = versionAt(2, commit2);
        long requested = Instant.parse("2026-09-19T13:06:09.113Z").toEpochMilli();
        Assertions.assertEquals(1, LanceSnapshotResolver.versionAtOrBefore(Arrays.asList(version1, version2),
                requested));
        Assertions.assertEquals(2, LanceSnapshotResolver.versionAtOrBefore(Arrays.asList(version1, version2),
                requested + 1));
        // Version 3 was committed half a millisecond before version 2, in the same millisecond.
        Version version3 = versionAt(3, commit2.minusNanos(500_000));
        Assertions.assertEquals(2, LanceSnapshotResolver.versionAtOrBefore(
                Arrays.asList(version1, version2, version3), requested + 1));
    }

    @Test
    public void testTimeSelectorComparesCommitTimesAcrossTheHistory() {
        // Commit times that go back: version 2 reports an earlier time than version 1.
        Assertions.assertEquals(1, LanceSnapshotResolver.versionAtOrBefore(
                Arrays.asList(version(1, 100), version(2, 90), version(3, 200)), null, 150, "150"));
        // A managed chain selects among the versions the namespace records, not every one storage lists:
        // storage's version 3 is not published yet.
        Assertions.assertEquals(2, LanceSnapshotResolver.versionAtOrBefore(
                Arrays.asList(version(1, 10), version(2, 20), version(3, 30)), recorded(1, 2), 100, "100"));
        // A version the namespace no longer records was removed, whether or not storage still holds
        // its manifest: nothing older is a candidate.
        LanceSnapshotResolver.HistoryRemovedException dropped = Assertions.assertThrows(
                LanceSnapshotResolver.HistoryRemovedException.class, () -> LanceSnapshotResolver.versionAtOrBefore(
                        Arrays.asList(version(1, 10), version(2, 20), version(3, 30)), recorded(1, 3), 25, "25"));
        Assertions.assertEquals(2, dropped.getVersion());
        LanceSnapshotResolver.HistoryRemovedException gap = Assertions.assertThrows(
                LanceSnapshotResolver.HistoryRemovedException.class, () -> LanceSnapshotResolver.versionAtOrBefore(
                        Arrays.asList(version(1, 10), version(2, 20), version(8, 80), version(9, 90)),
                        recorded(1, 2, 8, 9), 50, "50"));
        Assertions.assertEquals(7, gap.getVersion());
        Assertions.assertEquals(9, LanceSnapshotResolver.versionAtOrBefore(
                Arrays.asList(version(1, 10), version(2, 20), version(8, 80), version(9, 90)),
                recorded(1, 2, 8, 9), 95, "95"));
        // A branch's chain starts where it was forked; the versions before are not a gap.
        Assertions.assertEquals(3, LanceSnapshotResolver.versionAtOrBefore(
                Arrays.asList(version(2, 20), version(3, 30)), recorded(2, 3), 35, "35"));
        // The newest recorded version has no manifest yet: its commit time is unknown.
        LanceSnapshotResolver.HistoryRemovedException pending = Assertions.assertThrows(
                LanceSnapshotResolver.HistoryRemovedException.class, () -> LanceSnapshotResolver.versionAtOrBefore(
                        Arrays.asList(version(1, 10), version(2, 20), version(3, 30)), recorded(1, 2, 3, 4), 100,
                        "100"));
        Assertions.assertEquals(4, pending.getVersion());
        // A recorded version storage lacks was removed: nothing older is a candidate.
        LanceSnapshotResolver.HistoryRemovedException recordedRemoved = Assertions.assertThrows(
                LanceSnapshotResolver.HistoryRemovedException.class, () -> LanceSnapshotResolver.versionAtOrBefore(
                        Arrays.asList(version(1, 10), version(3, 30)), recorded(1, 2, 3), 20, "20"));
        Assertions.assertEquals(2, recordedRemoved.getVersion());
        // Cleanup removed version 2: nothing from before it is a candidate, even version 1.
        LanceSnapshotResolver.HistoryRemovedException removed = Assertions.assertThrows(
                LanceSnapshotResolver.HistoryRemovedException.class, () -> LanceSnapshotResolver.versionAtOrBefore(
                        Arrays.asList(version(1, 10), version(3, 30)), null, 20, "20"));
        Assertions.assertEquals(2, removed.getVersion());
        Assertions.assertEquals(3, LanceSnapshotResolver.versionAtOrBefore(
                Arrays.asList(version(1, 10), version(3, 30)), null, 30, "30"));
    }

    /**
     * How a version appears: listed in storage and recorded (for a managed chain), removed from
     * storage but still recorded, listed but not recorded, or neither.
     */
    private enum State { LISTED, REMOVED, UNRECORDED, DROPPED }

    /**
     * Checks the selection against a direct reading of its rule on random histories: times that
     * repeat, go back, and fall inside a requested millisecond, removed, unrecorded and dropped
     * versions, on storage and managed chains, including a managed chain whose newest recorded
     * version storage lacks or whose newest listed version the namespace does not record. Commit
     * times are in microseconds, requested times in milliseconds.
     */
    @Test
    public void testTimeSelectorMatchesItsRuleOnRandomHistories() {
        Random random = new Random(20260927L);
        for (int round = 0; round < 20000; round++) {
            boolean managed = random.nextBoolean();
            int count = 1 + random.nextInt(7);
            long[] times = new long[count + 1];
            State[] states = new State[count + 1];
            for (int id = 1; id <= count; id++) {
                times[id] = random.nextInt(10) * 1000L + random.nextInt(3) * 500L;
                State[] choices = managed ? State.values() : new State[] {State.LISTED, State.REMOVED};
                // The newest storage version is the open dataset, so it is always there; a managed
                // chain's records can end before or after it.
                states[id] = id == count && !managed ? State.LISTED : choices[random.nextInt(choices.length)];
            }
            long timestamp = random.nextInt(11) - 1;

            List<Version> listed = new ArrayList<>();
            NavigableSet<Long> recorded = managed ? new TreeSet<>() : null;
            for (int id = 1; id <= count; id++) {
                if (states[id] == State.LISTED || states[id] == State.UNRECORDED) {
                    listed.add(versionAtMicros(id, times[id]));
                }
                if (managed && (states[id] == State.LISTED || states[id] == State.REMOVED)) {
                    recorded.add((long) id);
                }
            }
            Collections.shuffle(listed, random);

            // The rule: the newest removed version cuts the history, and the latest commit at or
            // before the time wins. For storage, a removed version is one the listing shows a gap
            // for; for a managed chain, any number between its oldest and newest recorded version
            // that is not both recorded and listed.
            Long removed = null;
            if (managed) {
                long newest = recorded.isEmpty() ? 0 : recorded.last();
                long oldest = recorded.isEmpty() ? 1 : recorded.first();
                for (long id = newest; id >= oldest && removed == null; id--) {
                    if (states[(int) id] != State.LISTED) {
                        removed = id;
                    }
                }
            } else {
                int oldestListed = count;
                for (int id = count; id >= 1; id--) {
                    if (states[id] == State.LISTED) {
                        oldestListed = id;
                    }
                }
                for (int id = count; id >= 1 && removed == null; id--) {
                    if (states[id] == State.REMOVED && id > oldestListed) {
                        removed = (long) id;
                    }
                }
            }
            Long expected = null;
            for (int id = count; removed == null || id > removed; id--) {
                if (id < 1) {
                    break;
                }
                boolean candidate = states[id] == State.LISTED;
                if (candidate && times[id] <= timestamp * 1000
                        && (expected == null || times[id] > times[expected.intValue()])) {
                    expected = (long) id;
                }
            }

            String context = "round " + round + (managed ? " managed" : " storage") + " times "
                    + Arrays.toString(times) + " states " + Arrays.toString(states) + " at " + timestamp;
            try {
                long actual = LanceSnapshotResolver.versionAtOrBefore(listed, recorded, timestamp,
                        String.valueOf(timestamp));
                Assertions.assertEquals(expected, Long.valueOf(actual), context);
            } catch (LanceSnapshotResolver.HistoryRemovedException e) {
                Assertions.assertNull(expected, context);
                Assertions.assertEquals(removed, Long.valueOf(e.getVersion()), context);
            } catch (LanceSnapshotResolver.NoVersionAtOrBeforeException e) {
                Assertions.assertNull(expected, context);
                Assertions.assertNull(removed, context);
            }
        }
    }

    private static Version version(long id, long commitMillis) {
        return versionAt(id, Instant.ofEpochMilli(commitMillis));
    }

    private static Version versionAtMicros(long id, long commitMicros) {
        return versionAt(id, Instant.EPOCH.plusNanos(commitMicros * 1000));
    }

    private static Version versionAt(long id, Instant commitTime) {
        return new Version(id, ZonedDateTime.ofInstant(commitTime, ZoneOffset.UTC), new TreeMap<>());
    }

    private static NavigableSet<Long> recorded(long... ids) {
        NavigableSet<Long> result = new TreeSet<>();
        for (long id : ids) {
            result.add(id);
        }
        return result;
    }

    @Test
    public void testVersionNumbersAndTagNames() {
        Assertions.assertTrue(LanceSnapshotResolver.isVersionNumber("2"));
        Assertions.assertTrue(LanceSnapshotResolver.isVersionNumber("007"));
        Assertions.assertEquals(7, LanceSnapshotResolver.parseVersion("007"));
        // A signed number is a version, so '-1' is reported as invalid rather than as a missing tag.
        Assertions.assertTrue(LanceSnapshotResolver.isVersionNumber("-1"));
        Assertions.assertTrue(LanceSnapshotResolver.isVersionNumber("+3"));
        Assertions.assertEquals(3, LanceSnapshotResolver.parseVersion("+3"));
        IllegalArgumentException negative = Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceSnapshotResolver.parseVersion("-1"));
        Assertions.assertEquals("Lance FOR VERSION AS OF requires a positive version, but was -1",
                negative.getMessage());
        // Anything else names a tag, as for Iceberg and Paimon tables.
        for (String tag : new String[] {"v2", "-", "+", " 2", "2 ", "", "1.5", "1e3"}) {
            Assertions.assertFalse(LanceSnapshotResolver.isVersionNumber(tag), tag);
        }
        IllegalArgumentException outOfRange = Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceSnapshotResolver.parseVersion("99999999999999999999"));
        Assertions.assertEquals("Lance FOR VERSION AS OF version 99999999999999999999 is out of range",
                outOfRange.getMessage());
    }

    @Test
    public void testBoundSnapshotCarriesItsOwnSchema() {
        LanceTableMetadata intMetadata = metadata(10,
                Field.nullable("value", new ArrowType.Int(32, true)));
        LanceTableMetadata floatMetadata = metadata(11,
                Field.nullable("value", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));

        Assertions.assertEquals(Type.INT,
                LanceSchemaHelper.toDorisColumns(intMetadata.getSchema()).get(0).getType());
        Assertions.assertEquals(Type.FLOAT,
                LanceSchemaHelper.toDorisColumns(floatMetadata.getSchema()).get(0).getType());

        LanceMvccSnapshot version10 = new LanceMvccSnapshot(intMetadata);
        Assertions.assertSame(intMetadata, version10.getMetadata());
        Assertions.assertEquals(10, version10.getMetadata().getVersion());
        Assertions.assertEquals(10, version10.getMetadata().getFragments().get(0).getId());
        Assertions.assertEquals("http://minio:9000",
                version10.getMetadata().getLanceStorageOptions().get("aws_endpoint"));
        Assertions.assertTrue(version10.isSameSnapshot(new LanceMvccSnapshot(metadata(10,
                Field.nullable("value", new ArrowType.Int(32, true))))));
        Assertions.assertFalse(version10.isSameSnapshot(new LanceMvccSnapshot(floatMetadata)));
        Assertions.assertTrue(MvccTable.class.isAssignableFrom(LanceExternalTable.class));
    }

    private static LanceTableMetadata metadata(long version, Field field) {
        return LanceTableMetadata.createBasicSnapshot(new LanceTableAccess("s3://bucket/table.lance", Collections.singletonMap("aws_endpoint", "http://minio:9000")), version,
                new Schema(Collections.singletonList(field)),
                Collections.singletonList(new LanceFragmentInfo(version, 1, 1)));
    }
}
