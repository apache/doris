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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.lance.metadata.LanceRefSelector;
import org.apache.doris.datasource.lance.metadata.LanceSnapshotResolver;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LanceSearchSelectorTest {

    private static LanceRefSelector parse(String... keyValues) throws AnalysisException {
        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            params.put(keyValues[i], keyValues[i + 1]);
        }
        return LanceExternalSearchTableValuedFunction.parseSelector(params);
    }

    private static String error(String... keyValues) {
        return Assertions.assertThrows(AnalysisException.class, () -> parse(keyValues)).getDetailMessage();
    }

    private static void assertSnapshot(LanceRefSelector selector, TableSnapshot.VersionType type, String value) {
        Assertions.assertTrue(selector.getSnapshot().isPresent());
        Assertions.assertEquals(type, selector.getSnapshot().get().getType());
        Assertions.assertEquals(value, selector.getSnapshot().get().getValue());
    }

    @Test
    public void testNoSelectorIsLatestMain() throws Exception {
        Assertions.assertSame(LanceRefSelector.latest(),
                LanceExternalSearchTableValuedFunction.parseSelector(Collections.emptyMap()));
        Assertions.assertSame(LanceRefSelector.latest(), parse("column", "embedding"));
    }

    @Test
    public void testVersionIsAPositiveIntegerOnly() throws Exception {
        LanceRefSelector selector = parse("version", " 007 ");
        assertSnapshot(selector, TableSnapshot.VersionType.VERSION, "7");
        Assertions.assertFalse(selector.getBranch().isPresent());
        Assertions.assertFalse(selector.getTag().isPresent());
        // A tag name is not a version here, although FOR VERSION AS OF takes one.
        for (String invalid : new String[] {"0", "-1", "+3", "v3", "1.5", "release"}) {
            Assertions.assertEquals("'version' must be a positive integer, but was '" + invalid + "'",
                    error("version", invalid));
        }
        Assertions.assertTrue(error("version", "9223372036854775808").contains("out of range"));
    }

    @Test
    public void testTimestampFormats() throws Exception {
        assertSnapshot(parse("timestamp", "2026-09-20 12:00:00"), TableSnapshot.VersionType.TIME,
                "2026-09-20 12:00:00");
        assertSnapshot(parse("timestamp", " 2026-09-20 12:00:00.123 "), TableSnapshot.VersionType.TIME,
                "2026-09-20 12:00:00.123");
        for (String invalid : new String[] {"1758369600000", "2026-09-20", "2026-09-20T12:00:00",
                "2026-09-20 12:00:00.1234", "yesterday", "+300000000-01-01 00:00:00"}) {
            Assertions.assertTrue(error("timestamp", invalid).startsWith("'timestamp' must be "
                    + LanceSnapshotResolver.TIMESTAMP_FORMATS + " in the session time zone"), invalid);
        }
    }

    @Test
    public void testTimestampUsesSessionTimeZone() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = MemoTestUtils.createConnectContext();
        try {
            context.getSessionVariable().setTimeZone("UTC");
            long utc = LanceSnapshotResolver.parseTimestamp("2026-09-20 12:00:00").getAsLong();
            Assertions.assertEquals(1789905600000L, utc);
            Assertions.assertEquals(utc + 123,
                    LanceSnapshotResolver.parseTimestamp("2026-09-20 12:00:00.123").getAsLong());
            context.getSessionVariable().setTimeZone("Asia/Shanghai");
            Assertions.assertEquals(utc - 8 * 3600 * 1000L,
                    LanceSnapshotResolver.parseTimestamp("2026-09-20 12:00:00").getAsLong());
            Assertions.assertFalse(LanceSnapshotResolver.parseTimestamp("2026-09-20").isPresent());
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testTagStandsAlone() throws Exception {
        LanceRefSelector selector = parse("tag", "release");
        Assertions.assertEquals(Optional.of("release"), selector.getTag());
        Assertions.assertFalse(selector.getSnapshot().isPresent());
        Assertions.assertFalse(selector.getBranch().isPresent());
        // A numeric tag name is a tag here; FOR VERSION AS OF would read it as a version.
        Assertions.assertEquals(Optional.of("123"), parse("tag", "123").getTag());
        for (String other : new String[] {"version", "timestamp", "branch"}) {
            String value = other.equals("timestamp") ? "2026-09-20 12:00:00" : other.equals("version") ? "3" : "dev";
            Assertions.assertEquals("'tag' and '" + other + "' are mutually exclusive;"
                    + " a tag already determines its branch and version", error("tag", "release", other, value));
        }
        // Even main names a chain, which the tag already determines.
        Assertions.assertTrue(error("tag", "release", "branch", "main").contains("mutually exclusive"));
    }

    @Test
    public void testVersionAndTimestampAreMutuallyExclusive() {
        Assertions.assertEquals("'version' and 'timestamp' are mutually exclusive",
                error("version", "3", "timestamp", "2026-09-20 12:00:00"));
        Assertions.assertEquals("'version' and 'timestamp' are mutually exclusive",
                error("version", "3", "timestamp", "2026-09-20 12:00:00", "branch", "dev"));
    }

    @Test
    public void testBranchAloneOrWithVersionOrTimestamp() throws Exception {
        LanceRefSelector latest = parse("branch", "dev");
        Assertions.assertEquals(Optional.of("dev"), latest.getBranch());
        Assertions.assertFalse(latest.getSnapshot().isPresent());

        LanceRefSelector version = parse("branch", "team/dev", "version", "4");
        Assertions.assertEquals(Optional.of("team/dev"), version.getBranch());
        assertSnapshot(version, TableSnapshot.VersionType.VERSION, "4");

        LanceRefSelector time = parse("branch", "dev", "timestamp", "2026-09-20 12:00:00");
        Assertions.assertEquals(Optional.of("dev"), time.getBranch());
        assertSnapshot(time, TableSnapshot.VersionType.TIME, "2026-09-20 12:00:00");
    }

    @Test
    public void testMainBranchIsTheMainChain() throws Exception {
        Assertions.assertSame(LanceRefSelector.latest(), parse("branch", "main"));
        LanceRefSelector version = parse("branch", "main", "version", "4");
        Assertions.assertFalse(version.getBranch().isPresent());
        assertSnapshot(version, TableSnapshot.VersionType.VERSION, "4");
        LanceRefSelector time = parse("branch", "main", "timestamp", "2026-09-20 12:00:00");
        Assertions.assertFalse(time.getBranch().isPresent());
        assertSnapshot(time, TableSnapshot.VersionType.TIME, "2026-09-20 12:00:00");
    }

    /** The table hands the selector, unchanged, to the catalog's search metadata loaders. */
    @Test
    public void testTableForwardsTheSelectorToTheCatalog() throws Exception {
        org.apache.doris.datasource.lance.LanceExternalCatalog catalog =
                org.mockito.Mockito.mock(org.apache.doris.datasource.lance.LanceExternalCatalog.class);
        org.apache.doris.datasource.lance.LanceExternalDatabase db =
                org.mockito.Mockito.mock(org.apache.doris.datasource.lance.LanceExternalDatabase.class);
        org.mockito.Mockito.when(db.getRemoteName()).thenReturn("remote_db");
        org.apache.doris.datasource.lance.LanceExternalTable table =
                new org.apache.doris.datasource.lance.LanceExternalTable(1, "t", "remote_t", catalog, db);
        LanceRefSelector selector = parse("branch", "dev", "version", "4");
        table.loadMetadataForSearch(selector);
        org.mockito.Mockito.verify(catalog).loadTableMetadataForSearch("remote_db", "remote_t", selector);
        table.loadBasicMetadata(selector);
        org.mockito.Mockito.verify(catalog).loadBasicTableMetadata("remote_db", "remote_t", selector);
    }

    @Test
    public void testEmptySelectorIsAnError() {
        for (String key : new String[] {"version", "timestamp", "tag", "branch"}) {
            for (String empty : new String[] {"", "   "}) {
                Assertions.assertEquals("'" + key + "' must not be empty", error(key, empty));
            }
        }
        // An empty selector fails before any combination is checked.
        Assertions.assertEquals("'branch' must not be empty", error("tag", "release", "branch", ""));
    }
}
