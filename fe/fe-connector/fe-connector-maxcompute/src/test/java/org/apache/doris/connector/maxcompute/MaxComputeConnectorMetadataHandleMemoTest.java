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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Tables;
import com.aliyun.odps.table.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Guards the per-statement table-handle memo added to {@link MaxComputeConnectorMetadata}.
 *
 * <p><b>WHY this matters:</b> a single MaxCompute table is resolved by ~a dozen independent
 * planning sites within one statement (scan build, partition pruning, row-count, per-column
 * stats, write-capability probes, SHOW CREATE). Each {@code getTableHandle} used to fire a
 * fresh remote ODPS {@code tables().exists()} probe and build a new lazy {@code Table} whose
 * first schema access triggers its own reload. Because the metadata instance is created fresh
 * per statement (funnel-memoized one-per-statement), memoizing the resolved handle collapses
 * those repeats to one probe + one Table per (db, table) per statement, and makes read and
 * write share the same already-warmed Table.</p>
 *
 * <p>The maxcompute test module builds no live ODPS client, so a hand-written counting
 * {@link McStructureHelper} records how many times the remote probe / lazy-Table build fire.
 * {@code getTableHandle} never dereferences {@code odps} directly (it only forwards it to the
 * helper), so a {@code null} odps keeps the test offline — the same pattern as
 * {@link MaxComputeConnectorMetadataDropDbTest}.</p>
 */
public class MaxComputeConnectorMetadataHandleMemoTest {

    private MaxComputeConnectorMetadata metadataWith(McStructureHelper helper) {
        return new MaxComputeConnectorMetadata(
                null /* odps */, helper, "proj", "ep", "quota", Collections.emptyMap(),
                null); // null: partition cache unused by this test
    }

    @Test
    public void sameTableResolvedTwiceProbesAndBuildsOnce() {
        CountingStructureHelper helper = new CountingStructureHelper("db1.t1");
        MaxComputeConnectorMetadata metadata = metadataWith(helper);

        Optional<ConnectorTableHandle> first = metadata.getTableHandle(null, "db1", "t1");
        Optional<ConnectorTableHandle> second = metadata.getTableHandle(null, "db1", "t1");

        // WHY: the second resolution of the same table in one statement must cost ZERO remote
        // round trips and reuse the identical handle (hence the identical lazy Table, so the
        // schema reload happens once). MUTATION: dropping the memo makes the second call
        // re-probe/rebuild -> both lists size 2 (red) and the handles become distinct
        // (assertSame red).
        Assertions.assertTrue(first.isPresent() && second.isPresent(),
                "an existing table must resolve to a present handle");
        Assertions.assertEquals(Collections.singletonList("db1.t1"), helper.existProbes,
                "a table resolved twice in one statement must hit the remote exists() probe only once");
        Assertions.assertEquals(Collections.singletonList("db1.t1"), helper.odpsTableBuilds,
                "the lazy ODPS Table must be built once and shared, not rebuilt per resolution");
        Assertions.assertSame(first.get(), second.get(),
                "both resolutions must return the identical memoized handle (shared Table => shared reload)");
    }

    @Test
    public void distinctTablesAreResolvedIndependently() {
        CountingStructureHelper helper = new CountingStructureHelper("db1.t1", "db1.t2");
        MaxComputeConnectorMetadata metadata = metadataWith(helper);

        ConnectorTableHandle h1 = metadata.getTableHandle(null, "db1", "t1").orElseThrow();
        ConnectorTableHandle h2 = metadata.getTableHandle(null, "db1", "t2").orElseThrow();

        // WHY: distinct (db, table) keys must not collide in the memo — each table is resolved
        // exactly once on its own. This pins the value-equality List key (a broken key that
        // collapsed the two names would return one shared handle -> assertNotSame red).
        Assertions.assertNotSame(h1, h2, "different tables must resolve to different handles");
        Assertions.assertEquals(Arrays.asList("db1.t1", "db1.t2"), helper.existProbes,
                "each distinct table is probed once");
        Assertions.assertEquals(Arrays.asList("db1.t1", "db1.t2"), helper.odpsTableBuilds,
                "each distinct table builds its Table once");
    }

    @Test
    public void sameTableNameInDifferentDatabasesDoNotCollide() {
        CountingStructureHelper helper = new CountingStructureHelper("db1.t", "db2.t");
        MaxComputeConnectorMetadata metadata = metadataWith(helper);

        MaxComputeTableHandle a = (MaxComputeTableHandle) metadata.getTableHandle(null, "db1", "t").orElseThrow();
        MaxComputeTableHandle b = (MaxComputeTableHandle) metadata.getTableHandle(null, "db2", "t").orElseThrow();

        // WHY: the memo key must include the database, not just the table name. A statement that
        // joins db1.t and db2.t routes both resolutions through the one per-statement metadata
        // instance; a db-blind key would hand db1's handle back for db2.t -> the wrong project's
        // Table resolved silently. MUTATION: keying on tableName only makes the second call hit
        // db1's entry -> b == a (assertNotSame red), b.getDbName() == "db1" (red), and existProbes
        // drops to size 1 (red).
        Assertions.assertNotSame(a, b, "same table name in different databases must not share a memo entry");
        Assertions.assertEquals("db1", a.getDbName(), "first handle must carry its own database");
        Assertions.assertEquals("db2", b.getDbName(), "second handle must resolve db2, not reuse db1's handle");
        Assertions.assertEquals(Arrays.asList("db1.t", "db2.t"), helper.existProbes,
                "each database's table is probed independently");
    }

    @Test
    public void absentTableReprobesEachCallAndIsNeverMemoized() {
        CountingStructureHelper helper = new CountingStructureHelper(); // nothing present
        MaxComputeConnectorMetadata metadata = metadataWith(helper);

        Optional<ConnectorTableHandle> first = metadata.getTableHandle(null, "db1", "missing");
        Optional<ConnectorTableHandle> second = metadata.getTableHandle(null, "db1", "missing");

        // WHY: negatives are intentionally NOT memoized, so a missing table keeps the exact
        // pre-change behavior — a fresh exists() probe and a clean Optional.empty() on EVERY
        // call, never a cached present handle. MUTATION: caching the empty result would drop
        // existProbes to size 1 -> red.
        Assertions.assertFalse(first.isPresent(), "a missing table must resolve to empty");
        Assertions.assertFalse(second.isPresent(), "a missing table must resolve to empty every time");
        Assertions.assertEquals(Arrays.asList("db1.missing", "db1.missing"), helper.existProbes,
                "an absent table must re-probe on every call (negatives are not memoized)");
        Assertions.assertTrue(helper.odpsTableBuilds.isEmpty(),
                "an absent table must never build an ODPS Table handle");
    }

    /**
     * Counting fake: a table "exists" iff its {@code db.table} was passed to the constructor.
     * Records every {@code tableExist} probe and every {@code getOdpsTable} build so the tests
     * can assert the memo collapses repeats. All other methods return harmless defaults — they
     * are never invoked by {@code getTableHandle}.
     */
    private static final class CountingStructureHelper implements McStructureHelper {
        private final Set<String> presentTables;
        private final List<String> existProbes = new ArrayList<>();
        private final List<String> odpsTableBuilds = new ArrayList<>();

        CountingStructureHelper(String... present) {
            this.presentTables = new HashSet<>(Arrays.asList(present));
        }

        @Override
        public boolean tableExist(Odps mcClient, String dbName, String tableName) {
            String key = dbName + "." + tableName;
            existProbes.add(key);
            return presentTables.contains(key);
        }

        @Override
        public Table getOdpsTable(Odps mcClient, String dbName, String tableName) {
            odpsTableBuilds.add(dbName + "." + tableName);
            // A null Table is enough: the tests assert handle identity and build counts, never
            // touch the Table itself, keeping the fake offline.
            return null;
        }

        @Override
        public TableIdentifier getTableIdentifier(String dbName, String tableName) {
            return null;
        }

        // ---- unused by getTableHandle: harmless defaults ----

        @Override
        public List<String> listTableNames(Odps mcClient, String dbName) {
            return Collections.emptyList();
        }

        @Override
        public List<String> listDatabaseNames(Odps mcClient, String defaultProject) {
            return Collections.emptyList();
        }

        @Override
        public boolean databaseExist(Odps mcClient, String dbName) {
            return false;
        }

        @Override
        public List<Partition> getPartitions(Odps mcClient, String dbName, String tableName) {
            return Collections.emptyList();
        }

        @Override
        public Iterator<Partition> getPartitionIterator(Odps mcClient, String dbName, String tableName) {
            return Collections.emptyIterator();
        }

        @Override
        public Tables.TableCreator createTableCreator(Odps mcClient, String dbName,
                String tableName, TableSchema schema) {
            return null;
        }

        @Override
        public void dropTable(Odps mcClient, String dbName, String tableName, boolean ifExists)
                throws OdpsException {
        }

        @Override
        public void createDb(Odps mcClient, String dbName, boolean ifNotExists) {
        }

        @Override
        public void dropDb(Odps mcClient, String dbName, boolean ifExists) {
        }
    }
}
