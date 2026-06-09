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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Characterization tests for {@link PaimonConnectorMetadata}, pinning the read-path behavior
 * after the {@link PaimonCatalogOps} seam extraction (B0).
 *
 * <p>The seam fully covers every remote {@code Catalog} call the metadata makes, so each test
 * drives a {@link RecordingPaimonCatalogOps} fake and builds the metadata with a {@code null}
 * real catalog — the tests are entirely offline (no live remote catalog), which is the whole
 * point of introducing the seam.
 */
public class PaimonConnectorMetadataTest {

    private static PaimonConnectorMetadata metadataWith(RecordingPaimonCatalogOps ops) {
        // Read-path tests ignore the context; a default RecordingConnectorContext is a no-op wrapper.
        return new PaimonConnectorMetadata(ops, Collections.emptyMap(), new RecordingConnectorContext());
    }

    private static RowType rowType(String... columnNames) {
        RowType.Builder builder = RowType.builder();
        for (String name : columnNames) {
            builder.field(name, DataTypes.INT());
        }
        return builder.build();
    }

    @Test
    public void listDatabaseNamesDelegatesToOps() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.databases = Arrays.asList("db_a", "db_b");

        List<String> result = metadataWith(ops).listDatabaseNames(null);

        // WHY: listDatabaseNames must return exactly what the remote catalog reports, in order;
        // it is the only source of the catalog's database list shown to users.
        // MUTATION: returning Collections.emptyList() (dropping the delegation) -> red.
        Assertions.assertEquals(Arrays.asList("db_a", "db_b"), result);
        Assertions.assertEquals(Collections.singletonList("listDatabases"), ops.log,
                "listDatabaseNames must make exactly one listDatabases() call on the seam");
    }

    @Test
    public void databaseExistsTrueWhenGetDatabaseSucceeds() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();

        boolean exists = metadataWith(ops).databaseExists(null, "db1");

        // WHY: existence is defined as "getDatabase did not throw NotExist". A successful
        // getDatabase must map to true. MUTATION: returning false on success -> red.
        Assertions.assertTrue(exists);
        Assertions.assertEquals(Collections.singletonList("getDatabase:db1"), ops.log);
    }

    @Test
    public void databaseExistsFalseWhenGetDatabaseThrowsNotExist() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.throwDatabaseNotExist = true;

        boolean exists = metadataWith(ops).databaseExists(null, "ghost");

        // WHY: the contract is that DatabaseNotExistException means "absent" (false), NOT a
        // thrown error to the caller. MUTATION: removing the catch (letting the exception
        // propagate) or returning true -> red. This is exactly the branch a recording fake can
        // exercise but a live-catalog test cannot reliably force.
        Assertions.assertFalse(exists);
    }

    @Test
    public void listTableNamesDelegatesToOps() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.tables = Arrays.asList("t1", "t2");

        List<String> result = metadataWith(ops).listTableNames(null, "db1");

        // WHY: listTableNames must surface exactly the remote table list for the given db.
        // MUTATION: returning emptyList (dropping delegation) -> red.
        Assertions.assertEquals(Arrays.asList("t1", "t2"), result);
        Assertions.assertEquals(Collections.singletonList("listTables:db1"), ops.log);
    }

    @Test
    public void listTableNamesReturnsEmptyWhenDatabaseMissing() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.throwDatabaseNotExist = true;

        List<String> result = metadataWith(ops).listTableNames(null, "ghost");

        // WHY: a missing database must degrade to an empty list, not propagate the checked
        // DatabaseNotExistException to the SPI caller. MUTATION: removing that catch -> red.
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void getTableHandleCarriesPartitionAndPrimaryKeysAndSetsTransientTable() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1",
                rowType("id", "dt", "region"),
                Arrays.asList("dt", "region"),
                Collections.singletonList("id"));
        ops.table = table;

        Optional<ConnectorTableHandle> handleOpt = metadataWith(ops).getTableHandle(null, "db1", "t1");

        Assertions.assertTrue(handleOpt.isPresent());
        PaimonTableHandle handle = (PaimonTableHandle) handleOpt.get();
        // WHY: partition/primary keys are the serializable identity the FE later relies on for
        // partition pruning and bucketing; they MUST be copied from the live table onto the
        // handle. MUTATION: hardcoding emptyList for either -> red.
        Assertions.assertEquals(Arrays.asList("dt", "region"), handle.getPartitionKeys(),
                "partition keys must be carried from the Paimon table onto the handle");
        Assertions.assertEquals(Collections.singletonList("id"), handle.getPrimaryKeys(),
                "primary keys must be carried from the Paimon table onto the handle");
        // WHY: the transient Table is the fast path used by getColumnHandles; failing to set it
        // would force an extra remote reload on every column lookup. MUTATION: dropping
        // handle.setPaimonTable(table) -> getPaimonTable() is null -> red.
        Assertions.assertSame(table, handle.getPaimonTable(),
                "the resolved Paimon table must be stashed on the handle as the transient ref");
    }

    @Test
    public void getTableHandleEmptyWhenTableMissing() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.throwTableNotExist = true;

        Optional<ConnectorTableHandle> handleOpt = metadataWith(ops).getTableHandle(null, "db1", "ghost");

        // WHY: a missing table is an absent handle (Optional.empty), not a thrown error.
        // MUTATION: removing the TableNotExistException catch -> red.
        Assertions.assertFalse(handleOpt.isPresent());
    }

    @Test
    public void getColumnHandlesReloadFallbackReloadsWhenTransientTableNull() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        ops.table = new FakePaimonTable(
                "t1",
                rowType("id", "name"),
                Collections.emptyList(),
                Collections.emptyList());
        // A handle whose transient Table is null (e.g. after serialization across the FE/BE
        // boundary) — the metadata must reload via the seam rather than NPE.
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        Assertions.assertNull(handle.getPaimonTable(), "precondition: transient table is null");

        Map<String, ConnectorColumnHandle> handles = metadataWith(ops).getColumnHandles(null, handle);

        // WHY: this is the reload-fallback safety net. With a null transient Table, the only way
        // to get column handles is to re-fetch the table from the catalog seam. MUTATION:
        // removing the `if (table == null) { table = ops.getTable(id); }` block -> NPE on
        // table.rowType() -> red. The recorded getTable call proves the reload happened.
        Assertions.assertEquals(Arrays.asList("id", "name"), new java.util.ArrayList<>(handles.keySet()),
                "column handles must be derived from the reloaded table's row type, in order");
        Assertions.assertTrue(ops.log.contains("getTable:db1.t1"),
                "reload-fallback must re-fetch the table from the seam when the transient ref is null");
    }

    @Test
    public void getColumnHandlesUsesTransientTableWithoutReload() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1",
                rowType("id", "name"),
                Collections.emptyList(),
                Collections.emptyList());
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);

        Map<String, ConnectorColumnHandle> handles = metadataWith(ops).getColumnHandles(null, handle);

        // WHY: the fast path — when the transient Table is already present, getColumnHandles must
        // use it and NOT make a redundant remote getTable call. MUTATION: always reloading would
        // record a getTable entry -> red. This pins the reload as a fallback, not the default.
        Assertions.assertEquals(Arrays.asList("id", "name"), new java.util.ArrayList<>(handles.keySet()));
        Assertions.assertTrue(ops.log.isEmpty(),
                "with a present transient table, no remote getTable reload must happen");
    }

    @Test
    public void disablesCastPredicatePushdown() {
        PaimonConnectorMetadata metadata =
                new PaimonConnectorMetadata(null, Collections.emptyMap(), new RecordingConnectorContext());

        // WHY: the shared converter unwraps CAST shells, so if this returned true (the SPI
        // default), a predicate like CAST(str_col AS INT)=5 would be pushed to Paimon as
        // str_col="5" and used for file/partition pruning, silently dropping rows like "05"/" 5"
        // at the source (BE re-eval cannot recover source-dropped rows). Returning false keeps
        // CAST conjuncts BE-only, mirroring MaxCompute/Jdbc. MUTATION: removing the override (or
        // flipping it to true) reverts to the default true -> red. The getter touches no instance
        // field, so a null ops / null session keeps this offline.
        Assertions.assertFalse(metadata.supportsCastPredicatePushdown(null),
                "Paimon must disable CAST-predicate pushdown: the converter unwraps CAST shells "
                        + "and pushing the stripped predicate under-matches at the source, "
                        + "silently dropping rows BE re-eval cannot recover");
    }
}
