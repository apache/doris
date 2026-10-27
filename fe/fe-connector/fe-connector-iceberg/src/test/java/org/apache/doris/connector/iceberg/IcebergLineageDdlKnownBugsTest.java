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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.iceberg.IcebergCatalogOps.CatalogBackedIcebergCatalogOps;
import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class IcebergLineageDdlKnownBugsTest {
    private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "events");
    private static final IcebergTableHandle HANDLE = new IcebergTableHandle("db", "events");

    @Test
    public void upgradeRejectsExistingRowIdColumn() throws Exception {
        assertUpgradePreservesUniqueMetadataNames("_row_id");
        assertUpgradePreservesUniqueMetadataNames("_ROW_ID");
    }

    @Test
    public void upgradeRejectsExistingSequenceColumn() throws Exception {
        assertUpgradePreservesUniqueMetadataNames("_last_updated_sequence_number");
        assertUpgradePreservesUniqueMetadataNames("_LAST_UPDATED_SEQUENCE_NUMBER");
    }

    @Test
    public void effectiveSchemaRejectsExistingV3LineageNameCollision() throws Exception {
        for (String reservedName : Arrays.asList(
                "_row_id", "_ROW_ID", "_last_updated_sequence_number", "_LAST_UPDATED_SEQUENCE_NUMBER")) {
            try (InMemoryCatalog catalog = catalog()) {
                catalog.createTable(IDENTIFIER, new Schema(
                                Types.NestedField.optional(1, reservedName, Types.LongType.get())),
                        PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "3"));

                DorisConnectorException exception = Assertions.assertThrows(DorisConnectorException.class,
                        () -> metadata(catalog).getTableSchema(null, HANDLE));
                Assertions.assertTrue(exception.getMessage().contains(reservedName), exception.getMessage());
                Assertions.assertTrue(exception.getMessage().contains("row lineage"), exception.getMessage());
            }
        }
    }

    @Test
    public void upgradeAllowsSchemaWithoutLineageNameCollision() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            catalog.createTable(IDENTIFIER, new Schema(
                            Types.NestedField.optional(1, "id", Types.LongType.get())),
                    PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "2"));

            IcebergConnectorMetadata metadata = metadata(catalog);
            metadata.setTableProperties(null, HANDLE, Collections.singletonMap(
                    TableProperties.FORMAT_VERSION, "3"));

            Table upgraded = catalog.loadTable(IDENTIFIER);
            Assertions.assertEquals(3,
                    ((HasTableOperations) upgraded).operations().current().formatVersion());
            Assertions.assertEquals(1L, metadata.getTableSchema(null, HANDLE).getColumns().stream()
                    .filter(column -> "_row_id".equalsIgnoreCase(column.getName())).count());
            Assertions.assertEquals(1L, metadata.getTableSchema(null, HANDLE).getColumns().stream()
                    .filter(column -> "_last_updated_sequence_number".equalsIgnoreCase(column.getName())).count());
        }
    }

    @Test
    public void nonUpgradePropertyUpdateAllowsExistingLineageColumnName() throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            catalog.createTable(IDENTIFIER, new Schema(
                            Types.NestedField.optional(1, "_row_id", Types.LongType.get())),
                    PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "2"));

            IcebergConnectorMetadata metadata = metadata(catalog);
            metadata.setTableProperties(null, HANDLE, Collections.singletonMap(
                    TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "134217728"));

            Table table = catalog.loadTable(IDENTIFIER);
            Assertions.assertEquals(2,
                    ((HasTableOperations) table).operations().current().formatVersion());
            Assertions.assertEquals("134217728",
                    table.properties().get(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES));
            Assertions.assertEquals(1L, metadata.getTableSchema(null, HANDLE).getColumns().stream()
                    .filter(column -> "_row_id".equalsIgnoreCase(column.getName())).count());
        }
    }

    @Test
    public void revalidatesSchemaAddedBeforePropertyCommit() throws Exception {
        assertConcurrentSchemaChangeRejected(RaceMode.BEFORE_COMMIT);
    }

    @Test
    public void revalidatesSchemaAddedBeforeCommitRetry() throws Exception {
        assertConcurrentSchemaChangeRejected(RaceMode.COMMIT_CONFLICT);
    }

    private void assertConcurrentSchemaChangeRejected(RaceMode raceMode) throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            catalog.createTable(IDENTIFIER, new Schema(
                            Types.NestedField.optional(1, "id", Types.LongType.get())),
                    PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "2"));
            Table underlying = catalog.loadTable(IDENTIFIER);
            RacingTableOperations racingOps = new RacingTableOperations(
                    ((HasTableOperations) underlying).operations(), raceMode);

            Catalog catalogApi = new SingleTableCatalog(new BaseTable(racingOps, IDENTIFIER.toString()));
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "134217728");
            properties.put(TableProperties.FORMAT_VERSION, "3");

            DorisConnectorException exception = Assertions.assertThrows(DorisConnectorException.class,
                    () -> new CatalogBackedIcebergCatalogOps(catalogApi)
                            .setTableProperties("db", "events", properties));
            Assertions.assertTrue(exception.getMessage().contains("_ROW_ID"), exception.getMessage());
            Assertions.assertTrue(racingOps.injected(), "the test must introduce the concurrent schema change");

            Table after = catalog.loadTable(IDENTIFIER);
            Assertions.assertEquals(2,
                    ((HasTableOperations) after).operations().current().formatVersion(),
                    "the racing upgrade must not commit format-version 3");
            Assertions.assertFalse(after.properties().containsKey(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES),
                    "the racing property map must not commit its non-conflicting entries");
            Assertions.assertNotNull(after.schema().findField("_ROW_ID"));
        }
    }

    private void assertUpgradePreservesUniqueMetadataNames(String reservedName) throws Exception {
        try (InMemoryCatalog catalog = catalog()) {
            catalog.createTable(IDENTIFIER, new Schema(
                            Types.NestedField.optional(1, reservedName, Types.LongType.get())),
                    PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "2"));
            Table before = catalog.loadTable(IDENTIFIER);
            int versionsBefore = metadataVersions(before);
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "134217728");
            properties.put(TableProperties.FORMAT_VERSION, "3");

            DorisConnectorException exception = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata(catalog).setTableProperties(null, HANDLE, properties));
            Assertions.assertTrue(exception.getMessage().contains(reservedName), exception.getMessage());

            Table after = catalog.loadTable(IDENTIFIER);
            Assertions.assertEquals(2,
                    ((HasTableOperations) after).operations().current().formatVersion(),
                    "a rejected upgrade must preserve format-version");
            Assertions.assertFalse(after.properties().containsKey(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES),
                    "a rejected property map must not commit its non-conflicting entries");
            Assertions.assertEquals(versionsBefore, metadataVersions(after),
                    "a rejected upgrade must not create a metadata version");
            Assertions.assertEquals(1, after.schema().columns().size());
            Assertions.assertEquals(reservedName, after.schema().columns().get(0).name());
        }
    }

    private static int metadataVersions(Table table) {
        return ((HasTableOperations) table).operations().current().previousFiles().size();
    }

    private static InMemoryCatalog catalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("repro", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
        return catalog;
    }

    private static IcebergConnectorMetadata metadata(InMemoryCatalog catalog) {
        return new IcebergConnectorMetadata(new CatalogBackedIcebergCatalogOps(catalog),
                IcebergCatalogProperties.of(Collections.singletonMap(
                        IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, IcebergCatalogProperties.TYPE_REST)),
                new RecordingConnectorContext());
    }

    private enum RaceMode {
        BEFORE_COMMIT,
        COMMIT_CONFLICT
    }

    private static final class SingleTableCatalog implements Catalog {
        private final Table table;

        private SingleTableCatalog(Table table) {
            this.table = table;
        }

        @Override
        public List<TableIdentifier> listTables(Namespace namespace) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Table loadTable(TableIdentifier identifier) {
            return table;
        }

        @Override
        public boolean dropTable(TableIdentifier identifier, boolean purge) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameTable(TableIdentifier from, TableIdentifier to) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class RacingTableOperations implements TableOperations {
        private final TableOperations delegate;
        private final RaceMode raceMode;
        private TableMetadata current;
        private boolean injected;

        private RacingTableOperations(TableOperations delegate, RaceMode raceMode) {
            this.delegate = delegate;
            this.raceMode = raceMode;
            this.current = delegate.current();
        }

        private boolean injected() {
            return injected;
        }

        private void injectConcurrentColumn() {
            new BaseTable(delegate, IDENTIFIER.toString())
                    .updateSchema().addColumn("_ROW_ID", Types.LongType.get()).commit();
            current = delegate.refresh();
            injected = true;
        }

        @Override
        public TableMetadata current() {
            return current;
        }

        @Override
        public TableMetadata refresh() {
            current = delegate.refresh();
            if (raceMode == RaceMode.BEFORE_COMMIT && !injected) {
                injectConcurrentColumn();
            }
            return current;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata metadata) {
            if (raceMode == RaceMode.COMMIT_CONFLICT && !injected) {
                injectConcurrentColumn();
                throw new CommitFailedException("simulated concurrent schema commit");
            }
            delegate.commit(base, metadata);
        }

        @Override
        public FileIO io() {
            return delegate.io();
        }

        @Override
        public EncryptionManager encryption() {
            return delegate.encryption();
        }

        @Override
        public String metadataFileLocation(String fileName) {
            return delegate.metadataFileLocation(fileName);
        }

        @Override
        public LocationProvider locationProvider() {
            return delegate.locationProvider();
        }
    }
}
