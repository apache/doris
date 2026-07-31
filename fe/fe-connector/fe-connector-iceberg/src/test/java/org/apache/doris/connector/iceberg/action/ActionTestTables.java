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

import org.apache.doris.connector.api.ConnectorSession;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;

import java.util.Collections;
import java.util.Map;

/**
 * Shared no-Mockito test fixtures for the iceberg {@code ALTER TABLE EXECUTE} action bodies: a real
 * {@link InMemoryCatalog}, snapshot seeding via {@code newAppend().commit()}, and a minimal
 * {@link ConnectorSession}. Mirrors the {@code InMemoryCatalog} style of {@code IcebergConnectorTransactionTest}
 * / {@code IcebergScanPlanProviderTest}; the action bodies operate on the loaded SDK {@link Table} exactly as
 * {@code IcebergProcedureOps} hands it to them.
 */
final class ActionTestTables {

    static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    private ActionTestTables() {
    }

    static InMemoryCatalog freshCatalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        return catalog;
    }

    static TableIdentifier id(String table) {
        return TableIdentifier.of("db1", table);
    }

    static Table createTable(InMemoryCatalog catalog, String table, Map<String, String> props) {
        return catalog.createTable(id(table), SCHEMA, PartitionSpec.unpartitioned(), props);
    }

    static Table createTable(InMemoryCatalog catalog, String table) {
        return createTable(catalog, table, Collections.emptyMap());
    }

    /** Appends one data file as a new snapshot and returns the new current snapshot id. */
    static long appendSnapshot(InMemoryCatalog catalog, String table, String fileName, long records) {
        Table t = catalog.loadTable(id(table));
        t.newAppend().appendFile(dataFile(fileName, records)).commit();
        return catalog.loadTable(id(table)).currentSnapshot().snapshotId();
    }

    static DataFile dataFile(String fileName, long records) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("s3://b/db1/" + fileName)
                .withFileSizeInBytes(1024)
                .withRecordCount(records)
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    /** Commits a position-delete file as a new snapshot (creates a delete manifest carried forward by later ones). */
    static void addPositionDeleteSnapshot(InMemoryCatalog catalog, String table, String fileName,
            String referencedDataFile) {
        catalog.loadTable(id(table)).newRowDelta()
                .addDeletes(FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                        .ofPositionDeletes()
                        .withPath("s3://b/db1/" + fileName)
                        .withFormat(FileFormat.PARQUET)
                        .withFileSizeInBytes(64L)
                        .withRecordCount(1L)
                        .withReferencedDataFile("s3://b/db1/" + referencedDataFile)
                        .build())
                .commit();
    }

    /** Commits an equality-delete file as a new snapshot (a distinct delete manifest). */
    static void addEqualityDeleteSnapshot(InMemoryCatalog catalog, String table, String fileName) {
        catalog.loadTable(id(table)).newRowDelta()
                .addDeletes(FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                        .ofEqualityDeletes(1)
                        .withPath("s3://b/db1/" + fileName)
                        .withFormat(FileFormat.PARQUET)
                        .withFileSizeInBytes(64L)
                        .withRecordCount(1L)
                        .build())
                .commit();
    }

    static ConnectorSession session(String timeZone) {
        return new FakeSession(timeZone);
    }

    /** Minimal {@link ConnectorSession} exposing a time zone (the only field the actions consult). */
    private static final class FakeSession implements ConnectorSession {
        private final String timeZone;

        FakeSession(String timeZone) {
            this.timeZone = timeZone;
        }

        @Override
        public String getQueryId() {
            return "q";
        }

        @Override
        public String getUser() {
            return "u";
        }

        @Override
        public String getTimeZone() {
            return timeZone;
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public long getCatalogId() {
            return 0;
        }

        @Override
        public String getCatalogName() {
            return "test";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }
    }
}
