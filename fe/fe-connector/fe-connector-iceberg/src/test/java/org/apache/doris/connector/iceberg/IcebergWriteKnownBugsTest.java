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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class IcebergWriteKnownBugsTest {
    private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "events");
    private static final String SEED = "s3://bucket/events/seed.parquet";
    private static final String ADDED = "s3://bucket/events/added.parquet";
    private static final String DELETED = "s3://bucket/events/deleted.parquet";

    @Test
    public void deleteHonorsStandardSnapshotIsolationProperty() throws Exception {
        assertStandardSnapshotIsolation(WriteOperation.DELETE, "write.delete.isolation-level");
    }

    @Test
    public void updateHonorsStandardSnapshotIsolationProperty() throws Exception {
        assertStandardSnapshotIsolation(WriteOperation.UPDATE, "write.update.isolation-level");
    }

    @Test
    public void mergeHonorsStandardSnapshotIsolationProperty() throws Exception {
        assertStandardSnapshotIsolation(WriteOperation.MERGE, "write.merge.isolation-level");
    }

    private void assertStandardSnapshotIsolation(WriteOperation operation, String property) throws Exception {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog);
        table.updateProperties().set(property, "snapshot").commit();
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(SEED)
                .withFileSizeInBytes(1024).withRecordCount(3).build()).commit();
        IcebergConnectorTransaction transaction = transaction(catalog.loadTable(IDENTIFIER));
        transaction.beginWrite(session(), "db", "events", new IcebergWriteContext(
                operation, false, Collections.emptyMap(), Optional.empty()));
        catalog.loadTable(IDENTIFIER).newAppend().appendFile(DataFiles.builder(table.spec()).withPath(ADDED)
                .withFileSizeInBytes(1024).withRecordCount(1).build()).commit();
        transaction.addCommitData(serialize(fragment(DELETED, 1, TFileContent.POSITION_DELETES)));
        // Snapshot isolation permits a concurrent append while still validating referenced data files.
        Assertions.assertDoesNotThrow(transaction::commit);
    }

    @Test
    @Disabled("Known issue: mixed INSERT and DELETE MERGE actions undercount affected rows.")
    public void mergeCountsBothInsertedAndDeletedRows() throws Exception {
        InMemoryCatalog catalog = catalog();
        Table table = create(catalog);
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(SEED)
                .withFileSizeInBytes(1024).withRecordCount(3).build()).commit();
        IcebergConnectorTransaction transaction = transaction(catalog.loadTable(IDENTIFIER));
        transaction.beginWrite(session(), "db", "events", new IcebergWriteContext(
                WriteOperation.MERGE, false, Collections.emptyMap(), Optional.empty()));
        transaction.addCommitData(serialize(fragment(ADDED, 1, TFileContent.DATA)));
        transaction.addCommitData(serialize(fragment(DELETED, 1, TFileContent.POSITION_DELETES)));
        transaction.commit();
        // These actions target disjoint rows; counting both does not double-count an UPDATE pair.
        Assertions.assertEquals(2, transaction.getUpdateCnt());
    }

    private static InMemoryCatalog catalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("repro", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
        return catalog;
    }

    private static Table create(InMemoryCatalog catalog) {
        return catalog.createTable(IDENTIFIER, new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "2"));
    }

    private static IcebergConnectorTransaction transaction(Table table) {
        RecordingIcebergCatalogOps operations = new RecordingIcebergCatalogOps();
        operations.table = table;
        return new IcebergConnectorTransaction(9, operations, new RecordingConnectorContext());
    }

    private static TIcebergCommitData fragment(String path, long count, TFileContent content) {
        TIcebergCommitData fragment = new TIcebergCommitData();
        fragment.setFilePath(path);
        fragment.setFileContent(content);
        fragment.setFileSize(1024);
        fragment.setRowCount(count);
        fragment.setAffectedRows(count);
        if (content == TFileContent.POSITION_DELETES) {
            fragment.setReferencedDataFilePath(SEED);
        }
        return fragment;
    }

    private static byte[] serialize(TIcebergCommitData fragment) throws Exception {
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(fragment);
    }

    private static ConnectorSession session() {
        return new ConnectorSession() {
            @Override
            public String getQueryId() {
                return "repro";
            }

            @Override
            public String getUser() {
                return "test";
            }

            @Override
            public String getTimeZone() {
                return "UTC";
            }

            @Override
            public String getLocale() {
                return "en_US";
            }

            @Override
            public long getCatalogId() {
                return 1;
            }

            @Override
            public String getCatalogName() {
                return "repro";
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return Collections.emptyMap();
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return null;
            }
        };
    }
}
