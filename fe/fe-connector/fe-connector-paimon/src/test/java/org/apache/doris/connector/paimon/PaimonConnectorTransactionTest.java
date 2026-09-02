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

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.thrift.TPaimonCommitMessage;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PaimonConnectorTransactionTest {

    private static final Identifier TABLE = Identifier.create("db", "t");

    @TempDir
    Path warehouse;

    @Test
    public void appendCommitsRealPaimonMessagesAndDeduplicatesReports() throws Exception {
        Catalog catalog = createCatalog();
        PaimonConnectorTransaction transaction = transaction(catalog, 101L, false);
        byte[] report = prepareReport(catalog, transaction, GenericRow.of(1, 10L), 2L);

        transaction.addCommitData(report);
        transaction.addCommitData(report);
        transaction.commit();

        Assertions.assertEquals(2L, transaction.getUpdateCnt());
        Assertions.assertEquals(Collections.singletonList("1:10"), read(catalog));
    }

    @Test
    public void emptyOverwriteRemovesExistingRows() throws Exception {
        Catalog catalog = createCatalog();
        PaimonConnectorTransaction append = transaction(catalog, 102L, false);
        append.addCommitData(prepareReport(catalog, append, GenericRow.of(1, 10L), 1L));
        append.commit();

        PaimonConnectorTransaction overwrite = transaction(catalog, 103L, true);
        overwrite.commit();

        Assertions.assertTrue(read(catalog).isEmpty());
    }

    @Test
    public void rollbackAbortsPreparedFiles() throws Exception {
        Catalog catalog = createCatalog();
        PaimonConnectorTransaction transaction = transaction(catalog, 104L, false);
        transaction.addCommitData(prepareReport(catalog, transaction, GenericRow.of(1, 10L), 1L));

        transaction.rollback();

        Assertions.assertTrue(read(catalog).isEmpty());
    }

    @Test
    public void rollbackStillWorksAfterConcurrentSchemaChange() throws Exception {
        Catalog catalog = createCatalog();
        PaimonConnectorTransaction transaction = transaction(catalog, 105L, false);
        transaction.addCommitData(prepareReport(catalog, transaction, GenericRow.of(1, 10L), 1L));
        catalog.alterTable(TABLE, SchemaChange.addColumn("extra", DataTypes.BIGINT()), false);

        Assertions.assertThrows(DorisConnectorException.class, transaction::commit);
        transaction.rollback();

        Assertions.assertTrue(read(catalog).isEmpty());
    }

    @Test
    public void rejectsMalformedCommitPayload() throws Exception {
        Catalog catalog = createCatalog();
        PaimonConnectorTransaction transaction = transaction(catalog, 106L, false);
        TPaimonCommitMessage message = new TPaimonCommitMessage().setPayload(new byte[] {1, 2, 3});
        transaction.addCommitData(new TSerializer(new TBinaryProtocol.Factory()).serialize(message));

        Assertions.assertThrows(DorisConnectorException.class, transaction::commit);
    }

    private Catalog createCatalog() throws Exception {
        Catalog catalog = new FileSystemCatalog(
                LocalFileIO.create(), new org.apache.paimon.fs.Path(warehouse.toUri()));
        catalog.createDatabase("db", false);
        catalog.createTable(TABLE, Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("value", DataTypes.BIGINT())
                .build(), false);
        return catalog;
    }

    private static PaimonConnectorTransaction transaction(
            Catalog catalog, long transactionId, boolean overwrite) throws Exception {
        PaimonCatalogOps ops = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(catalog);
        FileStoreTable table = (FileStoreTable) catalog.getTable(TABLE);
        FileStoreTable writeTable = PaimonWriteBinding.configureTableForWrite(
                table, overwrite, Collections.emptyMap());
        PaimonConnectorTransaction transaction = new PaimonConnectorTransaction(
                transactionId, ops, new RecordingConnectorContext());
        transaction.bind(new PaimonWriteBinding(
                TABLE, writeTable, Collections.emptyMap(), overwrite,
                Collections.emptyMap(), PaimonWritePlanProvider.writeMetadataIdentity(writeTable)));
        return transaction;
    }

    private static byte[] prepareReport(Catalog catalog, PaimonConnectorTransaction transaction,
            InternalRow row, long rowCount) throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(TABLE);
        TableWriteImpl<?> writer = table.newWrite(transaction.getCommitUser());
        try {
            writer.write(row);
            List<CommitMessage> messages = writer.prepareCommit(true, transaction.getTransactionId());
            byte[] payload = frame(messages);
            TPaimonCommitMessage report = new TPaimonCommitMessage()
                    .setPayload(payload)
                    .setRowCount(rowCount);
            return new TSerializer(new TBinaryProtocol.Factory()).serialize(report);
        } finally {
            writer.close();
        }
    }

    private static byte[] frame(List<CommitMessage> messages) throws Exception {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        serializer.serializeList(messages, new DataOutputViewStreamWrapper(output));
        byte[] serialized = output.toByteArray();
        ByteBuffer framed = ByteBuffer.allocate(12 + serialized.length);
        framed.put(new byte[] {'D', 'P', 'C', 'M'});
        framed.putInt(serializer.getVersion());
        framed.putInt(serialized.length);
        framed.put(serialized);
        return framed.array();
    }

    private static List<String> read(Catalog catalog) throws Exception {
        ReadBuilder builder = catalog.getTable(TABLE).newReadBuilder();
        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(row.getInt(0) + ":" + row.getLong(1)));
        }
        Collections.sort(rows);
        return rows;
    }
}
