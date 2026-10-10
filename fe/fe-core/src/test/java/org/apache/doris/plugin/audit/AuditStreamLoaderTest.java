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

package org.apache.doris.plugin.audit;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InternalSchema;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.plugin.AuditEvent;

import com.google.common.base.Splitter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class AuditStreamLoaderTest {

    @Test
    public void testConnectionUsesGzipCompression() throws Exception {
        HttpURLConnection connection = Deencapsulation.invoke(AuditStreamLoader.class, "getConnection",
                "http://127.0.0.1:8030/api/db/table/_stream_load?", "label", "token", "query_id,time");

        Assertions.assertEquals("gz", connection.getRequestProperty("compress_type"));
        Assertions.assertEquals("0", connection.getRequestProperty("max_filter_ratio"));
        Assertions.assertEquals("query_id,time", connection.getRequestProperty("columns"));
    }

    @Test
    public void testOldAuditTableProjectionAndNewTableShape() {
        AuditLoader loader = new AuditLoader();
        StringBuilder rows = new StringBuilder();
        for (String id : List.of("first", "second")) {
            Deencapsulation.invoke(loader, "fillLogBuffer", new AuditEvent.AuditEventBuilder()
                    .setQueryId(id).setSpillWriteBytesToRemoteStorage(17)
                    .setSpillReadBytesFromRemoteStorage(19).setStmt("select 1").build(), rows);
        }
        AuditStreamLoader.PreparedBatch oldBatch = AuditStreamLoader.prepareBatch(rows, false);
        AuditStreamLoader.PreparedBatch newBatch = AuditStreamLoader.prepareBatch(rows, true);
        List<String> oldNames = Splitter.on(',').splitToList(oldBatch.columns);
        List<String> allNames = InternalSchema.AUDIT_SCHEMA.stream().map(c -> c.getName())
                .collect(Collectors.toList());
        Assertions.assertEquals(allNames.size() - 2, oldNames.size());
        Assertions.assertFalse(oldNames.contains("spill_write_bytes_to_remote_storage"));
        Assertions.assertFalse(oldNames.contains("spill_read_bytes_from_remote_storage"));
        Assertions.assertEquals(rows.toString(), newBatch.payload.toString());
        Assertions.assertEquals(String.join(",", allNames), newBatch.columns);
        for (String row : Splitter.on(AuditLoader.AUDIT_TABLE_LINE_DELIMITER)
                .omitEmptyStrings().split(oldBatch.payload)) {
            List<String> fields = Splitter.on(AuditLoader.AUDIT_TABLE_COL_SEPARATOR).splitToList(row);
            Assertions.assertEquals(oldNames.size(), fields.size());
            Assertions.assertEquals("select 1", fields.get(oldNames.indexOf("stmt")));
        }
        Assertions.assertTrue(oldBatch.payload.toString().contains("first"));
        Assertions.assertTrue(oldBatch.payload.toString().contains("second"));
    }

    @Test
    public void testPartialSchemaDoesNotEnableRemoteAuditColumns() {
        Column write = Mockito.mock(Column.class);
        Column read = Mockito.mock(Column.class);
        Mockito.when(write.getName()).thenReturn("spill_write_bytes_to_remote_storage");
        Mockito.when(read.getName()).thenReturn("spill_read_bytes_from_remote_storage");
        Assertions.assertFalse(AuditStreamLoader.hasRemoteSpillColumns(List.of(write)));
        Assertions.assertTrue(AuditStreamLoader.hasRemoteSpillColumns(List.of(write, read)));
    }

    @Test
    public void testFollowerSeesActualAuditTableSchemaBeforeSelectingLoadShape() {
        Column write = Mockito.mock(Column.class);
        Column read = Mockito.mock(Column.class);
        Mockito.when(write.getName()).thenReturn("spill_write_bytes_to_remote_storage");
        Mockito.when(read.getName()).thenReturn("spill_read_bytes_from_remote_storage");
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        Table table = Mockito.mock(Table.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDb(FeConstants.INTERNAL_DB_NAME)).thenReturn(Optional.of(database));
        Mockito.when(database.getTable(AuditLoader.AUDIT_LOG_TABLE)).thenReturn(Optional.of(table));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(table.getBaseSchema()).thenReturn(List.of(write));
            Assertions.assertFalse(AuditStreamLoader.targetHasRemoteSpillColumns());
            Mockito.when(table.getBaseSchema()).thenReturn(List.of(write, read));
            Assertions.assertTrue(AuditStreamLoader.targetHasRemoteSpillColumns());
        }
    }

    @Test
    public void testLoadResponseRejectsFailedOrFilteredBatch() {
        Assertions.assertFalse(new AuditStreamLoader.LoadResponse(500, "error", "").succeeded(2));
        Assertions.assertFalse(new AuditStreamLoader.LoadResponse(200, "OK", "{\"Status\":\"Fail\"}")
                .succeeded(2));
        Assertions.assertFalse(new AuditStreamLoader.LoadResponse(200, "OK",
                "{\"Status\":\"Success\",\"NumberLoadedRows\":1}").succeeded(2));
        Assertions.assertTrue(new AuditStreamLoader.LoadResponse(200, "OK",
                "{\"Status\":\"Success\",\"NumberLoadedRows\":1}").rejectedOrIncomplete(2));
        Assertions.assertTrue(new AuditStreamLoader.LoadResponse(200, "OK",
                "{\"Status\":\"Success\",\"NumberLoadedRows\":2,\"NumberFilteredRows\":1}")
                .rejectedOrIncomplete(2));
        Assertions.assertFalse(new AuditStreamLoader.LoadResponse(200, "OK",
                "{\"Status\":\"Success\"}").succeeded(2));
        Assertions.assertFalse(new AuditStreamLoader.LoadResponse(200, "OK",
                "{\"Status\":\"Label Already Exists\",\"ExistingJobStatus\":\"RUNNING\"}").succeeded(2));
        Assertions.assertTrue(new AuditStreamLoader.LoadResponse(200, "OK",
                "{\"Status\":\"Label Already Exists\",\"ExistingJobStatus\":\"FINISHED\"}").succeeded(2));
        Assertions.assertTrue(new AuditStreamLoader.LoadResponse(200, "OK",
                "{\"Status\":\"Success\",\"NumberLoadedRows\":2,\"NumberFilteredRows\":0}").succeeded(2));
    }

    @Test
    public void testWriteCompressedBody() throws Exception {
        String payload = "audit row 中文\u001fselect 1\u001e";
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();

        Deencapsulation.invoke(AuditStreamLoader.class, "writeCompressedBody",
                compressed, new StringBuilder(payload));

        byte[] bytes = compressed.toByteArray();
        Assertions.assertEquals(0x1f, bytes[0] & 0xff);
        Assertions.assertEquals(0x8b, bytes[1] & 0xff);
        Assertions.assertEquals(payload, decompress(bytes));
    }

    private static String decompress(byte[] compressed) throws IOException {
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(compressed));
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = gzipInputStream.read(buffer)) != -1) {
                output.write(buffer, 0, bytesRead);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}
