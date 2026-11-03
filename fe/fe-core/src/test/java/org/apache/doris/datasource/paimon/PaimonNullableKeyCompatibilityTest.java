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

package org.apache.doris.datasource.paimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableQueryAuth;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PaimonNullableKeyCompatibilityTest {
    @Test
    public void fileIndexesPreservesDeferredAuthorizationForEveryBranch() {
        CoreOptions options = Mockito.mock(CoreOptions.class);
        Mockito.when(options.queryAuthEnabled()).thenReturn(true);
        FileStoreTable main = Mockito.mock(FileStoreTable.class);
        FileStoreTable fallback = Mockito.mock(FileStoreTable.class);
        CatalogEnvironment environment = Mockito.mock(CatalogEnvironment.class);
        TableQueryAuth auth = Mockito.mock(TableQueryAuth.class);
        Mockito.when(main.coreOptions()).thenReturn(options);
        Mockito.when(fallback.coreOptions()).thenReturn(options);
        Mockito.when(main.catalogEnvironment()).thenReturn(environment);
        Mockito.when(fallback.catalogEnvironment()).thenReturn(environment);
        Mockito.when(environment.tableQueryAuth(options)).thenReturn(auth);
        PaimonSysExternalTable.authorizeDeferredScan(
                new FallbackReadFileStoreTable(main, fallback, true));
        Mockito.verify(auth, Mockito.times(2)).auth(null);
        Mockito.doThrow(new SecurityException("denied file indexes")).when(auth).auth(null);
        Assertions.assertThrows(SecurityException.class,
                () -> PaimonSysExternalTable.authorizeDeferredScan(main));
    }

    @Test
    public void nullablePrimaryKeyPreservesNullabilityAndMergeSemantics(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "nullable_keys");
            catalog.createTable(id, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("val", DataTypes.INT())
                    .primaryKey("id")
                    .option("bucket", "1")
                    .option("primary-key.nullable", "true")
                    .build(), false);
            Table table = catalog.getTable(id);
            Assertions.assertTrue(table.rowType().getTypeAt(0).isNullable());
            Assertions.assertTrue(PaimonUtil.parseSchema(table, false, false).get(0).isAllowNull());

            for (int value : new int[] {10, 20}) {
                BatchWriteBuilder builder = table.newBatchWriteBuilder();
                try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
                    write.write(GenericRow.of(null, value));
                    write.write(GenericRow.of(1, 100));
                    commit.commit(write.prepareCommit());
                }
            }

            Table backend = PaimonUtil.deserializeObject(PaimonUtil.serializeObject(table));
            Assertions.assertTrue(backend.rowType().getTypeAt(0).isNullable());
            ReadBuilder builder = backend.newReadBuilder();
            List<String> rows = new ArrayList<>();
            for (Split split : builder.newScan().plan().splits()) {
                try (RecordReader<InternalRow> reader = builder.newRead().createReader(split)) {
                    reader.forEachRemaining(row -> rows.add(
                            (row.isNullAt(0) ? "NULL" : Integer.toString(row.getInt(0))) + ":" + row.getInt(1)));
                }
            }
            Collections.sort(rows);
            Assertions.assertEquals(Arrays.asList("1:100", "NULL:20"), rows,
                    "Repeated NULL primary keys must merge as the same key across commits");
        }
    }

}
