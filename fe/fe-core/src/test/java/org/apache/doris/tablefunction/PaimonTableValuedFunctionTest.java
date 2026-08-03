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

import org.apache.doris.datasource.paimon.PaimonReaderOptions;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.privilege.PrivilegeChecker;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.PartitionsTable;
import org.apache.paimon.table.system.ReadOptimizedTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

public class PaimonTableValuedFunctionTest {

    @Test
    void testMetadataTableWrapsCpuCappedDataTable() {
        int localCapacity = Runtime.getRuntime().availableProcessors();
        Assumptions.assumeTrue(localCapacity < PaimonReaderOptions.MAX_MANIFEST_PARALLELISM);
        FileStoreTable dataTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable safeDataTable = Mockito.mock(FileStoreTable.class);
        Mockito.when(dataTable.options()).thenReturn(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), String.valueOf(localCapacity + 1)));
        Mockito.when(safeDataTable.options()).thenReturn(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), String.valueOf(localCapacity)));
        // The runtime cap must preserve the latest bound schema instead of replaying snapshot
        // options while constructing the disposable metadata-planning handle.
        Mockito.when(dataTable.copyWithoutTimeTravel(Mockito.anyMap())).thenReturn(safeDataTable);

        Table systemTable = PaimonTableValuedFunction.createRuntimeSafeSystemTable(
                dataTable, PartitionsTable.PARTITIONS);

        Assertions.assertTrue(systemTable instanceof PartitionsTable);
        Mockito.verify(dataTable).copyWithoutTimeTravel(Mockito.argThat((Map<String, String> options) ->
                String.valueOf(localCapacity).equals(
                        options.get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()))));
    }

    @Test
    void testReadOptimizedTableKeepsFallbackPairAsDirectSource() throws Exception {
        FileStoreTable main = Mockito.mock(FileStoreTable.class);
        FileStoreTable fallback = Mockito.mock(FileStoreTable.class);
        TableSchema schema = Mockito.mock(TableSchema.class);
        Mockito.when(schema.options()).thenReturn(Collections.emptyMap());
        Mockito.when(main.schema()).thenReturn(schema);
        Mockito.when(fallback.schema()).thenReturn(schema);
        Mockito.when(main.options()).thenReturn(Collections.emptyMap());
        Mockito.when(fallback.options()).thenReturn(Collections.emptyMap());
        FallbackReadFileStoreTable pair = new FallbackReadFileStoreTable(main, fallback);
        FileStoreTable privileged = PrivilegedFileStoreTable.wrap(
                pair, Mockito.mock(PrivilegeChecker.class), Identifier.create("db", "table"));

        Table systemTable = PaimonTableValuedFunction.createRuntimeSafeSystemTable(privileged, "ro");

        Assertions.assertInstanceOf(ReadOptimizedTable.class, systemTable);
        Field wrapped = ReadOptimizedTable.class.getDeclaredField("wrapped");
        wrapped.setAccessible(true);
        Assertions.assertSame(pair, wrapped.get(systemTable));
    }
}
