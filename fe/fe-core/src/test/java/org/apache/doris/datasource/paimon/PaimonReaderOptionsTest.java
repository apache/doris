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

import org.apache.doris.datasource.property.metastore.AbstractPaimonProperties;

import com.google.common.collect.ImmutableMap;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.privilege.PrivilegeChecker;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

public class PaimonReaderOptionsTest {

    @Test
    void testRejectUnsafeOptionsFromCreateOrAlterProperties() {
        for (Map<String, String> properties : new Map[] {
                ImmutableMap.of(AbstractPaimonProperties.TABLE_OPTION_PREFIX + "branch", "archive"),
                ImmutableMap.of(AbstractPaimonProperties.TABLE_OPTION_PREFIX + "read.batch-size", "0"),
                ImmutableMap.of(AbstractPaimonProperties.TABLE_OPTION_PREFIX
                        + "file-reader-async-threshold", "2 GB")
        }) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> PaimonReaderOptions.validateCatalogProperties(properties));
        }
    }

    @Test
    void testRejectUnsafeEffectivePhysicalTableOptions() {
        for (Map<String, String> options : new Map[] {
                ImmutableMap.of("read.batch-size", "0"),
                ImmutableMap.of("file-reader-async-threshold", "512 KB"),
                ImmutableMap.of("scan.manifest.parallelism", "0"),
                ImmutableMap.of("scan.manifest.parallelism",
                        String.valueOf(Runtime.getRuntime().availableProcessors() + 1))
        }) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> PaimonReaderOptions.validateEffectiveTableOptions(options));
        }
    }

    @Test
    void testRejectUnsafeOptionOnlyAfterFinalTableCopy() {
        Table physicalTable = Mockito.mock(Table.class);
        Table finalTable = Mockito.mock(Table.class);
        Mockito.when(physicalTable.copy(Collections.emptyMap())).thenReturn(finalTable);
        Mockito.when(finalTable.options()).thenReturn(ImmutableMap.of("read.batch-size", "0"));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonScanParams.applyOptions(physicalTable, Collections.emptyMap()));
    }

    @Test
    void testSafeRelationOptionOverridesUnsafePhysicalOption() {
        Table physicalTable = Mockito.mock(Table.class);
        Table finalTable = Mockito.mock(Table.class);
        Map<String, String> relationOptions = ImmutableMap.of("read.batch-size", "4096");
        Mockito.when(physicalTable.options()).thenReturn(ImmutableMap.of("read.batch-size", "0"));
        Mockito.when(physicalTable.copy(relationOptions)).thenReturn(finalTable);
        Mockito.when(finalTable.options()).thenReturn(relationOptions);

        Assertions.assertSame(finalTable, PaimonScanParams.applyOptions(physicalTable, relationOptions));
    }

    @Test
    void testRejectUnsafeHiddenFallbackTableAfterCopy() {
        FileStoreTable main = newFileStoreTable("main", Collections.emptyMap());
        FileStoreTable fallback = newFileStoreTable(
                "fallback", ImmutableMap.of("scan.manifest.parallelism", "0"));
        Table fallbackReadTable = new FallbackReadFileStoreTable(main, fallback);

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonScanParams.applyOptions(fallbackReadTable, Collections.emptyMap()));
    }

    @Test
    void testSafeRelationOptionOverridesUnsafeHiddenFallbackTable() {
        FileStoreTable main = newFileStoreTable("main", Collections.emptyMap());
        FileStoreTable fallback = newFileStoreTable(
                "fallback", ImmutableMap.of("scan.manifest.parallelism", "0"));
        Table fallbackReadTable = new FallbackReadFileStoreTable(main, fallback);

        Assertions.assertDoesNotThrow(() -> PaimonScanParams.applyOptions(
                fallbackReadTable, ImmutableMap.of("scan.manifest.parallelism", "1")));
    }

    @Test
    void testRejectUnsafeFallbackHiddenByPrivilegeDelegate() {
        FileStoreTable main = newFileStoreTable("privileged_main", Collections.emptyMap());
        FileStoreTable fallback = newFileStoreTable(
                "privileged_fallback", ImmutableMap.of("scan.manifest.parallelism", "0"));
        FileStoreTable fallbackReadTable = new FallbackReadFileStoreTable(main, fallback);
        FileStoreTable privilegedTable = PrivilegedFileStoreTable.wrap(
                fallbackReadTable,
                Mockito.mock(PrivilegeChecker.class),
                Identifier.create("db", "table"));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonReaderOptions.validateEffectiveTable(privilegedTable));
    }

    private FileStoreTable newFileStoreTable(String name, Map<String, String> options) {
        TableSchema schema = new TableSchema(
                0,
                Collections.singletonList(new DataField(0, "id", new IntType())),
                0,
                Collections.emptyList(),
                Collections.emptyList(),
                options,
                null);
        return new AppendOnlyFileStoreTable(
                Mockito.mock(FileIO.class), new Path("memory://" + name), schema, CatalogEnvironment.empty());
    }
}
