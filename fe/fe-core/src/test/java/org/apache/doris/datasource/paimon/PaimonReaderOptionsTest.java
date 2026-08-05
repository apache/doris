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
import com.google.common.collect.ImmutableSet;
import org.apache.paimon.CoreOptions;
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

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

public class PaimonReaderOptionsTest {

    @Test
    void testSupportsSafeBatchReadOptions() {
        Assertions.assertEquals(ImmutableSet.of(
                        CoreOptions.READ_BATCH_SIZE.key(),
                        CoreOptions.FILE_READER_ASYNC_THRESHOLD.key(),
                        CoreOptions.FILE_INDEX_READ_ENABLED.key(),
                        CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(),
                        CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(),
                        CoreOptions.SCAN_MANIFEST_PARALLELISM.key(),
                        CoreOptions.SCAN_PLAN_SORT_PARTITION.key()),
                PaimonReaderOptions.supportedOptions());

        Assertions.assertDoesNotThrow(() -> PaimonReaderOptions.validateCatalogProperties(ImmutableMap.of(
                AbstractPaimonProperties.TABLE_OPTION_PREFIX + "file-index.read.enabled", "false",
                AbstractPaimonProperties.TABLE_OPTION_PREFIX + "source.split.target-size", "64 MB",
                AbstractPaimonProperties.TABLE_OPTION_PREFIX + "source.split.open-file-cost", "1 MB",
                AbstractPaimonProperties.TABLE_OPTION_PREFIX + "scan.manifest.parallelism", "1",
                AbstractPaimonProperties.TABLE_OPTION_PREFIX + "scan.plan-sort-partition", "true")));
    }

    @Test
    void testRejectsInvalidSafeBatchReadOptions() {
        for (Map<String, String> options : new Map[] {
                ImmutableMap.of("file-index.read.enabled", "not-a-boolean"),
                ImmutableMap.of("source.split.target-size", "0 B"),
                ImmutableMap.of("source.split.open-file-cost", "-1 B"),
                ImmutableMap.of("scan.plan-sort-partition", "not-a-boolean")
        }) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> PaimonReaderOptions.validateEffectiveTableOptions(options));
        }
    }

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
                        String.valueOf(PaimonReaderOptions.MAX_MANIFEST_PARALLELISM + 1))
        }) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> PaimonReaderOptions.validateEffectiveTableOptions(options));
        }
    }

    @Test
    void testCatalogReplayUsesHardwareIndependentManifestParallelismBound() {
        String maximum = String.valueOf(PaimonReaderOptions.MAX_MANIFEST_PARALLELISM);
        Map<String, String> persisted = ImmutableMap.of(
                AbstractPaimonProperties.TABLE_OPTION_PREFIX + "scan.manifest.parallelism", maximum);

        Assertions.assertEquals(maximum,
                PaimonReaderOptions.compatibleCatalogOptions(persisted)
                        .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
        Assertions.assertDoesNotThrow(() -> PaimonReaderOptions.validateCatalogProperties(persisted));
    }

    @Test
    void testRuntimeCapNormalizesEveryPlanningLeafIndependently() {
        FileStoreTable empty = newFileStoreTable("empty", Collections.emptyMap());
        FileStoreTable explicit = newFileStoreTable("explicit", ImmutableMap.of(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "1"));

        FileStoreTable safeEmpty = (FileStoreTable) PaimonReaderOptions.runtimeSafeTable(empty, 512);
        FallbackReadFileStoreTable explicitThenEmpty = (FallbackReadFileStoreTable)
                PaimonReaderOptions.runtimeSafeTable(
                        new FallbackReadFileStoreTable(explicit, empty, true), 512);
        FallbackReadFileStoreTable emptyThenExplicit = (FallbackReadFileStoreTable)
                PaimonReaderOptions.runtimeSafeTable(
                        new FallbackReadFileStoreTable(empty, explicit, true), 512);
        FileStoreTable privileged = PrivilegedFileStoreTable.wrap(
                new FallbackReadFileStoreTable(explicit, empty, true),
                Mockito.mock(PrivilegeChecker.class), Identifier.create("db", "table"));
        Table normalizedDelegate = PaimonReaderOptions.runtimeSafeTable(privileged, 512);

        Assertions.assertEquals("256", safeEmpty.options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
        Assertions.assertEquals("1", explicitThenEmpty.wrapped().options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
        Assertions.assertEquals("256", explicitThenEmpty.other().options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
        Assertions.assertEquals("256", emptyThenExplicit.wrapped().options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
        Assertions.assertEquals("1", emptyThenExplicit.other().options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
        Assertions.assertInstanceOf(FallbackReadFileStoreTable.class, normalizedDelegate);
        Assertions.assertEquals("256", ((FallbackReadFileStoreTable) normalizedDelegate)
                .other().options().get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
    }

    @Test
    void testRuntimeCapPreservesPrimaryBranchPriority() throws Exception {
        FileStoreTable main = newFileStoreTable("main", ImmutableMap.of(
                CoreOptions.SCAN_PRIMARY_BRANCH.key(), "primary",
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "128"));
        FileStoreTable primary = newFileStoreTable("primary", Collections.emptyMap());
        Table safe = PaimonReaderOptions.runtimeSafeTable(
                new FallbackReadFileStoreTable(main, primary, false), 64);

        Field wrappedFirst = FallbackReadFileStoreTable.class.getDeclaredField("wrappedFirst");
        wrappedFirst.setAccessible(true);
        Assertions.assertFalse(wrappedFirst.getBoolean(safe));
    }

    @Test
    void testSystemSourceKeepsFallbackAsOutermostPlanningDecorator() {
        FileStoreTable main = newFileStoreTable("main", Collections.emptyMap());
        FileStoreTable fallback = newFileStoreTable("fallback", Collections.emptyMap());
        FallbackReadFileStoreTable pair = new FallbackReadFileStoreTable(main, fallback, true);
        FileStoreTable privileged = PrivilegedFileStoreTable.wrap(
                pair, Mockito.mock(PrivilegeChecker.class),
                Identifier.create("db", "privileged_fallback"));

        Assertions.assertSame(pair, PaimonTableDecorators.unwrapToFallbackOrBase(privileged));
    }

    @Test
    void testUnnormalizedEffectiveTableCannotGrowPaimonGlobalPool() {
        int localCapacity = Runtime.getRuntime().availableProcessors();
        if (localCapacity < PaimonReaderOptions.MAX_MANIFEST_PARALLELISM) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> PaimonReaderOptions.validateEffectiveTableOptions(ImmutableMap.of(
                            CoreOptions.SCAN_MANIFEST_PARALLELISM.key(),
                            String.valueOf(localCapacity + 1))));
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
        Table fallbackReadTable = new FallbackReadFileStoreTable(main, fallback, true);

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonScanParams.applyOptions(fallbackReadTable, Collections.emptyMap()));
    }

    @Test
    void testSafeRelationOptionOverridesUnsafeHiddenFallbackTable() {
        FileStoreTable main = newFileStoreTable("main", Collections.emptyMap());
        FileStoreTable fallback = newFileStoreTable(
                "fallback", ImmutableMap.of("scan.manifest.parallelism", "0"));
        Table fallbackReadTable = new FallbackReadFileStoreTable(main, fallback, true);

        Assertions.assertDoesNotThrow(() -> PaimonScanParams.applyOptions(
                fallbackReadTable, ImmutableMap.of("scan.manifest.parallelism", "1")));
    }

    @Test
    void testRuntimeCopyCapsManifestParallelismHiddenByFallbackTable() {
        int localCapacity = Runtime.getRuntime().availableProcessors();
        org.junit.jupiter.api.Assumptions.assumeTrue(
                localCapacity < PaimonReaderOptions.MAX_MANIFEST_PARALLELISM);
        FileStoreTable main = newFileStoreTable("main", Collections.emptyMap());
        FileStoreTable fallback = newFileStoreTable("fallback", ImmutableMap.of(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), String.valueOf(localCapacity + 1)));
        Table fallbackReadTable = new FallbackReadFileStoreTable(main, fallback, true);

        Table safeTable = PaimonReaderOptions.runtimeSafeTable(fallbackReadTable);

        Assertions.assertDoesNotThrow(() -> PaimonReaderOptions.validateEffectiveTable(safeTable));
        Assertions.assertEquals(String.valueOf(localCapacity),
                ((FallbackReadFileStoreTable) safeTable).other().options()
                        .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
    }

    @Test
    void testBackendManifestCapUsesExecutionCeilingNotSmallestBranch() {
        FileStoreTable main = newFileStoreTable("main_cap", ImmutableMap.of(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "1"));
        FileStoreTable fallback = newFileStoreTable("fallback_cap", ImmutableMap.of(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "128"));

        Assertions.assertEquals(
                Math.min(Runtime.getRuntime().availableProcessors(),
                        PaimonReaderOptions.MAX_MANIFEST_PARALLELISM),
                PaimonReaderOptions.backendManifestParallelismCap(
                        new FallbackReadFileStoreTable(main, fallback, true)).getAsInt());
    }

    @Test
    void testRejectUnsafeFallbackHiddenByPrivilegeDelegate() {
        FileStoreTable main = newFileStoreTable("privileged_main", Collections.emptyMap());
        FileStoreTable fallback = newFileStoreTable(
                "privileged_fallback", ImmutableMap.of("scan.manifest.parallelism", "0"));
        FileStoreTable fallbackReadTable = new FallbackReadFileStoreTable(main, fallback, true);
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
