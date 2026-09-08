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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
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
                PaimonReaderOptions.TABLE_OPTION_PREFIX + "file-index.read.enabled", "false",
                PaimonReaderOptions.TABLE_OPTION_PREFIX + "source.split.target-size", "64 MB",
                PaimonReaderOptions.TABLE_OPTION_PREFIX + "source.split.open-file-cost", "1 MB",
                PaimonReaderOptions.TABLE_OPTION_PREFIX + "scan.manifest.parallelism", "1",
                PaimonReaderOptions.TABLE_OPTION_PREFIX + "scan.plan-sort-partition", "true")));
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
                ImmutableMap.of(PaimonReaderOptions.TABLE_OPTION_PREFIX + "branch", "archive"),
                ImmutableMap.of(PaimonReaderOptions.TABLE_OPTION_PREFIX + "read.batch-size", "0"),
                ImmutableMap.of(PaimonReaderOptions.TABLE_OPTION_PREFIX
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
        Map<String, String> persisted = ImmutableMap.of(
                PaimonReaderOptions.TABLE_OPTION_PREFIX + "scan.manifest.parallelism",
                String.valueOf(PaimonReaderOptions.MAX_MANIFEST_PARALLELISM));

        Assertions.assertEquals(String.valueOf(PaimonReaderOptions.MAX_MANIFEST_PARALLELISM),
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
        PrivilegeChecker checker = (PrivilegeChecker) Proxy.newProxyInstance(
                PrivilegeChecker.class.getClassLoader(),
                new Class<?>[] {PrivilegeChecker.class},
                (proxy, method, args) -> null);
        FileStoreTable privileged = PrivilegedFileStoreTable.wrap(
                new FallbackReadFileStoreTable(explicit, empty, true), checker,
                Identifier.create("db", "table"));
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
    void testSystemSourceKeepsFallbackAsOutermostPlanningDecorator() {
        FileStoreTable main = newFileStoreTable("main", Collections.emptyMap());
        FileStoreTable fallback = newFileStoreTable("fallback", Collections.emptyMap());
        FallbackReadFileStoreTable pair = new FallbackReadFileStoreTable(main, fallback, true);
        PrivilegeChecker checker = (PrivilegeChecker) Proxy.newProxyInstance(
                PrivilegeChecker.class.getClassLoader(),
                new Class<?>[] {PrivilegeChecker.class},
                (proxy, method, args) -> null);
        FileStoreTable privileged = PrivilegedFileStoreTable.wrap(
                pair, checker,
                Identifier.create("db", "privileged_fallback"));

        Assertions.assertSame(pair, PaimonTableDecorators.unwrapToFallbackOrBase(privileged));
    }

    @Test
    void testFallbackWrapperOrderMatchesPaimonFactoryPrecedence() {
        FileStoreTable other = newFileStoreTable("other", Collections.emptyMap());
        Assertions.assertTrue(PaimonReaderOptions.isWrappedFirst(new FallbackReadFileStoreTable(
                newFileStoreTable("fallback", ImmutableMap.of(
                        CoreOptions.SCAN_FALLBACK_BRANCH.key(), "fallback",
                        CoreOptions.SCAN_PRIMARY_BRANCH.key(), "primary")), other, true)));
        Assertions.assertFalse(PaimonReaderOptions.isWrappedFirst(new FallbackReadFileStoreTable(
                newFileStoreTable("primary", ImmutableMap.of(
                        CoreOptions.SCAN_PRIMARY_BRANCH.key(), "primary")), other, false)));
        Assertions.assertTrue(PaimonReaderOptions.isWrappedFirst(new FallbackReadFileStoreTable(
                newFileStoreTable("default", Collections.emptyMap()), other, true)));
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
        FakePaimonTable physicalTable = fakeTable("physical", Collections.emptyMap());
        FakePaimonTable finalTable = fakeTable(
                "final", ImmutableMap.of("read.batch-size", "0"));
        physicalTable.copyResult = finalTable;

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonScanParams.applyOptions(physicalTable, Collections.emptyMap()));
    }

    @Test
    void testSafeRelationOptionOverridesUnsafePhysicalOption() {
        Map<String, String> relationOptions = ImmutableMap.of("read.batch-size", "4096");
        FakePaimonTable physicalTable = fakeTable(
                "physical", ImmutableMap.of("read.batch-size", "0"));
        FakePaimonTable finalTable = fakeTable("final", relationOptions);
        physicalTable.copyResult = finalTable;

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
    void testRuntimeSafeTableCapsHiddenFallbackPlanner() {
        int localCapacity = Runtime.getRuntime().availableProcessors();
        Assumptions.assumeTrue(localCapacity < PaimonReaderOptions.MAX_MANIFEST_PARALLELISM);
        FileStoreTable main = newFileStoreTable("main", Collections.emptyMap());
        FileStoreTable fallback = newFileStoreTable("fallback",
                ImmutableMap.of("scan.manifest.parallelism", String.valueOf(localCapacity + 1)));

        Table safe = PaimonReaderOptions.runtimeSafeTable(
                new FallbackReadFileStoreTable(main, fallback, true));

        Assertions.assertInstanceOf(FallbackReadFileStoreTable.class, safe);
        FallbackReadFileStoreTable pair = (FallbackReadFileStoreTable) safe;
        Assertions.assertEquals(String.valueOf(localCapacity),
                pair.other().options().get("scan.manifest.parallelism"));
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
        PrivilegeChecker checker = (PrivilegeChecker) Proxy.newProxyInstance(
                PrivilegeChecker.class.getClassLoader(),
                new Class<?>[] {PrivilegeChecker.class},
                (proxy, method, args) -> null);
        FileStoreTable privilegedTable = PrivilegedFileStoreTable.wrap(
                fallbackReadTable,
                checker,
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
                LocalFileIO.create(), new Path("memory://" + name), schema, CatalogEnvironment.empty());
    }

    private FakePaimonTable fakeTable(String name, Map<String, String> options) {
        FakePaimonTable table = new FakePaimonTable(
                name, org.apache.paimon.types.RowType.builder().field("id", new IntType()).build(),
                Collections.emptyList(), Collections.emptyList());
        table.setOptions(options);
        return table;
    }
}
