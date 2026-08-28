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

package org.apache.doris.paimon;

import org.apache.doris.common.jni.vec.ColumnType;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.BufferFileReader;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.privilege.PrivilegeChecker;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PaimonJniScannerTest {
    private static final String SERIALIZED_TABLE = "serialized_table";
    private static final String SERIALIZED_SYSTEM_SOURCE =
            "paimon.doris.serialized-system-source";
    private static final String SERIALIZED_TABLE_CACHE_KEY = "serialized_table_cache_key";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @After
    public void clearTableCache() {
        PaimonTableCache.clearForTest();
    }

    @Test
    public void testConstructorAcceptsEmptyProjection() {
        new PaimonJniScanner(128, createBaseParams());
    }

    @Test
    public void testConstructorRejectsMissingOrEmptyTableCacheKey() {
        Map<String, String> params = createBaseParams();
        params.remove(SERIALIZED_TABLE_CACHE_KEY);
        assertInvalidTableCacheKey(params);

        params.put(SERIALIZED_TABLE_CACHE_KEY, "");
        assertInvalidTableCacheKey(params);
    }

    @Test
    public void testWarmTableCacheHitReleasesSerializedTablePayloads() throws Exception {
        String cacheKey = "warm-table-cache-hit";
        Map<String, String> params = createBaseParams();
        params.put(SERIALIZED_TABLE_CACHE_KEY, cacheKey);
        params.put(SERIALIZED_TABLE, "serialized-table-payload");
        params.put(SERIALIZED_SYSTEM_SOURCE, "serialized-system-source-payload");
        Table cachedTable = tableWithOptions(Collections.emptyMap());
        PaimonTableCache.TableCacheEntry cacheEntry =
                new PaimonTableCache.TableCacheEntry(cachedTable, Collections.emptyList());
        Assert.assertTrue(PaimonTableCache.publish(cacheKey, cacheEntry));

        PaimonJniScanner scanner = new PaimonJniScanner(128, params);
        Method initTableFromCache = PaimonJniScanner.class.getDeclaredMethod("initTableFromCache");
        initTableFromCache.setAccessible(true);

        Assert.assertTrue((Boolean) initTableFromCache.invoke(scanner));
        Assert.assertFalse(params.containsKey(SERIALIZED_TABLE));
        Assert.assertFalse(params.containsKey(SERIALIZED_SYSTEM_SOURCE));
        Field tableField = PaimonJniScanner.class.getDeclaredField("table");
        tableField.setAccessible(true);
        Assert.assertSame(cachedTable, tableField.get(scanner));

        scanner.close();
        Assert.assertEquals(1, PaimonTableCache.size());
        PaimonTableCache.release(cacheKey, cacheEntry);
        Assert.assertEquals(0, PaimonTableCache.size());
    }

    @Test
    public void testOldFeSerializedAsyncThresholdIsRejectedInEveryChild() throws Exception {
        Table visible = (Table) Proxy.newProxyInstance(Table.class.getClassLoader(),
                new Class[] {Table.class}, new SerializableTableHandler(
                        Collections.singletonMap(CoreOptions.FILE_READER_ASYNC_THRESHOLD.key(), "0 B")));
        FileStoreTable main = serializableFileStoreTable(Collections.emptyMap());
        FileStoreTable fallback = serializableFileStoreTable(Collections.singletonMap(
                CoreOptions.FILE_READER_ASYNC_THRESHOLD.key(), "2 GB"));
        for (Table configuredTable : Arrays.asList(
                visible, new FallbackReadFileStoreTable(main, fallback, true))) {
            Map<String, String> params = createBaseParams();
            params.put("serialized_table", Base64.getUrlEncoder().withoutPadding().encodeToString(
                    InstantiationUtil.serializeObject(configuredTable)));
            PaimonJniScanner scanner = new PaimonJniScanner(1024, params);
            Method initTable = PaimonJniScanner.class.getDeclaredMethod("initTable");
            initTable.setAccessible(true);

            InvocationTargetException failure = Assert.assertThrows(
                    InvocationTargetException.class, () -> initTable.invoke(scanner));
            Assert.assertTrue(failure.getCause() instanceof IllegalArgumentException);
            Assert.assertTrue(failure.getCause().getMessage()
                    .contains(CoreOptions.FILE_READER_ASYNC_THRESHOLD.key()));
        }
    }

    @Test
    public void testOldFeSerializedSystemSourceRejectsZeroSplitTarget() {
        FileStoreTable main = serializableFileStoreTable(Collections.emptyMap());
        FileStoreTable fallback = serializableFileStoreTable(Collections.singletonMap(
                CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(), "0 B"));
        Table filesTable = SystemTableLoader.load(
                "files", new FallbackReadFileStoreTable(main, fallback, true));

        IllegalArgumentException failure = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> PaimonJniScanner.applyDefaultReadBatchSize(filesTable, 1024));
        Assert.assertTrue(failure.getMessage()
                .contains(CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key()));
    }

    @Test
    public void testOldFeSerializedSystemSourceRejectsFallbackZeroReadBatch() throws Exception {
        FileStoreTable main = serializableFileStoreTable(Collections.emptyMap());
        FileStoreTable fallback = serializableFileStoreTable(Collections.singletonMap(
                CoreOptions.READ_BATCH_SIZE.key(), "0"));
        Table readerBackedSystemTable = SystemTableLoader.load(
                "audit_log", new FallbackReadFileStoreTable(main, fallback, true));
        Map<String, String> params = createBaseParams();
        params.put("serialized_table", Base64.getUrlEncoder().withoutPadding().encodeToString(
                InstantiationUtil.serializeObject(readerBackedSystemTable)));
        PaimonJniScanner scanner = new PaimonJniScanner(1024, params);
        Method initTable = PaimonJniScanner.class.getDeclaredMethod("initTable");
        initTable.setAccessible(true);

        InvocationTargetException failure = Assert.assertThrows(
                InvocationTargetException.class, () -> initTable.invoke(scanner));
        Assert.assertTrue(failure.getCause() instanceof IllegalArgumentException);
        Assert.assertTrue(failure.getCause().getMessage()
                .contains(CoreOptions.READ_BATCH_SIZE.key()));
    }

    @Test
    public void testConfiguredPaimonReadBatchSizeIsNotOverwrittenByDorisBatchSize() {
        Table configuredTable = tableWithOptions(Collections.singletonMap(
                CoreOptions.READ_BATCH_SIZE.key(), "4096"));

        Assert.assertSame(configuredTable,
                PaimonJniScanner.applyDefaultReadBatchSize(configuredTable, 1024));
        Assert.assertEquals("4096", configuredTable.options().get(CoreOptions.READ_BATCH_SIZE.key()));
    }

    @Test
    public void testOldFeSerializedZeroReadBatchIsRejected() {
        Table configuredTable = tableWithOptions(Collections.singletonMap(
                CoreOptions.READ_BATCH_SIZE.key(), "0"));

        try {
            PaimonJniScanner.applyDefaultReadBatchSize(configuredTable, 1024);
            Assert.fail("an unvalidated old-FE batch size must not reach the Paimon reader");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains(CoreOptions.READ_BATCH_SIZE.key()));
        }
    }

    @Test
    public void testOldFeSerializedFallbackZeroReadBatchIsRejected() {
        FileStoreTable main = serializableFileStoreTable(Collections.emptyMap());
        FileStoreTable fallback = serializableFileStoreTable(
                Collections.singletonMap(CoreOptions.READ_BATCH_SIZE.key(), "0"));

        try {
            PaimonJniScanner.applyDefaultReadBatchSize(
                    new FallbackReadFileStoreTable(main, fallback, true), 1024);
            Assert.fail("a hidden old-FE batch size must not reach the fallback reader");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains(CoreOptions.READ_BATCH_SIZE.key()));
        }
    }

    @Test
    public void testBackendManifestCapReachesHiddenFallbackPlanner() {
        FileStoreTable main = serializableFileStoreTable(Collections.emptyMap());
        FileStoreTable fallback = serializableFileStoreTable(
                Collections.singletonMap(CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "8"));

        Table safe = PaimonJniScanner.applyBackendManifestParallelism(
                new FallbackReadFileStoreTable(main, fallback, true), "8", 4);

        Assert.assertTrue(safe instanceof FallbackReadFileStoreTable);
        Assert.assertEquals("4", ((FallbackReadFileStoreTable) safe).other()
                .options().get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
    }

    @Test
    public void testAdvertisedFeCapStillChecksSerializedChildren() {
        FileStoreTable main = serializableFileStoreTable(Collections.emptyMap());
        FileStoreTable fallback = serializableFileStoreTable(
                Collections.singletonMap(CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "200"));

        Table safe = PaimonJniScanner.applyBackendManifestParallelism(
                new FallbackReadFileStoreTable(main, fallback, true), "32", 64);

        Assert.assertEquals("32", ((FallbackReadFileStoreTable) safe).other()
                .options().get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
    }

    @Test
    public void testOldFeManifestBackstopKeepsStableMaximum() {
        FileStoreTable visible = serializableFileStoreTable(
                Collections.singletonMap(CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "300"));
        FileStoreTable main = serializableFileStoreTable(Collections.emptyMap());
        FileStoreTable fallback = serializableFileStoreTable(
                Collections.singletonMap(CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "300"));

        Table safeVisible = PaimonJniScanner.applyBackendManifestParallelism(
                visible, null, 512);
        Table safeFallback = PaimonJniScanner.applyBackendManifestParallelism(
                new FallbackReadFileStoreTable(main, fallback, true), null, 512);

        Assert.assertEquals("256", safeVisible.options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
        Assert.assertEquals("256", ((FallbackReadFileStoreTable) safeFallback).other()
                .options().get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
    }

    @Test
    public void testMissingManifestParallelismGetsStableBackendCap() {
        FileStoreTable table = serializableFileStoreTable(Collections.emptyMap());

        Table safe = PaimonJniScanner.applyBackendManifestParallelism(table, null, 512);

        Assert.assertEquals("256",
                safe.options().get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
    }

    @Test
    public void testFallbackManifestParallelismIsCappedPerBranch() {
        FileStoreTable main = serializableFileStoreTable(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "1"));
        FileStoreTable fallback = serializableFileStoreTable(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "128"));
        Table pair = new FallbackReadFileStoreTable(main, fallback, true);

        FallbackReadFileStoreTable unchanged = (FallbackReadFileStoreTable)
                PaimonJniScanner.applyBackendManifestParallelism(pair, "128", 128);
        Assert.assertEquals("1", unchanged.wrapped().options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
        Assert.assertEquals("128", unchanged.other().options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));

        FallbackReadFileStoreTable capped = (FallbackReadFileStoreTable)
                PaimonJniScanner.applyBackendManifestParallelism(pair, "128", 64);
        Assert.assertEquals("1", capped.wrapped().options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
        Assert.assertEquals("64", capped.other().options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
    }

    @Test
    public void testBackendCapPreservesPrimaryBranchPriority() throws Exception {
        Map<String, String> mainOptions = new HashMap<>();
        mainOptions.put(CoreOptions.SCAN_PRIMARY_BRANCH.key(), "primary");
        mainOptions.put(CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "128");
        FileStoreTable main = serializableFileStoreTable(mainOptions);
        FileStoreTable primary = serializableFileStoreTable(Collections.emptyMap());
        Table safe = PaimonJniScanner.applyBackendManifestParallelism(
                new FallbackReadFileStoreTable(main, primary, false), null, 64);

        Field wrappedFirst = FallbackReadFileStoreTable.class.getDeclaredField("wrappedFirst");
        wrappedFirst.setAccessible(true);
        Assert.assertFalse(wrappedFirst.getBoolean(safe));
    }

    @Test
    public void testFallbackWrapperOrderMatchesPaimonFactoryPrecedence() {
        Assert.assertTrue(PaimonJniScanner.isWrappedFirst(fallbackTable(ImmutableMap.of(
                CoreOptions.SCAN_FALLBACK_BRANCH.key(), "fallback",
                CoreOptions.SCAN_PRIMARY_BRANCH.key(), "primary"))));
        Assert.assertFalse(PaimonJniScanner.isWrappedFirst(fallbackTable(ImmutableMap.of(
                CoreOptions.SCAN_PRIMARY_BRANCH.key(), "primary"))));
        Assert.assertTrue(PaimonJniScanner.isWrappedFirst(fallbackTable(ImmutableMap.of(
                CoreOptions.SCAN_PRIMARY_BRANCH.key(), "  "))));
        Assert.assertTrue(PaimonJniScanner.isWrappedFirst(fallbackTable(ImmutableMap.of(
                CoreOptions.CHAIN_TABLE_ENABLED.key(), "true",
                CoreOptions.SCAN_PRIMARY_BRANCH.key(), "primary"))));
        Assert.assertTrue(PaimonJniScanner.isWrappedFirst(fallbackTable(Collections.emptyMap())));
    }

    @Test
    public void testBackendCapTraversesPrivilegeDelegate() {
        FileStoreTable main = serializableFileStoreTable(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "1"));
        FileStoreTable fallback = serializableFileStoreTable(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "128"));
        PrivilegeChecker checker = (PrivilegeChecker) Proxy.newProxyInstance(
                PrivilegeChecker.class.getClassLoader(),
                new Class<?>[] {PrivilegeChecker.class},
                (proxy, method, args) -> null);
        FileStoreTable privileged = PrivilegedFileStoreTable.wrap(
                new FallbackReadFileStoreTable(main, fallback, true), checker,
                Identifier.create("db", "table"));

        Table safe = PaimonJniScanner.applyBackendManifestParallelism(
                privileged, null, 64);
        FileStoreTable planningTable = safe instanceof DelegatedFileStoreTable
                && !(safe instanceof FallbackReadFileStoreTable)
                ? ((DelegatedFileStoreTable) safe).wrapped() : (FileStoreTable) safe;

        Assert.assertTrue(planningTable instanceof FallbackReadFileStoreTable);
        FallbackReadFileStoreTable pair = (FallbackReadFileStoreTable) planningTable;
        Assert.assertEquals("1", pair.wrapped().options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
        Assert.assertEquals("64", pair.other().options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
    }

    @Test
    public void testOldFeSystemWrapperPreservesIndependentFallbackLimits() throws Exception {
        FileStoreTable main = serializableFileStoreTable(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "1"));
        FileStoreTable fallback = serializableFileStoreTable(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "128"));
        Table wrapper = SystemTableLoader.load(
                "partitions", new FallbackReadFileStoreTable(main, fallback, true));

        Table safe = PaimonJniScanner.applyBackendManifestParallelism(
                wrapper, null, 64);
        Field storeTable = safe.getClass().getDeclaredField("storeTable");
        storeTable.setAccessible(true);
        FallbackReadFileStoreTable pair = (FallbackReadFileStoreTable) storeTable.get(safe);

        Assert.assertEquals("1", pair.wrapped().options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
        Assert.assertEquals("64", pair.other().options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
    }

    @Test
    public void testSystemWrapperExposesSafeFallbackBehindPrivilegeDelegate() throws Exception {
        FileStoreTable main = serializableFileStoreTable(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "1"));
        FileStoreTable fallback = serializableFileStoreTable(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "2"));
        PrivilegeChecker checker = (PrivilegeChecker) Proxy.newProxyInstance(
                PrivilegeChecker.class.getClassLoader(),
                new Class<?>[] {PrivilegeChecker.class},
                (proxy, method, args) -> null);
        FileStoreTable privileged = PrivilegedFileStoreTable.wrap(
                new FallbackReadFileStoreTable(main, fallback, true), checker,
                Identifier.create("db", "table"));
        Table wrapper = SystemTableLoader.load("partitions", privileged);

        Table safe = PaimonJniScanner.applyBackendManifestParallelism(
                wrapper, null, 64);
        Field storeTable = safe.getClass().getDeclaredField("storeTable");
        storeTable.setAccessible(true);

        Assert.assertTrue(storeTable.get(safe) instanceof FallbackReadFileStoreTable);
    }

    @Test
    public void testBackendCapRebuildsWrapperFromTransportedSystemSource() throws Exception {
        FileStoreTable source = serializableFileStoreTable(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "32"));
        Table hiddenWrapper = SystemTableLoader.load("partitions", source);

        Table safe = PaimonJniScanner.applyBackendManifestParallelism(
                hiddenWrapper, "16", 4, source, "partitions");

        Field storeTable = safe.getClass().getDeclaredField("storeTable");
        storeTable.setAccessible(true);
        Assert.assertEquals(SystemTableLoader.load("partitions", source).getClass(), safe.getClass());
        Assert.assertEquals("4", ((FileStoreTable) storeTable.get(safe)).options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));

        FileStoreTable largeSource = serializableFileStoreTable(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "300"));
        Table largeWrapper = SystemTableLoader.load("partitions", largeSource);
        Table stableSafe = PaimonJniScanner.applyBackendManifestParallelism(
                largeWrapper, null, 512);
        Field stableStoreTable = stableSafe.getClass().getDeclaredField("storeTable");
        stableStoreTable.setAccessible(true);
        Assert.assertEquals("256", ((FileStoreTable) stableStoreTable.get(stableSafe)).options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
    }

    @Test
    public void testOldFeSystemWrapperGetsBackendManifestBackstop() throws Exception {
        FileStoreTable source = serializableFileStoreTable(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "32"));
        Table hiddenWrapper = SystemTableLoader.load("partitions", source);

        Table safe = PaimonJniScanner.applyBackendManifestParallelism(
                hiddenWrapper, null, 4);

        Field storeTable = safe.getClass().getDeclaredField("storeTable");
        storeTable.setAccessible(true);
        Assert.assertEquals("4", ((FileStoreTable) storeTable.get(safe)).options()
                .get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
    }

    private static FileStoreTable serializableFileStoreTable(Map<String, String> options) {
        return (FileStoreTable) Proxy.newProxyInstance(FileStoreTable.class.getClassLoader(),
                new Class[] {FileStoreTable.class}, new SerializableTableHandler(options));
    }

    private static FallbackReadFileStoreTable fallbackTable(Map<String, String> options) {
        return new FallbackReadFileStoreTable(
                serializableFileStoreTable(options),
                serializableFileStoreTable(Collections.emptyMap()), true);
    }

    @Test
    public void testSerializedReadBatchSizeReachesReaderTableInitialization() throws Exception {
        Table configuredTable = (Table) Proxy.newProxyInstance(Table.class.getClassLoader(),
                new Class[] {Table.class}, new SerializableTableHandler(
                        Collections.singletonMap(CoreOptions.READ_BATCH_SIZE.key(), "4096")));
        Map<String, String> params = createBaseParams();
        params.put("serialized_table", Base64.getUrlEncoder().withoutPadding().encodeToString(
                InstantiationUtil.serializeObject(configuredTable)));
        PaimonJniScanner scanner = new PaimonJniScanner(1024, params);

        Method initTable = PaimonJniScanner.class.getDeclaredMethod("initTable");
        initTable.setAccessible(true);
        initTable.invoke(scanner);
        Field tableField = PaimonJniScanner.class.getDeclaredField("table");
        tableField.setAccessible(true);
        Table readerTable = (Table) tableField.get(scanner);

        Assert.assertEquals("4096", readerTable.options().get(CoreOptions.READ_BATCH_SIZE.key()));
    }

    @Test
    public void testConstructorDecodesDelimiterSafeProjectionNames() {
        Map<String, String> params = createBaseParams();
        params.put("required_fields", "legacy,value,is,ambiguous");
        params.put("required_fields_base64", encodeFields(
                "region,code", "hash#name", "地区 名"));
        params.put("columns_types", "string#string#string");
        params.put("columns_types_base64", encodeFields("string", "string", "string"));

        PaimonJniScanner scanner = new PaimonJniScanner(128, params);

        Assert.assertArrayEquals(new String[] {"region,code", "hash#name", "地区 名"},
                PaimonJniScanner.requiredFields(params));
        Assert.assertEquals("3", scanner.getStatistics().get("gauge:PaimonJniRequiredFieldCount"));
        Assert.assertEquals(0, PaimonJniScanner.getFieldIndex(
                Arrays.asList("region,code", "hash#name", "地区 名"), "REGION,CODE"));
    }

    @Test
    public void testConstructorDecodesDelimiterSafeNestedFieldNamesAndTypes() {
        Map<String, String> params = createBaseParams();
        params.put("required_fields_base64", encodeFields("nested#value"));
        params.put("columns_types", "legacy#type#framing");
        params.put("columns_types_base64", encodeFields(
                "struct<$aGFzaCNuYW1l:string,$cmVnaW9uLGNvZGU:string,$Y29sb246bmFtZQ:string>"));

        InspectablePaimonJniScanner scanner = new InspectablePaimonJniScanner(128, params);

        Assert.assertEquals(1, scanner.requiredTypes().length);
        ColumnType structType = scanner.requiredTypes()[0];
        Assert.assertTrue(structType.isStruct());
        Assert.assertEquals(Arrays.asList("hash#name", "region,code", "colon:name"),
                structType.getChildNames());
    }

    @Test
    public void testVariantAccessPathsStayAlignedWithRequiredFields() {
        Map<String, String> params = createBaseParams();
        params.put("variant_access_path.1.0", encodeFields("name"));
        params.put("variant_access_path.1.1", encodeFields("profile", "city"));

        List<List<List<String>>> paths = PaimonJniScanner.variantAccessPathsByColumn(params, 3);
        Assert.assertTrue(paths.get(0).isEmpty());
        Assert.assertEquals(Collections.singletonList("name"), paths.get(1).get(0));
        Assert.assertEquals(Arrays.asList("profile", "city"), paths.get(1).get(1));
        Assert.assertTrue(paths.get(2).isEmpty());
    }

    @Test
    public void testConstructorDistinguishesEmptyIdentifierFromEmptyProjection() {
        Map<String, String> oneEmptyField = createBaseParams();
        oneEmptyField.put("required_fields_base64", "$");
        oneEmptyField.put("columns_types_base64", "$c3RyaW5n");

        new PaimonJniScanner(128, oneEmptyField);

        Assert.assertArrayEquals(new String[] {""}, PaimonJniScanner.requiredFields(oneEmptyField));
        Map<String, String> noFields = createBaseParams();
        noFields.put("required_fields_base64", "");
        noFields.put("columns_types_base64", "");
        Assert.assertEquals(0, PaimonJniScanner.requiredFields(noFields).length);
        new PaimonJniScanner(128, noFields);
    }

    @Test
    public void testDebugSummaryNeverIncludesRawScannerParams() {
        Map<String, String> params = createBaseParams();
        params.put("hadoop.fs.s3a.secret.key", "FAKE_SECRET_MARKER");
        params.put("paimon.jdbc.url", "jdbc:test?password=FAKE_PASSWORD_MARKER");

        Logger logger = (Logger) LogManager.getLogger(PaimonJniScanner.class);
        Level originalLevel = logger.getLevel();
        CapturingAppender appender = new CapturingAppender();
        appender.start();
        logger.addAppender(appender);
        Configurator.setLevel(PaimonJniScanner.class.getName(), Level.DEBUG);
        try {
            new PaimonJniScanner(128, params);
        } finally {
            logger.removeAppender(appender);
            appender.stop();
            Configurator.setLevel(PaimonJniScanner.class.getName(), originalLevel);
        }

        String captured = String.join("\n", appender.messages);
        Assert.assertTrue(captured.contains("batchSize=128, requiredFieldCount=0"));
        Assert.assertFalse(captured.contains("FAKE_SECRET_MARKER"));
        Assert.assertFalse(captured.contains("FAKE_PASSWORD_MARKER"));
        Assert.assertFalse(captured.contains("hadoop.fs.s3a.secret.key"));
        Assert.assertFalse(captured.contains("paimon.jdbc.url"));
    }

    @Test
    public void testThreadCountSampleIsSharedWithinTtl() {
        AtomicLong ticker = new AtomicLong(1000L);
        AtomicInteger sampleCalls = new AtomicInteger();
        PaimonJniScanner.CachedThreadCounter counter = new PaimonJniScanner.CachedThreadCounter(
                100L, ticker::get, () -> {
                    sampleCalls.incrementAndGet();
                    return 7;
                });

        Assert.assertEquals(7, counter.get());
        Assert.assertEquals(7, counter.get());
        Assert.assertEquals(1, sampleCalls.get());

        ticker.addAndGet(100L);
        Assert.assertEquals(7, counter.get());
        Assert.assertEquals(2, sampleCalls.get());
    }

    @Test
    public void testConstructorAcceptsDorisShortTimeZoneForNonTemporalProjection() {
        Map<String, String> params = createBaseParams();
        params.put("required_fields", "id");
        params.put("columns_types", "int");
        params.put("time_zone", "EST");

        new PaimonJniScanner(128, params);
    }

    @Test
    public void testIOManagerOptionHelpers() throws Exception {
        Map<String, String> params = createBaseParams();
        Assert.assertFalse(PaimonJniScanner.isIOManagerEnabled(params));

        params.put(PaimonJniScanner.ENABLE_JNI_IO_MANAGER, "true");
        File tempDir = new File(temporaryFolder.getRoot(), "paimon-io-manager");
        params.put(PaimonJniScanner.JNI_IO_MANAGER_TMP_DIR, tempDir.getAbsolutePath());

        Assert.assertTrue(PaimonJniScanner.isIOManagerEnabled(params));
        Assert.assertEquals(tempDir.getAbsolutePath(), PaimonJniScanner.getIOManagerTempDirs(params));
        Assert.assertNull(PaimonJniScanner.getIOManagerImplClass(params));
        PaimonJniScanner.createIOManager(tempDir.getAbsolutePath()).close();
        Assert.assertTrue(tempDir.exists());
    }

    @Test
    public void testCreateDefaultAndCustomIOManager() throws Exception {
        File tempDir = new File(temporaryFolder.getRoot(), "paimon-io-manager-impl");
        IOManager defaultIOManager = PaimonJniScanner.createIOManager(tempDir.getAbsolutePath());
        Assert.assertTrue(defaultIOManager instanceof IOManagerImpl);
        defaultIOManager.close();

        Map<String, String> params = createBaseParams();
        params.put(PaimonJniScanner.JNI_IO_MANAGER_IMPL_CLASS, TestIOManager.class.getName());
        Assert.assertEquals(TestIOManager.class.getName(), PaimonJniScanner.getIOManagerImplClass(params));
        IOManager customIOManager = PaimonJniScanner.createIOManager(
                tempDir.getAbsolutePath(), PaimonJniScanner.getIOManagerImplClass(params));
        Assert.assertTrue(customIOManager instanceof TestIOManager);
        Assert.assertArrayEquals(new String[] {tempDir.getAbsolutePath()}, customIOManager.tempDirs());
    }

    @Test
    public void testCloseCleansIOManagerTempDirectory() throws Exception {
        File tempDir = temporaryFolder.newFolder("paimon-io-manager-clean");
        IOManager ioManager = PaimonJniScanner.createIOManager(tempDir.getAbsolutePath());
        FileIOChannel.ID channel = ioManager.createChannel();
        File spillFile = channel.getPathFile();
        Assert.assertTrue(spillFile.createNewFile());
        File spillDir = spillFile.getParentFile();
        Assert.assertTrue(spillDir.exists());

        PaimonJniScanner scanner = new PaimonJniScanner(128, createBaseParams());
        Field ioManagerField = PaimonJniScanner.class.getDeclaredField("ioManager");
        ioManagerField.setAccessible(true);
        ioManagerField.set(scanner, ioManager);
        Assert.assertEquals("1", scanner.getStatistics().get("gauge:PaimonJniIOManagerEnabled"));

        scanner.close();
        Assert.assertFalse(spillDir.exists());
    }

    @Test
    public void testStatisticsIncludePaimonDiagnostics() throws Exception {
        Map<String, String> params = createBaseParams();
        params.put("paimon_split", "encoded-split");
        params.put("paimon_predicate", "encoded-predicate");
        PaimonJniScanner scanner = new PaimonJniScanner(128, params);
        setTableOptions(scanner, Collections.singletonMap(
                "file-reader-async-threshold", "10 mebibytes"));

        Map<String, String> statistics = scanner.getStatistics();

        Assert.assertEquals("0", statistics.get("gauge:PaimonJniIOManagerEnabled"));
        Assert.assertEquals("0", statistics.get("gauge:PaimonJniRequiredFieldCount"));
        Assert.assertEquals("13", statistics.get("counter:PaimonJniSplitEncodedLength"));
        Assert.assertEquals("17", statistics.get("counter:PaimonJniPredicateEncodedLength"));
        Assert.assertEquals("1", statistics.get("gauge:PaimonJniAsyncThresholdConfigured"));
        Assert.assertEquals(String.valueOf(10L * 1024L * 1024L),
                statistics.get("bytes_gauge:PaimonJniAsyncThresholdBytes"));
        Assert.assertTrue(statistics.containsKey("gauge:PaimonJniAsyncReaderThreadCount"));
        Assert.assertTrue(statistics.containsKey("gauge:PaimonJniActiveScannerCount"));
        Assert.assertFalse(statistics.containsKey("peak:PaimonJniActiveScannerPeakCount"));
        Assert.assertFalse(statistics.containsKey("peak:PaimonJniAsyncReaderThreadPeakCount"));
        Assert.assertTrue(statistics.containsKey("counter:PaimonJniReadBatchCalls"));
        Assert.assertTrue(statistics.containsKey("timer:PaimonJniScannerOpenTime"));
        Assert.assertTrue(statistics.containsKey("timer:PaimonJniReadBatchTime"));
        Assert.assertTrue(Long.parseLong(statistics.get("bytes_gauge:PaimonJniJvmHeapUsed")) > 0);
        Assert.assertTrue(Long.parseLong(statistics.get("bytes_gauge:PaimonJniJvmHeapCommitted")) > 0);
    }

    @Test
    public void testCountThreadsByNamePrefix() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            started.countDown();
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "paimon-reader-async-thread-test");

        thread.start();
        try {
            Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
            Assert.assertTrue(PaimonJniScanner.countThreadsByNamePrefix("paimon-reader-async-thread") >= 1);
        } finally {
            release.countDown();
            thread.join(5000);
        }
    }

    @Test
    public void testParseDataSizeBytes() {
        Assert.assertEquals(Long.valueOf(1024L),
                PaimonJniScanner.parseDataSizeBytes("1 kibibytes").get());
        Assert.assertEquals(Long.valueOf(10L * 1024L * 1024L),
                PaimonJniScanner.parseDataSizeBytes("10 mebibytes").get());
        Assert.assertEquals(Long.valueOf(2L * 1024L * 1024L * 1024L),
                PaimonJniScanner.parseDataSizeBytes("2GB").get());
        Assert.assertFalse(PaimonJniScanner.parseDataSizeBytes("1 gib").isPresent());
        Assert.assertFalse(PaimonJniScanner.parseDataSizeBytes("unknown").isPresent());
    }

    @Test
    public void testCloseReleasesActiveRecordIterator() throws Exception {
        PaimonJniScanner scanner = new PaimonJniScanner(128, createBaseParams());
        AtomicBoolean released = new AtomicBoolean(false);
        RecordReader.RecordIterator<InternalRow> recordIterator =
                new RecordReader.RecordIterator<InternalRow>() {
                    @Override
                    public InternalRow next() {
                        return null;
                    }

                    @Override
                    public void releaseBatch() {
                        released.set(true);
                    }
                };

        Field recordIteratorField = PaimonJniScanner.class.getDeclaredField("recordIterator");
        recordIteratorField.setAccessible(true);
        recordIteratorField.set(scanner, recordIterator);

        scanner.close();

        Assert.assertTrue(released.get());
        Assert.assertNull(recordIteratorField.get(scanner));
    }

    @Test
    public void testFailedCloseReleasesCacheAndRetainsResourcesForRetry() throws Exception {
        String cacheKey = "retryable-close";
        Map<String, String> params = createBaseParams();
        params.put(SERIALIZED_TABLE_CACHE_KEY, cacheKey);
        PaimonJniScanner scanner = new PaimonJniScanner(128, params);
        AtomicInteger iteratorCloseCalls = new AtomicInteger();
        RecordReader.RecordIterator<InternalRow> recordIterator =
                new RecordReader.RecordIterator<InternalRow>() {
                    @Override
                    public InternalRow next() {
                        return null;
                    }

                    @Override
                    public void releaseBatch() {
                        if (iteratorCloseCalls.incrementAndGet() == 1) {
                            throw new RuntimeException("injected iterator close failure");
                        }
                    }
                };
        AtomicInteger readerCloseCalls = new AtomicInteger();
        RecordReader<InternalRow> reader = new RecordReader<InternalRow>() {
            @Override
            public RecordIterator<InternalRow> readBatch() {
                return null;
            }

            @Override
            public void close() throws IOException {
                if (readerCloseCalls.incrementAndGet() == 1) {
                    throw new IOException("injected reader close failure");
                }
            }
        };
        RetryableIOManager ioManager = new RetryableIOManager();

        Field recordIteratorField = PaimonJniScanner.class.getDeclaredField("recordIterator");
        recordIteratorField.setAccessible(true);
        recordIteratorField.set(scanner, recordIterator);
        Field readerField = PaimonJniScanner.class.getDeclaredField("reader");
        readerField.setAccessible(true);
        readerField.set(scanner, reader);
        Field ioManagerField = PaimonJniScanner.class.getDeclaredField("ioManager");
        ioManagerField.setAccessible(true);
        ioManagerField.set(scanner, ioManager);
        PaimonTableCache.TableCacheEntry cacheEntry =
                new PaimonTableCache.TableCacheEntry(tableWithOptions(Collections.emptyMap()),
                        Collections.emptyList());
        Assert.assertTrue(PaimonTableCache.publish(cacheKey, cacheEntry));
        Field cacheEntryField = PaimonJniScanner.class.getDeclaredField("tableCacheEntry");
        cacheEntryField.setAccessible(true);
        cacheEntryField.set(scanner, cacheEntry);

        try {
            scanner.close();
            Assert.fail("expected the first close to fail");
        } catch (IOException expected) {
            Assert.assertEquals("Failed to release Paimon record iterator", expected.getMessage());
        }
        Assert.assertSame(recordIterator, recordIteratorField.get(scanner));
        Assert.assertSame(reader, readerField.get(scanner));
        Assert.assertSame(ioManager, ioManagerField.get(scanner));
        Assert.assertEquals(0, PaimonTableCache.size());

        scanner.close();
        Assert.assertNull(recordIteratorField.get(scanner));
        Assert.assertNull(readerField.get(scanner));
        Assert.assertNull(ioManagerField.get(scanner));
        Assert.assertEquals(2, iteratorCloseCalls.get());
        Assert.assertEquals(2, readerCloseCalls.get());
        Assert.assertEquals(2, ioManager.closeCalls.get());
        Assert.assertEquals(0, PaimonTableCache.size());
    }

    private Map<String, String> createBaseParams() {
        Map<String, String> params = new HashMap<>();
        params.put("required_fields", "");
        params.put("columns_types", "");
        params.put("paimon_split", "");
        params.put("paimon_predicate", "");
        params.put(SERIALIZED_TABLE_CACHE_KEY, "test-table-cache-key");
        return params;
    }

    private void assertInvalidTableCacheKey(Map<String, String> params) {
        try {
            new PaimonJniScanner(128, params);
            Assert.fail("expected constructor to reject an invalid table cache key");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains(SERIALIZED_TABLE_CACHE_KEY));
        }
    }

    private String encodeFields(String... fields) {
        return Arrays.stream(fields)
                .map(field -> "$" + Base64.getEncoder().encodeToString(field.getBytes(StandardCharsets.UTF_8)))
                .collect(java.util.stream.Collectors.joining(","));
    }

    private void setTableOptions(PaimonJniScanner scanner, Map<String, String> options) throws Exception {
        Table table = tableWithOptions(options);
        Field tableField = PaimonJniScanner.class.getDeclaredField("table");
        tableField.setAccessible(true);
        tableField.set(scanner, table);
    }

    private Table tableWithOptions(Map<String, String> options) {
        return (Table) Proxy.newProxyInstance(
                Table.class.getClassLoader(), new Class[] {Table.class}, (proxy, method, args) -> {
                    if ("options".equals(method.getName())) {
                        return options;
                    }
                    if ("toString".equals(method.getName())) {
                        return "TestPaimonTable";
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    public static class TestIOManager implements IOManager {
        private final String[] tempDirs;

        public TestIOManager(String[] tempDirs) {
            this.tempDirs = tempDirs;
        }

        @Override
        public FileIOChannel.ID createChannel() {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIOChannel.ID createChannel(String channelName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String[] tempDirs() {
            return tempDirs;
        }

        @Override
        public String pickTempDir() {
            return tempDirs.length == 0 ? null : tempDirs[0];
        }

        @Override
        public FileIOChannel.Enumerator createChannelEnumerator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channel) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferFileReader createBufferFileReader(FileIOChannel.ID channel) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    public static class RetryableIOManager extends TestIOManager {
        private final AtomicInteger closeCalls = new AtomicInteger();

        public RetryableIOManager() {
            super(new String[0]);
        }

        @Override
        public void close() {
            if (closeCalls.incrementAndGet() == 1) {
                throw new RuntimeException("injected IO manager close failure");
            }
        }
    }

    private static class CapturingAppender extends AbstractAppender {
        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

        CapturingAppender() {
            super("paimon-test-capture", null, null, false, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
    }

    private static class SerializableTableHandler implements InvocationHandler, Serializable {
        private static final long serialVersionUID = 1L;
        private final Map<String, String> options;

        SerializableTableHandler(Map<String, String> options) {
            this.options = options;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            if ("options".equals(method.getName())) {
                return options;
            }
            if ("rowType".equals(method.getName())) {
                return new RowType(Collections.emptyList());
            }
            if ("schema".equals(method.getName())) {
                return new TableSchema(0L, Collections.emptyList(), 0,
                        Collections.emptyList(), Collections.emptyList(), options, "");
            }
            if (("copy".equals(method.getName()) || "copyWithoutTimeTravel".equals(method.getName()))
                    && args != null && args.length == 1 && args[0] instanceof Map) {
                Map<String, String> copied = new HashMap<>(options);
                copied.putAll((Map<String, String>) args[0]);
                return serializableFileStoreTable(copied);
            }
            if ("toString".equals(method.getName())) {
                return "SerializablePaimonTable";
            }
            throw new UnsupportedOperationException(method.getName());
        }
    }

    private static class InspectablePaimonJniScanner extends PaimonJniScanner {
        InspectablePaimonJniScanner(int batchSize, Map<String, String> params) {
            super(batchSize, params);
        }

        ColumnType[] requiredTypes() {
            return types;
        }
    }

    @Test
    public void testGetFieldIndexMatchesMixedCaseColumns() {
        Assert.assertEquals(1, PaimonJniScanner.getFieldIndex(Arrays.asList("data", "mIxEd_COL", "PART"),
                "mixed_col"));
        Assert.assertEquals(2, PaimonJniScanner.getFieldIndex(Arrays.asList("data", "mIxEd_COL", "PART"),
                "part"));
        Assert.assertEquals(-1, PaimonJniScanner.getFieldIndex(Arrays.asList("data", "mIxEd_COL", "PART"),
                "missing_col"));
    }
}
