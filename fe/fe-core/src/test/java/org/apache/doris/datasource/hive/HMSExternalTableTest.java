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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.fs.FileSystemDirectoryLister;
import org.apache.doris.thrift.TFileFormatType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;


/**
 * Test class for HMSExternalTable, focusing on view-related functionality
 */
public class HMSExternalTableTest {
    private TestHMSExternalTable table;
    private static final String TEST_VIEW_TEXT = "SELECT * FROM test_table";
    private static final String TEST_EXPANDED_VIEW = "/* Presto View */";

    // Real example of a Presto View definition
    private static final String PRESTO_VIEW_ORIGINAL = "/* Presto View: eyJvcmlnaW5hbFNxbCI6IlNFTEVDVFxuICBkZXBhcnRtZW50XG4sIGxlbmd0aChkZXBhcnRtZW50KSBkZXBhcnRtZW50X2xlbmd0aFxuLCBkYXRlX3RydW5jKCd5ZWFyJywgaGlyZV9kYXRlKSB5ZWFyXG5GUk9NXG4gIGVtcGxveWVlc1xuIiwiY2F0YWxvZyI6ImhpdmUiLCJzY2hlbWEiOiJtbWNfaGl2ZSIsImNvbHVtbnMiOlt7Im5hbWUiOiJkZXBhcnRtZW50IiwidHlwZSI6InZhcmNoYXIifSx7Im5hbWUiOiJkZXBhcnRtZW50X2xlbmd0aCIsInR5cGUiOiJiaWdpbnQifSx7Im5hbWUiOiJ5ZWFyIiwidHlwZSI6ImRhdGUifV0sIm93bmVyIjoidHJpbm8vbWFzdGVyLTEtMS5jLTA1OTYxNzY2OThiZDRkMTcuY24tYmVpamluZy5lbXIuYWxpeXVuY3MuY29tIiwicnVuQXNJbnZva2VyIjpmYWxzZX0= */";

    // Expected SQL query after decoding and parsing
    private static final String EXPECTED_SQL = "SELECT\n  department\n, length(department) department_length\n, date_trunc('year', hire_date) year\nFROM\n  employees\n";

    private HMSExternalCatalog mockCatalog = Mockito.mock(HMSExternalCatalog.class);

    private HMSExternalDatabase mockDb;

    @BeforeEach
    public void setUp() {
        // Create a mock database with minimal required functionality
        mockDb = new HMSExternalDatabase(mockCatalog, 1L, "test_db", "remote_test_db") {
            @Override
            public String getFullName() {
                return "test_catalog.test_db";
            }
        };

        table = new TestHMSExternalTable(mockCatalog, mockDb);
    }

    @Test
    public void testGetViewText_Normal() {
        // Test regular view text retrieval
        table.setViewOriginalText(TEST_VIEW_TEXT);
        table.setViewExpandedText(TEST_VIEW_TEXT);
        Assertions.assertEquals(TEST_VIEW_TEXT, table.getViewText());
    }

    @Test
    public void testGetViewText_PrestoView() {
        // Test Presto view parsing including base64 decode and JSON extraction
        table.setViewOriginalText(PRESTO_VIEW_ORIGINAL);
        table.setViewExpandedText(TEST_EXPANDED_VIEW);
        Assertions.assertEquals(EXPECTED_SQL, table.getViewText());
    }

    @Test
    public void testGetViewText_InvalidPrestoView() {
        // Test handling of invalid Presto view definition
        String invalidPrestoView = "/* Presto View: invalid_base64_content */";
        table.setViewOriginalText(invalidPrestoView);
        table.setViewExpandedText(TEST_EXPANDED_VIEW);
        Assertions.assertEquals(invalidPrestoView, table.getViewText());
    }

    @Test
    public void testGetViewText_EmptyExpandedView() {
        // Test handling of empty expanded view text
        table.setViewOriginalText(TEST_VIEW_TEXT);
        table.setViewExpandedText("");
        Assertions.assertEquals(TEST_VIEW_TEXT, table.getViewText());
    }

    // -------------------------------------------------------------------------
    // Tests for SUPPORTED_HIVE_FILE_FORMATS whitelist (LZO input formats)
    // -------------------------------------------------------------------------

    @Test
    public void testSupportedFileFormats_ContainsCompressionLzoTextInputFormat() {
        // twitter hadoop-lzo (GPL): com.hadoop.compression.lzo.LzoTextInputFormat
        Assertions.assertTrue(
                HMSExternalTable.SUPPORTED_HIVE_FILE_FORMATS.contains(
                        "com.hadoop.compression.lzo.LzoTextInputFormat"),
                "com.hadoop.compression.lzo.LzoTextInputFormat should be in the supported formats whitelist");
    }

    @Test
    public void testSupportedFileFormats_ContainsMapreduceLzoTextInputFormat() {
        // lzo-hadoop (org.anarres) mapreduce API: com.hadoop.mapreduce.LzoTextInputFormat
        Assertions.assertTrue(
                HMSExternalTable.SUPPORTED_HIVE_FILE_FORMATS.contains(
                        "com.hadoop.mapreduce.LzoTextInputFormat"),
                "com.hadoop.mapreduce.LzoTextInputFormat should be in the supported formats whitelist");
    }

    @Test
    public void testSupportedFileFormats_ContainsDeprecatedLzoTextInputFormat() {
        // lzo-hadoop (org.anarres) legacy mapred API: com.hadoop.mapred.DeprecatedLzoTextInputFormat
        Assertions.assertTrue(
                HMSExternalTable.SUPPORTED_HIVE_FILE_FORMATS.contains(
                        "com.hadoop.mapred.DeprecatedLzoTextInputFormat"),
                "com.hadoop.mapred.DeprecatedLzoTextInputFormat should be in the supported formats whitelist");
    }

    // -------------------------------------------------------------------------
    // Tests for getFileFormatType: LZO tables must reject INSERT INTO
    // -------------------------------------------------------------------------

    /**
     * Build a minimal Hive Table SD stub with the given InputFormat class name.
     */
    private Table buildRemoteTableWithInputFormat(String inputFormatName) {
        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        StorageDescriptor sd = new StorageDescriptor();
        sd.setInputFormat(inputFormatName);
        sd.setSerdeInfo(serDeInfo);
        Table remoteTable = new Table();
        remoteTable.setSd(sd);
        return remoteTable;
    }

    @Test
    public void testGetFileFormatType_LzoTextInputFormat_ReturnsText() throws UserException {
        // LZO tables use the LazySimpleSerDe (text SerDe); getFileFormatType() must return
        // FORMAT_TEXT so that the read path can decode the CSV-like payload inside each .lzo block.
        // The INSERT rejection lives in BindSink.bindHiveTableSink(), NOT here.
        String lzoFormat = "com.hadoop.compression.lzo.LzoTextInputFormat";
        Table remoteTable = buildRemoteTableWithInputFormat(lzoFormat);
        TestHMSExternalTableWithRemote lzoTable = new TestHMSExternalTableWithRemote(
                mockCatalog, mockDb, remoteTable);
        TFileFormatType type = lzoTable.getFileFormatType(null);
        Assertions.assertEquals(TFileFormatType.FORMAT_TEXT, type,
                "LZO table with LazySimpleSerDe should resolve to FORMAT_TEXT for reading");
    }

    @Test
    public void testGetFileFormatType_DeprecatedLzoTextInputFormat_ReturnsText() throws UserException {
        String lzoFormat = "com.hadoop.mapred.DeprecatedLzoTextInputFormat";
        Table remoteTable = buildRemoteTableWithInputFormat(lzoFormat);
        TestHMSExternalTableWithRemote lzoTable = new TestHMSExternalTableWithRemote(
                mockCatalog, mockDb, remoteTable);
        TFileFormatType type = lzoTable.getFileFormatType(null);
        Assertions.assertEquals(TFileFormatType.FORMAT_TEXT, type,
                "DeprecatedLzoTextInputFormat table should also resolve to FORMAT_TEXT for reading");
    }

    @Test
    public void testGetFileFormatType_MapreduceLzoTextInputFormat_ReturnsText() throws UserException {
        String lzoFormat = "com.hadoop.mapreduce.LzoTextInputFormat";
        Table remoteTable = buildRemoteTableWithInputFormat(lzoFormat);
        TestHMSExternalTableWithRemote lzoTable = new TestHMSExternalTableWithRemote(
                mockCatalog, mockDb, remoteTable);
        TFileFormatType type = lzoTable.getFileFormatType(null);
        Assertions.assertEquals(TFileFormatType.FORMAT_TEXT, type,
                "com.hadoop.mapreduce.LzoTextInputFormat table should also resolve to FORMAT_TEXT for reading");
    }

    @Test
    public void testFetchRowCountFillsMetaCacheOnlyWhenRequested() throws Exception {
        long catalogId = 100L;
        String localDbName = "test_db";
        String partitionValue = "2026-05-21";
        String inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
        String partitionLocation = "file:///tmp/doris_hms_row_count_cache/dt=2026-05-21";

        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        HMSExternalDatabase db = Mockito.mock(HMSExternalDatabase.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        Mockito.when(catalog.getName()).thenReturn("test_catalog");
        Mockito.when(catalog.getProperties()).thenReturn(ImmutableMap.of());
        Mockito.when(db.getFullName()).thenReturn(localDbName);

        Table remoteTable = buildRemoteTableWithInputFormat(inputFormat);
        remoteTable.setParameters(ImmutableMap.of());
        TestHMSExternalTableForMetaCache table = new TestHMSExternalTableForMetaCache(
                catalog, db, remoteTable, partitionValue);
        Deencapsulation.setField(table, "dlaType", HMSExternalTable.DLAType.HIVE);

        List<HivePartition> partitions = Collections.singletonList(new HivePartition(
                null, false, inputFormat, partitionLocation, Collections.singletonList(partitionValue),
                Collections.emptyMap()));
        HiveExternalMetaCache.FileCacheValue fileCacheValue = new HiveExternalMetaCache.FileCacheValue();
        HiveExternalMetaCache.HiveFileStatus status = new HiveExternalMetaCache.HiveFileStatus();
        status.setLength(128L);
        fileCacheValue.getFiles().add(status);
        List<HiveExternalMetaCache.FileCacheValue> files = Collections.singletonList(fileCacheValue);

        HiveExternalMetaCache hiveCache = Mockito.mock(HiveExternalMetaCache.class);
        Mockito.when(hiveCache.getAllPartitionsWithCache(Mockito.eq(table), Mockito.anyList()))
                .thenReturn(partitions);
        Mockito.when(hiveCache.getAllPartitionsWithoutCache(Mockito.eq(table), Mockito.anyList()))
                .thenReturn(partitions);
        Mockito.when(hiveCache.getFilesByPartitions(Mockito.eq(partitions), Mockito.eq(true), Mockito.eq(true),
                        Mockito.any(FileSystemDirectoryLister.class), Mockito.isNull()))
                .thenReturn(files);
        Mockito.when(hiveCache.getFilesByPartitions(Mockito.eq(partitions), Mockito.eq(false), Mockito.eq(true),
                        Mockito.any(FileSystemDirectoryLister.class), Mockito.isNull()))
                .thenReturn(files);

        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr extMetaCacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(extMetaCacheMgr);
        Mockito.when(extMetaCacheMgr.hive(catalogId)).thenReturn(hiveCache);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            Assertions.assertEquals(32L, table.fetchRowCountWithMetaCache(true));
            Mockito.verify(hiveCache).getAllPartitionsWithCache(Mockito.eq(table), Mockito.anyList());
            Mockito.verify(hiveCache, Mockito.never())
                    .getAllPartitionsWithoutCache(Mockito.eq(table), Mockito.anyList());
            Mockito.verify(hiveCache).getFilesByPartitions(Mockito.eq(partitions), Mockito.eq(true),
                    Mockito.eq(true), Mockito.any(FileSystemDirectoryLister.class), Mockito.isNull());

            Mockito.clearInvocations(hiveCache);

            Assertions.assertEquals(32L, table.fetchRowCount());
            Mockito.verify(hiveCache).getAllPartitionsWithoutCache(Mockito.eq(table), Mockito.anyList());
            Mockito.verify(hiveCache, Mockito.never())
                    .getAllPartitionsWithCache(Mockito.eq(table), Mockito.anyList());
            Mockito.verify(hiveCache).getFilesByPartitions(Mockito.eq(partitions), Mockito.eq(false),
                    Mockito.eq(true), Mockito.any(FileSystemDirectoryLister.class), Mockito.isNull());
        }
    }

    /**
     * Variant that exposes a pre-built remote table for getFileFormatType tests.
     */
    private static class TestHMSExternalTableWithRemote extends HMSExternalTable {
        private final Table remoteTable;

        public TestHMSExternalTableWithRemote(HMSExternalCatalog catalog,
                HMSExternalDatabase db, Table remoteTable) {
            super(1L, "test_table", "test_table", catalog, db);
            this.remoteTable = remoteTable;
        }

        @Override
        public Table getRemoteTable() {
            return remoteTable;
        }

        @Override
        protected synchronized void makeSureInitialized() {
            this.objectCreated = true;
        }
    }

    private static class TestHMSExternalTableForMetaCache extends TestHMSExternalTableWithRemote {
        private final Column dataColumn = new Column("c1", Type.INT);
        private final Column partitionColumn = new Column("dt", Type.VARCHAR);
        private final HiveExternalMetaCache.HivePartitionValues partitionValues;

        public TestHMSExternalTableForMetaCache(HMSExternalCatalog catalog, HMSExternalDatabase db,
                Table remoteTable, String partitionValue) throws Exception {
            super(catalog, db, remoteTable);
            PartitionKey partitionKey = PartitionKey.createListPartitionKeyWithTypes(
                    Lists.newArrayList(new org.apache.doris.analysis.PartitionValue(partitionValue)),
                    Lists.newArrayList(Type.VARCHAR),
                    true);
            PartitionItem partitionItem = new ListPartitionItem(Lists.newArrayList(partitionKey));
            this.partitionValues = new HiveExternalMetaCache.HivePartitionValues(
                    ImmutableMap.of("dt=" + partitionValue, partitionItem),
                    ImmutableMap.of("dt=" + partitionValue, Collections.singletonList(partitionValue)));
        }

        @Override
        public List<Column> getFullSchema() {
            return Lists.newArrayList(dataColumn, partitionColumn);
        }

        @Override
        public boolean isView() {
            return false;
        }

        @Override
        public List<Type> getPartitionColumnTypes(java.util.Optional<org.apache.doris.datasource.mvcc.MvccSnapshot>
                snapshot) {
            return Collections.singletonList(Type.VARCHAR);
        }

        @Override
        public List<Column> getPartitionColumns() {
            return Collections.singletonList(partitionColumn);
        }

        @Override
        public List<Column> getPartitionColumns(java.util.Optional<org.apache.doris.datasource.mvcc.MvccSnapshot>
                snapshot) {
            return Collections.singletonList(partitionColumn);
        }

        @Override
        public HiveExternalMetaCache.HivePartitionValues getHivePartitionValues(
                java.util.Optional<org.apache.doris.datasource.mvcc.MvccSnapshot> snapshot) {
            return partitionValues;
        }
    }

    /**
     * Test implementation of HMSExternalTable that allows setting view texts
     * Uses parent's getViewText() implementation for actual testing
     */
    private static class TestHMSExternalTable extends HMSExternalTable {
        private String viewExpandedText;
        private String viewOriginalText;

        public TestHMSExternalTable(HMSExternalCatalog catalog, HMSExternalDatabase db) {
            super(1L, "test_table", "test_table", catalog, db);
        }

        @Override
        public String getViewExpandedText() {
            return viewExpandedText;
        }

        @Override
        public String getViewOriginalText() {
            return viewOriginalText;
        }

        public void setViewExpandedText(String viewExpandedText) {
            this.viewExpandedText = viewExpandedText;
        }

        public void setViewOriginalText(String viewOriginalText) {
            this.viewOriginalText = viewOriginalText;
        }

        @Override
        protected synchronized void makeSureInitialized() {
            this.objectCreated = true;
        }
    }
}
