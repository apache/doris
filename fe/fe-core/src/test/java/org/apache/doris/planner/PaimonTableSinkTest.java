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

package org.apache.doris.planner;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.trees.plans.commands.insert.PaimonInsertCommandContext;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THiveColumnType;
import org.apache.doris.thrift.TPaimonTableSink;

import mockit.Mock;
import mockit.MockUp;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PaimonTableSinkTest {

    /**
     * Test that bindDataSink() correctly builds TPaimonTableSink for a non-partitioned parquet table.
     */
    @Test
    public void testBindDataSinkParquetNonPartitioned() throws AnalysisException {
        List<Column> columns = Arrays.asList(
                new Column("k", PrimitiveType.INT),
                new Column("v", PrimitiveType.STRING));

        mockPaimonTable("s3://my-bucket/warehouse/db/tbl",
                "parquet", Collections.emptyList(), columns, new HashMap<>());

        PaimonExternalTable table = buildMockTable("test_db", "test_tbl", columns);
        PaimonTableSink sink = new PaimonTableSink(table);
        sink.bindDataSink(Optional.empty());

        Assert.assertNotNull(sink.tDataSink);
        Assert.assertEquals(TDataSinkType.PAIMON_TABLE_SINK, sink.tDataSink.getType());

        TPaimonTableSink tSink = sink.tDataSink.getPaimonTableSink();
        Assert.assertEquals("test_db", tSink.getDbName());
        Assert.assertEquals("test_tbl", tSink.getTbName());
        Assert.assertEquals(TFileFormatType.FORMAT_PARQUET, tSink.getFileFormat());
        Assert.assertEquals(TFileCompressType.SNAPPYBLOCK, tSink.getCompressionType());
        Assert.assertEquals("s3://my-bucket/warehouse/db/tbl", tSink.getOutputPath());
        Assert.assertEquals(TFileType.FILE_S3, tSink.getFileType());
        Assert.assertFalse(tSink.isSetPartitionColumns());

        // columns: all REGULAR since no partition keys
        Assert.assertEquals(2, tSink.getColumns().size());
        tSink.getColumns().forEach(c -> Assert.assertEquals(THiveColumnType.REGULAR, c.getColumnType()));
    }

    /**
     * Test that bindDataSink() correctly builds TPaimonTableSink for an ORC partitioned table.
     */
    @Test
    public void testBindDataSinkOrcPartitioned() throws AnalysisException {
        List<Column> columns = Arrays.asList(
                new Column("k", PrimitiveType.INT),
                new Column("v", PrimitiveType.STRING),
                new Column("dt", PrimitiveType.STRING));
        List<String> partitionKeys = Collections.singletonList("dt");

        mockPaimonTable("hdfs://namenode:9000/warehouse/db/tbl",
                "orc", partitionKeys, columns, new HashMap<>());

        PaimonExternalTable table = buildMockTable("test_db", "part_tbl", columns);
        PaimonTableSink sink = new PaimonTableSink(table);
        sink.bindDataSink(Optional.empty());

        TPaimonTableSink tSink = sink.tDataSink.getPaimonTableSink();
        Assert.assertEquals(TFileFormatType.FORMAT_ORC, tSink.getFileFormat());
        Assert.assertEquals(TFileCompressType.ZLIB, tSink.getCompressionType());
        Assert.assertEquals(TFileType.FILE_HDFS, tSink.getFileType());
        Assert.assertTrue(tSink.isSetPartitionColumns());
        Assert.assertEquals(Collections.singletonList("dt"), tSink.getPartitionColumns());

        // dt should be PARTITION_KEY, k and v should be REGULAR
        Map<String, THiveColumnType> typeMap = new HashMap<>();
        tSink.getColumns().forEach(c -> typeMap.put(c.getName(), c.getColumnType()));
        Assert.assertEquals(THiveColumnType.REGULAR, typeMap.get("k"));
        Assert.assertEquals(THiveColumnType.REGULAR, typeMap.get("v"));
        Assert.assertEquals(THiveColumnType.PARTITION_KEY, typeMap.get("dt"));
    }

    /**
     * Test that INSERT OVERWRITE sets the overwrite flag in TPaimonTableSink.
     */
    @Test
    public void testBindDataSinkWithOverwriteContext() throws AnalysisException {
        List<Column> columns = Arrays.asList(
                new Column("k", PrimitiveType.INT),
                new Column("v", PrimitiveType.STRING));

        mockPaimonTable("s3://my-bucket/warehouse/db/tbl",
                "parquet", Collections.emptyList(), columns, new HashMap<>());

        PaimonExternalTable table = buildMockTable("test_db", "test_tbl", columns);
        PaimonTableSink sink = new PaimonTableSink(table);

        PaimonInsertCommandContext ctx = new PaimonInsertCommandContext(true);
        sink.bindDataSink(Optional.of(ctx));

        TPaimonTableSink tSink = sink.tDataSink.getPaimonTableSink();
        Assert.assertTrue(tSink.isOverwrite());
    }

    /**
     * Test that hadoop config from catalog is populated in the thrift struct.
     */
    @Test
    public void testBindDataSinkHadoopConfig() throws AnalysisException {
        List<Column> columns = Collections.singletonList(new Column("id", PrimitiveType.INT));
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("s3.endpoint", "http://minio:9000");
        catalogOptions.put("s3.access_key", "admin");

        mockPaimonTable("s3://warehouse/db/tbl", "parquet",
                Collections.emptyList(), columns, catalogOptions);

        PaimonExternalTable table = buildMockTable("db", "tbl", columns);
        PaimonTableSink sink = new PaimonTableSink(table);
        sink.bindDataSink(Optional.empty());

        TPaimonTableSink tSink = sink.tDataSink.getPaimonTableSink();
        Assert.assertNotNull(tSink.getHadoopConfig());
        Assert.assertEquals("http://minio:9000", tSink.getHadoopConfig().get("s3.endpoint"));
        Assert.assertEquals("admin", tSink.getHadoopConfig().get("s3.access_key"));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private PaimonExternalTable buildMockTable(String dbName, String tblName, List<Column> columns) {
        new MockUp<PaimonExternalTable>() {
            @Mock
            public String getDbName() {
                return dbName;
            }

            @Mock
            public String getName() {
                return tblName;
            }

            @Mock
            public List<Column> getColumns() {
                return columns;
            }

            @Mock
            public PaimonExternalCatalog getCatalog() {
                return buildMockCatalog();
            }
        };
        // Return null; all methods are mocked via MockUp
        return null;
    }

    private PaimonExternalCatalog buildMockCatalog() {
        return null; // methods mocked via MockUp<PaimonExternalCatalog>
    }

    /**
     * Sets up MockUp for PaimonExternalTable.getPaimonTable() and PaimonExternalCatalog.getPaimonOptionsMap()
     * so that PaimonTableSink.bindDataSink() can run without real Paimon infrastructure.
     */
    private void mockPaimonTable(String location, String fileFormat,
            List<String> partitionKeys, List<Column> columns,
            Map<String, String> catalogOptions) {
        new MockUp<PaimonExternalCatalog>() {
            @Mock
            public Map<String, String> getPaimonOptionsMap() {
                return new HashMap<>(catalogOptions);
            }
        };

        new MockUp<PaimonExternalTable>() {
            @Mock
            public org.apache.paimon.table.Table getPaimonTable(Optional<List<String>> requiredFields) {
                FileStoreTable mockTable = org.mockito.Mockito.mock(FileStoreTable.class);

                // location
                org.mockito.Mockito.when(mockTable.location()).thenReturn(new Path(location));

                // options: file format
                Map<String, String> opts = new HashMap<>();
                opts.put(CoreOptions.FILE_FORMAT.key(), fileFormat);
                org.mockito.Mockito.when(mockTable.options()).thenReturn(opts);

                // partition keys
                org.mockito.Mockito.when(mockTable.partitionKeys())
                        .thenReturn(new ArrayList<>(partitionKeys));

                return mockTable;
            }
        };
    }
}
