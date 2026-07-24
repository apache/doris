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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.trees.plans.commands.insert.BaseExternalTableInsertCommandContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TPaimonTableSink;
import org.apache.doris.thrift.TPaimonWriteShuffleMode;

import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class PaimonTableSinkTest {

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testBindDataSinkIncludesInsertContextAndBucketMetadata() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("alice", "127.0.0.1"));
        ctx.getSessionVariable().enablePaimonJniWriter = true;
        ctx.getSessionVariable().enablePaimonJniCompact = false;
        ctx.getSessionVariable().paimonTargetFileSize = 1024L;
        ctx.getSessionVariable().paimonWriteBufferSize = 2048L;
        ctx.setThreadLocalInfo();

        PaimonExternalTable table = mockPaimonTable(true);
        PaimonTableSink sink = new PaimonTableSink(table);
        sink.setCols(Arrays.asList(
                new Column("id", PrimitiveType.INT),
                new Column("pt", PrimitiveType.STRING)));

        BaseExternalTableInsertCommandContext insertCtx = new BaseExternalTableInsertCommandContext();
        insertCtx.setTxnId(9527L);
        insertCtx.setCommitUser("alice");

        sink.bindDataSink(Optional.of(insertCtx));

        TPaimonTableSink thriftSink = sink.tDataSink.getPaimonTableSink();
        Assertions.assertEquals("ctl", thriftSink.getCatalogName());
        Assertions.assertEquals("db1", thriftSink.getDbName());
        Assertions.assertEquals("tbl1", thriftSink.getTbName());
        Assertions.assertEquals(Collections.singletonList("pt"), thriftSink.getPartitionKeys());
        Assertions.assertEquals(Collections.singletonList("id"), thriftSink.getBucketKeys());
        Assertions.assertEquals(8, thriftSink.getBucketNum());
        Assertions.assertEquals(TPaimonWriteShuffleMode.PAIMON_SHUFFLE_BUCKET, thriftSink.getShuffleMode());
        Assertions.assertEquals(Arrays.asList("id", "pt"), thriftSink.getColumnNames());
        Assertions.assertTrue(thriftSink.isSetSerializedTable());
        Assertions.assertEquals("hdfs://namenode:8020/warehouse/db1.db/tbl1", thriftSink.getTableLocation());
        Assertions.assertEquals("9527", thriftSink.getOptions().get("doris.commit_identifier"));
        Assertions.assertEquals("alice", thriftSink.getOptions().get("doris.commit_user"));
        Assertions.assertEquals("true", thriftSink.getOptions().get("paimon_use_jni"));
        Assertions.assertEquals("false", thriftSink.getOptions().get("paimon_use_jni_compact"));
        Assertions.assertEquals("1024", thriftSink.getOptions().get("target-file-size"));
        Assertions.assertEquals("2048", thriftSink.getOptions().get("write-buffer-size"));
        Assertions.assertEquals("hadoop", thriftSink.getOptions().get("hadoop.user.name"));
        Assertions.assertEquals("hadoop", thriftSink.getOptions().get("hadoop.username"));
    }

    @Test
    public void testBindDataSinkUsesPartitionShuffleForPartitionedTable() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("carol", "127.0.0.1"));
        ctx.setThreadLocalInfo();

        PaimonExternalTable table = mockPaimonTable(0, Collections.emptyList(), true);
        PaimonTableSink sink = new PaimonTableSink(table);
        sink.setCols(Arrays.asList(
                new Column("id", PrimitiveType.INT),
                new Column("pt", PrimitiveType.STRING)));

        sink.bindDataSink(Optional.empty());

        TPaimonTableSink thriftSink = sink.tDataSink.getPaimonTableSink();
        Assertions.assertEquals(Collections.singletonList("pt"), thriftSink.getPartitionKeys());
        Assertions.assertEquals(TPaimonWriteShuffleMode.PAIMON_SHUFFLE_PARTITION, thriftSink.getShuffleMode());
    }

    @Test
    public void testBindDataSinkFallsBackToFieldNamesWhenBucketKeysMissing() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("dave", "127.0.0.1"));
        ctx.setThreadLocalInfo();

        PaimonExternalTable table = mockPaimonTable(8, Collections.emptyList(), false);
        PaimonTableSink sink = new PaimonTableSink(table);
        sink.setCols(Arrays.asList(
                new Column("id", PrimitiveType.INT),
                new Column("pt", PrimitiveType.STRING)));

        sink.bindDataSink(Optional.empty());

        TPaimonTableSink thriftSink = sink.tDataSink.getPaimonTableSink();
        Assertions.assertEquals(Arrays.asList("id", "pt"), thriftSink.getBucketKeys());
        Assertions.assertEquals(TPaimonWriteShuffleMode.PAIMON_SHUFFLE_BUCKET, thriftSink.getShuffleMode());
    }

    @Test
    public void testBindDataSinkFallsBackToRandomShuffleWithoutBucketOrPartition() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("bob", "127.0.0.1"));
        ctx.setThreadLocalInfo();

        PaimonExternalTable table = mockPaimonTable(false);
        PaimonTableSink sink = new PaimonTableSink(table);
        sink.setCols(Collections.singletonList(new Column("id", PrimitiveType.INT)));

        sink.bindDataSink(Optional.empty());

        TPaimonTableSink thriftSink = sink.tDataSink.getPaimonTableSink();
        Assertions.assertTrue(thriftSink.getPartitionKeys().isEmpty());
        Assertions.assertTrue(thriftSink.getBucketKeys().isEmpty());
        Assertions.assertEquals(TPaimonWriteShuffleMode.PAIMON_SHUFFLE_RANDOM, thriftSink.getShuffleMode());
    }

    private static PaimonExternalTable mockPaimonTable(boolean bucketed) {
        return mockPaimonTable(bucketed ? 8 : 0,
                bucketed ? Collections.singletonList("id") : Collections.emptyList(), bucketed);
    }

    private static PaimonExternalTable mockPaimonTable(int bucketNum, java.util.List<String> bucketKeys,
            boolean partitioned) {
        CatalogProperty catalogProperty = Mockito.mock(CatalogProperty.class);
        Mockito.when(catalogProperty.getHadoopProperties()).thenReturn(Collections.emptyMap());

        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);
        Map<String, String> paimonOptions = new HashMap<>();
        paimonOptions.put(CatalogOptions.WAREHOUSE.key(), "hdfs://namenode:8020/warehouse");
        Mockito.when(catalog.getPaimonOptionsMap()).thenReturn(paimonOptions);

        FileStoreTable paimonTable = Mockito.mock(FileStoreTable.class, Mockito.withSettings().serializable());
        TableSchema schema = Mockito.mock(TableSchema.class, Mockito.withSettings().serializable());
        Mockito.when(schema.numBuckets()).thenReturn(bucketNum);
        Mockito.when(schema.bucketKeys()).thenReturn(bucketKeys);
        Mockito.when(schema.fieldNames()).thenReturn(Arrays.asList("id", "pt"));
        Mockito.when(paimonTable.schema()).thenReturn(schema);
        Mockito.when(paimonTable.location()).thenReturn(new Path("hdfs://namenode:8020/warehouse/db1.db/tbl1"));

        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getDbName()).thenReturn("db1");
        Mockito.when(table.getName()).thenReturn("tbl1");
        Mockito.when(table.getPaimonTable(Mockito.any())).thenReturn(paimonTable);
        Mockito.when(table.getPartitionColumns(Mockito.any())).thenReturn(partitioned
                ? Collections.singletonList(new Column("pt", PrimitiveType.STRING))
                : Collections.emptyList());
        return table;
    }
}
