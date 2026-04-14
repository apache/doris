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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergTransaction;
import org.apache.doris.datasource.iceberg.source.IcebergScanNode;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.planner.BaseExternalTableDataSink;
import org.apache.doris.planner.IcebergMergeSink;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionManager;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergMergeExecutorTest {

    private static class TestIcebergScanNode extends IcebergScanNode {
        TestIcebergScanNode() {
            super(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), new SessionVariable(), ScanContext.EMPTY);
        }
    }

    @Test
    public void testFinalizeSinkAndBeforeExecPropagateRewritableDeleteMetadata() throws Exception {
        String referencedDataFile = "file:///tmp/data.parquet";
        DeleteFile deleteFile = FileMetadata.deleteFileBuilder(org.apache.iceberg.PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath("file:///tmp/delete.puffin")
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(128L)
                .withRecordCount(3L)
                .withReferencedDataFile(referencedDataFile)
                .withContentOffset(32L)
                .withContentSizeInBytes(64L)
                .build();
        TIcebergDeleteFileDesc deleteFileDesc = new TIcebergDeleteFileDesc();
        deleteFileDesc.setPath("file:///tmp/delete.puffin");

        TestIcebergScanNode scanNode = new TestIcebergScanNode();
        scanNode.deleteFilesByReferencedDataFile.put(referencedDataFile, Collections.singletonList(deleteFile));
        scanNode.deleteFilesDescByReferencedDataFile.put(referencedDataFile, Collections.singletonList(deleteFileDesc));

        IcebergTransaction transaction = Mockito.mock(IcebergTransaction.class);
        TransactionManager transactionManager = Mockito.mock(TransactionManager.class);
        Mockito.when(transactionManager.getTransaction(11L)).thenReturn(transaction);

        IcebergExternalTable table = mockIcebergExternalTable(3, transactionManager);
        NereidsPlanner planner = mockPlanner(Collections.singletonList(scanNode));
        ConnectContext ctx = new ConnectContext();
        ctx.setQueryId(new TUniqueId(3L, 4L));
        ctx.getSessionVariable().setEnableNereidsDistributePlanner(false);
        ctx.setThreadLocalInfo();

        IcebergMergeExecutor executor = new IcebergMergeExecutor(ctx, table, "label", planner, false, -1L);
        IcebergMergeSink sink = new IcebergMergeSink(table, new org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext());

        executor.finalizeSinkForMerge(null, sink, null);
        TDataSink tDataSink = getTDataSink(sink);
        Assertions.assertEquals(1, tDataSink.getIcebergMergeSink().getRewritableDeleteFileSetsSize());
        Assertions.assertEquals(referencedDataFile,
                tDataSink.getIcebergMergeSink().getRewritableDeleteFileSets().get(0).getReferencedDataFilePath());

        Expression conflictFilter = Expressions.equal("id", 1);
        executor.setConflictDetectionFilter(java.util.Optional.of(conflictFilter));
        executor.txnId = 11L;
        executor.beforeExec();

        Mockito.verify(transaction).beginMerge(table);
        ArgumentCaptor<Map<String, List<DeleteFile>>> deleteFilesCaptor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(transaction).setRewrittenDeleteFilesByReferencedDataFile(deleteFilesCaptor.capture());
        Assertions.assertSame(deleteFile, deleteFilesCaptor.getValue().get(referencedDataFile).get(0));
        Mockito.verify(transaction).setConflictDetectionFilter(conflictFilter);
    }

    private static NereidsPlanner mockPlanner(List<org.apache.doris.planner.ScanNode> scanNodes) {
        NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        Mockito.when(planner.getFragments()).thenReturn(Collections.emptyList());
        Mockito.when(planner.getScanNodes()).thenReturn(scanNodes);
        Mockito.when(planner.getDescTable()).thenReturn(new DescriptorTable());
        Mockito.when(planner.getRuntimeFilters()).thenReturn(Collections.emptyList());
        Mockito.when(planner.getTopnFilters()).thenReturn(Collections.emptyList());
        return planner;
    }

    private static IcebergExternalTable mockIcebergExternalTable(int formatVersion,
            TransactionManager transactionManager) {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        org.apache.iceberg.PartitionSpec spec = org.apache.iceberg.PartitionSpec.unpartitioned();
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
        properties.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
        properties.put(TableProperties.PARQUET_COMPRESSION, "snappy");
        properties.put(TableProperties.WRITE_DATA_LOCATION, "file:///tmp/iceberg_tbl/data");

        Table icebergTable = Mockito.mock(Table.class);
        Mockito.when(icebergTable.properties()).thenReturn(properties);
        Mockito.when(icebergTable.spec()).thenReturn(spec);
        Mockito.when(icebergTable.specs()).thenReturn(Collections.singletonMap(spec.specId(), spec));
        Mockito.when(icebergTable.location()).thenReturn("file:///tmp/iceberg_tbl");
        Mockito.when(icebergTable.schema()).thenReturn(schema);
        Mockito.when(icebergTable.sortOrder()).thenReturn(SortOrder.unsorted());
        Mockito.when(icebergTable.name()).thenReturn("db.tbl");

        CatalogProperty catalogProperty = Mockito.mock(CatalogProperty.class);
        Mockito.when(catalogProperty.getMetastoreProperties()).thenReturn(null);
        Mockito.when(catalogProperty.getStoragePropertiesMap()).thenReturn(Collections.emptyMap());

        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);
        Mockito.when(catalog.getTransactionManager()).thenReturn(transactionManager);
        Mockito.when(catalog.getName()).thenReturn("iceberg");

        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getId()).thenReturn(1L);

        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.isView()).thenReturn(false);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getDbName()).thenReturn("db");
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(table.getIcebergTable()).thenReturn(icebergTable);
        return table;
    }

    private static TDataSink getTDataSink(BaseExternalTableDataSink sink) throws ReflectiveOperationException {
        Field field = BaseExternalTableDataSink.class.getDeclaredField("tDataSink");
        field.setAccessible(true);
        return (TDataSink) field.get(sink);
    }
}
