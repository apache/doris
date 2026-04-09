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

package org.apache.doris.datasource.iceberg.helper;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.source.IcebergScanNode;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergRewritableDeleteFileSet;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergRewritableDeletePlannerTest {

    private static class TestIcebergScanNode extends IcebergScanNode {
        TestIcebergScanNode() {
            super(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), new SessionVariable(), ScanContext.EMPTY);
        }
    }

    @Test
    public void testCollectForDeleteReturnsEmptyWhenTableFormatVersionIsLessThanThree() throws Exception {
        IcebergExternalTable table = mockIcebergExternalTable(2);
        NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        Mockito.when(planner.getScanNodes()).thenReturn(Collections.singletonList(buildScanNode(
                "file:///tmp/data.parquet",
                buildDeleteFile("file:///tmp/delete.parquet", "file:///tmp/data.parquet"),
                buildDeleteFileDesc("file:///tmp/delete.parquet"))));

        IcebergRewritableDeletePlan plan = IcebergRewritableDeletePlanner.collectForDelete(table, planner);

        Assertions.assertTrue(plan.getThriftDeleteFileSets().isEmpty());
        Assertions.assertTrue(plan.getDeleteFilesByReferencedDataFile().isEmpty());
    }

    @Test
    public void testCollectForMergeAggregatesDeleteFilesAcrossIcebergScanNodes() throws Exception {
        String firstDataFile = "file:///tmp/data-1.parquet";
        String secondDataFile = "file:///tmp/data-2.parquet";
        DeleteFile firstDeleteFile = buildDeleteFile("file:///tmp/delete-1.puffin", firstDataFile);
        DeleteFile secondDeleteFile = buildDeleteFile("file:///tmp/delete-2.puffin", secondDataFile);
        TestIcebergScanNode firstScanNode = buildScanNode(
                firstDataFile, firstDeleteFile, buildDeleteFileDesc("file:///tmp/delete-1.puffin"));
        TestIcebergScanNode secondScanNode = buildScanNode(
                secondDataFile, secondDeleteFile, buildDeleteFileDesc("file:///tmp/delete-2.puffin"));
        ScanNode otherScanNode = Mockito.mock(ScanNode.class);

        IcebergExternalTable table = mockIcebergExternalTable(3);
        NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        Mockito.when(planner.getScanNodes()).thenReturn(Arrays.asList(firstScanNode, otherScanNode, secondScanNode));

        IcebergRewritableDeletePlan plan = IcebergRewritableDeletePlanner.collectForMerge(table, planner);

        Assertions.assertEquals(2, plan.getThriftDeleteFileSets().size());
        Assertions.assertEquals(2, plan.getDeleteFilesByReferencedDataFile().size());
        Assertions.assertSame(firstDeleteFile, plan.getDeleteFilesByReferencedDataFile().get(firstDataFile).get(0));
        Assertions.assertSame(secondDeleteFile, plan.getDeleteFilesByReferencedDataFile().get(secondDataFile).get(0));

        List<String> referencedDataFiles = plan.getThriftDeleteFileSets().stream()
                .map(TIcebergRewritableDeleteFileSet::getReferencedDataFilePath)
                .sorted()
                .collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList(firstDataFile, secondDataFile), referencedDataFiles);

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> plan.getDeleteFilesByReferencedDataFile().put("new", Collections.emptyList()));
    }

    private static TestIcebergScanNode buildScanNode(
            String referencedDataFile,
            DeleteFile deleteFile,
            TIcebergDeleteFileDesc deleteFileDesc) {
        TestIcebergScanNode scanNode = new TestIcebergScanNode();
        scanNode.deleteFilesByReferencedDataFile.put(referencedDataFile, Collections.singletonList(deleteFile));
        scanNode.deleteFilesDescByReferencedDataFile.put(referencedDataFile, Collections.singletonList(deleteFileDesc));
        return scanNode;
    }

    private static DeleteFile buildDeleteFile(String deleteFilePath, String referencedDataFilePath) {
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath(deleteFilePath)
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(128L)
                .withRecordCount(4L)
                .withReferencedDataFile(referencedDataFilePath)
                .withContentOffset(16L)
                .withContentSizeInBytes(64L)
                .build();
    }

    private static TIcebergDeleteFileDesc buildDeleteFileDesc(String deleteFilePath) {
        TIcebergDeleteFileDesc deleteFileDesc = new TIcebergDeleteFileDesc();
        deleteFileDesc.setPath(deleteFilePath);
        return deleteFileDesc;
    }

    private static IcebergExternalTable mockIcebergExternalTable(int formatVersion) {
        Table icebergTable = Mockito.mock(Table.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
        Mockito.when(icebergTable.properties()).thenReturn(properties);

        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.getIcebergTable()).thenReturn(icebergTable);
        return table;
    }
}
