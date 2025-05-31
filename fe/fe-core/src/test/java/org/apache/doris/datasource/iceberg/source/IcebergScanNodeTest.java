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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.security.authentication.PreExecutionAuthenticator;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IcebergScanNodeTest {

    @Mocked
    HadoopTableOperations hadoopTableOperations;
    @Mocked
    Snapshot snapshot;

    @Test
    public void testIsBatchMode() throws UserException {
        SessionVariable sessionVariable = new SessionVariable();
        IcebergScanNode icebergScanNode = new IcebergScanNode(new PlanNodeId(1), new TupleDescriptor(new TupleId(1)), sessionVariable);

        new Expectations(icebergScanNode) {{
                icebergScanNode.getPushDownAggNoGroupingOp();
                result = TPushAggOp.COUNT;
                icebergScanNode.getCountFromSnapshot();
                result = 1L;
            }};
        Assert.assertFalse(icebergScanNode.isBatchMode());

        BaseTable mockTable = new BaseTable(hadoopTableOperations, "mockTable");
        new Expectations(icebergScanNode) {{
                icebergScanNode.getPushDownAggNoGroupingOp();
                result = TPushAggOp.NONE;
                Deencapsulation.setField(icebergScanNode, "icebergTable", mockTable);
            }};
        TableScan tableScan = mockTable.newScan();
        new Expectations(mockTable) {{
                mockTable.currentSnapshot();
                result = null;
                icebergScanNode.createTableScan();
                result = tableScan;
            }};
        Assert.assertFalse(icebergScanNode.isBatchMode());

        new Expectations(mockTable) {{
                mockTable.currentSnapshot();
                result = snapshot;
            }};
        new Expectations(sessionVariable) {{
                sessionVariable.getEnableExternalTableBatchMode();
                result = false;
            }};
        Assert.assertFalse(icebergScanNode.isBatchMode());


        new Expectations(sessionVariable) {{
                sessionVariable.getEnableExternalTableBatchMode();
                result = true;
            }};
        new Expectations(icebergScanNode) {{
                Deencapsulation.setField(icebergScanNode, "preExecutionAuthenticator", new PreExecutionAuthenticator());
            }};
        new Expectations() {{
                sessionVariable.getNumFilesInBatchMode();
                result = 1024;
            }};

        mockManifestFile("p", 10, 0);
        Assert.assertFalse(icebergScanNode.isBatchMode());

        mockManifestFile("p", 0, 10);
        Assert.assertFalse(icebergScanNode.isBatchMode());

        mockManifestFile("p", 10, 10);
        Assert.assertFalse(icebergScanNode.isBatchMode());

        mockManifestFile("p", 1024, 0);
        Assert.assertTrue(icebergScanNode.isBatchMode());

        mockManifestFile("p", 0, 1024);
        Assert.assertTrue(icebergScanNode.isBatchMode());

        new Expectations() {{
                sessionVariable.getNumFilesInBatchMode();
                result = 100;
            }};

        mockManifestFile("p", 10, 0);
        Assert.assertFalse(icebergScanNode.isBatchMode());

        mockManifestFile("p", 0, 10);
        Assert.assertFalse(icebergScanNode.isBatchMode());

        mockManifestFile("p", 10, 10);
        Assert.assertFalse(icebergScanNode.isBatchMode());

        mockManifestFile("p", 0, 100);
        Assert.assertTrue(icebergScanNode.isBatchMode());

        mockManifestFile("p", 100, 0);
        Assert.assertTrue(icebergScanNode.isBatchMode());

        mockManifestFile("p", 10, 90);
        Assert.assertTrue(icebergScanNode.isBatchMode());
    }

    private void mockManifestFile(String path, int addedFileCount, int existingFileCount) {
        new MockUp<IcebergUtils>() {
            @Mock
            CloseableIterable<ManifestFile> getMatchingManifest(List<ManifestFile> dataManifests,
                                                                Map<Integer, PartitionSpec> specsById,
                                                                Expression dataFilte) {
                return CloseableIterable.withNoopClose(new ArrayList<ManifestFile>() {{
                        add(genManifestFile(path, addedFileCount, existingFileCount));
                    }}
                );
            }
        };
    }

    private ManifestFile genManifestFile(String path, int addedFileCount, int existingFileCount) {
        return new GenericManifestFile(
            path,
            10, // length
            1, // specId
            ManifestContent.DATA,
            1, // sequenceNumber
            1, // minSeqNumber
            1L, // snapshotid
            addedFileCount,
            1,
            existingFileCount,
            1,
            0, // deleteFilesCount
            0,
            Lists.newArrayList(),
            null
        );
    }
}
