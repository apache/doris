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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonFileExternalCatalog;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PaimonScanNodeTest {

    @Mocked
    private SessionVariable sv;

    @Mocked
    private PaimonFileExternalCatalog paimonFileExternalCatalog;

    @Test
    public void testSplitWeight() throws UserException {

        TupleDescriptor desc = new TupleDescriptor(new TupleId(3));
        PaimonScanNode paimonScanNode = new PaimonScanNode(new PlanNodeId(1), desc, false, sv);

        paimonScanNode.setSource(new PaimonSource());

        DataFileMeta dfm1 = DataFileMeta.forAppend("f1.parquet", 64 * 1024 * 1024, 1, SimpleStats.EMPTY_STATS, 1, 1, 1,
                Collections.emptyList(), null, null, null, null);
        BinaryRow binaryRow1 = BinaryRow.singleColumn(1);
        DataSplit ds1 = DataSplit.builder()
                .rawConvertible(true)
                .withPartition(binaryRow1)
                .withBucket(1)
                .withBucketPath("b1")
                .withDataFiles(Collections.singletonList(dfm1))
                .build();

        DataFileMeta dfm2 = DataFileMeta.forAppend("f2.parquet", 32 * 1024 * 1024, 2, SimpleStats.EMPTY_STATS, 1, 1, 1,
                Collections.emptyList(), null, null, null, null);
        BinaryRow binaryRow2 = BinaryRow.singleColumn(1);
        DataSplit ds2 = DataSplit.builder()
                .rawConvertible(true)
                .withPartition(binaryRow2)
                .withBucket(1)
                .withBucketPath("b1")
                .withDataFiles(Collections.singletonList(dfm2))
                .build();


        new MockUp<PaimonScanNode>() {
            @Mock
            public List<org.apache.paimon.table.source.Split> getPaimonSplitFromAPI() {
                return new ArrayList<org.apache.paimon.table.source.Split>() {{
                        add(ds1);
                        add(ds2);
                    }};
            }
        };

        new MockUp<PaimonSource>() {
            @Mock
            public ExternalCatalog getCatalog() {
                return paimonFileExternalCatalog;
            }
        };

        new MockUp<ExternalCatalog>() {
            @Mock
            public Map<String, String> getProperties() {
                return Collections.emptyMap();
            }
        };

        new Expectations() {{
                sv.isForceJniScanner();
                result = false;

                sv.getIgnoreSplitType();
                result = "NONE";
            }};

        // native
        mockNativeReader();
        List<org.apache.doris.spi.Split> s1 = paimonScanNode.getSplits(1);
        PaimonSplit s11 = (PaimonSplit) s1.get(0);
        PaimonSplit s12 = (PaimonSplit) s1.get(1);
        Assert.assertEquals(2, s1.size());
        Assert.assertEquals(100, s11.getSplitWeight().getRawValue());
        Assert.assertNull(s11.getSplit());
        Assert.assertEquals(50, s12.getSplitWeight().getRawValue());
        Assert.assertNull(s12.getSplit());

        // jni
        mockJniReader();
        List<org.apache.doris.spi.Split> s2 = paimonScanNode.getSplits(1);
        PaimonSplit s21 = (PaimonSplit) s2.get(0);
        PaimonSplit s22 = (PaimonSplit) s2.get(1);
        Assert.assertEquals(2, s2.size());
        Assert.assertNotNull(s21.getSplit());
        Assert.assertNotNull(s22.getSplit());
        Assert.assertEquals(100, s21.getSplitWeight().getRawValue());
        Assert.assertEquals(50, s22.getSplitWeight().getRawValue());
    }

    private void mockJniReader() {
        new MockUp<PaimonScanNode>() {
            @Mock
            public boolean supportNativeReader(Optional<List<RawFile>> optRawFiles) {
                return false;
            }
        };
    }

    private void mockNativeReader() {
        new MockUp<PaimonScanNode>() {
            @Mock
            public boolean supportNativeReader(Optional<List<RawFile>> optRawFiles) {
                return true;
            }
        };
    }
}
