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

package org.apache.doris.catalog;

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionKeyDesc.PartitionKeyValueType;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class RangePartitionInfoTest {

    private List<Column> partitionColumns;
    private RangePartitionInfo partitionInfo;

    private List<SinglePartitionDesc> singlePartitionDescs;

    @Before
    public void setUp() {
        partitionColumns = new LinkedList<Column>();
        singlePartitionDescs = new LinkedList<SinglePartitionDesc>();
    }

    @Test(expected = DdlException.class)
    public void testTinyInt() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.TINYINT), true, null, "", "");
        partitionColumns.add(k1);

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createLessThan(Lists.newArrayList(new PartitionValue("-128"))),
                null));


        partitionInfo = new RangePartitionInfo(partitionColumns);
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
    }

    @Test(expected = DdlException.class)
    public void testSmallInt() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.SMALLINT), true, null, "", "");
        partitionColumns.add(k1);

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createLessThan(Lists.newArrayList(new PartitionValue("-32768"))),
                null));

        partitionInfo = new RangePartitionInfo(partitionColumns);
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
    }

    @Test(expected = DdlException.class)
    public void testInt() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        partitionColumns.add(k1);

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createLessThan(Lists.newArrayList(new PartitionValue("-2147483648"))),
                null));

        partitionInfo = new RangePartitionInfo(partitionColumns);
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
    }

    @Test(expected = DdlException.class)
    public void testBigInt() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1", PartitionKeyDesc.createLessThan(Lists
                .newArrayList(new PartitionValue("-9223372036854775808"))), null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p2", PartitionKeyDesc.createLessThan(Lists
                .newArrayList(new PartitionValue("-9223372036854775806"))), null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p3", PartitionKeyDesc.createLessThan(Lists
                .newArrayList(new PartitionValue("0"))), null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p4", PartitionKeyDesc.createLessThan(Lists
                .newArrayList(new PartitionValue("9223372036854775806"))), null));

        partitionInfo = new RangePartitionInfo(partitionColumns);

        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
    }

    @Test
    public void testBigIntNormal() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1", PartitionKeyDesc.createLessThan(Lists
                .newArrayList(new PartitionValue("-9223372036854775806"))), null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p2", PartitionKeyDesc.createLessThan(Lists
                .newArrayList(new PartitionValue("-9223372036854775805"))), null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p3", PartitionKeyDesc.createLessThan(Lists
                .newArrayList(new PartitionValue("0"))), null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p4", PartitionKeyDesc.createLessThan(Lists
                .newArrayList(new PartitionValue("9223372036854775806"))), null));

        partitionInfo = new RangePartitionInfo(partitionColumns);

        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
    }

    /**
     *    PARTITION BY RANGE(`k1`, `k2`) (
     *       PARTITION p0 VALUES  [("20190101", "100"),("20190101", "200")),
     *       PARTITION p1 VALUES  [("20190105", "10"),("20190107", "10")),
     *       PARTITION p2 VALUES  [("20181231", "10"),("20190101", "100")),
     *       PARTITION p3 VALUES  [("20190105", "100"),("20190120", MAXVALUE))
     *   )
     */
    @Test
    public void testFixedRange() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")),
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("200")));
        PartitionKeyDesc p2 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("20190105"), new PartitionValue("10")),
                Lists.newArrayList(new PartitionValue("20190107"), new PartitionValue("10")));
        PartitionKeyDesc p3 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("20181231"), new PartitionValue("10")),
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")));
        PartitionKeyDesc p4 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("20190105"), new PartitionValue("100")),
                Lists.newArrayList(new PartitionValue("20190120"), new PartitionValue("10000000000")));

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1", p1, null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p2", p2, null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p3", p3, null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p4", p4, null));

        partitionInfo = new RangePartitionInfo(partitionColumns);

        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(columns, null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
    }

    /**
     * 失败用例 less than && fixed
     *    partition by range(k1,k2,k3) (
     *       partition p1 values less than("2019-02-01", "100", "200"),
     *       partition p2 values [("2020-02-01", "100", "200"), (MAXVALUE)),
     *       partition p3 values less than("2021-02-01")
     * )
     */
    @Test(expected = AnalysisException.class)
    public void testFixedRange1() throws DdlException, AnalysisException {
        //add columns
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k3 = new Column("k3", new ScalarType(PrimitiveType.INT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);
        partitionColumns.add(k3);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = PartitionKeyDesc.createLessThan(
                Lists.newArrayList(new PartitionValue("2019-02-01"), new PartitionValue("100"), new PartitionValue("200")));
        PartitionKeyDesc p2 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("2020-02-01"), new PartitionValue("100"), new PartitionValue("200")),
                Lists.newArrayList(new PartitionValue("2020-03-01")));
        PartitionKeyDesc p3 = PartitionKeyDesc.createLessThan(
                Lists.newArrayList(new PartitionValue("2021-02-01")));

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1", p1, null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p2", p2, null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p3", p3, null));
        partitionInfo = new RangePartitionInfo(partitionColumns);
        PartitionKeyValueType partitionKeyValueType = PartitionKeyValueType.INVALID;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            // check partitionType
            if (partitionKeyValueType == PartitionKeyValueType.INVALID) {
                partitionKeyValueType = singlePartitionDesc.getPartitionKeyDesc().getPartitionType();
            } else if (partitionKeyValueType != singlePartitionDesc.getPartitionKeyDesc().getPartitionType()) {
                throw new AnalysisException("You can only use one of these methods to create partitions");
            }
            singlePartitionDesc.analyze(partitionColumns.size(), null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
    }

    /**
     *    PARTITION BY RANGE(`k1`, `k2`) (
     *     PARTITION p1 VALUES  [(), ("20190301", "400"))
     * )
     */
    @Test
    public void testFixedRange2() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = PartitionKeyDesc.createFixed(new ArrayList<>(),
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("200")));

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1", p1, null));

        partitionInfo = new RangePartitionInfo(partitionColumns);

        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(columns, null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
    }

    /**
     * 失败用例
     *    PARTITION BY RANGE(`k1`, `k2`) (
     *     PARTITION p1 VALUES  [("20190301", "400"), ())
     * )
     */
    @Test (expected = AnalysisException.class)
    public void testFixedRange3() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("200")),
                new ArrayList<>());

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1", p1, null));

        partitionInfo = new RangePartitionInfo(partitionColumns);

        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(columns, null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
    }

    /**
     *    PARTITION BY RANGE(`k1`, `k2`) (
     *       PARTITION p0 VALUES  [("20190101", "100"),("20190201"))
     *   )
     */
    @Test
    public void testFixedRange4() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")),
                Lists.newArrayList(new PartitionValue("20190201")));

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1", p1, null));

        partitionInfo = new RangePartitionInfo(partitionColumns);

        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(columns, null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
    }

    /**
     * 失败用例
     *    PARTITION BY RANGE(`k1`, `k2`) (
     *       PARTITION p0 VALUES  [("20190101", "100"),("20190101", "100"))
     *   )
     */
    @Test (expected = DdlException.class)
    public void testFixedRange5() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")),
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")));

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1", p1, null));

        partitionInfo = new RangePartitionInfo(partitionColumns);

        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(columns, null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
    }


    @Test (expected = DdlException.class)
    public void testFixedRange6() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        partitionColumns.add(k1);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("2021-06-01")),
                Lists.newArrayList(new PartitionValue("2021-06-02")));

        PartitionKeyDesc p2 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("2021-07-01")),
                Lists.newArrayList(new PartitionValue("2021-08-01")));

        PartitionKeyDesc p3 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("2021-06-01")),
                Lists.newArrayList(new PartitionValue("2021-07-01")));

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1", p1, null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p2", p2, null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p3", p3, null));
        partitionInfo = new RangePartitionInfo(partitionColumns);

        long partitionId = 20000L;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(columns, null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, partitionId++, false);
        }
    }

    @Test(expected = AnalysisException.class)
    public void testFixedRange7() throws DdlException, AnalysisException {
        //add columns
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATEV2), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k3 = new Column("k3", new ScalarType(PrimitiveType.INT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);
        partitionColumns.add(k3);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = PartitionKeyDesc.createLessThan(Lists.newArrayList(new PartitionValue("2019-02-01"),
                new PartitionValue("100"), new PartitionValue("200")));
        PartitionKeyDesc p2 = PartitionKeyDesc.createFixed(Lists.newArrayList(new PartitionValue("2020-02-01"),
                new PartitionValue("100"), new PartitionValue("200")),
                Lists.newArrayList(new PartitionValue("2020-03-01")));
        PartitionKeyDesc p3 = PartitionKeyDesc.createLessThan(Lists.newArrayList(new PartitionValue("2021-02-01")));

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1", p1, null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p2", p2, null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p3", p3, null));
        partitionInfo = new RangePartitionInfo(partitionColumns);
        PartitionKeyValueType partitionKeyValueType = PartitionKeyValueType.INVALID;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            // check partitionType
            if (partitionKeyValueType == PartitionKeyValueType.INVALID) {
                partitionKeyValueType = singlePartitionDesc.getPartitionKeyDesc().getPartitionType();
            } else if (partitionKeyValueType != singlePartitionDesc.getPartitionKeyDesc().getPartitionType()) {
                throw new AnalysisException("You can only use one of these methods to create partitions");
            }
            singlePartitionDesc.analyze(partitionColumns.size(), null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
    }

    @Test (expected = DdlException.class)
    public void testFixedRange8() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATEV2), true, null, "", "");
        partitionColumns.add(k1);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("2021-06-01")),
                Lists.newArrayList(new PartitionValue("2021-06-02")));

        PartitionKeyDesc p2 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("2021-07-01")),
                Lists.newArrayList(new PartitionValue("2021-08-01")));

        PartitionKeyDesc p3 = PartitionKeyDesc.createFixed(
                Lists.newArrayList(new PartitionValue("2021-06-01")),
                Lists.newArrayList(new PartitionValue("2021-07-01")));

        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1", p1, null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p2", p2, null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p3", p3, null));
        partitionInfo = new RangePartitionInfo(partitionColumns);

        long partitionId = 20000L;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(columns, null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, partitionId++, false);
        }
    }

    @Test
    public void testSerialization() throws IOException, AnalysisException, DdlException {
        // 1. Write objects to file
        final Path path = Files.createTempFile("rangePartitionInfo", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        partitionInfo = new RangePartitionInfo(partitionColumns);

        Text.writeString(out, GsonUtils.GSON.toJson(partitionInfo));
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        RangePartitionInfo partitionInfo2 = GsonUtils.GSON.fromJson(Text.readString(in), RangePartitionInfo.class);

        Assert.assertEquals(partitionInfo.getType(), partitionInfo2.getType());

        // 3. delete files
        in.close();
        Files.delete(path);
    }
}
