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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ListPartitionInfoTest {
    private List<Column> partitionColumns;
    private ListPartitionInfo partitionInfo;

    private List<SinglePartitionDesc> singlePartitionDescs;

    @Before
    public void setUp() {
        partitionColumns = new LinkedList<>();
        singlePartitionDescs = new LinkedList<>();
    }

    @Test
    public void testTinyInt() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.TINYINT), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("-128")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));

        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
        Assert.assertEquals("-128", ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getStringValue());

    }

    @Test
    public void testSmallInt() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.SMALLINT), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("-32768")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));

        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
        Assert.assertEquals("-32768", ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testInt() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("-2147483648")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));

        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
        Assert.assertEquals("-2147483648", ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testBigInt() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("-9223372036854775808")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));

        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
        Assert.assertEquals("-9223372036854775808", ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testLargeInt() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.LARGEINT), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("-170141183460469231731687303715884105728")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));

        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
        Assert.assertEquals("-170141183460469231731687303715884105728", ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testString() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.CHAR), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("Beijing")));
        inValues.add(Lists.newArrayList(new PartitionValue("Shanghai")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));

        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
        Assert.assertEquals("Beijing", ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getStringValue());
        Assert.assertEquals("Shanghai", ((ListPartitionItem) partitionItem).getItems().get(1).getKeys().get(0).getStringValue());
    }

    @Test
    public void testBoolean() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.BOOLEAN), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("true")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));

        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
        Assert.assertEquals(true, ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getRealValue());
    }

    @Test(expected = DdlException.class)
    public void testDuplicateKey() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.VARCHAR), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("beijing")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p2",
                PartitionKeyDesc.createIn(inValues), null));


        partitionInfo = new ListPartitionInfo(partitionColumns);
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
    }

    @Test
    public void testMultiPartitionKeys() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.VARCHAR), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.INT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("beijing"), new PartitionValue("100")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));


        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(2, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }

        Assert.assertEquals("beijing", ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getRealValue());
        Assert.assertEquals(100, ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(1).getLongValue());
    }

    @Test
    public void testMultiAutotoSql() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.VARCHAR), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.INT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);

        ArrayList<Expr> partitionExprs = new ArrayList<>();
        SlotRef s1 = new SlotRef(new TableName("tbl"), "k1");
        SlotRef s2 = new SlotRef(new TableName("tbl"), "k2");
        partitionExprs.add(s1);
        partitionExprs.add(s2);

        partitionInfo = new ListPartitionInfo(true, partitionExprs, partitionColumns);
        OlapTable table = new OlapTable();

        String sql = partitionInfo.toSql(table, null);

        String expected = "AUTO PARTITION BY LIST (`k1`, `k2`)";
        Assert.assertTrue("got: " + sql + ", should have: " + expected, sql.contains(expected));
    }

    @Test
    public void testListPartitionNullMax() throws AnalysisException, DdlException {
        PartitionItem partitionItem = null;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.INT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);
        partitionInfo = new ListPartitionInfo(partitionColumns);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("", true), PartitionValue.MAX_VALUE));
        SinglePartitionDesc singlePartitionDesc = new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null);
        singlePartitionDesc.analyze(2, null);
        partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);

        Assert.assertEquals("((NULL, MAXVALUE))", ((ListPartitionItem) partitionItem).toSql());

        inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("", true), new PartitionValue("", true)));
        singlePartitionDesc = new SinglePartitionDesc(false, "p2",
        PartitionKeyDesc.createIn(inValues), null);
        singlePartitionDesc.analyze(2, null);
        partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);

        Assert.assertEquals("((NULL, NULL))", ((ListPartitionItem) partitionItem).toSql());

        inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(PartitionValue.MAX_VALUE, new PartitionValue("", true)));
        singlePartitionDesc = new SinglePartitionDesc(false, "p3",
        PartitionKeyDesc.createIn(inValues), null);
        singlePartitionDesc.analyze(2, null);
        partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);

        Assert.assertEquals("((MAXVALUE, NULL))", ((ListPartitionItem) partitionItem).toSql());

        inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(PartitionValue.MAX_VALUE, PartitionValue.MAX_VALUE));
        singlePartitionDesc = new SinglePartitionDesc(false, "p4",
        PartitionKeyDesc.createIn(inValues), null);
        singlePartitionDesc.analyze(2, null);
        partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);

        Assert.assertEquals("((MAXVALUE, MAXVALUE))", ((ListPartitionItem) partitionItem).toSql());

        inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("", true), new PartitionValue("", true)));
        inValues.add(Lists.newArrayList(PartitionValue.MAX_VALUE, new PartitionValue("", true)));
        inValues.add(Lists.newArrayList(new PartitionValue("", true), PartitionValue.MAX_VALUE));
        singlePartitionDesc = new SinglePartitionDesc(false, "p5",
        PartitionKeyDesc.createIn(inValues), null);
        singlePartitionDesc.analyze(2, null);
        partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);

        Assert.assertEquals("((NULL, NULL),(MAXVALUE, NULL),(NULL, MAXVALUE))", ((ListPartitionItem) partitionItem).toSql());
    }
}
