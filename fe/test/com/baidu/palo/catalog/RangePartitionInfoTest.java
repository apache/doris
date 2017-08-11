// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.catalog;

import com.baidu.palo.analysis.PartitionKeyDesc;
import com.baidu.palo.analysis.SingleRangePartitionDesc;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.DdlException;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class RangePartitionInfoTest {

    private List<Column> partitionColumns;
    private RangePartitionInfo partitionInfo;

    private List<SingleRangePartitionDesc> singleRangePartitionDescs;

    @Before
    public void setUp() {
        partitionColumns = new LinkedList<Column>();
        singleRangePartitionDescs = new LinkedList<SingleRangePartitionDesc>();
    }

    @Test(expected = DdlException.class)
    public void testTinyInt() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ColumnType(PrimitiveType.TINYINT), true, null, "", "");
        partitionColumns.add(k1);

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1",
                                                                   new PartitionKeyDesc(Lists .newArrayList("-128")),
                                                                   null));


        partitionInfo = new RangePartitionInfo(partitionColumns);
        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L);
        }
    }

    @Test(expected = DdlException.class)
    public void testSmallInt() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ColumnType(PrimitiveType.SMALLINT), true, null, "", "");
        partitionColumns.add(k1);

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1",
                                                                   new PartitionKeyDesc(Lists.newArrayList("-32768")),
                                                                   null));

        partitionInfo = new RangePartitionInfo(partitionColumns);
        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L);
        }
    }

    @Test(expected = DdlException.class)
    public void testInt() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ColumnType(PrimitiveType.INT), true, null, "", "");
        partitionColumns.add(k1);

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1",
                                                                   new PartitionKeyDesc(Lists
                                                                           .newArrayList("-2147483648")),
                                                                   null));

        partitionInfo = new RangePartitionInfo(partitionColumns);
        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L);
        }
    }

    @Test(expected = DdlException.class)
    public void testBigInt() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ColumnType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", new PartitionKeyDesc(Lists
                .newArrayList("-9223372036854775808")), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p2", new PartitionKeyDesc(Lists
                .newArrayList("-9223372036854775806")), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p3", new PartitionKeyDesc(Lists
                .newArrayList("0")), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p4", new PartitionKeyDesc(Lists
                .newArrayList("9223372036854775806")), null));

        partitionInfo = new RangePartitionInfo(partitionColumns);

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L);
        }
    }

    @Test
    public void testBigIntNormal() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ColumnType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", new PartitionKeyDesc(Lists
                .newArrayList("-9223372036854775806")), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p2", new PartitionKeyDesc(Lists
                .newArrayList("-9223372036854775805")), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p3", new PartitionKeyDesc(Lists
                .newArrayList("0")), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p4", new PartitionKeyDesc(Lists
                .newArrayList("9223372036854775806")), null));

        partitionInfo = new RangePartitionInfo(partitionColumns);

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L);
        }
    }

}
