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

package org.apache.doris.analysis;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ColumnType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;

public class ColumnTest {
    private ColumnType intCol;
    private ColumnType stringCol;
    private ColumnType floatCol;

    @Before
    public void setUp() {
        intCol = ColumnType.createType(PrimitiveType.INT);
        stringCol = ColumnType.createChar(10);
        floatCol = ColumnType.createType(PrimitiveType.FLOAT);
    }

    @Test
    public void testNormal() throws AnalysisException {
        Column column = new Column("col", intCol);
        column.analyze(true);

        Assert.assertEquals("`col` int(11) NOT NULL COMMENT \"\"", column.toString());
        Assert.assertEquals("col", column.getName());
        Assert.assertEquals(PrimitiveType.INT, column.getColumnType().getType());
        Assert.assertNull(column.getAggregationType());
        Assert.assertNull(column.getDefaultValue());

        // default
        column = new Column("col", intCol, true, null, "10", "");
        column.analyze(true);
        Assert.assertNull(column.getAggregationType());
        Assert.assertEquals("10", column.getDefaultValue());
        Assert.assertEquals("`col` int(11) NOT NULL DEFAULT \"10\" COMMENT \"\"", column.toSql());

        // agg
        column = new Column("col", floatCol, false, AggregateType.SUM, "10", "");
        column.analyze(true);
        Assert.assertEquals("10", column.getDefaultValue());
        Assert.assertEquals(AggregateType.SUM, column.getAggregationType());
        Assert.assertEquals("`col` float SUM NOT NULL DEFAULT \"10\" COMMENT \"\"", column.toSql());
    }

    @Test(expected = AnalysisException.class)
    public void testFloatKey() throws AnalysisException {
        Column column = new Column("col", floatCol);
        column.setIsKey(true);
        column.analyze(true);
    }

    @Test(expected = AnalysisException.class)
    public void testStrSum() throws AnalysisException {
        Column column = new Column("col", stringCol, false, AggregateType.SUM, null, "");
        column.analyze(true);
    }

}
