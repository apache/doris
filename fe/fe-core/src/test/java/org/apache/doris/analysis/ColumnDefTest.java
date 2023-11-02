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

import org.apache.doris.analysis.ColumnDef.DefaultValue;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ColumnDefTest {
    private TypeDef intCol;
    private TypeDef stringCol;
    private TypeDef floatCol;
    private TypeDef booleanCol;
    private ConnectContext ctx;

    @Before
    public void setUp() {
        intCol = new TypeDef(ScalarType.createType(PrimitiveType.INT));
        stringCol = new TypeDef(ScalarType.createChar(10));
        floatCol = new TypeDef(ScalarType.createType(PrimitiveType.FLOAT));
        booleanCol = new TypeDef(ScalarType.createType(PrimitiveType.BOOLEAN));

        ctx = new ConnectContext();
        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return ctx;
            }
        };
    }

    @Test
    public void testNormal() throws AnalysisException {
        ColumnDef column = new ColumnDef("col", intCol);
        column.analyze(true);

        Assert.assertEquals("`col` INT NOT NULL COMMENT \"\"", column.toString());
        Assert.assertEquals("col", column.getName());
        Assert.assertEquals(PrimitiveType.INT, column.getType().getPrimitiveType());
        Assert.assertNull(column.getAggregateType());
        Assert.assertNull(column.getDefaultValue());

        // default
        column = new ColumnDef("col", intCol, true, null, false, new DefaultValue(true, "10"), "");
        column.analyze(true);
        Assert.assertNull(column.getAggregateType());
        Assert.assertEquals("10", column.getDefaultValue());
        Assert.assertEquals("`col` INT NOT NULL DEFAULT \"10\" COMMENT \"\"", column.toSql());

        // agg
        column = new ColumnDef("col", floatCol, false, AggregateType.SUM, false, new DefaultValue(true, "10"), "");
        column.analyze(true);
        Assert.assertEquals("10", column.getDefaultValue());
        Assert.assertEquals(AggregateType.SUM, column.getAggregateType());
        Assert.assertEquals("`col` FLOAT SUM NOT NULL DEFAULT \"10\" COMMENT \"\"", column.toSql());
    }

    @Test
    public void testReplaceIfNotNull() throws AnalysisException {
        { // CHECKSTYLE IGNORE THIS LINE
            // not allow null
            ColumnDef column = new ColumnDef("col", intCol, false, AggregateType.REPLACE_IF_NOT_NULL, false, DefaultValue.NOT_SET, "");
            column.analyze(true);
            Assert.assertEquals(AggregateType.REPLACE_IF_NOT_NULL, column.getAggregateType());
            Assert.assertEquals("`col` INT REPLACE_IF_NOT_NULL NULL DEFAULT \"null\" COMMENT \"\"", column.toSql());
        } // CHECKSTYLE IGNORE THIS LINE
        { // CHECKSTYLE IGNORE THIS LINE
            // not allow null
            ColumnDef column = new ColumnDef("col", intCol, false, AggregateType.REPLACE_IF_NOT_NULL, false, new DefaultValue(true, "10"), "");
            column.analyze(true);
            Assert.assertEquals(AggregateType.REPLACE_IF_NOT_NULL, column.getAggregateType());
            Assert.assertEquals("`col` INT REPLACE_IF_NOT_NULL NULL DEFAULT \"10\" COMMENT \"\"", column.toSql());
        } // CHECKSTYLE IGNORE THIS LINE
    }

    @Test(expected = AnalysisException.class)
    public void testFloatKey() throws AnalysisException {
        ColumnDef column = new ColumnDef("col", floatCol);
        column.setIsKey(true);
        column.analyze(true);
    }

    @Test(expected = AnalysisException.class)
    public void testStrSum() throws AnalysisException {
        ColumnDef column = new ColumnDef("col", stringCol, false, AggregateType.SUM, true, DefaultValue.NOT_SET, "");
        column.analyze(true);
    }

    @Test
    public void testBooleanDefaultValue() throws AnalysisException {
        ColumnDef column1 = new ColumnDef("col", booleanCol, true, null, true, new DefaultValue(true, "1"), "");
        column1.analyze(true);
        Assert.assertEquals("1", column1.getDefaultValue());

        ColumnDef column2 = new ColumnDef("col", booleanCol, true, null, true, new DefaultValue(true, "true"), "");
        column2.analyze(true);
        Assert.assertEquals("true", column2.getDefaultValue());

        ColumnDef column3 = new ColumnDef("col", booleanCol, true, null, true, new DefaultValue(true, "10"), "");
        try {
            column3.analyze(true);
        } catch (AnalysisException e) {
            Assert.assertEquals("errCode = 2, detailMessage = Invalid BOOLEAN literal: 10", e.getMessage());
        }
    }

    @Test
    public void testArray() throws AnalysisException {
        TypeDef typeDef = new TypeDef(new ArrayType(Type.INT));
        ColumnDef columnDef = new ColumnDef("array", typeDef, false, null, true, DefaultValue.NOT_SET, "");
        Column column = columnDef.toColumn();
        Assert.assertEquals(1, column.getChildren().size());
        Column childColumn = column.getChildren().get(0);
        Assert.assertEquals("item", childColumn.getName());
        Assert.assertEquals(Type.INT, childColumn.getType());
        Assert.assertTrue(childColumn.isAllowNull());
    }
}
