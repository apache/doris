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

package org.apache.doris.statistics.util;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.DdlException;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;


public class InternalQueryResultTest {
    private InternalQueryResult queryResult;
    private InternalQueryResult.ResultRow resultRow;

    @Before
    public void setUp() throws Exception {
        List<String> columns = Arrays.asList("c1", "c2", "c3", "c4", "c5");
        List<PrimitiveType> types = Arrays.asList(PrimitiveType.STRING,
                PrimitiveType.INT, PrimitiveType.FLOAT,
                PrimitiveType.DOUBLE, PrimitiveType.BIGINT);
        queryResult = new InternalQueryResult();
        List<String> values = Arrays.asList("s1", "1000", "0.1", "0.0001", "1000000");
        resultRow = new ResultRow(columns, types, values);
    }

    @Test
    public void testGetColumnIndex() {
        Assert.assertEquals(0, resultRow.getColumnIndex("c1"));
        Assert.assertEquals(1, resultRow.getColumnIndex("c2"));
        Assert.assertEquals(2, resultRow.getColumnIndex("c3"));
        Assert.assertEquals(3, resultRow.getColumnIndex("c4"));
        Assert.assertEquals(4, resultRow.getColumnIndex("c5"));
    }

    @Test
    public void testGetColumnName() throws Exception {
        Assert.assertEquals("c1", resultRow.getColumnName(0));
        Assert.assertEquals("c2", resultRow.getColumnName(1));
        Assert.assertEquals("c3", resultRow.getColumnName(2));
        Assert.assertEquals("c4", resultRow.getColumnName(3));
        Assert.assertEquals("c5", resultRow.getColumnName(4));
    }

    @Test
    public void testGetColumnTypeWithIndex() {
        try {
            Assert.assertEquals(PrimitiveType.STRING, resultRow.getColumnType(0));
            Assert.assertEquals(PrimitiveType.INT, resultRow.getColumnType(1));
            Assert.assertEquals(PrimitiveType.FLOAT, resultRow.getColumnType(2));
            Assert.assertEquals(PrimitiveType.DOUBLE, resultRow.getColumnType(3));
            Assert.assertEquals(PrimitiveType.BIGINT, resultRow.getColumnType(4));
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetColumnTypeWithName() {
        try {
            Assert.assertEquals(PrimitiveType.STRING, resultRow.getColumnType("c1"));
            Assert.assertEquals(PrimitiveType.INT, resultRow.getColumnType("c2"));
            Assert.assertEquals(PrimitiveType.FLOAT, resultRow.getColumnType("c3"));
            Assert.assertEquals(PrimitiveType.DOUBLE, resultRow.getColumnType("c4"));
            Assert.assertEquals(PrimitiveType.BIGINT, resultRow.getColumnType("c5"));
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetColumnValueWithIndex() throws Exception {
        Assert.assertEquals("s1", resultRow.getColumnValue(0).toString());
        Assert.assertEquals(1000, Integer.parseInt((String) resultRow.getColumnValue(1)));
        Assert.assertEquals(0.1f, Float.parseFloat((String) resultRow.getColumnValue(2)), 0.0001);
        Assert.assertEquals(0.0001, Double.parseDouble((String) resultRow.getColumnValue(3)), 0.0001);
        Assert.assertEquals(1000000, Long.parseLong((String) resultRow.getColumnValue(4)));
    }

    @Test
    public void testGetColumnValueWithName() throws Exception {
        Assert.assertEquals("s1", resultRow.getColumnValue(0).toString());
        Assert.assertEquals(1000, Integer.parseInt((String) resultRow.getColumnValue(1)));
        Assert.assertEquals(0.1f, Float.parseFloat((String) resultRow.getColumnValue(2)), 0.0001);
        Assert.assertEquals(0.0001, Double.parseDouble((String) resultRow.getColumnValue(3)), 0.0001);
        Assert.assertEquals(1000000, Long.parseLong((String) resultRow.getColumnValue(4)));
    }

    @Test
    public void testGetTypeValue() throws Exception {
        Assert.assertEquals("s1", resultRow.getString(0));
        Assert.assertEquals(1000, resultRow.getInt(1));
        Assert.assertEquals(0.1f, resultRow.getFloat(2), 0.0001);
        Assert.assertEquals(0.0001, resultRow.getDouble(3), 0.0001);
        Assert.assertEquals(1000000, resultRow.getLong(4));
    }
}
