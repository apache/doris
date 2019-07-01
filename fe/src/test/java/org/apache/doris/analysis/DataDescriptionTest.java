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

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import mockit.Mocked;
import mockit.internal.startup.Startup;

public class DataDescriptionTest {

    @Mocked
    private PaloAuth auth;
    @Mocked
    private ConnectContext ctx;

    static {
        Startup.initializeIfPossible();
    }

    @Before
    public void setUp() {
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void testNormal() throws AnalysisException {
        DataDescription desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                null, null, false, null);
        desc.analyze("testDb");
        Assert.assertEquals("DATA INFILE ('abc.txt') INTO TABLE testTable", desc.toString());

        desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"), null, null, true, null);
        desc.analyze("testDb");
        Assert.assertEquals("DATA INFILE ('abc.txt') NEGATIVE INTO TABLE testTable", desc.toString());

        desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt", "bcd.txt"), null, null, true, null);
        desc.analyze("testDb");
        Assert.assertEquals("DATA INFILE ('abc.txt', 'bcd.txt') NEGATIVE INTO TABLE testTable", desc.toString());

        desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                Lists.newArrayList("col1", "col2"), null, true, null);
        desc.analyze("testDb");
        Assert.assertEquals("DATA INFILE ('abc.txt') NEGATIVE INTO TABLE testTable (col1, col2)", desc.toString());
        Assert.assertEquals("testTable", desc.getTableName());
        Assert.assertEquals("[col1, col2]", desc.getColumnNames().toString());
        Assert.assertEquals("[abc.txt]", desc.getFilePaths().toString());
        Assert.assertTrue(desc.isNegative());
        Assert.assertNull(desc.getColumnSeparator());

        desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt", "bcd.txt"),
                Lists.newArrayList("col1", "col2"), new ColumnSeparator("\t"), true, null);
        desc.analyze("testDb");
        Assert.assertEquals("DATA INFILE ('abc.txt', 'bcd.txt') NEGATIVE INTO TABLE testTable"
                        +  " COLUMNS TERMINATED BY '\t' (col1, col2)",
                desc.toString());

        // hive \x01 column separator
        desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt", "bcd.txt"),
                Lists.newArrayList("col1", "col2"), new ColumnSeparator("\\x01"), true, null);
        desc.analyze("testDb");
        Assert.assertEquals("DATA INFILE ('abc.txt', 'bcd.txt') NEGATIVE INTO TABLE testTable"
                        +  " COLUMNS TERMINATED BY '\\x01' (col1, col2)",
                desc.toString());

        // with partition
        desc = new DataDescription("testTable", Lists.newArrayList("p1", "p2"), Lists.newArrayList("abc.txt"),
                null, null, false, null);
        desc.analyze("testDb");
        Assert.assertEquals("DATA INFILE ('abc.txt') INTO TABLE testTable PARTITION (p1, p2)", desc.toString());
        
        // alignment_timestamp func
        List<Expr> params = Lists.newArrayList();
        params.add(new StringLiteral("day"));
        params.add(new SlotRef(null, "k2"));
        BinaryPredicate predicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"), 
                new FunctionCallExpr("alignment_timestamp", params));
        desc = new DataDescription("testTable", Lists.newArrayList("p1", "p2"), Lists.newArrayList("abc.txt"),
                Lists.newArrayList("k2", "k3"), null, false, Lists.newArrayList((Expr) predicate));
        desc.analyze("testDb");
        String sql = "DATA INFILE ('abc.txt') INTO TABLE testTable PARTITION (p1, p2) (k2, k3)" 
                + " SET (`k1` = alignment_timestamp('day', `k2`))";
        Assert.assertEquals(sql, desc.toString());

        // replace_value func
        params.clear();
        params.add(new StringLiteral("-"));
        params.add(new StringLiteral("10"));
        predicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"),
                new FunctionCallExpr("replace_value", params));
        desc = new DataDescription("testTable", Lists.newArrayList("p1", "p2"), Lists.newArrayList("abc.txt"),
                Lists.newArrayList("k2", "k3"), null, false, Lists.newArrayList((Expr) predicate));
        desc.analyze("testDb");
        sql = "DATA INFILE ('abc.txt') INTO TABLE testTable PARTITION (p1, p2) (k2, k3)"
                + " SET (`k1` = replace_value('-', '10'))";
        Assert.assertEquals(sql, desc.toString());

        // replace_value null
        params.clear();
        params.add(new StringLiteral(""));
        params.add(new NullLiteral());
        predicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"),
                new FunctionCallExpr("replace_value", params));
        desc = new DataDescription("testTable", Lists.newArrayList("p1", "p2"), Lists.newArrayList("abc.txt"),
                Lists.newArrayList("k2", "k3"), null, false, Lists.newArrayList((Expr) predicate));
        desc.analyze("testDb");
        sql = "DATA INFILE ('abc.txt') INTO TABLE testTable PARTITION (p1, p2) (k2, k3)"
                + " SET (`k1` = replace_value('', NULL))";
        Assert.assertEquals(sql, desc.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoTable() throws AnalysisException {
        DataDescription desc = new DataDescription("", null, Lists.newArrayList("abc.txt"),
                null, null, false, null);
        desc.analyze("testDb");
    }

    @Test(expected = AnalysisException.class)
    public void testNoFile() throws AnalysisException {
        DataDescription desc = new DataDescription("testTable", null, null, null, null, false, null);
        desc.analyze("testDb");
    }

    @Test(expected = AnalysisException.class)
    public void testDupCol() throws AnalysisException {
        DataDescription desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                Lists.newArrayList("col1", "col1"), null, false, null);
        desc.analyze("testDb");
    }
}
