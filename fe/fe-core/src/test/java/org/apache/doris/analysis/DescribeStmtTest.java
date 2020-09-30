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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import mockit.Mock;
import mockit.MockUp;

public class DescribeStmtTest {
    private Analyzer analyzer;
    private Catalog catalog;
    private ConnectContext ctx;

    @Before
    public void setUp() {
        ctx = new ConnectContext(null);
        ctx.setQualifiedUser("root");
        ctx.setRemoteIP("192.168.1.1");

        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        catalog = AccessTestUtil.fetchAdminCatalog();

        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return ctx;
            }
        };

        new MockUp<Catalog>() {
            @Mock
            Catalog getCurrentCatalog() {
                return catalog;
            }
        };
    }

    @Ignore
    @Test
    public void testNormal() throws UserException {
        DescribeStmt stmt = new DescribeStmt(new TableName("", "testTbl"), false);
        stmt.analyze(analyzer);
        Assert.assertEquals("DESCRIBE `testCluster:testDb.testTbl`", stmt.toString());
        Assert.assertEquals(6, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("testCluster:testDb", stmt.getDb());
        Assert.assertEquals("testTbl", stmt.getTableName());
    }

    @Test
    public void testAllNormal() throws UserException {
        DescribeStmt stmt = new DescribeStmt(new TableName("", "testTbl"), true);
        stmt.analyze(analyzer);
        Assert.assertEquals("DESCRIBE `testCluster:testDb.testTbl` ALL", stmt.toString());
        Assert.assertEquals(9, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("IndexKeysType", stmt.getMetaData().getColumn(1).getName());
        Assert.assertEquals("testCluster:testDb", stmt.getDb());
        Assert.assertEquals("testTbl", stmt.getTableName());
    }
}
