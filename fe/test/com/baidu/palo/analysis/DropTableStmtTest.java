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

package com.baidu.palo.analysis;

import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.InternalException;

import org.junit.Assert;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class DropTableStmtTest {
    private TableName tbl;
    private TableName noDbTbl;
    private Analyzer analyzer;
    private Analyzer noDbAnalyzer;

    @Before
    public void setUp() {
        tbl = new TableName("db1", "table1");
        noDbTbl = new TableName("", "table1");
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);

        noDbAnalyzer = EasyMock.createMock(Analyzer.class);
        EasyMock.expect(noDbAnalyzer.getDefaultDb()).andReturn("").anyTimes();
        EasyMock.expect(noDbAnalyzer.getClusterName()).andReturn("testCluster").anyTimes();
        EasyMock.replay(noDbAnalyzer);
    }

    @Test
    public void testNormal() throws InternalException, AnalysisException {
        DropTableStmt stmt = new DropTableStmt(false, tbl);
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:db1", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertEquals("DROP TABLE `testCluster:db1`.`table1`", stmt.toString());
    }

    @Test
    public void testDefaultNormal() throws InternalException, AnalysisException {
        DropTableStmt stmt = new DropTableStmt(false, noDbTbl);
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:testDb", stmt.getDbName());
        Assert.assertEquals("table1", stmt.getTableName());
        Assert.assertEquals("DROP TABLE `testCluster:testDb`.`table1`", stmt.toSql());
    }

    @Test(expected = AnalysisException.class)
    public void testNoPriv() throws InternalException, AnalysisException {
        DropTableStmt stmt = new DropTableStmt(false, tbl);
        stmt.analyze(AccessTestUtil.fetchBlockAnalyzer());
        Assert.fail("No exception throws");
    }

    @Test(expected = AnalysisException.class)
    public void testNoDbFail() throws InternalException, AnalysisException {
        DropTableStmt stmt = new DropTableStmt(false, noDbTbl);
        stmt.analyze(noDbAnalyzer);
        Assert.fail("No Exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testNoTableFail() throws InternalException, AnalysisException {
        DropTableStmt stmt = new DropTableStmt(false, new TableName("db1", ""));
        stmt.analyze(noDbAnalyzer);
        Assert.fail("No Exception throws.");
    }

}