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
import com.baidu.palo.mysql.privilege.PaloAuth;
import com.baidu.palo.mysql.privilege.PrivPredicate;
import com.baidu.palo.qe.ConnectContext;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.internal.startup.Startup;

public class AlterTableStmtTest {
    private Analyzer analyzer;

    @Mocked
    private PaloAuth auth;

    static {
        Startup.initializeIfPossible();
    }

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);

        new NonStrictExpectations() {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                result = true;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                result = true;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                result = true;
            }
        };
    }

    @Test
    public void testNormal() throws AnalysisException, InternalException {
        List<AlterClause> ops = Lists.newArrayList();
        ops.add(new DropColumnClause("col1", "", null));
        ops.add(new DropColumnClause("col2", "", null));
        AlterTableStmt stmt = new AlterTableStmt(new TableName("testDb", "testTbl"), ops);
        stmt.analyze(analyzer);
        Assert.assertEquals("ALTER TABLE `testCluster:testDb`.`testTbl` DROP COLUMN `col1`, \nDROP COLUMN `col2`",
                stmt.toSql());
        Assert.assertEquals("testCluster:testDb", stmt.getTbl().getDb());
        Assert.assertEquals(2, stmt.getOps().size());
    }

    @Test
    public void testNoPriv() throws AnalysisException, InternalException {
        List<AlterClause> ops = Lists.newArrayList();
        ops.add(new DropColumnClause("col1", "", null));
        AlterTableStmt stmt = new AlterTableStmt(new TableName("testDb", "testTbl"), ops);
        stmt.analyze(AccessTestUtil.fetchBlockAnalyzer());
    }

    @Test(expected = AnalysisException.class)
    public void testNoTable() throws AnalysisException, InternalException {
        List<AlterClause> ops = Lists.newArrayList();
        ops.add(new DropColumnClause("col1", "", null));
        AlterTableStmt stmt = new AlterTableStmt(null, ops);
        stmt.analyze(analyzer);

        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testNoClause() throws AnalysisException, InternalException {
        List<AlterClause> ops = Lists.newArrayList();
        AlterTableStmt stmt = new AlterTableStmt(new TableName("testDb", "testTbl"), ops);
        stmt.analyze(analyzer);

        Assert.fail("No exception throws.");
    }
}