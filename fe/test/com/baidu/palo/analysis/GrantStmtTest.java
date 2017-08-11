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

import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.InternalException;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class GrantStmtTest {
    private Analyzer analyzer;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    @Test
    public void testNormal() throws AnalysisException, InternalException {
        GrantStmt stmt;

        List<AccessPrivilege> privileges = Lists.newArrayList(AccessPrivilege.ALL);
        stmt = new GrantStmt("testUser", "testDb", privileges);
        stmt.analyze(analyzer);
        Assert.assertEquals("GRANT ALL ON testCluster:testDb TO 'testCluster:testUser'", stmt.toString());
        Assert.assertEquals("testCluster:testUser", stmt.getUser());
        Assert.assertEquals("testCluster:testDb", stmt.getDb());
        Assert.assertEquals(AccessPrivilege.ALL, stmt.getPrivilege());

        privileges = Lists.newArrayList(AccessPrivilege.READ_ONLY, AccessPrivilege.ALL);
        stmt = new GrantStmt("testUser", "testDb", privileges);
        stmt.analyze(analyzer);
        Assert.assertEquals("GRANT READ_ONLY, ALL ON testCluster:testDb TO 'testCluster:testUser'", stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoPriv() throws AnalysisException, InternalException {
        GrantStmt stmt;

        List<AccessPrivilege> privileges = Lists.newArrayList(AccessPrivilege.ALL);
        stmt = new GrantStmt("testUser", "testDb", privileges);
        stmt.analyze(AccessTestUtil.fetchBlockAnalyzer());
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testUserFail() throws AnalysisException, InternalException {
        GrantStmt stmt;

        List<AccessPrivilege> privileges = Lists.newArrayList(AccessPrivilege.ALL);
        stmt = new GrantStmt("", "testDb", privileges);
        stmt.analyze(analyzer);
        Assert.fail("No exeception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testDbFail() throws AnalysisException, InternalException {
        GrantStmt stmt;

        List<AccessPrivilege> privileges = Lists.newArrayList(AccessPrivilege.ALL);
        stmt = new GrantStmt("testUser", "", privileges);
        stmt.analyze(analyzer);
        Assert.fail("No exeception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testPrivFail() throws AnalysisException, InternalException {
        GrantStmt stmt;

        stmt = new GrantStmt("testUser", "testDb", null);
        stmt.analyze(analyzer);
        Assert.fail("No exeception throws.");
    }
}