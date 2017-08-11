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
import org.junit.Before;
import org.junit.Test;

public class CreateUserStmtTest {
    private Analyzer analyzer;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    @Test
    public void testToString() throws InternalException, AnalysisException {
        CreateUserStmt stmt = new CreateUserStmt(new UserDesc("user", "passwd", true));
        stmt.analyze(analyzer);

        Assert.assertEquals("CREATE USER 'testCluster:user' IDENTIFIED BY 'passwd'", stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");

        stmt = new CreateUserStmt(new UserDesc("user", "*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0", false));
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:user", stmt.getUser());

        Assert.assertEquals(
                "CREATE USER 'testCluster:user' IDENTIFIED BY PASSWORD '*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0'",
                stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");

        stmt = new CreateUserStmt(new UserDesc("user", "", false));
        stmt.analyze(analyzer);

        Assert.assertEquals("CREATE USER 'testCluster:user'", stmt.toString());
        Assert.assertEquals(new String(stmt.getPassword()), "");
    }

    @Test(expected = AnalysisException.class)
    public void testNoPriv() throws InternalException, AnalysisException {
        CreateUserStmt stmt = new CreateUserStmt(new UserDesc("user", "passwd", true));
        stmt.analyze(AccessTestUtil.fetchBlockAnalyzer());
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testEmptyUser() throws InternalException, AnalysisException {
        CreateUserStmt stmt = new CreateUserStmt(new UserDesc("", "passwd", true));
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testBadPass() throws InternalException, AnalysisException {
        CreateUserStmt stmt = new CreateUserStmt(new UserDesc("", "passwd", false));
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}