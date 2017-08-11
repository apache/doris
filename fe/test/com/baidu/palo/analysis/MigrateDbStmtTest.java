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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.InternalException;

public class MigrateDbStmtTest {

    private static Analyzer analyzer;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    @Test
    public void testNormal() throws InternalException, AnalysisException {
        final ClusterName cn1 = new ClusterName("testCluster1", "testDb1");
        final ClusterName cn2 = new ClusterName("testCluster2", "testDb2");
        final MigrateDbStmt stmt = new MigrateDbStmt(cn1, cn2);
        stmt.analyze(analyzer);
        final String sql = "MIGRATE DATABASE " + stmt.getSrcCluster() + "." + stmt.getSrcDb() + " "
                + stmt.getDesCluster() + "." + stmt.getDesDb();
        Assert.assertEquals(sql, stmt.toSql());
    }

    @Test(expected = AnalysisException.class)
    public void testParamError() throws InternalException, AnalysisException {
        final ClusterName cn1 = new ClusterName("testCluster1", "");
        final ClusterName cn2 = new ClusterName("testCluster2", "testDb2");
        final MigrateDbStmt stmt = new MigrateDbStmt(cn1, cn2);
        stmt.analyze(AccessTestUtil.fetchBlockAnalyzer());
        Assert.fail("no exception");
    }

    @Test(expected = AnalysisException.class)
    public void testNoPriv() throws InternalException, AnalysisException {
        final ClusterName cn1 = new ClusterName("testCluster1", "testDb1");
        final ClusterName cn2 = new ClusterName("testCluster2", "testDb2");
        final MigrateDbStmt stmt = new MigrateDbStmt(cn1, cn2);
        stmt.analyze(AccessTestUtil.fetchBlockAnalyzer());
        Assert.fail("no exception");
    }
}
