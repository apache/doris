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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import mockit.Mocked;

public class MigrateDbStmtTest {

    private static Analyzer analyzer;

    @Mocked
    private PaloAuth auth;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        Config.disable_cluster_feature = false;
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        final ClusterName cn1 = new ClusterName("testCluster1", "testDb1");
        final ClusterName cn2 = new ClusterName("testCluster2", "testDb2");
        final MigrateDbStmt stmt = new MigrateDbStmt(cn1, cn2);
        stmt.analyze(analyzer);
        final String sql = "MIGRATE DATABASE " + stmt.getSrcCluster() + "." + stmt.getSrcDb() + " "
                + stmt.getDestCluster() + "." + stmt.getDestDb();
        Assert.assertEquals(sql, stmt.toSql());
    }

    @Test(expected = AnalysisException.class)
    public void testParamError() throws UserException, AnalysisException {
        final ClusterName cn1 = new ClusterName("testCluster1", "");
        final ClusterName cn2 = new ClusterName("testCluster2", "testDb2");
        final MigrateDbStmt stmt = new MigrateDbStmt(cn1, cn2);
        stmt.analyze(AccessTestUtil.fetchBlockAnalyzer());
        Assert.fail("no exception");
    }
}
