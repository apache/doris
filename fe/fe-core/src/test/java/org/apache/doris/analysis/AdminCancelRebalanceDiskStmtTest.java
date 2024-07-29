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

import org.apache.doris.catalog.Env;
import org.apache.doris.clone.RebalancerTestUtil;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class AdminCancelRebalanceDiskStmtTest {

    private static Analyzer analyzer;

    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private ConnectContext ctx;

    @Before()
    public void setUp() {
        MockedAuth.mockedAccess(accessManager);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);

        List<Long> beIds = Lists.newArrayList(10001L, 10002L, 10003L, 10004L);
        beIds.forEach(id -> Env.getCurrentSystemInfo().addBackend(RebalancerTestUtil.createBackend(id, 2048, 0)));
    }

    @Test
    public void testParticularBackends() throws AnalysisException {
        List<String> backends = Lists.newArrayList(
                "192.168.0.10003:9051", "192.168.0.10004:9051", "192.168.0.10005:9051", "192.168.0.10006:9051");
        final AdminCancelRebalanceDiskStmt stmt = new AdminCancelRebalanceDiskStmt(backends);
        stmt.analyze(analyzer);
        Assert.assertEquals(2, stmt.getBackends().size());
    }

    @Test
    public void testEmpty() throws AnalysisException {
        List<String> backends = Lists.newArrayList();
        final AdminCancelRebalanceDiskStmt stmt = new AdminCancelRebalanceDiskStmt(backends);
        stmt.analyze(analyzer);
        Assert.assertEquals(0, stmt.getBackends().size());
    }

    @Test
    public void testNull() throws AnalysisException {
        final AdminCancelRebalanceDiskStmt stmt = new AdminCancelRebalanceDiskStmt(null);
        stmt.analyze(analyzer);
        Assert.assertEquals(4, stmt.getBackends().size());
    }

}
