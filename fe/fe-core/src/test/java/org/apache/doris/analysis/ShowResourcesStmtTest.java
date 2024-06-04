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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.system.SystemInfoService;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowResourcesStmtTest {
    private Analyzer analyzer;
    private Env env;

    private SystemInfoService systemInfoService;

    FakeEnv fakeEnv;

    @Before
    public void setUp() {
        fakeEnv = new FakeEnv();

        systemInfoService = AccessTestUtil.fetchSystemInfoService();
        FakeEnv.setSystemInfo(systemInfoService);

        env = AccessTestUtil.fetchAdminCatalog();
        FakeEnv.setEnv(env);

        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        ShowResourcesStmt stmt = new ShowResourcesStmt(null, null, null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW RESOURCES", stmt.toString());
    }

    @Test
    public void testWhere() throws UserException, AnalysisException {
        ShowResourcesStmt stmt = new ShowResourcesStmt(null, null, null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW RESOURCES", stmt.toString());

        SlotRef slotRef = new SlotRef(null, "name");
        StringLiteral stringLiteral = new StringLiteral("abc");
        BinaryPredicate binaryPredicate = new BinaryPredicate(Operator.EQ, slotRef, stringLiteral);
        stmt = new ShowResourcesStmt(null, binaryPredicate, null, new LimitElement(10));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW RESOURCES WHERE (`name` = \'abc\') LIMIT 10", stmt.toString());

        LikePredicate likePredicate = new LikePredicate(org.apache.doris.analysis.LikePredicate.Operator.LIKE,
                slotRef, stringLiteral);
        stmt = new ShowResourcesStmt(null, likePredicate, null, new LimitElement(10));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW RESOURCES WHERE `name` LIKE \'abc\' LIMIT 10", stmt.toString());

        stmt = new ShowResourcesStmt("m%", likePredicate, null, new LimitElement(10));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW RESOURCES LIKE \'m%\' LIMIT 10", stmt.toString());
    }
}
