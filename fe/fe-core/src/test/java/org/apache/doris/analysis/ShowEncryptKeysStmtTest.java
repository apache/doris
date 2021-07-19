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

import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FakeCatalog;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ShowEncryptKeysStmtTest {
    @Mocked
    private Analyzer analyzer;
    private Catalog catalog;

    @Mocked
    private PaloAuth auth;
    @Mocked
    private ConnectContext ctx;
    private FakeCatalog fakeCatalog;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() {
        fakeCatalog = new FakeCatalog();
        catalog = AccessTestUtil.fetchAdminCatalog();
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.188.3.1");
        FakeCatalog.setCatalog(catalog);

        new Expectations() {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";

                analyzer.getCatalog();
                minTimes = 0;
                result = catalog;

                analyzer.getClusterName();
                minTimes = 0;
                result = "testCluster";
            }
        };
    }

    @Test
    public void testNormal() throws UserException {
        ShowEncryptKeysStmt stmt = new ShowEncryptKeysStmt(null, "%my%");
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW ENCRYPTKEYS FROM `testDb` LIKE `%my%`", stmt.toString());
    }
}
