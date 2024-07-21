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

import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class ShowDataStmtTest {

    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private Analyzer analyzer;
    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;
    @Mocked
    private ConnectContext ctx;
    @Mocked
    private TabletInvertedIndex invertedIndex;

    private Database db;

    @Before
    public void setUp() throws UserException {
        accessManager = new AccessControllerManager(new Auth());
        new Expectations() {
            {
                Env.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;
            }
        };

        db = CatalogMocker.mockDb();
        catalog = Deencapsulation.newInstance(InternalCatalog.class);

        new Expectations() {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                Env.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDbOrAnalysisException(anyString);
                minTimes = 0;
                result = db;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                ctx.getQualifiedUser();
                minTimes = 0;
                result = "root";

                ctx.getRemoteIP();
                minTimes = 0;
                result = "192.168.1.1";
            }
        };

        new Expectations() {
            {
                accessManager.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                accessManager.checkDbPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                accessManager.checkTblPriv((ConnectContext) any, anyString, anyString, anyString,
                        (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };

        AccessTestUtil.fetchAdminAccess();
    }

    @Test
    public void testNormal() throws AnalysisException, UserException {
        ShowDataStmt stmt = new ShowDataStmt(null, null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW DATA", stmt.toString());
        Assert.assertEquals(4, stmt.getMetaData().getColumnCount());
        Assert.assertEquals(false, stmt.hasTable());

        SlotRef slotRefOne = new SlotRef(null, "ReplicaCount");
        OrderByElement orderByElementOne = new OrderByElement(slotRefOne, false, false);
        SlotRef slotRefTwo = new SlotRef(null, "Size");
        OrderByElement orderByElementTwo = new OrderByElement(slotRefTwo, false, false);

        stmt = new ShowDataStmt(new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, "testDb", "test_tbl"),
                Arrays.asList(orderByElementOne, orderByElementTwo), null);
        stmt.analyze(analyzer);
        Assert.assertEquals(
                "SHOW DATA FROM `testDb`.`test_tbl` ORDER BY `ReplicaCount` DESC, `Size` DESC",
                stmt.toString());
        Assert.assertEquals(6, stmt.getMetaData().getColumnCount());
        Assert.assertEquals(true, stmt.hasTable());

        stmt = new ShowDataStmt(null, Arrays.asList(orderByElementOne, orderByElementTwo), null);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW DATA ORDER BY `ReplicaCount` DESC, `Size` DESC",
                stmt.toString());
    }
}
