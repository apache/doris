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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import mockit.Expectations;
import mockit.Mocked;

import java.util.Arrays;

public class ShowDataStmtTest {

    @Mocked
    private PaloAuth auth;
    @Mocked
    private Analyzer analyzer;
    @Mocked
    private Catalog catalog;
    @Mocked
    private ConnectContext ctx;
    @Mocked
    private TabletInvertedIndex invertedIndex;

    private Database db;

    @Before
    public void setUp() throws UserException {
        auth = new PaloAuth();

        

        new Expectations() {
            {
                Catalog.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;
            }
        };

        db = CatalogMocker.mockDb();

        new Expectations() {
            {
                analyzer.getClusterName();
                minTimes = 0;
                result = SystemInfoService.DEFAULT_CLUSTER;

                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testCluster:testDb";

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                Catalog.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;

                catalog.getAuth();
                minTimes = 0;
                result = auth;

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
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
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
        Assert.assertEquals("SHOW DATA FROM `testCluster:testDb`", stmt.toString());
        Assert.assertEquals(3, stmt.getMetaData().getColumnCount());
        Assert.assertEquals(false, stmt.hasTable());

        SlotRef slotRefOne = new SlotRef(null, "ReplicaCount");
        OrderByElement orderByElementOne = new OrderByElement(slotRefOne, false, false);
        SlotRef slotRefTwo = new SlotRef(null, "Size");
        OrderByElement orderByElementTwo = new OrderByElement(slotRefTwo, false, false);

        stmt = new ShowDataStmt("testDb", "test_tbl", Arrays.asList(orderByElementOne, orderByElementTwo));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW DATA FROM `default_cluster:testDb`.`test_tbl` ORDER BY `ReplicaCount` DESC, `Size` DESC", stmt.toString());
        Assert.assertEquals(5, stmt.getMetaData().getColumnCount());
        Assert.assertEquals(true, stmt.hasTable());

        stmt = new ShowDataStmt(null, null, Arrays.asList(orderByElementOne, orderByElementTwo));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW DATA FROM `testCluster:testDb` ORDER BY `ReplicaCount` DESC, `Size` DESC", stmt.toString());
    }
}
