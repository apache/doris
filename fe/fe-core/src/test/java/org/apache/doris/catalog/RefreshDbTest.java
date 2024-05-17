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

package org.apache.doris.catalog;

import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.RefreshDbStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.test.TestExternalTable;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.DdlExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class RefreshDbTest extends TestWithFeService {
    private static Env env;
    private ConnectContext rootCtx;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        rootCtx = createDefaultCtx();
        env = Env.getCurrentEnv();
        // 1. create test catalog
        CreateCatalogStmt testCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt("create catalog test1 properties(\n"
                + "    \"type\" = \"test\",\n"
                + "    \"catalog_provider.class\" "
                + "= \"org.apache.doris.catalog.RefreshTableTest$RefreshTableProvider\"\n"
                + ");",
                rootCtx);
        env.getCatalogMgr().createCatalog(testCatalog);
    }

    @Override
    protected void runAfterAll() throws Exception {
        super.runAfterAll();
        rootCtx.setThreadLocalInfo();
        DropCatalogStmt stmt = (DropCatalogStmt) parseAndAnalyzeStmt("drop catalog test1");
        env.getCatalogMgr().dropCatalog(stmt);
    }

    @Test
    public void testRefreshDatabase() throws Exception {
        CatalogIf test1 = env.getCatalogMgr().getCatalog("test1");
        TestExternalDatabase db1 = (TestExternalDatabase) test1.getDbNullable("db1");
        long l1 = db1.getLastUpdateTime();
        Assertions.assertTrue(l1 == 0);
        TestExternalTable table = db1.getTable("tbl11").get();
        long l2 = db1.getLastUpdateTime();
        Assertions.assertTrue(l2 > l1);
        Assertions.assertFalse(table.isObjectCreated());
        table.makeSureInitialized();
        Assertions.assertTrue(table.isObjectCreated());
        RefreshDbStmt refreshDbStmt = new RefreshDbStmt("test1", "db1", null);
        try {
            DdlExecutor.execute(Env.getCurrentEnv(), refreshDbStmt);
        } catch (Exception e) {
            // Do nothing
        }
        long l3 = db1.getLastUpdateTime();
        Assertions.assertTrue(l3 == l2);
        Assertions.assertFalse(table.isObjectCreated());
        test1.getDbNullable("db1").getTables();
        Assertions.assertFalse(table.isObjectCreated());
        try {
            DdlExecutor.execute(Env.getCurrentEnv(), refreshDbStmt);
        } catch (Exception e) {
            // Do nothing
        }
        Assertions.assertFalse(((ExternalDatabase) test1.getDbNullable("db1")).isInitialized());
        table.makeSureInitialized();
        long l4 = db1.getLastUpdateTime();
        Assertions.assertTrue(l4 > l3);
        Assertions.assertTrue(((ExternalDatabase) test1.getDbNullable("db1")).isInitialized());
        try {
            DdlExecutor.execute(Env.getCurrentEnv(), refreshDbStmt);
        } catch (Exception e) {
            // Do nothing
        }
    }

    @Test
    public void testRefreshPriv() throws Exception {
        Auth auth = Env.getCurrentEnv().getAuth();
        // create user1
        auth.createUser((CreateUserStmt) parseAndAnalyzeStmt(
                "create user 'user1'@'%' identified by 'pwd1';", rootCtx));
        // grant only create_priv to user1 on test1.db1.tbl11
        GrantStmt grantStmt = (GrantStmt) parseAndAnalyzeStmt(
                "grant create_priv on test1.db1.* to 'user1'@'%';", rootCtx);
        auth.grant(grantStmt);

        // mock login user1
        UserIdentity user1 = new UserIdentity("user1", "%");
        user1.analyze();
        ConnectContext user1Ctx = createCtx(user1, "127.0.0.1");
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Access denied for user 'user1' to database 'db1'",
                () -> parseAndAnalyzeStmt("refresh database test1.db1", user1Ctx));
        ConnectContext.remove();

        // add drop priv to user1
        rootCtx.setThreadLocalInfo();
        grantStmt = (GrantStmt) parseAndAnalyzeStmt(
                "grant drop_priv on test1.db1.* to 'user1'@'%';", rootCtx);
        auth.grant(grantStmt);
        ConnectContext.remove();

        // user1 can do refresh table
        user1Ctx.setThreadLocalInfo();
        ExceptionChecker.expectThrowsNoException(
                () -> parseAndAnalyzeStmt("refresh database test1.db1", user1Ctx));
    }

    public static class RefreshTableProvider implements TestExternalCatalog.TestCatalogProvider {
        public static final Map<String, Map<String, List<Column>>> MOCKED_META;

        static {
            MOCKED_META = Maps.newHashMap();
            Map<String, List<Column>> tblSchemaMap1 = Maps.newHashMap();
            // db1
            tblSchemaMap1.put("tbl11", Lists.newArrayList(
                new Column("a11", PrimitiveType.BIGINT),
                new Column("a12", PrimitiveType.STRING),
                new Column("a13", PrimitiveType.FLOAT)));
            tblSchemaMap1.put("tbl12", Lists.newArrayList(
                new Column("b21", PrimitiveType.BIGINT),
                new Column("b22", PrimitiveType.STRING),
                new Column("b23", PrimitiveType.FLOAT)));
            MOCKED_META.put("db1", tblSchemaMap1);
            // db2
            Map<String, List<Column>> tblSchemaMap2 = Maps.newHashMap();
            tblSchemaMap2.put("tbl21", Lists.newArrayList(
                new Column("c11", PrimitiveType.BIGINT),
                new Column("c12", PrimitiveType.STRING),
                new Column("c13", PrimitiveType.FLOAT)));
            MOCKED_META.put("db2", tblSchemaMap2);
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return MOCKED_META;
        }
    }
}
