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
import org.apache.doris.analysis.RefreshTableStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaTable;
import org.apache.doris.datasource.infoschema.ExternalMysqlDatabase;
import org.apache.doris.datasource.infoschema.ExternalMysqlTable;
import org.apache.doris.datasource.test.TestExternalCatalog;
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

public class RefreshTableTest extends TestWithFeService {
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
    public void testRefreshTable() throws Exception {
        CatalogIf test1 = env.getCatalogMgr().getCatalog("test1");
        TestExternalTable table = (TestExternalTable) test1.getDbNullable("db1").getTable("tbl11").get();
        Assertions.assertFalse(table.isObjectCreated());
        long l1 = table.getSchemaUpdateTime();
        Assertions.assertTrue(l1 == 0);
        table.makeSureInitialized();
        Assertions.assertTrue(table.isObjectCreated());
        long l2 = table.getSchemaUpdateTime();
        Assertions.assertTrue(l2 == l1);
        RefreshTableStmt refreshTableStmt = new RefreshTableStmt(new TableName("test1", "db1", "tbl11"));
        try {
            DdlExecutor.execute(Env.getCurrentEnv(), refreshTableStmt);
        } catch (Exception e) {
            // Do nothing
        }
        Assertions.assertFalse(table.isObjectCreated());
        long l3 = table.getSchemaUpdateTime();
        Assertions.assertTrue(l3 == l2);
        table.getFullSchema();
        // only table.getFullSchema() can change table.lastUpdateTime
        long l4 = table.getSchemaUpdateTime();
        Assertions.assertTrue(l4 > l3);
        // updateTime is equal to schema update time as default
        long l5 = table.getUpdateTime();
        Assertions.assertTrue(l5 == l4);

        // external info schema db
        ExternalInfoSchemaDatabase infoDb = (ExternalInfoSchemaDatabase) test1.getDbNullable(InfoSchemaDb.DATABASE_NAME);
        Assertions.assertNotNull(infoDb);
        for (String tblName : SchemaTable.TABLE_MAP.keySet()) {
            ExternalInfoSchemaTable infoTbl = (ExternalInfoSchemaTable) infoDb.getTableNullable(tblName);
            Assertions.assertNotNull(infoTbl);
            List<Column> schema = infoTbl.getFullSchema();
            Assertions.assertEquals(SchemaTable.TABLE_MAP.get(tblName).getColumns().size(), schema.size());
        }
        // external mysql db
        ExternalMysqlDatabase mysqlDb = (ExternalMysqlDatabase) test1.getDbNullable(MysqlDb.DATABASE_NAME);
        Assertions.assertNotNull(mysqlDb);
        for (String tblName : MysqlDBTable.TABLE_MAP.keySet()) {
            ExternalMysqlTable mysqlTbl = (ExternalMysqlTable) mysqlDb.getTableNullable(tblName);
            Assertions.assertNotNull(mysqlTbl);
            List<Column> schema = mysqlTbl.getFullSchema();
            Assertions.assertEquals(MysqlDBTable.TABLE_MAP.get(tblName).getColumns().size(), schema.size());
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
                "grant create_priv on test1.db1.tbl11 to 'user1'@'%';", rootCtx);
        auth.grant(grantStmt);

        // mock login user1
        UserIdentity user1 = new UserIdentity("user1", "%");
        user1.analyze();
        ConnectContext user1Ctx = createCtx(user1, "127.0.0.1");
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Access denied",
                () -> parseAndAnalyzeStmt("refresh table test1.db1.tbl11", user1Ctx));
        ConnectContext.remove();

        // add drop priv to user1
        rootCtx.setThreadLocalInfo();
        grantStmt = (GrantStmt) parseAndAnalyzeStmt(
                "grant drop_priv on test1.db1.tbl11 to 'user1'@'%';", rootCtx);
        auth.grant(grantStmt);
        ConnectContext.remove();

        // user1 can do refresh table
        user1Ctx.setThreadLocalInfo();
        ExceptionChecker.expectThrowsNoException(
                () -> parseAndAnalyzeStmt("refresh table test1.db1.tbl11", user1Ctx));
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
