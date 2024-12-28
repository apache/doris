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

package org.apache.doris.datasource;

import org.apache.doris.analysis.AlterCatalogCommentStmt;
import org.apache.doris.analysis.AlterCatalogNameStmt;
import org.apache.doris.analysis.AlterCatalogPropertyStmt;
import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.CreateRoleStmt;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.ShowCatalogStmt;
import org.apache.doris.analysis.ShowCreateCatalogStmt;
import org.apache.doris.analysis.SwitchStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EsResource;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.es.EsExternalCatalog;
import org.apache.doris.datasource.es.EsExternalDatabase;
import org.apache.doris.datasource.es.EsExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheKey;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheValue;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.HivePartitionValues;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.PartitionValueCacheKey;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.planner.ColumnBound;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.PartitionPrunerV2Base.UniqueId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CatalogMgrTest extends TestWithFeService {
    private static final String MY_CATALOG = "my_catalog";
    private static Auth auth;
    private static Env env;
    private static UserIdentity user1;
    private static UserIdentity user2;
    private CatalogMgr mgr;
    private ResourceMgr resourceMgr;
    private ExternalMetaCacheMgr externalMetaCacheMgr;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        mgr = Env.getCurrentEnv().getCatalogMgr();
        resourceMgr = Env.getCurrentEnv().getResourceMgr();
        externalMetaCacheMgr = Env.getCurrentEnv().getExtMetaCacheMgr();
        ConnectContext rootCtx = createDefaultCtx();
        env = Env.getCurrentEnv();
        auth = env.getAuth();

        // grant with no catalog is switched, internal catalog works.
        CreateRoleStmt createRole1 = (CreateRoleStmt) parseAndAnalyzeStmt("create role role1;", rootCtx);
        auth.createRole(createRole1);
        auth.createUser((CreateUserStmt) parseAndAnalyzeStmt(
                "create user 'user1'@'%' identified by 'pwd1' default role 'role1';", rootCtx));
        user1 = new UserIdentity("user1", "%");
        user1.analyze();
        // user1 has the privileges of testc which is granted by ctl.db.tbl format.
        // TODO: 2023/1/20 zdtodo
        //        Assert.assertTrue(auth.getDbPrivTable().hasPrivsOfCatalog(user1, "testc"));

        // create hms catalog
        CreateCatalogStmt hiveCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog hive properties('type' = 'hms', 'hive.metastore.uris' = 'thrift://192.168.0.1:9083');",
                rootCtx);
        env.getCatalogMgr().createCatalog(hiveCatalog);
        // deprecated: create hms catalog by properties
        CreateCatalogStmt hiveCatalog2 = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog hive2 properties('type' = 'hms', 'hive.metastore.uris' = 'thrift://192.168.0.1:9083');",
                rootCtx);
        env.getCatalogMgr().createCatalog(hiveCatalog2);

        CreateCatalogStmt iceBergCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog iceberg properties('type' = 'hms', 'iceberg.hive.metastore.uris' = 'thrift://192.168.0.1:9083');",
                rootCtx);
        env.getCatalogMgr().createCatalog(iceBergCatalog);

        // create es catalog
        CreateCatalogStmt esCatalog = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog es properties('type' = 'es', 'hosts' = 'http://192.168.0.1', 'user' = 'user1');",
                rootCtx);
        env.getCatalogMgr().createCatalog(esCatalog);
        // deprecated: create es catalog by properties
        CreateCatalogStmt esCatalog2 = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog es2 properties('type' = 'es', 'elasticsearch.hosts' = 'http://192.168.0.1',"
                        + " 'elasticsearch.username' = 'user1');",
                rootCtx);
        env.getCatalogMgr().createCatalog(esCatalog2);

        createDbAndTableForCatalog(env.getCatalogMgr().getCatalog("hive"));
        createDbAndTableForCatalog(env.getCatalogMgr().getCatalog("es"));

        // switch to hive.
        SwitchStmt switchHive = (SwitchStmt) parseAndAnalyzeStmt("switch hive;", rootCtx);
        env.changeCatalog(rootCtx, switchHive.getCatalogName());
        CreateRoleStmt createRole2 = (CreateRoleStmt) parseAndAnalyzeStmt("create role role2;", rootCtx);
        auth.createRole(createRole2);
        GrantStmt grantRole2 = (GrantStmt) parseAndAnalyzeStmt("grant grant_priv on tpch.customer to role 'role2';",
                rootCtx);
        auth.grant(grantRole2);
        auth.createUser((CreateUserStmt) parseAndAnalyzeStmt(
                "create user 'user2'@'%' identified by 'pwd2' default role 'role2';", rootCtx));
        user2 = new UserIdentity("user2", "%");
        user2.analyze();
    }

    private void createDbAndTableForCatalog(CatalogIf catalog) {
        List<Column> schema = Lists.newArrayList();
        schema.add(new Column("k1", PrimitiveType.INT));
        if (catalog instanceof HMSExternalCatalog) {
            HMSExternalCatalog hmsCatalog = (HMSExternalCatalog) catalog;
            HMSExternalDatabase db = new HMSExternalDatabase(hmsCatalog, 10000, "hive_db1", "hive_db1");
            HMSExternalTable tbl = new HMSExternalTable(10001, "hive_tbl1", "hive_db1", hmsCatalog, db);
            tbl.setNewFullSchema(schema);
            db.addTableForTest(tbl);
            hmsCatalog.addDatabaseForTest(db);
        } else if (catalog instanceof EsExternalCatalog) {
            EsExternalCatalog esCatalog = (EsExternalCatalog) catalog;
            EsExternalDatabase db = new EsExternalDatabase(esCatalog, 10002, "es_db1", "es_db1");
            EsExternalTable tbl = new EsExternalTable(10003, "es_tbl1", "es_tbl1", esCatalog, db);
            tbl.setNewFullSchema(schema);
            db.addTableForTest(tbl);
            esCatalog.addDatabaseForTest(db);
        }
    }

    @Test
    public void testNormalCase() throws Exception {
        String createCatalogSql = "CREATE CATALOG hms_catalog "
                + "comment 'hms comment'"
                + "properties( \"type\" = \"hms\", \"hive.metastore.uris\"=\"thrift://localhost:9083\" )";
        CreateCatalogStmt createStmt = (CreateCatalogStmt) parseAndAnalyzeStmt(createCatalogSql);
        mgr.createCatalog(createStmt);

        String showCatalogSql = "SHOW CATALOGS";
        ShowCatalogStmt showStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showCatalogSql);
        ShowResultSet showResultSet = mgr.showCatalogs(showStmt);
        Assertions.assertEquals(7, showResultSet.getResultRows().size());

        //test result order
        Assertions.assertEquals("es", showResultSet.getResultRows().get(0).get(1));
        Assertions.assertEquals("internal", showResultSet.getResultRows().get(6).get(1));

        // Can't alter catalog with resource directly
        String alterCltWithResource = "ALTER CATALOG hive SET PROPERTIES"
                + " ('hive.metastore.uris' = 'thrift://192.168.0.2:9084');";
        mgr.alterCatalogProps((AlterCatalogPropertyStmt) parseAndAnalyzeStmt(alterCltWithResource));
        Assertions.assertEquals("thrift://192.168.0.2:9084",
                mgr.getCatalog("hive").getProperties().get("hive.metastore.uris"));

        showCatalogSql = "SHOW CATALOGS LIKE 'hms%'";
        showStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showCatalogSql);
        showResultSet = mgr.showCatalogs(showStmt);
        Assertions.assertEquals(1, showResultSet.getResultRows().size());

        String alterCatalogNameFailSql = "ALTER CATALOG hms_catalog RENAME hive;";
        AlterCatalogNameStmt alterNameFailStmt = (AlterCatalogNameStmt) parseAndAnalyzeStmt(alterCatalogNameFailSql);

        try {
            mgr.alterCatalogName(alterNameFailStmt);
            Assert.fail("Catalog with name hive already exist, rename should be failed");
        } catch (DdlException e) {
            Assert.assertEquals(e.getMessage(),
                    "errCode = 2, detailMessage = Catalog with name hive already exist");
        }

        String alterCatalogNameSql = "ALTER CATALOG hms_catalog RENAME " + MY_CATALOG + ";";
        AlterCatalogNameStmt alterNameStmt = (AlterCatalogNameStmt) parseAndAnalyzeStmt(alterCatalogNameSql);
        mgr.alterCatalogName(alterNameStmt);

        // test modify property
        String alterCatalogProps = "ALTER CATALOG " + MY_CATALOG + " SET PROPERTIES"
                + " (\"type\" = \"hms\", \"hive.metastore.uris\" = \"thrift://172.16.5.9:9083\");";
        AlterCatalogPropertyStmt alterPropStmt = (AlterCatalogPropertyStmt) parseAndAnalyzeStmt(alterCatalogProps);
        mgr.alterCatalogProps(alterPropStmt);

        CatalogIf catalog = env.getCatalogMgr().getCatalog(MY_CATALOG);
        // type, hive.metastore.uris and create_time
        Assert.assertEquals(5, catalog.getProperties().size());
        Assert.assertEquals("thrift://172.16.5.9:9083", catalog.getProperties().get("hive.metastore.uris"));

        // test add property
        Map<String, String> alterProps2 = Maps.newHashMap();
        alterProps2.put("dfs.nameservices", "service1");
        alterProps2.put("dfs.ha.namenodes.service1", "nn1,nn2");
        alterProps2.put("dfs.namenode.rpc-address.service1.nn1", "nn1_host:rpc_port");
        alterProps2.put("dfs.namenode.rpc-address.service1.nn2", "nn2_host:rpc_port");
        alterProps2.put("dfs.client.failover.proxy.provider.service1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        AlterCatalogPropertyStmt alterStmt = new AlterCatalogPropertyStmt(MY_CATALOG, alterProps2);
        mgr.alterCatalogProps(alterStmt);
        catalog = env.getCatalogMgr().getCatalog(MY_CATALOG);
        Assert.assertEquals(10, catalog.getProperties().size());
        Assert.assertEquals("service1", catalog.getProperties().get("dfs.nameservices"));

        String showDetailCatalog = "SHOW CATALOG my_catalog";
        ShowCatalogStmt showDetailStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showDetailCatalog);
        showResultSet = mgr.showCatalogs(showDetailStmt);

        Assert.assertEquals(10, showResultSet.getResultRows().size());
        for (List<String> row : showResultSet.getResultRows()) {
            Assertions.assertEquals(2, row.size());
            if (row.get(0).equalsIgnoreCase("type")) {
                Assertions.assertEquals("hms", row.get(1));
            } else if (row.get(0).equalsIgnoreCase("dfs.ha.namenodes.service1")) {
                Assertions.assertEquals("nn1,nn2", row.get(1));
            }
        }

        String showCreateCatalog = "SHOW CREATE CATALOG my_catalog";
        ShowCreateCatalogStmt showCreateStmt = (ShowCreateCatalogStmt) parseAndAnalyzeStmt(showCreateCatalog);
        showResultSet = mgr.showCreateCatalog(showCreateStmt);

        Assert.assertEquals(1, showResultSet.getResultRows().size());
        List<String> result = showResultSet.getResultRows().get(0);
        Assertions.assertEquals("my_catalog", result.get(0));
        Assertions.assertTrue(result.get(1).startsWith("\nCREATE CATALOG `my_catalog`\nCOMMENT \"hms comment\"\n PROPERTIES ("));
        Assertions.assertTrue(!result.get(1).contains(ExternalCatalog.CREATE_TIME));
        Assertions.assertTrue(!result.get(1).contains(ExternalCatalog.USE_META_CACHE));

        testCatalogMgrPersist();

        String dropCatalogSql = "DROP CATALOG " + MY_CATALOG;
        DropCatalogStmt dropCatalogStmt = (DropCatalogStmt) parseAndAnalyzeStmt(dropCatalogSql);
        mgr.dropCatalog(dropCatalogStmt);

        showCatalogSql = "SHOW CATALOGS";
        showStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showCatalogSql);
        showResultSet = mgr.showCatalogs(showStmt);
        Assertions.assertEquals(6, showResultSet.getResultRows().size());

        //test alter fileCache
        testAlterFileCache();
    }

    private void testCatalogMgrPersist() throws Exception {
        File file = new File("./CatalogMgrTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        mgr.write(dos);
        dos.flush();
        dos.close();

        CatalogIf internalCatalog = mgr.getCatalog(InternalCatalog.INTERNAL_CATALOG_ID);
        CatalogIf internalCatalog2 = mgr.getInternalCatalog();
        Assert.assertTrue(internalCatalog == internalCatalog2);
        CatalogIf myCatalog = mgr.getCatalog(MY_CATALOG);
        Assert.assertNotNull(myCatalog);

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        CatalogMgr mgr2 = CatalogMgr.read(dis);

        Assert.assertEquals(7, mgr2.listCatalogs().size());
        Assert.assertEquals(myCatalog.getId(), mgr2.getCatalog(MY_CATALOG).getId());
        Assert.assertEquals(0, mgr2.getInternalCatalog().getId());
        Assert.assertEquals(0, mgr2.getCatalog(InternalCatalog.INTERNAL_CATALOG_ID).getId());
        Assert.assertEquals(0, mgr2.getCatalog(InternalCatalog.INTERNAL_CATALOG_NAME).getId());

        EsExternalCatalog esExternalCatalog = (EsExternalCatalog) mgr2.getCatalog("es");
        Assert.assertNotNull(esExternalCatalog);
        Map<String, String> properties = esExternalCatalog.getCatalogProperty().getProperties();
        Assert.assertEquals("user1", properties.get(EsResource.USER));
        Assert.assertEquals("http://192.168.0.1", properties.get(EsResource.HOSTS));
        Assert.assertEquals("user1", esExternalCatalog.getUsername());
        Assert.assertEquals("http://192.168.0.1", esExternalCatalog.getNodes()[0]);

        CatalogIf hms = mgr2.getCatalog(MY_CATALOG);
        properties = hms.getProperties();
        Assert.assertEquals(10, properties.size());
        Assert.assertEquals("hms", properties.get("type"));
        Assert.assertEquals("thrift://172.16.5.9:9083", properties.get("hive.metastore.uris"));

        // 3. delete files
        dis.close();
        file.delete();
    }

    @Test
    public void testSwitchCommand() throws Exception {
        // mock the login of user1
        ConnectContext user1Ctx = createCtx(user1, "127.0.0.1");
        // user1 can switch to internal catalog
        parseAndAnalyzeStmt("switch " + InternalCatalog.INTERNAL_CATALOG_NAME + ";", user1Ctx);
        Assert.assertEquals(InternalCatalog.INTERNAL_CATALOG_NAME, user1Ctx.getDefaultCatalog());
        // user1 can't switch to hive
        try {
            parseAndAnalyzeStmt("switch hive;", user1Ctx);
            Assert.fail("user1 switch to hive with no privilege.");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(),
                    "errCode = 2, detailMessage = Access denied for user 'user1' to catalog 'hive'");
        }
        Assert.assertEquals(InternalCatalog.INTERNAL_CATALOG_NAME, user1Ctx.getDefaultCatalog());

        // mock the login of user2
        ConnectContext user2Ctx = createCtx(user2, "127.0.0.1");
        // user2 can switch to internal catalog
        parseAndAnalyzeStmt("switch " + InternalCatalog.INTERNAL_CATALOG_NAME + ";", user2Ctx);
        Assert.assertEquals(InternalCatalog.INTERNAL_CATALOG_NAME, user2Ctx.getDefaultCatalog());

        String showCatalogSql = "SHOW CATALOGS";
        ShowCatalogStmt showStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showCatalogSql);
        ShowResultSet showResultSet = mgr.showCatalogs(showStmt, user2Ctx.getCurrentCatalog().getName());
        Assertions.assertEquals("Yes", showResultSet.getResultRows().get(1).get(3));
        Assertions.assertEquals("No", showResultSet.getResultRows().get(0).get(3));

        // user2 can switch to hive
        SwitchStmt switchHive = (SwitchStmt) parseAndAnalyzeStmt("switch hive;", user2Ctx);
        env.changeCatalog(user2Ctx, switchHive.getCatalogName());
        Assert.assertEquals(user2Ctx.getDefaultCatalog(), "hive");

        showCatalogSql = "SHOW CATALOGS";
        showStmt = (ShowCatalogStmt) parseAndAnalyzeStmt(showCatalogSql);
        showResultSet = mgr.showCatalogs(showStmt, user2Ctx.getCurrentCatalog().getName());
        Assertions.assertEquals("Yes", showResultSet.getResultRows().get(0).get(3));
    }

    @Test
    public void testShowCatalogStmtWithPrivileges() throws Exception {
        // mock the login of user1
        ConnectContext user1Ctx = createCtx(user1, "127.0.0.1");
        ShowCatalogStmt user1Show = (ShowCatalogStmt) parseAndAnalyzeStmt("show catalogs;", user1Ctx);
        List<List<String>> user1ShowResult = env.getCatalogMgr().showCatalogs(user1Show).getResultRows();
        Assert.assertEquals(user1ShowResult.size(), 1);
        Assert.assertEquals(user1ShowResult.get(0).get(1), InternalCatalog.INTERNAL_CATALOG_NAME);
        Assert.assertEquals(user1ShowResult.get(0).get(0), String.valueOf(InternalCatalog.INTERNAL_CATALOG_ID));

        // have privilege and match
        user1Show = (ShowCatalogStmt) parseAndAnalyzeStmt("show catalogs like 'inter%';", user1Ctx);
        user1ShowResult = env.getCatalogMgr().showCatalogs(user1Show).getResultRows();
        Assert.assertEquals(user1ShowResult.size(), 1);
        Assert.assertEquals(user1ShowResult.get(0).get(1), InternalCatalog.INTERNAL_CATALOG_NAME);
        Assert.assertEquals(user1ShowResult.get(0).get(0), String.valueOf(InternalCatalog.INTERNAL_CATALOG_ID));

        // mock the login of user2
        ConnectContext user2Ctx = createCtx(user2, "127.0.0.1");
        ShowCatalogStmt user2Show = (ShowCatalogStmt) parseAndAnalyzeStmt("show catalogs;", user2Ctx);
        List<List<String>> user2ShowResult = env.getCatalogMgr().showCatalogs(user2Show).getResultRows();
        Assert.assertEquals(user2ShowResult.size(), 2);
        Assert.assertTrue(user2ShowResult.stream().map(l -> l.get(1)).anyMatch(c -> c.equals("hive")));

        // have privilege but not match
        user2Show = (ShowCatalogStmt) parseAndAnalyzeStmt("show catalogs like 'ice%';", user2Ctx);
        user2ShowResult = env.getCatalogMgr().showCatalogs(user2Show).getResultRows();
        Assert.assertEquals(user2ShowResult.size(), 0);

        // access denied
        ShowCatalogStmt user2ShowHive = (ShowCatalogStmt) parseAndAnalyzeStmt("show catalog hive;", user2Ctx);
        List<List<String>> user2ShowHiveResult = env.getCatalogMgr().showCatalogs(user2ShowHive).getResultRows();
        Assert.assertTrue(
                user2ShowHiveResult.stream().map(l -> l.get(0)).anyMatch(c -> c.equals("hive.metastore.uris")));
        try {
            env.getCatalogMgr()
                    .showCatalogs((ShowCatalogStmt) parseAndAnalyzeStmt("show catalog iceberg;", user2Ctx));
            Assert.fail("");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(),
                    "errCode = 2, detailMessage = Access denied for user 'user2' to catalog 'iceberg'");
        }

        //test show create catalog: have permission to hive, have no permission to iceberg;
        ShowCreateCatalogStmt user2ShowCreateHive = (ShowCreateCatalogStmt) parseAndAnalyzeStmt(
                "show create catalog hive;", user2Ctx);
        List<List<String>> user2ShowCreateHiveResult = env.getCatalogMgr().showCreateCatalog(user2ShowCreateHive)
                .getResultRows();
        Assert.assertTrue(
                user2ShowCreateHiveResult.stream().map(l -> l.get(0)).anyMatch(c -> c.equals("hive")));
        try {
            env.getCatalogMgr()
                    .showCreateCatalog(
                            (ShowCreateCatalogStmt) parseAndAnalyzeStmt("show create catalog iceberg;", user2Ctx));
            Assert.fail("");
        } catch (AnalysisException e) {
            Assert.assertEquals(e.getMessage(),
                    "errCode = 2, detailMessage = Access denied for user 'user2' to catalog 'iceberg'");
        }
    }

    @Test
    public void testAddMultiColumnPartitionsCache() {
        HMSExternalCatalog hiveCatalog = (HMSExternalCatalog) mgr.getCatalog("hive");
        HiveMetaStoreCache metaStoreCache = externalMetaCacheMgr.getMetaStoreCache(hiveCatalog);
        PartitionValueCacheKey partitionValueCacheKey = new PartitionValueCacheKey("hiveDb", "hiveTable",
                Lists.newArrayList(Type.INT, Type.SMALLINT));
        HivePartitionValues hivePartitionValues = loadPartitionValues(partitionValueCacheKey,
                Lists.newArrayList("y=2020/m=1", "y=2020/m=2"), metaStoreCache);
        metaStoreCache.putPartitionValuesCacheForTest(partitionValueCacheKey, hivePartitionValues);
        metaStoreCache.addPartitionsCache("hiveDb", "hiveTable", Lists.newArrayList("y=2020/m=3", "y=2020/m=4"),
                partitionValueCacheKey.getTypes());
        HivePartitionValues partitionValues = metaStoreCache.getPartitionValues(partitionValueCacheKey);
        Assert.assertEquals(partitionValues.getPartitionNameToIdMap().size(), 4);
    }

    @Test
    public void testDropMultiColumnPartitionsCache() {
        HMSExternalCatalog hiveCatalog = (HMSExternalCatalog) mgr.getCatalog("hive");
        HiveMetaStoreCache metaStoreCache = externalMetaCacheMgr.getMetaStoreCache(hiveCatalog);
        PartitionValueCacheKey partitionValueCacheKey = new PartitionValueCacheKey("hiveDb", "hiveTable",
                Lists.newArrayList(Type.INT, Type.SMALLINT));
        HivePartitionValues hivePartitionValues = loadPartitionValues(partitionValueCacheKey,
                Lists.newArrayList("y=2020/m=1", "y=2020/m=2"), metaStoreCache);
        metaStoreCache.putPartitionValuesCacheForTest(partitionValueCacheKey, hivePartitionValues);
        metaStoreCache.dropPartitionsCache("hiveDb", "hiveTable", Lists.newArrayList("y=2020/m=1", "y=2020/m=2"),
                false);
        HivePartitionValues partitionValues = metaStoreCache.getPartitionValues(partitionValueCacheKey);
        Assert.assertEquals(partitionValues.getPartitionNameToIdMap().size(), 0);
    }

    @Test
    public void testAddSingleColumnPartitionsCache() {
        HMSExternalCatalog hiveCatalog = (HMSExternalCatalog) mgr.getCatalog("hive");
        HiveMetaStoreCache metaStoreCache = externalMetaCacheMgr.getMetaStoreCache(hiveCatalog);
        PartitionValueCacheKey partitionValueCacheKey = new PartitionValueCacheKey("hiveDb", "hiveTable",
                Lists.newArrayList(Type.SMALLINT));
        HivePartitionValues hivePartitionValues = loadPartitionValues(partitionValueCacheKey,
                Lists.newArrayList("m=1", "m=2"), metaStoreCache);
        metaStoreCache.putPartitionValuesCacheForTest(partitionValueCacheKey, hivePartitionValues);
        metaStoreCache.addPartitionsCache("hiveDb", "hiveTable", Lists.newArrayList("m=3", "m=4"),
                partitionValueCacheKey.getTypes());
        HivePartitionValues partitionValues = metaStoreCache.getPartitionValues(partitionValueCacheKey);
        Assert.assertEquals(partitionValues.getPartitionNameToIdMap().size(), 4);
        Assert.assertEquals(partitionValues.getPartitionNameToIdMap().inverse().size(), 4);
    }

    @Test
    public void testDropSingleColumnPartitionsCache() {
        HMSExternalCatalog hiveCatalog = (HMSExternalCatalog) mgr.getCatalog("hive");
        HiveMetaStoreCache metaStoreCache = externalMetaCacheMgr.getMetaStoreCache(hiveCatalog);
        PartitionValueCacheKey partitionValueCacheKey = new PartitionValueCacheKey("hiveDb", "hiveTable",
                Lists.newArrayList(Type.SMALLINT));
        HivePartitionValues hivePartitionValues = loadPartitionValues(partitionValueCacheKey,
                Lists.newArrayList("m=1", "m=2"), metaStoreCache);
        metaStoreCache.putPartitionValuesCacheForTest(partitionValueCacheKey, hivePartitionValues);
        metaStoreCache.dropPartitionsCache("hiveDb", "hiveTable", Lists.newArrayList("m=1", "m=2"),
                false);
        HivePartitionValues partitionValues = metaStoreCache.getPartitionValues(partitionValueCacheKey);
        Assert.assertEquals(partitionValues.getPartitionNameToIdMap().size(), 0);
    }

    @Test
    public void testAddPartitionsCacheToLargeTable() {
        HMSExternalCatalog hiveCatalog = (HMSExternalCatalog) mgr.getCatalog("hive");
        HiveMetaStoreCache metaStoreCache = externalMetaCacheMgr.getMetaStoreCache(hiveCatalog);
        PartitionValueCacheKey partitionValueCacheKey = new PartitionValueCacheKey("hiveDb", "hiveTable",
                Lists.newArrayList(Type.INT));
        List<String> pNames = new ArrayList<>(100000);
        for (int i = 1; i <= 100000; i++) {
            pNames.add("m=" + i);
        }
        HivePartitionValues hivePartitionValues = loadPartitionValues(partitionValueCacheKey,
                pNames, metaStoreCache);
        metaStoreCache.putPartitionValuesCacheForTest(partitionValueCacheKey, hivePartitionValues);
        long start = System.currentTimeMillis();
        metaStoreCache.addPartitionsCache("hiveDb", "hiveTable", Lists.newArrayList("m=100001"),
                partitionValueCacheKey.getTypes());
        //387 in 4c16g
        System.out.println("testAddPartitionsCacheToLargeTable use time mills:" + (System.currentTimeMillis() - start));
        HivePartitionValues partitionValues = metaStoreCache.getPartitionValues(partitionValueCacheKey);
        Assert.assertEquals(partitionValues.getPartitionNameToIdMap().size(), 100001);
    }

    private HivePartitionValues loadPartitionValues(PartitionValueCacheKey key, List<String> partitionNames,
            HiveMetaStoreCache metaStoreCache) {
        // partition name format: nation=cn/city=beijing
        Map<Long, PartitionItem> idToPartitionItem = Maps.newHashMapWithExpectedSize(partitionNames.size());
        BiMap<String, Long> partitionNameToIdMap = HashBiMap.create(partitionNames.size());
        Map<Long, List<UniqueId>> idToUniqueIdsMap = Maps.newHashMapWithExpectedSize(partitionNames.size());
        long idx = 0;
        for (String partitionName : partitionNames) {
            long partitionId = idx++;
            ListPartitionItem listPartitionItem = metaStoreCache.toListPartitionItem(partitionName, key.getTypes());
            idToPartitionItem.put(partitionId, listPartitionItem);
            partitionNameToIdMap.put(partitionName, partitionId);
        }

        Map<UniqueId, Range<PartitionKey>> uidToPartitionRange = null;
        Map<Range<PartitionKey>, UniqueId> rangeToId = null;
        RangeMap<ColumnBound, UniqueId> singleColumnRangeMap = null;
        Map<UniqueId, Range<ColumnBound>> singleUidToColumnRangeMap = null;
        if (key.getTypes().size() > 1) {
            // uidToPartitionRange and rangeToId are only used for multi-column partition
            uidToPartitionRange = ListPartitionPrunerV2.genUidToPartitionRange(idToPartitionItem, idToUniqueIdsMap);
            rangeToId = ListPartitionPrunerV2.genRangeToId(uidToPartitionRange);
        } else {
            Preconditions.checkState(key.getTypes().size() == 1, key.getTypes());
            // singleColumnRangeMap is only used for single-column partition
            singleColumnRangeMap = ListPartitionPrunerV2.genSingleColumnRangeMap(idToPartitionItem, idToUniqueIdsMap);
            singleUidToColumnRangeMap = ListPartitionPrunerV2.genSingleUidToColumnRange(singleColumnRangeMap);
        }
        Map<Long, List<String>> partitionValuesMap = ListPartitionPrunerV2.getPartitionValuesMap(idToPartitionItem);
        return new HivePartitionValues(idToPartitionItem, uidToPartitionRange, rangeToId, singleColumnRangeMap,
                partitionNameToIdMap, idToUniqueIdsMap, singleUidToColumnRangeMap, partitionValuesMap);
    }

    @Test
    public void testInvalidCreateCatalogProperties() throws Exception {
        String createCatalogSql = "CREATE CATALOG bad_hive1 PROPERTIES (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'hadoop.username' = 'hive',\n"
                + "    'dfs.nameservices'='your-nameservice',\n"
                + "    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',\n"
                + "    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',\n"
                + "    'dfs.client.failover.proxy.provider.your-nameservice'"
                + "='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'\n"
                + ");";
        CreateCatalogStmt createStmt1 = (CreateCatalogStmt) parseAndAnalyzeStmt(createCatalogSql);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Missing dfs.ha.namenodes.your-nameservice property",
                () -> mgr.createCatalog(createStmt1));

        createCatalogSql = "CREATE CATALOG bad_hive2 PROPERTIES (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'hadoop.username' = 'hive',\n"
                + "    'dfs.nameservices'='your-nameservice',\n"
                + "    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',\n"
                + "    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',\n"
                + "    'dfs.client.failover.proxy.provider.your-nameservice'"
                + "='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'\n"
                + ");";
        CreateCatalogStmt createStmt2 = (CreateCatalogStmt) parseAndAnalyzeStmt(createCatalogSql);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Missing dfs.namenode.rpc-address.your-nameservice.nn1 property",
                () -> mgr.createCatalog(createStmt2));

        createCatalogSql = "CREATE CATALOG good_hive PROPERTIES (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'hadoop.username' = 'hive',\n"
                + "    'dfs.nameservices'='your-nameservice',\n"
                + "    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',\n"
                + "    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',\n"
                + "    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007'\n"
                + ");";
        CreateCatalogStmt createStmt3 = (CreateCatalogStmt) parseAndAnalyzeStmt(createCatalogSql);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Missing dfs.client.failover.proxy.provider.your-nameservice property",
                () -> mgr.createCatalog(createStmt3));

        createCatalogSql = "CREATE CATALOG bad_jdbc PROPERTIES (\n"
                + "    \"type\"=\"jdbc\",\n"
                + "    \"user\"=\"root\",\n"
                + "    \"password\"=\"123456\",\n"
                + "    \"jdbc_url\" = \"jdbc:mysql://127.0.0.1:3306/demo\",\n"
                + "    \"driver_class\" = \"com.mysql.jdbc.Driver\"\n"
                + ")";
        CreateCatalogStmt createStmt4 = (CreateCatalogStmt) parseAndAnalyzeStmt(createCatalogSql);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Required property 'driver_url' is missing",
                () -> mgr.createCatalog(createStmt4));

        createCatalogSql = "CREATE CATALOG bad_hive_3 PROPERTIES (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'hadoop.username' = 'hive',\n"
                + "    'dfs.nameservices'='your-nameservice',\n"
                + "    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',\n"
                + "    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',\n"
                + "    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',\n"
                + "    'file.meta.cache.ttl-second'='2m'\n"
                + ");";
        CreateCatalogStmt createStmt5 = (CreateCatalogStmt) parseAndAnalyzeStmt(createCatalogSql);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "The parameter file.meta.cache.ttl-second is wrong, value is 2m",
                () -> mgr.createCatalog(createStmt5));

        createCatalogSql = "CREATE CATALOG bad_hive_4 PROPERTIES (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'hadoop.username' = 'hive',\n"
                + "    'dfs.nameservices'='your-nameservice',\n"
                + "    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',\n"
                + "    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',\n"
                + "    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',\n"
                + "    'file.meta.cache.ttl-second'=''\n"
                + ");";
        CreateCatalogStmt createStmt6 = (CreateCatalogStmt) parseAndAnalyzeStmt(createCatalogSql);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "The parameter file.meta.cache.ttl-second is wrong, value is ",
                () -> mgr.createCatalog(createStmt6));

        createCatalogSql = "CREATE CATALOG good_hive_2 PROPERTIES (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'hadoop.username' = 'hive',\n"
                + "    'dfs.nameservices'='your-nameservice',\n"
                + "    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',\n"
                + "    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',\n"
                + "    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',\n"
                + "    'file.meta.cache.ttl-second'='60',\n"
                + "    'dfs.client.failover.proxy.provider.your-nameservice'"
                + "='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'\n"
                + ");";
        CreateCatalogStmt createStmt7 = (CreateCatalogStmt) parseAndAnalyzeStmt(createCatalogSql);
        ExceptionChecker.expectThrowsNoException(() -> mgr.createCatalog(createStmt7));
    }

    @Test
    public void testInvalidAndValidAlterCatalogProperties() throws Exception {

        String catalogName = "test_hive1";
        String createCatalogSql = "CREATE CATALOG test_hive1 PROPERTIES (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://127.0.0.1:7007',\n"
                + "    'dfs.nameservices' = 'HANN',\n"
                + "    'dfs.ha.namenodes.HANN'='nn1,nn2',\n"
                + "    'dfs.namenode.rpc-address.HANN.nn1'='127.0.0.1:4007',\n"
                + "    'dfs.namenode.rpc-address.HANN.nn2'='127.0.0.1:4008',\n"
                + "    'dfs.client.failover.proxy.provider.HANN'"
                + "='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'\n"
                + ");";
        CreateCatalogStmt createStmt = (CreateCatalogStmt) parseAndAnalyzeStmt(createCatalogSql);
        mgr.createCatalog(createStmt);

        String alterCatalogSql = "ALTER CATALOG test_hive1 SET PROPERTIES (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://127.0.0.1:7007',\n"
                + "    'dfs.nameservices' = 'HANN',\n"
                + "    'dfs.ha.namenodes.HANN'='',\n"
                + "    'dfs.client.failover.proxy.provider.HANN'"
                + "='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'\n"
                + ");";
        AlterCatalogPropertyStmt alterCatalogPropertyStmt1 = (AlterCatalogPropertyStmt) parseAndAnalyzeStmt(
                alterCatalogSql);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Missing dfs.ha.namenodes.HANN property",
                () -> mgr.alterCatalogProps(alterCatalogPropertyStmt1));

        alterCatalogSql = "ALTER CATALOG test_hive1 SET PROPERTIES (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://127.0.0.1:7007',\n"
                + "    'dfs.nameservices' = 'HANN',\n"
                + "    'dfs.ha.namenodes.HANN'='nn1,nn3',\n"
                + "    'dfs.namenode.rpc-address.HANN.nn1'='127.0.0.1:4007',\n"
                + "    'dfs.namenode.rpc-address.HANN.nn3'='127.0.0.1:4007',\n"
                + "    'dfs.client.failover.proxy.provider.HANN'"
                + "=''\n"
                + ");";
        AlterCatalogPropertyStmt alterCatalogPropertyStmt2 = (AlterCatalogPropertyStmt) parseAndAnalyzeStmt(
                alterCatalogSql);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Missing dfs.client.failover.proxy.provider.HANN property",
                () -> mgr.alterCatalogProps(alterCatalogPropertyStmt2));

        alterCatalogSql = "ALTER CATALOG test_hive1 SET PROPERTIES (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://127.0.0.1:7007',\n"
                + "    'dfs.nameservices' = 'HANN',\n"
                + "    'dfs.ha.namenodes.HANN'='nn1,nn3',\n"
                + "    'dfs.namenode.rpc-address.HANN.nn1'='127.0.0.1:4007',\n"
                + "    'dfs.namenode.rpc-address.HANN.nn3'='127.0.0.1:4007',\n"
                + "    'dfs.client.failover.proxy.provider.HANN'"
                + "='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'\n"
                + ");";
        AlterCatalogPropertyStmt alterCatalogPropertyStmt3 = (AlterCatalogPropertyStmt) parseAndAnalyzeStmt(
                alterCatalogSql);
        mgr.alterCatalogProps(alterCatalogPropertyStmt3);

        CatalogIf catalog = env.getCatalogMgr().getCatalog(catalogName);
        Assert.assertEquals(11, catalog.getProperties().size());
        Assert.assertEquals("nn1,nn3", catalog.getProperties().get("dfs.ha.namenodes.HANN"));
    }

    public void testAlterFileCache() throws Exception {
        String catalogName = "good_hive_3";
        String createCatalogSql = "CREATE CATALOG " + catalogName
                + " COMMENT 'create comment'\n"
                + " PROPERTIES (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'hadoop.username' = 'hive',\n"
                + "    'dfs.nameservices'='your-nameservice',\n"
                + "    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',\n"
                + "    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',\n"
                + "    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',\n"
                + "    'file.meta.cache.ttl-second'='60',\n"
                + "    'dfs.client.failover.proxy.provider.your-nameservice'"
                + "='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'\n"
                + ");";
        CreateCatalogStmt createStmt8 = (CreateCatalogStmt) parseAndAnalyzeStmt(createCatalogSql);
        ExceptionChecker.expectThrowsNoException(() -> mgr.createCatalog(createStmt8));

        HMSExternalCatalog hiveCatalog = (HMSExternalCatalog) mgr.getCatalog(catalogName);
        HiveMetaStoreCache metaStoreCache = externalMetaCacheMgr.getMetaStoreCache(hiveCatalog);
        LoadingCache<FileCacheKey, FileCacheValue> preFileCache = metaStoreCache.getFileCacheRef().get();


        // 1. properties contains `file.meta.cache.ttl-second`, it should not be equal
        String alterCatalogProp = "ALTER CATALOG " + catalogName + " SET PROPERTIES"
                + " ('file.meta.cache.ttl-second'='120');";
        mgr.alterCatalogProps((AlterCatalogPropertyStmt) parseAndAnalyzeStmt(alterCatalogProp));
        Assertions.assertEquals("120", mgr.getCatalog(catalogName).getProperties().get("file.meta.cache.ttl-second"));

        Assertions.assertNotEquals(preFileCache, metaStoreCache.getFileCacheRef().get());
        preFileCache = metaStoreCache.getFileCacheRef().get();

        // 2. properties not contains `file.meta.cache.ttl-second`, it should be equal
        alterCatalogProp = "ALTER CATALOG " + catalogName + " SET PROPERTIES"
                + " (\"type\" = \"hms\", \"hive.metastore.uris\" = \"thrift://172.16.5.9:9083\");";
        mgr.alterCatalogProps((AlterCatalogPropertyStmt) parseAndAnalyzeStmt(alterCatalogProp));
        Assertions.assertEquals(preFileCache, metaStoreCache.getFileCacheRef().get());
    }

    @Test
    public void testCatalogWithComment() throws Exception {
        ConnectContext rootCtx = createDefaultCtx();
        CreateCatalogStmt catalogWithComment = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog hive_c comment 'create' properties('type' = 'hms', 'hive.metastore.uris' = 'thrift://192.168.0.1:9083');",
                rootCtx);
        env.getCatalogMgr().createCatalog(catalogWithComment);
        Assertions.assertNotNull(env.getCatalogMgr().getCatalog("hive_c").getComment());

        String alterComment = "ALTER CATALOG hive_c SET PROPERTIES"
                + " (\"comment\" = \"alter comment\");";
        mgr.alterCatalogProps((AlterCatalogPropertyStmt) parseAndAnalyzeStmt(alterComment));
        // we do not set `comment` auto by `comment in properties`
        Assertions.assertEquals("create", env.getCatalogMgr().getCatalog("hive_c").getComment());
    }

    @Test
    public void testAlterCatalogComment() throws Exception {
        ConnectContext rootCtx = createDefaultCtx();
        CreateCatalogStmt catalogWithComment = (CreateCatalogStmt) parseAndAnalyzeStmt(
                "create catalog hive_c1 comment 'create' properties('type' = 'hms', 'hive.metastore.uris' = 'thrift://192.168.0.1:9083');",
                rootCtx);
        env.getCatalogMgr().createCatalog(catalogWithComment);
        Assertions.assertEquals("create", env.getCatalogMgr().getCatalog("hive_c1").getComment());

        String alterComment = "ALTER CATALOG hive_c1 MODIFY COMMENT 'new_comment';";
        mgr.alterCatalogComment((AlterCatalogCommentStmt) parseAndAnalyzeStmt(alterComment));
        // we do not set `comment` auto by `comment in properties`
        Assertions.assertEquals("new_comment", env.getCatalogMgr().getCatalog("hive_c1").getComment());
    }
}
