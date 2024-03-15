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
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.Load;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.task.LoadTaskInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

public class LoadStmtTest {
    private List<DataDescription> dataDescriptions;
    private Analyzer analyzer;

    @Mocked
    private ConnectContext ctx;
    @Mocked
    DataDescription desc;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        dataDescriptions = Lists.newArrayList();
        dataDescriptions.add(desc);
        new Expectations() {
            {
                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                ctx.getQualifiedUser();
                minTimes = 0;
                result = "user";

                desc.toSql();
                minTimes = 0;
                result = "XXX";
            }
        };
    }

    @Test
    public void testNormal(@Injectable DataDescription desc, @Mocked Env env,
            @Injectable ResourceMgr resourceMgr, @Injectable AccessControllerManager accessManager)
            throws UserException, AnalysisException {
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(desc);
        String resourceName = "spark0";
        SparkResource resource = new SparkResource(resourceName);

        new Expectations() {
            {
                desc.getMergeType();
                result = LoadTask.MergeType.APPEND;
                desc.toSql();
                minTimes = 0;
                result = "XXX";
                desc.getTableName();
                minTimes = 0;
                result = "testTbl";
                desc.analyzeFullDbName("testDb", (Analyzer) any);
                minTimes = 0;
                result = "testDb";
                env.getResourceMgr();
                result = resourceMgr;
                resourceMgr.getResource(resourceName);
                result = resource;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkResourcePriv((ConnectContext) any, resourceName, PrivPredicate.USAGE);
                result = true;
            }
        };

        LoadStmt stmt = new LoadStmt(new LabelName("testDb", "testLabel"), dataDescriptionList, (BrokerDesc) null, null, "");
        stmt.analyze(analyzer);
        Assert.assertEquals("testDb", stmt.getLabel().getDbName());
        Assert.assertEquals(dataDescriptionList, stmt.getDataDescriptions());
        Assert.assertNull(stmt.getProperties());

        Assert.assertEquals("LOAD LABEL `testDb`.`testLabel`\n"
                + "(XXX)", stmt.toString());

        // test ResourceDesc
        stmt = new LoadStmt(new LabelName("testDb", "testLabel"), dataDescriptionList,
                            new ResourceDesc(resourceName, null), null, "");
        stmt.analyze(analyzer);
        Assert.assertEquals(EtlJobType.SPARK, stmt.getResourceDesc().getEtlJobType());
        Assert.assertEquals("LOAD LABEL `testDb`.`testLabel`\n(XXX)\nWITH RESOURCE 'spark0'",
                            stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoData() throws UserException, AnalysisException {
        new Expectations() {
            {
                desc.analyze(anyString);
                minTimes = 0;
            }
        };

        LoadStmt stmt = new LoadStmt(new LabelName("testDb", "testLabel"), null, (BrokerDesc) null, null, "");
        stmt.analyze(analyzer);

        Assert.fail("No exception throws.");
    }

    @Test
    public void testRewrite() throws Exception {
        LoadTaskInfo.ImportColumnDescs columnDescs = new LoadTaskInfo.ImportColumnDescs();
        List<ImportColumnDesc> columns1 = getColumns("c1,c2,c3,tmp_c4=c1 + 1, tmp_c5 = tmp_c4+1");
        columnDescs.descs = columns1;
        columnDescs.isColumnDescsRewrited = false;
        Load.rewriteColumns(columnDescs);
        String orig = "((`c1` + 1) + 1)";
        Assert.assertEquals(orig, columns1.get(4).getExpr().toString());

        List<ImportColumnDesc> columns2 = getColumns("c1,c2,c3,tmp_c5 = tmp_c4+1, tmp_c4=c1 + 1");
        columnDescs.descs = columns2;
        columnDescs.isColumnDescsRewrited = false;
        String orig2 = "(`tmp_c4` + 1)";
        Load.rewriteColumns(columnDescs);
        Assert.assertEquals(orig2, columns2.get(3).getExpr().toString());

        List<ImportColumnDesc> columns3 = getColumns("c1,c2,c3");
        columnDescs.descs = columns3;
        columnDescs.isColumnDescsRewrited = false;
        String orig3 = "c3";
        Load.rewriteColumns(columnDescs);
        Assert.assertEquals(orig3, columns3.get(2).toString());

        List<ImportColumnDesc> columns4 = getColumns("c1, c1=ifnull(c1, 0), c2=ifnull(c1, 0)");
        columnDescs.descs = columns4;
        columnDescs.isColumnDescsRewrited = false;
        Load.rewriteColumns(columnDescs);
        Assert.assertEquals("c1", columns4.get(0).toString());
        Assert.assertEquals("c1=ifnull(`c1`, 0)", columns4.get(1).toString());
        Assert.assertEquals("c2=ifnull(ifnull(`c1`, 0), 0)", columns4.get(2).toString());
        // will not rewrite again
        Assert.assertTrue(columnDescs.isColumnDescsRewrited);
        Load.rewriteColumns(columnDescs);
        Assert.assertEquals("c1", columns4.get(0).toString());
        Assert.assertEquals("c1=ifnull(`c1`, 0)", columns4.get(1).toString());
        Assert.assertEquals("c2=ifnull(ifnull(`c1`, 0), 0)", columns4.get(2).toString());

    }

    private List<ImportColumnDesc> getColumns(String columns) throws Exception {
        String columnsSQL = "COLUMNS (" + columns + ")";
        return ((ImportColumnsStmt) SqlParserUtils.getFirstStmt(
                new SqlParser(new SqlScanner(new StringReader(columnsSQL))))).getColumns();
    }

    @Test
    public void testMySqlLoadData(@Injectable DataDescription desc) throws UserException, IOException {
        File temp = File.createTempFile("testMySqlLoadData", ".txt");
        new Expectations() {
            {
                desc.isClientLocal();
                minTimes = 0;
                result = false;

                desc.getFilePaths();
                minTimes = 0;
                result = Lists.newArrayList(temp.getPath());

                desc.toSql();
                minTimes = 0;
                result = "XXX";

                desc.getTableName();
                minTimes = 0;
                result = "testTbl";

                desc.analyzeFullDbName(null, (Analyzer) any);
                minTimes = 0;
                result = "testDb";

                desc.getMergeType();
                minTimes = 0;
                result = LoadTask.MergeType.APPEND;
            }
        };

        LoadStmt stmt = new LoadStmt(desc, Maps.newHashMap(), "");
        try {
            stmt.analyze(analyzer);
        } catch (AnalysisException ae) {
            Assert.assertEquals("errCode = 2, detailMessage = Load local data from fe local is not enabled."
                    + " If you want to use it, plz set the `mysql_load_server_secure_path` for FE to be a right path.",
                    ae.getMessage());
        }
        Config.mysql_load_server_secure_path = "/root";
        try {
            stmt.analyze(analyzer);
        } catch (AnalysisException ae) {
            Assert.assertEquals("errCode = 2, detailMessage = Local file should be under the secure path of FE.",
                    ae.getMessage());
        }
        Config.mysql_load_server_secure_path = "/";
        stmt.analyze(analyzer);
        Assert.assertNull(stmt.getLabel().getDbName());
        Assert.assertEquals(EtlJobType.LOCAL_FILE, stmt.getEtlJobType());

        // unified load stmt
        UnifiedLoadStmt unifiedStmt = UnifiedLoadStmt.buildMysqlLoadStmt(desc, Maps.newHashMap(), "");
        Config.mysql_load_server_secure_path = "/";
        unifiedStmt.analyze(analyzer);
        Assert.assertTrue(unifiedStmt.getProxyStmt() instanceof LoadStmt);
        Assert.assertNull(((LoadStmt) unifiedStmt.getProxyStmt()).getLabel().getDbName());
        Assert.assertEquals(unifiedStmt.getRedirectStatus(), RedirectStatus.NO_FORWARD);
    }

    @Test
    public void testMySqlLoadPath(@Injectable DataDescription desc) throws UserException, IOException {
        File temp = File.createTempFile("testMySqlLoadData_path", ".txt");
        String parentPath = temp.getParent();
        String fakePath = parentPath + "/../fake_path";
        new Expectations() {
            {
                desc.isClientLocal();
                minTimes = 0;
                result = false;

                desc.getFilePaths();
                minTimes = 0;
                result = Lists.newArrayList(fakePath);

                desc.toSql();
                minTimes = 0;
                result = "XXX";

                desc.getTableName();
                minTimes = 0;
                result = "testTbl";

                desc.analyzeFullDbName(null, (Analyzer) any);
                minTimes = 0;
                result = "testDb";

                desc.getMergeType();
                minTimes = 0;
                result = LoadTask.MergeType.APPEND;
            }
        };

        LoadStmt stmt = new LoadStmt(desc, Maps.newHashMap(), "");
        Config.mysql_load_server_secure_path = parentPath;
        try {
            stmt.analyze(analyzer);
        } catch (AnalysisException ae) {
            Assert.assertEquals("errCode = 2, detailMessage = Local file should be under the secure path of FE.",
                    ae.getMessage());
        }
    }
}
