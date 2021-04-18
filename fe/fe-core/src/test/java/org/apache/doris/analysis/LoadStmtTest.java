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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.Load;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

public class LoadStmtTest {
    private List<DataDescription> dataDescriptions;
    private Analyzer analyzer;

    @Mocked
    private PaloAuth auth;
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
                result = "default_cluster:user";

                desc.toSql();
                minTimes = 0;
                result = "XXX";
            }
        };
    }

    @Test
    public void testNormal(@Injectable DataDescription desc, @Mocked Catalog catalog,
                           @Injectable ResourceMgr resourceMgr, @Injectable PaloAuth auth) throws UserException, AnalysisException {
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(desc);
        String resourceName = "spark0";
        SparkResource resource = new SparkResource(resourceName);

        new Expectations(){
            {
                desc.getMergeType();
                result = LoadTask.MergeType.APPEND;
                desc.toSql();
                minTimes = 0;
                result = "XXX";
                catalog.getResourceMgr();
                result = resourceMgr;
                resourceMgr.getResource(resourceName);
                result = resource;
                catalog.getAuth();
                result = auth;
                auth.checkResourcePriv((ConnectContext) any, resourceName, PrivPredicate.USAGE);
                result = true;
            }
        };

        LoadStmt stmt = new LoadStmt(new LabelName("testDb", "testLabel"), dataDescriptionList, null, null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:testDb", stmt.getLabel().getDbName());
        Assert.assertEquals(dataDescriptionList, stmt.getDataDescriptions());
        Assert.assertNull(stmt.getProperties());

        Assert.assertEquals("LOAD LABEL `testCluster:testDb`.`testLabel`\n"
                + "(XXX)", stmt.toString());

        // test ResourceDesc
        // TODO(wyb): spark-load
        Config.enable_spark_load = true;
        stmt = new LoadStmt(new LabelName("testDb", "testLabel"), dataDescriptionList,
                            new ResourceDesc(resourceName, null), null);
        stmt.analyze(analyzer);
        Assert.assertEquals(EtlJobType.SPARK, stmt.getResourceDesc().getEtlJobType());
        Assert.assertEquals("LOAD LABEL `testCluster:testDb`.`testLabel`\n(XXX)\nWITH RESOURCE 'spark0'",
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

        LoadStmt stmt = new LoadStmt(new LabelName("testDb", "testLabel"), null, null, null, null);
        stmt.analyze(analyzer);

        Assert.fail("No exception throws.");
    }

    @Test
    public void testRewrite() throws Exception{
        List<ImportColumnDesc> columns1 = getColumns("c1,c2,c3,tmp_c4=c1 + 1, tmp_c5 = tmp_c4+1");
        Load.rewriteColumns(columns1);
        String orig = "`c1` + 1 + 1";
        Assert.assertEquals(orig, columns1.get(4).getExpr().toString());

        List<ImportColumnDesc> columns2 = getColumns("c1,c2,c3,tmp_c5 = tmp_c4+1, tmp_c4=c1 + 1");
        String orig2 = "`tmp_c4` + 1";
        Load.rewriteColumns(columns2);
        Assert.assertEquals(orig2, columns2.get(3).getExpr().toString());

        List<ImportColumnDesc> columns3 = getColumns("c1,c2,c3");
        String orig3 = "c3";
        Load.rewriteColumns(columns3);
        Assert.assertEquals(orig3, columns3.get(2).toString());

    }

    private List<ImportColumnDesc> getColumns(String columns) throws Exception {
        String columnsSQL = "COLUMNS (" + columns + ")";
        return ((ImportColumnsStmt) SqlParserUtils.getFirstStmt(
            new SqlParser(
                new SqlScanner(
                    new StringReader(columnsSQL))))).getColumns();
    }
}