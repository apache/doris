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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class CreateCatalogStmtTest {
    private Analyzer analyzer;

    @Mocked
    private PaloAuth auth;
    @Mocked
    private ConnectContext ctx;

    @Before()
    public void setUp() throws DdlException {
        Config.enable_multi_catalog = true;
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "%");
    }

    @Test
    public void testAnalyzeNormal() throws UserException {
        Map<String, String> props = Maps.newHashMap();
        props.put("type", "hms");
        props.put("hive.metastore.uris", "thrift://localhost:9083");
        CreateCatalogStmt stmt = new CreateCatalogStmt(false, "testCatalog", props);
        stmt.analyze(analyzer);
        Assert.assertEquals("testCatalog", stmt.getCatalogName());
        Assert.assertNotNull(stmt.getProperties());
        Assert.assertEquals(2, stmt.getProperties().size());
    }

    @Test(expected = AnalysisException.class)
    public void testAnalyzeWithException() throws UserException {
        Map<String, String> props = Maps.newHashMap();
        props.put("type", "hms");
        props.put("hive.metastore.uris", "thrift://localhost:9083");
        CreateCatalogStmt stmt = new CreateCatalogStmt(false, "", props);
        stmt.analyze(analyzer);
        Assert.fail("no exception");
    }

    @Test(expected = AnalysisException.class)
    public void testBuildInException() throws UserException {
        Map<String, String> props = Maps.newHashMap();
        props.put("type", "hms");
        props.put("hive.metastore.uris", "thrift://localhost:9083");
        CreateCatalogStmt stmt = new CreateCatalogStmt(false, InternalCatalog.INTERNAL_CATALOG_NAME, props);
        stmt.analyze(analyzer);
        Assert.fail("no exception");
    }

    @Test(expected = AnalysisException.class)
    public void testPropsTypeException() throws UserException {
        Map<String, String> props = Maps.newHashMap();
        props.put("hive.metastore.uris", "thrift://localhost:9083");
        CreateCatalogStmt stmt = new CreateCatalogStmt(false, InternalCatalog.INTERNAL_CATALOG_NAME, props);
        stmt.analyze(analyzer);
        Assert.fail("no exception");
    }
}
