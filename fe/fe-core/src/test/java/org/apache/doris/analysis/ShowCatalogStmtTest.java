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
import org.apache.doris.common.UserException;

import org.junit.Assert;
import org.junit.Test;

public class ShowCatalogStmtTest {
    @Test
    public void testNormal() throws UserException, AnalysisException {
        Config.enable_multi_catalog = true;
        final Analyzer analyzer =  AccessTestUtil.fetchBlockAnalyzer();
        ShowCatalogStmt stmt = new ShowCatalogStmt();
        stmt.analyze(analyzer);
        Assert.assertNull(stmt.getCatalogName());
        Assert.assertEquals(3, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("SHOW CATALOGS", stmt.toSql());

        stmt = new ShowCatalogStmt(null, "%hive%");
        stmt.analyze(analyzer);
        Assert.assertNull(stmt.getCatalogName());
        Assert.assertNotNull(stmt.getPattern());
        Assert.assertEquals(3, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("SHOW CATALOGS LIKE '%hive%'", stmt.toSql());

        stmt = new ShowCatalogStmt("testCatalog", null);
        stmt.analyze(analyzer);
        Assert.assertNotNull(stmt.getCatalogName());
        Assert.assertEquals(2, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("SHOW CATALOG testCatalog", stmt.toSql());
    }
}
