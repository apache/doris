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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowRollupStmtTest {
    private Analyzer analyzer;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    @Test
    public void testNormal() throws AnalysisException {
        // use default database
        ShowRollupStmt stmt = new ShowRollupStmt(new TableName("", "tbl"), "");
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:testDb", stmt.getDb());
        Assert.assertEquals("tbl", stmt.getTbl());
        Assert.assertEquals("SHOW ROLLUP FROM `testCluster:testDb`.`tbl`", stmt.toString());

        // use table database
        stmt = new ShowRollupStmt(new TableName("testDb1", "tbl"), "");
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:testDb1", stmt.getDb());
        Assert.assertEquals("tbl", stmt.getTbl());
        Assert.assertEquals("SHOW ROLLUP FROM `testCluster:testDb1`.`tbl`", stmt.toString());

        // use db database
        stmt = new ShowRollupStmt(new TableName("testDb1", "tbl"), "testDb2");
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:testDb2", stmt.getDb());
        Assert.assertEquals("tbl", stmt.getTbl());
        Assert.assertEquals("SHOW ROLLUP FROM `testCluster:testDb2`.`tbl`", stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoTbl() throws AnalysisException {
        // use default database
        ShowRollupStmt stmt = new ShowRollupStmt(new TableName("testDb", ""), "");
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}