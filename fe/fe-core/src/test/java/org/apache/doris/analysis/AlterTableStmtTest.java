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
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class AlterTableStmtTest {
    private Analyzer analyzer;
    private String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    @Mocked
    private AccessControllerManager accessManager;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);

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
            }
        };
    }

    @Test
    public void testNormal() throws UserException {
        List<AlterClause> ops = Lists.newArrayList();
        ops.add(new DropColumnClause("col1", "", null));
        ops.add(new DropColumnClause("col2", "", null));
        AlterTableStmt stmt = new AlterTableStmt(new TableName(internalCtl, "testDb", "testTbl"), ops);
        stmt.analyze(analyzer);
        Assert.assertEquals("ALTER TABLE `testDb`.`testTbl` DROP COLUMN `col1`, \nDROP COLUMN `col2`",
                stmt.toSql());
        Assert.assertEquals("testDb", stmt.getTbl().getDb());
        Assert.assertEquals(2, stmt.getOps().size());
    }

    @Test
    public void testAddRollup() throws UserException {
        List<AlterClause> ops = Lists.newArrayList();
        ops.add(new AddRollupClause("index1", Lists.newArrayList("col1", "col2"), null, "testTbl", null));
        ops.add(new AddRollupClause("index2", Lists.newArrayList("col2", "col3"), null, "testTbl", null));
        AlterTableStmt stmt = new AlterTableStmt(new TableName(internalCtl, "testDb", "testTbl"), ops);
        stmt.analyze(analyzer);
        Assert.assertEquals("ALTER TABLE `testDb`.`testTbl`"
                        + " ADD ROLLUP `index1` (`col1`, `col2`) FROM `testTbl`, \n"
                        + " `index2` (`col2`, `col3`) FROM `testTbl`",
                stmt.toSql());
        Assert.assertEquals("testDb", stmt.getTbl().getDb());
        Assert.assertEquals(2, stmt.getOps().size());
    }

    @Test(expected = AnalysisException.class)
    public void testNoTable() throws UserException {
        List<AlterClause> ops = Lists.newArrayList();
        ops.add(new DropColumnClause("col1", "", null));
        AlterTableStmt stmt = new AlterTableStmt(null, ops);
        stmt.analyze(analyzer);

        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testNoClause() throws UserException {
        List<AlterClause> ops = Lists.newArrayList();
        AlterTableStmt stmt = new AlterTableStmt(new TableName(internalCtl, "testDb", "testTbl"), ops);
        stmt.analyze(analyzer);

        Assert.fail("No exception throws.");
    }

    @Test
    public void testEnableFeature() throws UserException {
        List<AlterClause> ops = Lists.newArrayList();
        Map<String, String> properties = Maps.newHashMap();
        properties.put("function_column.sequence_type", "int");
        ops.add(new EnableFeatureClause("sequence_load", properties));
        AlterTableStmt stmt = new AlterTableStmt(new TableName(internalCtl, "testDb", "testTbl"), ops);
        stmt.analyze(analyzer);

        Assert.assertEquals("ALTER TABLE `testDb`.`testTbl` ENABLE FEATURE \"sequence_load\" WITH PROPERTIES (\"function_column.sequence_type\" = \"int\")",
                stmt.toSql());
        Assert.assertEquals("testDb", stmt.getTbl().getDb());
    }
}
