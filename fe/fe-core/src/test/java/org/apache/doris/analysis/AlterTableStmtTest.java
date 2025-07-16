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
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
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
    public void testEnableFeatureClause() {
        List<AlterClause> ops = Lists.newArrayList();
        ops.add(new EnableFeatureClause("BATCH_DELETE"));
        AlterTableStmt alterTableStmt = new AlterTableStmt(new TableName(internalCtl, "db", "test"), ops);
        Assert.assertEquals(alterTableStmt.toSql(), "ALTER TABLE `db`.`test` ENABLE FEATURE \"BATCH_DELETE\"");
        ops.clear();
        ops.add(new EnableFeatureClause("UPDATE_FLEXIBLE_COLUMNS"));
        alterTableStmt = new AlterTableStmt(new TableName(internalCtl, "db", "test"), ops);
        Assert.assertEquals(alterTableStmt.toSql(),
                "ALTER TABLE `db`.`test` ENABLE FEATURE \"UPDATE_FLEXIBLE_COLUMNS\"");
        ops.clear();
        Map<String, String> properties = new HashMap<>();
        properties.put("function_column.sequence_type", "int");
        ops.add(new EnableFeatureClause("SEQUENCE_LOAD", properties));
        alterTableStmt = new AlterTableStmt(new TableName(internalCtl, "db", "test"), ops);
        Assert.assertEquals(alterTableStmt.toSql(),
                "ALTER TABLE `db`.`test` ENABLE FEATURE \"SEQUENCE_LOAD\" WITH PROPERTIES (\"function_column.sequence_type\" = \"int\")");
    }
}
